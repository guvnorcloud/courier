use clap::Parser;
use std::path::PathBuf;
use tracing::{error, info, warn};

mod buffer;
mod config;
mod discovery;
mod heartbeat;
mod intelligence;
mod pipeline;
mod sinks;
mod sources;
mod state;
mod transforms;

#[derive(Parser)]
#[command(name = "courier", version, about = "Guvnor Cloud log courier agent")]
struct Cli {
    #[arg(short, long, default_value = "/etc/courier/courier.yaml")]
    config: PathBuf,
    #[arg(long)]
    validate: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .json()
        .init();

    let cli = Cli::parse();
    info!(version = env!("CARGO_PKG_VERSION"), "Courier starting");

    let cfg = config::load(&cli.config)?;
    if cli.validate {
        info!("Configuration is valid");
        return Ok(());
    }

    // Run host discovery and start heartbeat if Guvnor config is present
    let metrics = heartbeat::HeartbeatMetrics::new();
    let start_time = std::time::Instant::now();

    let hb_result = if let Some(guvnor_cfg) = &cfg.guvnor {
        let discovery_data = discovery::discover();
        info!(
            processes = discovery_data.processes.len(),
            file_tree_roots = discovery_data.file_tree.len(),
            recommendations = discovery_data.recommendations.len(),
            "Host discovery complete"
        );
        let (_hb_handle, rx, s3_creds) = heartbeat::start_heartbeat(
            guvnor_cfg.clone(),
            metrics.clone(),
            start_time,
            Some(discovery_data),
        ).await;
        info!(agent_id = %guvnor_cfg.agent_id, "Heartbeat started");
        Some((rx, s3_creds))
    } else {
        None
    };

    // Main config reload loop
    let mut current_cfg = cfg;
    let mut reload_rx = hb_result.as_ref().map(|(rx, _)| rx.clone());
    let s3_creds = hb_result.as_ref().map(|(_, c)| c.clone());

    loop {
        // Initialize intelligence engine for this config cycle
        let analyzer = match current_cfg.intelligence.tier {
            config::IntelligenceTier::Off => {
                info!("Intelligence: off");
                None
            }
            config::IntelligenceTier::Rules => {
                info!("Intelligence: rules engine");
                let handle = intelligence::analyzer::start(
                    current_cfg.intelligence.clone(),
                    None,
                    current_cfg.guvnor.as_ref().map(|g| g.api_url.clone()),
                    current_cfg.guvnor.as_ref().map(|g| g.agent_id.clone()),
                    current_cfg.guvnor.as_ref().map(|g| g.token.clone()),
                ).await;
                Some(handle)
            }
            config::IntelligenceTier::Llm => {
                info!("Intelligence: LLM (BitNet)");
                let resolved = intelligence::model_manager::ensure_model(
                    &current_cfg.data_dir,
                    &current_cfg.intelligence.model_repo,
                    &current_cfg.intelligence.model_file,
                ).await?;
                let engine = intelligence::engine::start(&current_cfg.intelligence, resolved.path).await?;
                let handle = intelligence::analyzer::start(
                    current_cfg.intelligence.clone(),
                    Some(engine),
                    current_cfg.guvnor.as_ref().map(|g| g.api_url.clone()),
                    current_cfg.guvnor.as_ref().map(|g| g.agent_id.clone()),
                    current_cfg.guvnor.as_ref().map(|g| g.token.clone()),
                ).await;
                Some(handle)
            }
        };

        let state_store = state::StateStore::open(&current_cfg.data_dir)?;
        let guvnor_cfg = current_cfg.guvnor.clone();

        let mut pipeline = pipeline::Pipeline::new(
            current_cfg.clone(),
            state_store,
            metrics.clone(),
            analyzer,
        );

        if let Some(ref creds) = s3_creds {
            pipeline = pipeline.with_s3_creds(creds.clone());
        }

        if let Some(rx) = reload_rx.take() {
            pipeline = pipeline.with_config_reload(rx.clone());
            reload_rx = Some(rx);
        }

        info!("Courier running");

        let shutdown = pipeline::shutdown_signal();

        let should_reload = tokio::select! {
            result = pipeline.run() => {
                match result {
                    Ok(true) => true,   // config reload requested
                    Ok(false) => false, // normal exit
                    Err(e) => {
                        error!(error = %e, "Pipeline error");
                        false
                    }
                }
            }
            _ = shutdown => {
                info!("Shutdown signal received");
                false
            }
        };

        if !should_reload {
            // Always send a final offline heartbeat on clean shutdown
            if let Some(ref guvnor_cfg) = guvnor_cfg {
                if guvnor_cfg.ephemeral {
                    info!("Ephemeral mode: deregistering agent");
                    heartbeat::deregister(guvnor_cfg).await;
                } else {
                    info!("Sending offline heartbeat");
                    heartbeat::send_offline(guvnor_cfg).await;
                }
            }
            info!("Courier stopped");
            return Ok(());
        }

        // Config reload: fetch new config from bootstrap endpoint and save to disk
        info!("Config reload triggered — fetching updated config");

        if let Some(ref guvnor) = guvnor_cfg {
            match config::load_remote(&guvnor.api_url, &guvnor.token, &guvnor.agent_id).await {
                Ok(new_cfg) => {
                    // Write updated config to disk so it persists across restarts
                    match serde_yaml::to_string(&new_cfg) {
                        Ok(yaml) => {
                            if let Err(e) = std::fs::write(&cli.config, &yaml) {
                                warn!(error = %e, "Failed to write updated config to disk");
                            } else {
                                info!(path = %cli.config.display(), "Updated config saved to disk");
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to serialize updated config");
                        }
                    }
                    current_cfg = new_cfg;
                    info!("Config reloaded — restarting pipeline");
                }
                Err(e) => {
                    warn!(error = %e, "Failed to fetch updated config — retrying with current config");
                    // Re-load from disk as fallback
                    match config::load(&cli.config) {
                        Ok(cfg) => { current_cfg = cfg; }
                        Err(e) => {
                            error!(error = %e, "Failed to reload config from disk — exiting");
                            return Err(e);
                        }
                    }
                }
            }
        } else {
            // No guvnor config means no remote reload, just re-read from disk
            match config::load(&cli.config) {
                Ok(cfg) => { current_cfg = cfg; }
                Err(e) => {
                    error!(error = %e, "Failed to reload config from disk — exiting");
                    return Err(e);
                }
            }
        }

        // Reset the watch channel for next cycle — consume the current value
        // so rx.changed() doesn't fire immediately on the next pipeline run
        if let Some(ref mut rx) = reload_rx {
            let _ = rx.borrow_and_update();
        }
    }
}

use clap::Parser;
use std::path::PathBuf;
use tracing::{error, info};

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
#[command(name = "courier", about = "Guvnor Cloud log courier agent")]
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
    if let Some(guvnor_cfg) = &cfg.guvnor {
        let discovery_data = discovery::discover();
        info!(
            processes = discovery_data.processes.len(),
            log_dirs = discovery_data.log_dirs.len(),
            recommendations = discovery_data.recommendations.len(),
            "Host discovery complete"
        );
        heartbeat::start_heartbeat(guvnor_cfg.clone(), metrics.clone(), start_time, Some(discovery_data)).await;
        info!(agent_id = %guvnor_cfg.agent_id, "Heartbeat started");
    }

    // Initialize intelligence engine if configured
    let analyzer = match cfg.intelligence.tier {
        config::IntelligenceTier::Off => {
            info!("Intelligence: off");
            None
        }
        config::IntelligenceTier::Rules => {
            info!("Intelligence: rules engine");
            let handle = intelligence::analyzer::start(
                cfg.intelligence.clone(),
                None,
                cfg.guvnor.as_ref().map(|g| g.api_url.clone()),
                cfg.guvnor.as_ref().map(|g| g.agent_id.clone()),
                cfg.guvnor.as_ref().map(|g| g.token.clone()),
            ).await;
            Some(handle)
        }
        config::IntelligenceTier::Llm => {
            info!("Intelligence: LLM (BitNet)");
            let resolved = intelligence::model_manager::ensure_model(
                &cfg.data_dir,
                &cfg.intelligence.model_repo,
                &cfg.intelligence.model_file,
            ).await?;
            let engine = intelligence::engine::start(&cfg.intelligence, resolved.path).await?;
            let handle = intelligence::analyzer::start(
                cfg.intelligence.clone(),
                Some(engine),
                cfg.guvnor.as_ref().map(|g| g.api_url.clone()),
                cfg.guvnor.as_ref().map(|g| g.agent_id.clone()),
                cfg.guvnor.as_ref().map(|g| g.token.clone()),
            ).await;
            Some(handle)
        }
    };

    let state_store = state::StateStore::open(&cfg.data_dir)?;
    let guvnor_cfg = cfg.guvnor.clone();
    let pipeline = pipeline::Pipeline::new(cfg, state_store, metrics, analyzer);
    let shutdown = pipeline::shutdown_signal();

    info!("Courier running");
    tokio::select! {
        result = pipeline.run() => {
            if let Err(e) = result { error!(error = %e, "Pipeline error"); }
        }
        _ = shutdown => { info!("Shutdown signal received"); }
    }

    // Deregister if ephemeral mode
    if let Some(ref guvnor_cfg) = guvnor_cfg {
        if guvnor_cfg.ephemeral {
            info!("Ephemeral mode: deregistering agent");
            heartbeat::deregister(guvnor_cfg).await;
        }
    }

    info!("Courier stopped");
    Ok(())
}

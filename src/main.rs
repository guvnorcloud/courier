use clap::Parser;
use std::path::PathBuf;
use tracing::{error, info};

mod buffer;
mod config;
mod heartbeat;
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

    // Start heartbeat if Guvnor config is present
    let metrics = heartbeat::HeartbeatMetrics::new();
    let start_time = std::time::Instant::now();
    if let Some(guvnor_cfg) = &cfg.guvnor {
        heartbeat::start_heartbeat(guvnor_cfg.clone(), metrics.clone(), start_time).await;
        info!(agent_id = %guvnor_cfg.agent_id, "Heartbeat started");
    }

    let state_store = state::StateStore::open(&cfg.data_dir)?;
    let pipeline = pipeline::Pipeline::new(cfg, state_store, metrics);
    let shutdown = pipeline::shutdown_signal();

    info!("Courier running");
    tokio::select! {
        result = pipeline.run() => {
            if let Err(e) = result { error!(error = %e, "Pipeline error"); }
        }
        _ = shutdown => { info!("Shutdown signal received"); }
    }

    info!("Courier stopped");
    Ok(())
}

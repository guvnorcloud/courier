use crate::config::Config;
use crate::heartbeat::HeartbeatMetrics;
use crate::intelligence::analyzer::AnalyzerHandle;
use crate::sources::{self, LogEvent};
use crate::transforms;
use crate::buffer;
use crate::sinks::s3_parquet::S3ParquetSink;
use crate::state::StateStore;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::{info, error, debug};

pub struct Pipeline {
    config: Config,
    state: StateStore,
    metrics: Arc<HeartbeatMetrics>,
    analyzer: Option<AnalyzerHandle>,
}

impl Pipeline {
    pub fn new(config: Config, state: StateStore, metrics: Arc<HeartbeatMetrics>, analyzer: Option<AnalyzerHandle>) -> Self {
        Self { config, state, metrics, analyzer }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let (tx, mut buf) = buffer::create(self.config.buffer.memory_size);

        // If no sources configured, hold the channel open and wait for shutdown.
        // The agent stays alive to heartbeat and send discovery data.
        if self.config.sources.is_empty() {
            info!("No sources configured — running in discovery-only mode");
            info!("Agent will heartbeat and await configuration via claim");
            // Hold the sender so the channel stays open
            let _keep_alive = tx;
            // Just wait forever (shutdown_signal handles ctrl-c)
            tokio::signal::ctrl_c().await.ok();
            return Ok(());
        }

        let _handles = sources::start_sources(&self.config.sources, tx, &self.state).await?;
        let sink = S3ParquetSink::new(self.config.sink.clone()).await?;
        let mut batch: Vec<LogEvent> = Vec::with_capacity(self.config.sink.batch.max_events);
        let mut batch_bytes: usize = 0;
        let batch_cfg = self.config.sink.batch.clone();
        let transforms = self.config.transforms.clone();
        let mut tick = interval(Duration::from_secs(batch_cfg.timeout_secs));
        let metrics = self.metrics.clone();
        let analyzer = self.analyzer;

        info!("Pipeline running with {} sources", self.config.sources.len());
        loop {
            tokio::select! {
                event = buf.recv() => {
                    match event {
                        Some(mut ev) => {
                            if !transforms::apply(&transforms, &mut ev) { continue; }
                            // Feed to intelligence analyzer (non-blocking)
                            if let Some(ref a) = analyzer { a.feed(&ev); }
                            batch_bytes += ev.message.len();
                            batch.push(ev);
                            if batch.len() >= batch_cfg.max_events || batch_bytes >= batch_cfg.max_bytes {
                                flush(&sink, &mut batch, &mut batch_bytes, &metrics).await;
                            }
                        }
                        None => { flush(&sink, &mut batch, &mut batch_bytes, &metrics).await; break; }
                    }
                }
                _ = tick.tick() => {
                    if !batch.is_empty() {
                        debug!(events = batch.len(), "Timer flush");
                        flush(&sink, &mut batch, &mut batch_bytes, &metrics).await;
                    }
                }
            }
        }
        Ok(())
    }
}

async fn flush(
    sink: &S3ParquetSink,
    batch: &mut Vec<LogEvent>,
    bytes: &mut usize,
    metrics: &Arc<HeartbeatMetrics>,
) {
    if batch.is_empty() { return; }
    let count = batch.len();
    let byte_count = *bytes;
    let events = std::mem::take(batch);
    *bytes = 0;
    if let Err(e) = sink.write_batch(events).await {
        error!(error = %e, "Flush failed");
    } else {
        metrics.events_sent.fetch_add(count as u64, Ordering::Relaxed);
        metrics.bytes_sent.fetch_add(byte_count as u64, Ordering::Relaxed);
    }
}

pub async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.expect("signal");
}

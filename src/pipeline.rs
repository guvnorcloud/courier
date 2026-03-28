use crate::config::{Config, SinkType};
use crate::heartbeat::HeartbeatMetrics;
use crate::sinks::s3_parquet::CredentialsHolder;
use crate::sinks::http::{HttpSink, HttpSinkConfig};
use crate::intelligence::analyzer::AnalyzerHandle;
use crate::sources::{self, LogEvent};
use crate::transforms;
use crate::buffer;
use crate::sinks::s3_parquet::S3ParquetSink;
use crate::state::StateStore;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::{interval, Duration};
use tracing::{info, error, debug};

enum Sink {
    S3(S3ParquetSink),
    Http(HttpSink),
}

impl Sink {
    async fn write_batch(&self, events: Vec<LogEvent>) -> anyhow::Result<()> {
        match self {
            Sink::S3(s) => s.write_batch(events).await,
            Sink::Http(h) => h.write_batch(events).await,
        }
    }
}

pub struct Pipeline {
    config: Config,
    state: StateStore,
    metrics: Arc<HeartbeatMetrics>,
    analyzer: Option<AnalyzerHandle>,
    config_reload_rx: Option<watch::Receiver<bool>>,
    s3_creds: Option<CredentialsHolder>,
}

impl Pipeline {
    pub fn new(
        config: Config,
        state: StateStore,
        metrics: Arc<HeartbeatMetrics>,
        analyzer: Option<AnalyzerHandle>,
    ) -> Self {
        Self { config, state, metrics, analyzer, config_reload_rx: None, s3_creds: None }
    }

    pub fn with_s3_creds(mut self, creds: CredentialsHolder) -> Self {
        self.s3_creds = Some(creds);
        self
    }

    pub fn with_config_reload(mut self, rx: watch::Receiver<bool>) -> Self {
        self.config_reload_rx = Some(rx);
        self
    }

    /// Run the pipeline. Returns Ok(true) if a config reload was requested,
    /// Ok(false) for normal shutdown.
    pub async fn run(mut self) -> anyhow::Result<bool> {
        let (tx, mut buf) = buffer::create(self.config.buffer.memory_size);

        // If no sources configured, hold the channel open and wait for shutdown or config reload.
        if self.config.sources.is_empty() {
            info!("No sources configured — running in discovery-only mode");
            info!("Agent will heartbeat and await configuration via claim");
            let _keep_alive = tx;

            if let Some(ref mut rx) = self.config_reload_rx {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => { return Ok(false); }
                    result = rx.changed() => {
                        if result.is_ok() && *rx.borrow() {
                            info!("Config reload signal received in discovery-only mode");
                            return Ok(true);
                        }
                        return Ok(false);
                    }
                }
            } else {
                tokio::signal::ctrl_c().await.ok();
                return Ok(false);
            }
        }

        let _handles = sources::start_sources(&self.config.sources, tx, &self.state).await?;
        let sink = match self.config.sink.sink_type {
            SinkType::Http => {
                let endpoint = self.config.sink.endpoint.clone()
                    .unwrap_or_else(|| {
                        // Derive from guvnor API URL if not set
                        let base = self.config.guvnor.as_ref()
                            .map(|g| g.api_url.trim_end_matches('/').to_string())
                            .unwrap_or_default();
                        format!("{}/api/v1/courier/logs/ingest", base)
                    });
                let token = self.config.guvnor.as_ref()
                    .map(|g| g.token.clone())
                    .unwrap_or_default();
                let agent_id = self.config.guvnor.as_ref()
                    .map(|g| g.agent_id.clone())
                    .unwrap_or_default();
                info!(endpoint = %endpoint, "Using HTTP sink");
                Sink::Http(HttpSink::new(HttpSinkConfig { endpoint, token, agent_id }))
            }
            SinkType::S3 => {
                let creds = self.s3_creds.clone().unwrap_or_else(|| Arc::new(tokio::sync::RwLock::new(None)));
                Sink::S3(S3ParquetSink::new(self.config.sink.clone(), creds).await?)
            }
        };
        let mut batch: Vec<LogEvent> = Vec::with_capacity(self.config.sink.batch.max_events);
        let mut batch_bytes: usize = 0;
        let batch_cfg = self.config.sink.batch.clone();
        let transforms = self.config.transforms.clone();
        let mut tick = interval(Duration::from_secs(batch_cfg.timeout_secs));
        let metrics = self.metrics.clone();
        let analyzer = self.analyzer;

        info!("Pipeline running with {} sources", self.config.sources.len());

        // If we have a config reload receiver, include it in the select loop
        if let Some(ref mut rx) = self.config_reload_rx {
            loop {
                tokio::select! {
                    event = buf.recv() => {
                        match event {
                            Some(mut ev) => {
                                if !transforms::apply(&transforms, &mut ev) { continue; }
                                if let Some(ref a) = analyzer { a.feed(&ev); }
                                batch_bytes += ev.message.len();
                                batch.push(ev);
                                if batch.len() >= batch_cfg.max_events || batch_bytes >= batch_cfg.max_bytes {
                                    flush(&sink, &mut batch, &mut batch_bytes, &metrics).await;
                                }
                            }
                            None => { flush(&sink, &mut batch, &mut batch_bytes, &metrics).await; return Ok(false); }
                        }
                    }
                    _ = tick.tick() => {
                        if !batch.is_empty() {
                            debug!(events = batch.len(), "Timer flush");
                            flush(&sink, &mut batch, &mut batch_bytes, &metrics).await;
                        }
                    }
                    result = rx.changed() => {
                        if result.is_ok() && *rx.borrow() {
                            // Flush remaining events before reloading
                            flush(&sink, &mut batch, &mut batch_bytes, &metrics).await;
                            info!("Config reload signal received — restarting pipeline");
                            return Ok(true);
                        }
                    }
                }
            }
        } else {
            loop {
                tokio::select! {
                    event = buf.recv() => {
                        match event {
                            Some(mut ev) => {
                                if !transforms::apply(&transforms, &mut ev) { continue; }
                                if let Some(ref a) = analyzer { a.feed(&ev); }
                                batch_bytes += ev.message.len();
                                batch.push(ev);
                                if batch.len() >= batch_cfg.max_events || batch_bytes >= batch_cfg.max_bytes {
                                    flush(&sink, &mut batch, &mut batch_bytes, &metrics).await;
                                }
                            }
                            None => { flush(&sink, &mut batch, &mut batch_bytes, &metrics).await; return Ok(false); }
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
        }
    }
}

async fn flush(
    sink: &Sink,
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

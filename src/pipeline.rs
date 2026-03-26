use crate::config::Config;
use crate::sources::{self, LogEvent};
use crate::transforms;
use crate::buffer;
use crate::sinks::s3_parquet::S3ParquetSink;
use crate::state::StateStore;
use tokio::time::{interval, Duration};
use tracing::{info, error, debug};

pub struct Pipeline { config: Config, state: StateStore }

impl Pipeline {
    pub fn new(config: Config, state: StateStore) -> Self { Self { config, state } }

    pub async fn run(self) -> anyhow::Result<()> {
        let (tx, mut buf) = buffer::create(self.config.buffer.memory_size);
        let _handles = sources::start_sources(&self.config.sources, tx, &self.state).await?;
        let sink = S3ParquetSink::new(self.config.sink.clone()).await?;
        let mut batch: Vec<LogEvent> = Vec::with_capacity(self.config.sink.batch.max_events);
        let mut batch_bytes: usize = 0;
        let batch_cfg = self.config.sink.batch.clone();
        let transforms = self.config.transforms.clone();
        let mut tick = interval(Duration::from_secs(batch_cfg.timeout_secs));

        info!("Pipeline running");
        loop {
            tokio::select! {
                event = buf.recv() => {
                    match event {
                        Some(mut ev) => {
                            if !transforms::apply(&transforms, &mut ev) { continue; }
                            batch_bytes += ev.message.len();
                            batch.push(ev);
                            if batch.len() >= batch_cfg.max_events || batch_bytes >= batch_cfg.max_bytes {
                                flush(&sink, &mut batch, &mut batch_bytes).await;
                            }
                        }
                        None => { flush(&sink, &mut batch, &mut batch_bytes).await; break; }
                    }
                }
                _ = tick.tick() => {
                    if !batch.is_empty() {
                        debug!(events = batch.len(), "Timer flush");
                        flush(&sink, &mut batch, &mut batch_bytes).await;
                    }
                }
            }
        }
        Ok(())
    }
}

async fn flush(sink: &S3ParquetSink, batch: &mut Vec<LogEvent>, bytes: &mut usize) {
    if batch.is_empty() { return; }
    let events = std::mem::take(batch);
    *bytes = 0;
    if let Err(e) = sink.write_batch(events).await { error!(error = %e, "Flush failed"); }
}

pub async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.expect("signal");
}

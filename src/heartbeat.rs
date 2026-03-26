use crate::config::GuvnorConfig;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn};

pub struct HeartbeatMetrics {
    pub events_sent: AtomicU64,
    pub bytes_sent: AtomicU64,
}

impl HeartbeatMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            events_sent: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
        })
    }
}

pub async fn start_heartbeat(
    guvnor: GuvnorConfig,
    metrics: Arc<HeartbeatMetrics>,
    start_time: std::time::Instant,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut tick = interval(Duration::from_secs(30));

        loop {
            tick.tick().await;

            let body = serde_json::json!({
                "agent_id": guvnor.agent_id,
                "token": guvnor.token,
                "hostname": gethostname::gethostname().to_string_lossy().to_string(),
                "status": "running",
                "events_sent": metrics.events_sent.load(Ordering::Relaxed),
                "bytes_sent": metrics.bytes_sent.load(Ordering::Relaxed),
                "uptime_secs": start_time.elapsed().as_secs(),
                "version": env!("CARGO_PKG_VERSION"),
            });

            match client
                .post(format!("{}/api/v1/courier/heartbeat", guvnor.api_url))
                .json(&body)
                .timeout(Duration::from_secs(10))
                .send()
                .await
            {
                Ok(resp) => {
                    if resp.status().is_success() {
                        if let Ok(data) = resp.json::<serde_json::Value>().await {
                            if data
                                .get("config_updated")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false)
                            {
                                info!("Config update available — will reload on next cycle");
                                // TODO: trigger config reload
                            }
                        }
                        debug!("Heartbeat sent");
                    } else {
                        warn!(status = %resp.status(), "Heartbeat failed");
                    }
                }
                Err(e) => warn!(error = %e, "Heartbeat error"),
            }
        }
    })
}

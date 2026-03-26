use crate::config::GuvnorConfig;
use crate::discovery::HostDiscovery;
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
    discovery: Option<HostDiscovery>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut tick = interval(Duration::from_secs(30));
        let mut first_beat = true;
        let discovery_data = discovery;

        loop {
            tick.tick().await;

            let mut body = serde_json::json!({
                "agent_id": guvnor.agent_id,
                "token": guvnor.token,
                "hostname": gethostname::gethostname().to_string_lossy().to_string(),
                "status": "running",
                "events_sent": metrics.events_sent.load(Ordering::Relaxed),
                "bytes_sent": metrics.bytes_sent.load(Ordering::Relaxed),
                "uptime_secs": start_time.elapsed().as_secs(),
                "version": env!("CARGO_PKG_VERSION"),
            });

            // Include discovery data on the first heartbeat
            if first_beat {
                if let Some(ref disc) = discovery_data {
                    if let Ok(disc_val) = serde_json::to_value(disc) {
                        body.as_object_mut().unwrap().insert("discovery".to_string(), disc_val);
                    }
                }
                first_beat = false;
            }

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
                                info!("Config update available -- will reload on next cycle");
                                // TODO: trigger config reload
                            }
                            // If server requests discovery re-send
                            if data
                                .get("send_discovery")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false)
                            {
                                first_beat = true;
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

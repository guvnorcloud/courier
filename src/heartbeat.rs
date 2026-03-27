use crate::config::GuvnorConfig;
use crate::discovery::HostDiscovery;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn};

pub async fn deregister(config: &GuvnorConfig) {
    let client = reqwest::Client::new();
    let url = format!("{}/courier/heartbeat", config.api_url);
    let body = serde_json::json!({
        "agent_id": config.agent_id,
        "token": config.token,
        "status": "deregistered",
    });
    match client.post(&url).json(&body).send().await {
        Ok(resp) if resp.status().is_success() => {
            info!("Agent deregistered");
        }
        Ok(resp) => {
            warn!(status = %resp.status(), "Deregister failed");
        }
        Err(e) => {
            warn!(error = %e, "Deregister failed");
        }
    }
}

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

/// Start the heartbeat loop. Returns a join handle and a watch receiver
/// that signals `true` when the backend reports config_updated.
pub async fn start_heartbeat(
    guvnor: GuvnorConfig,
    metrics: Arc<HeartbeatMetrics>,
    start_time: std::time::Instant,
    discovery: Option<HostDiscovery>,
) -> (tokio::task::JoinHandle<()>, watch::Receiver<bool>) {
    let (config_tx, config_rx) = watch::channel(false);

    let handle = tokio::spawn(async move {
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
                                info!("Config update available — signaling reload");
                                let _ = config_tx.send(true);
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
    });

    (handle, config_rx)
}

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

/// Send a final heartbeat marking the agent as offline (clean shutdown).
pub async fn send_offline(config: &GuvnorConfig) {
    let client = reqwest::Client::new();
    let url = format!("{}/courier/heartbeat", config.api_url);
    let body = serde_json::json!({
        "agent_id": config.agent_id,
        "token": config.token,
        "status": "offline",
    });
    match client.post(&url).json(&body).timeout(std::time::Duration::from_secs(5)).send().await {
        Ok(resp) if resp.status().is_success() => {
            info!("Agent marked offline");
        }
        _ => {
            warn!("Failed to send offline heartbeat");
        }
    }
}

/// S3 credentials vended by the heartbeat for Guvnor-hosted buckets.
#[derive(Debug, Clone)]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub expiration: String,
}

pub struct HeartbeatMetrics {
    pub events_sent: AtomicU64,
    pub bytes_sent: AtomicU64,
}

/// Shared state for S3 credentials vended by the heartbeat.
pub type S3CredentialsHolder = Arc<tokio::sync::RwLock<Option<S3Credentials>>>;

impl HeartbeatMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            events_sent: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
        })
    }
}

/// Start the heartbeat loop. Returns a join handle, a watch receiver
/// for config_updated, and a shared holder for S3 credentials.
pub async fn start_heartbeat(
    guvnor: GuvnorConfig,
    metrics: Arc<HeartbeatMetrics>,
    start_time: std::time::Instant,
    discovery: Option<HostDiscovery>,
) -> (tokio::task::JoinHandle<()>, watch::Receiver<bool>, S3CredentialsHolder) {
    let s3_creds: S3CredentialsHolder = Arc::new(tokio::sync::RwLock::new(None));
    let (config_tx, config_rx) = watch::channel(false);

    let s3_creds_clone = s3_creds.clone();
    let handle = tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut tick = interval(Duration::from_secs(30));
        let mut first_beat = true;
        let discovery_data = discovery;
        let s3_creds = s3_creds_clone;

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
                .post(format!("{}/courier/heartbeat", guvnor.api_url))
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
                            // Parse vended S3 credentials
                            if let Some(creds) = data.get("s3_credentials") {
                                if let (Some(ak), Some(sk), Some(st)) = (
                                    creds.get("access_key_id").and_then(|v| v.as_str()),
                                    creds.get("secret_access_key").and_then(|v| v.as_str()),
                                    creds.get("session_token").and_then(|v| v.as_str()),
                                ) {
                                    let new_creds = S3Credentials {
                                        access_key_id: ak.to_string(),
                                        secret_access_key: sk.to_string(),
                                        session_token: st.to_string(),
                                        expiration: creds.get("expiration").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                    };
                                    *s3_creds.write().await = Some(new_creds);
                                    debug!("S3 credentials updated from heartbeat");
                                }
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

    (handle, config_rx, s3_creds)
}

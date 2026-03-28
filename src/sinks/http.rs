use crate::sources::LogEvent;
use reqwest::Client;
use serde::Serialize;
use std::time::Duration;
use tracing::{info, error, warn};

#[derive(Debug, Clone)]
pub struct HttpSinkConfig {
    pub endpoint: String,
    pub token: String,
    pub agent_id: String,
}

pub struct HttpSink {
    config: HttpSinkConfig,
    client: Client,
}

#[derive(Serialize)]
struct IngestRequest {
    agent_id: String,
    events: Vec<IngestEvent>,
}

#[derive(Serialize)]
struct IngestEvent {
    timestamp: i64,
    message: String,
    source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    level: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fields: Option<std::collections::HashMap<String, String>>,
}

impl HttpSink {
    pub fn new(config: HttpSinkConfig) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(4)
            .build()
            .expect("Failed to build HTTP client");

        info!(endpoint = %config.endpoint, "HTTP sink ready");
        Self { config, client }
    }

    pub async fn write_batch(&self, events: Vec<LogEvent>) -> anyhow::Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let count = events.len();
        let ingest_events: Vec<IngestEvent> = events
            .into_iter()
            .map(|ev| {
                let level = ev.fields.get("level").cloned();
                let fields = if ev.fields.is_empty() {
                    None
                } else {
                    Some(ev.fields)
                };
                IngestEvent {
                    timestamp: ev.timestamp,
                    message: ev.message,
                    source: ev.source,
                    host: Some(ev.host),
                    file: ev.file,
                    level,
                    fields,
                }
            })
            .collect();

        let body = IngestRequest {
            agent_id: self.config.agent_id.clone(),
            events: ingest_events,
        };

        // Retry with backoff: 1s, 2s, 4s
        let mut last_err = None;
        for attempt in 0..3 {
            if attempt > 0 {
                let delay = Duration::from_secs(1 << attempt);
                warn!(attempt, delay_secs = delay.as_secs(), "Retrying ingest");
                tokio::time::sleep(delay).await;
            }

            match self
                .client
                .post(&self.config.endpoint)
                .header("Authorization", format!("Bearer {}", self.config.token))
                .json(&body)
                .send()
                .await
            {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        info!(events = count, status = %status, "Batch ingested via HTTP");
                        return Ok(());
                    }
                    if status.is_client_error() {
                        // 4xx — don't retry (bad request, auth error, etc.)
                        let text = resp.text().await.unwrap_or_default();
                        error!(status = %status, body = %text, "Ingest rejected (not retrying)");
                        return Err(anyhow::anyhow!("Ingest rejected: {} {}", status, text));
                    }
                    // 5xx — retry
                    let text = resp.text().await.unwrap_or_default();
                    last_err = Some(anyhow::anyhow!("Server error: {} {}", status, text));
                }
                Err(e) => {
                    last_err = Some(anyhow::anyhow!("HTTP request failed: {}", e));
                }
            }
        }

        error!(events = count, "All retries exhausted for HTTP ingest");
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("Unknown error")))
    }
}

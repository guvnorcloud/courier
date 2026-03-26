//! Inference engine — loads a BitNet model and analyzes log batches.
//!
//! Runs on a dedicated blocking thread via `tokio::task::spawn_blocking`
//! so inference never blocks the async pipeline.

use crate::config::IntelligenceConfig;
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, error};

/// A single finding produced by log analysis.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Finding {
    pub severity: Severity,
    pub category: String,
    pub summary: String,
    pub details: String,
    /// Which log lines triggered this finding (indices into the batch)
    pub evidence_lines: Vec<usize>,
    /// Confidence score 0.0 - 1.0
    pub confidence: f32,
    pub timestamp: String,
    pub host: String,
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Low,
    Medium,
    High,
    Critical,
}

impl Severity {
    pub fn score(&self) -> f32 {
        match self {
            Severity::Info => 0.1,
            Severity::Low => 0.25,
            Severity::Medium => 0.5,
            Severity::High => 0.75,
            Severity::Critical => 1.0,
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "critical" => Severity::Critical,
            "high" => Severity::High,
            "medium" => Severity::Medium,
            "low" => Severity::Low,
            _ => Severity::Info,
        }
    }
}

/// A batch of log lines to analyze.
pub struct AnalysisRequest {
    pub lines: Vec<String>,
    pub host: String,
    pub source: String,
    pub respond: oneshot::Sender<Vec<Finding>>,
}

/// The engine handle — send analysis requests through this.
#[derive(Clone)]
pub struct EngineHandle {
    tx: mpsc::Sender<AnalysisRequest>,
}

impl EngineHandle {
    /// Submit a batch for analysis. Returns findings or empty vec on timeout/error.
    pub async fn analyze(&self, lines: Vec<String>, host: String, source: String) -> Vec<Finding> {
        let (respond, rx) = oneshot::channel();
        let req = AnalysisRequest { lines, host, source, respond };
        if self.tx.send(req).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }
}

/// Start the inference engine. Downloads model if needed, loads it, and
/// returns a handle for submitting work.
pub async fn start(
    config: &IntelligenceConfig,
    model_path: PathBuf,
) -> anyhow::Result<EngineHandle> {
    let (tx, rx) = mpsc::channel::<AnalysisRequest>(32);
    let config = config.clone();
    let path = model_path.clone();

    info!(model = %path.display(), "Loading BitNet model");

    // Spawn the inference loop on a blocking thread
    tokio::task::spawn_blocking(move || {
        inference_loop(rx, &config, &path);
    });

    Ok(EngineHandle { tx })
}

/// The main inference loop — runs on a dedicated OS thread.
#[cfg(feature = "intelligence")]
fn inference_loop(
    mut rx: mpsc::Receiver<AnalysisRequest>,
    config: &IntelligenceConfig,
    model_path: &std::path::Path,
) {
    use bitnet_llm::{Model, SessionConfig};

    // Load the model
    let model = match Model::load(model_path) {
        Ok(m) => {
            info!("BitNet model loaded successfully");
            m
        }
        Err(e) => {
            error!(error = %e, "Failed to load BitNet model — intelligence disabled");
            return;
        }
    };

    let session_config = SessionConfig::default()
        .with_max_tokens(config.max_tokens);

    // Process requests
    while let Some(req) = rx.blocking_recv() {
        let findings = analyze_batch(&model, &session_config, &req, config);
        let _ = req.respond.send(findings);
    }

    info!("Inference engine shutting down");
}

/// Fallback when compiled without the intelligence feature.
#[cfg(not(feature = "intelligence"))]
fn inference_loop(
    mut rx: mpsc::Receiver<AnalysisRequest>,
    _config: &IntelligenceConfig,
    _model_path: &std::path::Path,
) {
    error!("Courier compiled without `intelligence` feature — LLM tier unavailable");
    // Drain requests, return empty findings
    while let Some(req) = rx.blocking_recv() {
        let _ = req.respond.send(Vec::new());
    }
}

/// Build the analysis prompt and parse the model's response into findings.
#[cfg(feature = "intelligence")]
fn analyze_batch(
    model: &bitnet_llm::Model,
    session_config: &bitnet_llm::SessionConfig,
    req: &AnalysisRequest,
    config: &IntelligenceConfig,
) -> Vec<Finding> {
    let prompt = build_prompt(&req.lines, &req.source, &config.categories);
    debug!(lines = req.lines.len(), prompt_len = prompt.len(), "Analyzing batch");

    let mut session = model.create_session(session_config.clone());
    match session.generate(&prompt) {
        Ok(response) => {
            debug!(response_len = response.len(), "Model response received");
            parse_findings(&response, req, config)
        }
        Err(e) => {
            error!(error = %e, "Inference failed");
            Vec::new()
        }
    }
}

fn build_prompt(lines: &[String], source: &str, categories: &[String]) -> String {
    let categories_str = categories.join(", ");
    let log_block: String = lines
        .iter()
        .enumerate()
        .map(|(i, line)| format!("[{}] {}", i, line))
        .collect::<Vec<_>>()
        .join("\n");

    format!(
r#"You are a log analysis agent. Analyze these log lines from source "{source}" and identify any issues.

Categories to check: {categories_str}

For each finding, output exactly this JSON format (one per line):
{{"severity":"critical|high|medium|low|info","category":"<category>","summary":"<brief>","details":"<explanation>","lines":[<indices>],"confidence":<0.0-1.0>}}

If no issues found, output: {{"none":true}}

Log lines:
{log_block}

Analysis:"#
    )
}

/// Parse the model's text output into structured findings.
fn parse_findings(
    response: &str,
    req: &AnalysisRequest,
    config: &IntelligenceConfig,
) -> Vec<Finding> {
    let mut findings = Vec::new();
    let now = chrono::Utc::now().to_rfc3339();

    for line in response.lines() {
        let line = line.trim();
        if line.is_empty() || line.contains("\"none\":true") {
            continue;
        }

        // Try to parse each line as a JSON finding
        if let Ok(raw) = serde_json::from_str::<serde_json::Value>(line) {
            let severity = raw.get("severity")
                .and_then(|v| v.as_str())
                .map(Severity::from_str)
                .unwrap_or(Severity::Info);

            let confidence = raw.get("confidence")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.5) as f32;

            // Filter by severity threshold
            if severity.score() < config.severity_threshold {
                continue;
            }

            findings.push(Finding {
                severity,
                category: raw.get("category")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string(),
                summary: raw.get("summary")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Issue detected")
                    .to_string(),
                details: raw.get("details")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                evidence_lines: raw.get("lines")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter().filter_map(|v| v.as_u64().map(|n| n as usize)).collect())
                    .unwrap_or_default(),
                confidence,
                timestamp: now.clone(),
                host: req.host.clone(),
            });
        }
    }

    findings
}

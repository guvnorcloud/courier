//! Log analyzer — the bridge between the pipeline and the inference engine.
//!
//! Collects log events, batches them, and runs analysis via either the rules
//! engine or the LLM engine. Findings are reported to the Guvnor API.

use crate::config::{IntelligenceConfig, IntelligenceTier, IntelligenceRule};
use crate::sources::LogEvent;
use super::engine::{EngineHandle, Finding, Severity};
use tokio::sync::mpsc;
use tracing::{info, warn, debug};

/// Handle for sending log events into the analyzer.
#[derive(Clone)]
pub struct AnalyzerHandle {
    tx: mpsc::Sender<LogEvent>,
}

impl AnalyzerHandle {
    /// Feed a log event into the analyzer. Non-blocking, drops if buffer full.
    pub fn feed(&self, event: &LogEvent) {
        let _ = self.tx.try_send(event.clone());
    }
}

/// Start the analyzer. Returns a handle for feeding events.
pub async fn start(
    config: IntelligenceConfig,
    engine: Option<EngineHandle>,
    guvnor_api: Option<String>,
    agent_id: Option<String>,
    token: Option<String>,
) -> AnalyzerHandle {
    // Buffer up to 1000 events before dropping
    let (tx, rx) = mpsc::channel::<LogEvent>(1000);

    tokio::spawn(analyzer_loop(config, engine, rx, guvnor_api, agent_id, token));

    AnalyzerHandle { tx }
}

async fn analyzer_loop(
    config: IntelligenceConfig,
    engine: Option<EngineHandle>,
    mut rx: mpsc::Receiver<LogEvent>,
    guvnor_api: Option<String>,
    agent_id: Option<String>,
    token: Option<String>,
) {
    let batch_size = config.batch_size;
    let mut batch: Vec<LogEvent> = Vec::with_capacity(batch_size);
    let mut tick = tokio::time::interval(tokio::time::Duration::from_secs(30));
    let rules = compile_rules(&config.rules);

    info!(tier = ?config.tier, batch_size, "Analyzer running");

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Some(ev) => {
                        batch.push(ev);
                        if batch.len() >= batch_size {
                            let events = std::mem::take(&mut batch);
                            process_batch(&config, &engine, &rules, &events, &guvnor_api, &agent_id, &token).await;
                        }
                    }
                    None => {
                        // Channel closed — process remaining and exit
                        if !batch.is_empty() {
                            process_batch(&config, &engine, &rules, &batch, &guvnor_api, &agent_id, &token).await;
                        }
                        info!("Analyzer shutting down");
                        break;
                    }
                }
            }
            _ = tick.tick() => {
                if !batch.is_empty() {
                    let events = std::mem::take(&mut batch);
                    process_batch(&config, &engine, &rules, &events, &guvnor_api, &agent_id, &token).await;
                }
            }
        }
    }
}

async fn process_batch(
    config: &IntelligenceConfig,
    engine: &Option<EngineHandle>,
    rules: &[CompiledRule],
    events: &[LogEvent],
    guvnor_api: &Option<String>,
    agent_id: &Option<String>,
    token: &Option<String>,
) {
    let findings = match config.tier {
        IntelligenceTier::Off => return,
        IntelligenceTier::Rules => {
            run_rules(rules, events)
        }
        IntelligenceTier::Llm => {
            let mut findings = run_rules(rules, events);
            if let Some(eng) = engine {
                let lines: Vec<String> = events.iter().map(|e| e.message.clone()).collect();
                let host = events.first().map(|e| e.host.clone()).unwrap_or_default();
                let source = events.first().map(|e| e.source.clone()).unwrap_or_default();
                let llm_findings = eng.analyze(lines, host, source).await;
                findings.extend(llm_findings);
            }
            findings
        }
    };

    if findings.is_empty() {
        debug!(events = events.len(), "No findings in batch");
        return;
    }

    info!(
        findings = findings.len(),
        events = events.len(),
        "Findings detected"
    );
    for f in &findings {
        match f.severity {
            Severity::Critical | Severity::High => {
                warn!(
                    severity = ?f.severity,
                    category = %f.category,
                    summary = %f.summary,
                    confidence = f.confidence,
                    "Finding"
                );
            }
            _ => {
                info!(
                    severity = ?f.severity,
                    category = %f.category,
                    summary = %f.summary,
                    "Finding"
                );
            }
        }
    }

    // Report findings to Guvnor API
    if let (Some(api), Some(aid), Some(tok)) = (guvnor_api, agent_id, token) {
        report_findings(api, aid, tok, &findings).await;
    }
}

async fn report_findings(api_url: &str, agent_id: &str, token: &str, findings: &[Finding]) {
    let url = format!("{}/courier/agents/{}/findings", api_url, agent_id);
    let client = reqwest::Client::new();
    match client
        .post(&url)
        .bearer_auth(token)
        .json(findings)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            debug!(count = findings.len(), "Findings reported to Guvnor");
        }
        Ok(resp) => {
            warn!(status = %resp.status(), "Failed to report findings");
        }
        Err(e) => {
            warn!(error = %e, "Failed to report findings");
        }
    }
}

// ── Rules engine ──────────────────────────────────────────────────

struct CompiledRule {
    name: String,
    pattern: regex::Regex,
    severity: Severity,
    category: String,
    description: String,
}

fn compile_rules(rules: &[IntelligenceRule]) -> Vec<CompiledRule> {
    let mut compiled = Vec::new();

    // Built-in rules (always active)
    let builtins = vec![
        ("auth_failure", r"(?i)(authentication fail|login fail|invalid (password|credentials|token)|unauthorized|401\b|access denied)", "high", "security", "Authentication failure detected"),
        ("brute_force", r"(?i)(too many (attempts|requests|failures)|rate limit|throttl|brute.?force|blocked.*(ip|address))", "critical", "security", "Possible brute force or rate limiting"),
        ("privilege_escalation", r"(?i)(privilege escalat|sudo.*fail|permission denied.*root|su:.*fail|setuid)", "critical", "security", "Privilege escalation attempt"),
        ("sql_injection", r#"(?i)(sql.*injection|union.*select|drop\s+table|;.*--|\b(or|and)\b.*['"].*=.*['"])"#, "critical", "security", "Possible SQL injection"),
        ("path_traversal", r"\.\./\.\./|%2e%2e/|%252e%252e", "high", "security", "Path traversal attempt"),
        ("oom", r"(?i)(out of memory|oom.?kill|cannot allocate|memory (exhausted|allocation failed))", "critical", "error", "Out of memory condition"),
        ("disk_full", r"(?i)(no space left|disk (full|quota)|ENOSPC|filesystem.*full)", "critical", "error", "Disk space exhausted"),
        ("segfault", r"(?i)(segfault|segmentation fault|SIGSEGV|core dump|signal 11)", "high", "error", "Process crash (segfault)"),
        ("connection_refused", r"(?i)(connection refused|ECONNREFUSED|connect.*fail|couldn't connect)", "medium", "error", "Connection failure"),
        ("high_latency", r"(?i)(timeout|timed?\s*out|deadline exceeded|took \d{4,}ms|latency.*\d{4,})", "medium", "performance", "High latency or timeout"),
        ("cert_expiry", r"(?i)(certificate.*expir|cert.*invalid|ssl.*error|tls.*fail|x509)", "high", "security", "Certificate or TLS issue"),
        ("panic", r"(?i)(panic|FATAL|unhandled exception|stack overflow|thread.*abort)", "critical", "error", "Application panic or fatal error"),
    ];

    let num_builtins = builtins.len();
    for (name, pattern, severity, category, description) in builtins {
        if let Ok(re) = regex::Regex::new(pattern) {
            compiled.push(CompiledRule {
                name: name.to_string(),
                pattern: re,
                severity: Severity::from_str(severity),
                category: category.to_string(),
                description: description.to_string(),
            });
        }
    }

    // User-defined rules
    for rule in rules {
        if let Ok(re) = regex::Regex::new(&rule.pattern) {
            compiled.push(CompiledRule {
                name: rule.name.clone(),
                pattern: re,
                severity: Severity::from_str(&rule.severity),
                category: rule.category.clone(),
                description: rule.description.clone(),
            });
        }
    }

    info!(builtin = num_builtins, custom = rules.len(), "Rules compiled");
    compiled
}

fn run_rules(rules: &[CompiledRule], events: &[LogEvent]) -> Vec<Finding> {
    let mut findings: Vec<Finding> = Vec::new();
    let now = chrono::Utc::now().to_rfc3339();

    for (i, event) in events.iter().enumerate() {
        for rule in rules {
            if rule.pattern.is_match(&event.message) {
                // Deduplicate: don't re-fire the same rule for adjacent lines
                let dominated = findings.iter().any(|f| {
                    f.summary == rule.description
                        && f.evidence_lines.last().map_or(false, |&last| i - last <= 3)
                });
                if dominated {
                    // Just add this line to the existing finding
                    if let Some(existing) = findings.iter_mut().rev().find(|f| f.summary == rule.description) {
                        existing.evidence_lines.push(i);
                    }
                    continue;
                }

                findings.push(Finding {
                    severity: rule.severity,
                    category: rule.category.clone(),
                    summary: rule.description.clone(),
                    details: format!(
                        "Rule '{}' matched: {}",
                        rule.name,
                        truncate(&event.message, 200)
                    ),
                    evidence_lines: vec![i],
                    confidence: 1.0,
                    timestamp: now.clone(),
                    host: event.host.clone(),
                });
            }
        }
    }

    findings
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max { s } else { &s[..max] }
}

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuvnorConfig {
    /// Guvnor API URL
    pub api_url: String,
    /// Bootstrap token for authentication
    pub token: String,
    /// Agent ID (assigned during bootstrap)
    pub agent_id: String,
    /// Config refresh interval in seconds
    #[serde(default = "default_refresh_interval")]
    pub refresh_interval_secs: u64,
    /// If true, agent deregisters on shutdown (for ephemeral containers)
    #[serde(default)]
    pub ephemeral: bool,
}

fn default_refresh_interval() -> u64 { 300 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default)]
    pub sources: HashMap<String, SourceConfig>,
    #[serde(default)]
    pub transforms: Vec<TransformConfig>,
    pub sink: SinkConfig,
    #[serde(default)]
    pub buffer: BufferConfig,
    #[serde(default)]
    pub guvnor: Option<GuvnorConfig>,
    #[serde(default)]
    pub intelligence: IntelligenceConfig,
}

/// Intelligence tier — controls how logs are analyzed on-agent
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum IntelligenceTier {
    /// No analysis — just forward logs
    Off,
    /// Rule-based: regex patterns, thresholds, keyword matching
    Rules,
    /// LLM-powered: BitNet 1.58-bit model runs on CPU
    Llm,
}

impl Default for IntelligenceTier {
    fn default() -> Self { Self::Off }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntelligenceConfig {
    #[serde(default)]
    pub tier: IntelligenceTier,

    /// Model ID on HuggingFace (default: microsoft/bitnet-b1.58-2B-4T-gguf)
    #[serde(default = "default_model_repo")]
    pub model_repo: String,

    /// GGUF filename within the repo
    #[serde(default = "default_model_file")]
    pub model_file: String,

    /// Max RAM budget for the model in MB (default: 512)
    #[serde(default = "default_model_max_ram_mb")]
    pub max_ram_mb: usize,

    /// How many log lines to batch before running analysis
    #[serde(default = "default_analysis_batch")]
    pub batch_size: usize,

    /// Max tokens the model generates per analysis
    #[serde(default = "default_max_tokens")]
    pub max_tokens: usize,

    /// Severity threshold to report findings (0.0 - 1.0)
    #[serde(default = "default_severity_threshold")]
    pub severity_threshold: f32,

    /// Categories to watch for
    #[serde(default = "default_categories")]
    pub categories: Vec<String>,

    /// Custom rules for the rules tier
    #[serde(default)]
    pub rules: Vec<IntelligenceRule>,
}

impl Default for IntelligenceConfig {
    fn default() -> Self {
        Self {
            tier: IntelligenceTier::Off,
            model_repo: default_model_repo(),
            model_file: default_model_file(),
            max_ram_mb: default_model_max_ram_mb(),
            batch_size: default_analysis_batch(),
            max_tokens: default_max_tokens(),
            severity_threshold: default_severity_threshold(),
            categories: default_categories(),
            rules: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntelligenceRule {
    pub name: String,
    pub pattern: String,
    pub severity: String, // critical, high, medium, low, info
    pub category: String,
    pub description: String,
}

fn default_model_repo() -> String { "microsoft/bitnet-b1.58-2B-4T-gguf".into() }
fn default_model_file() -> String { "bitnet-b1.58-2B-4T-gguf-Q4_K_M.gguf".into() }
fn default_model_max_ram_mb() -> usize { 512 }
fn default_analysis_batch() -> usize { 50 }
fn default_max_tokens() -> usize { 256 }
fn default_severity_threshold() -> f32 { 0.3 }
fn default_categories() -> Vec<String> {
    vec![
        "security".into(),
        "error".into(),
        "anomaly".into(),
        "performance".into(),
    ]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceConfig {
    #[serde(rename = "file")]
    File(FileSourceConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSourceConfig {
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default)]
    pub read_from_beginning: bool,
    #[serde(default = "default_max_line")]
    pub max_line_length: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TransformConfig {
    #[serde(rename = "filter")]
    Filter { include: Option<String>, exclude: Option<String> },
    #[serde(rename = "parse")]
    Parse { format: String, pattern: Option<String> },
    #[serde(rename = "mask")]
    Mask { fields: Vec<MaskRule> },
    #[serde(rename = "add_fields")]
    AddFields { fields: HashMap<String, String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaskRule {
    pub pattern: String,
    #[serde(default = "default_mask")]
    pub replacement: String,
    #[serde(default)]
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    pub bucket: String,
    #[serde(default = "default_prefix")]
    pub key_prefix: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default = "default_compression")]
    pub compression: String,
    #[serde(default)]
    pub batch: BatchConfig,
    /// Write token for anonymous S3 puts (Guvnor-hosted buckets)
    #[serde(default)]
    pub write_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    #[serde(default = "default_batch_bytes")]
    pub max_bytes: usize,
    #[serde(default = "default_batch_events")]
    pub max_events: usize,
    #[serde(default = "default_batch_secs")]
    pub timeout_secs: u64,
}
impl Default for BatchConfig {
    fn default() -> Self { Self { max_bytes: 5*1024*1024, max_events: 10_000, timeout_secs: 10 } }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferConfig {
    #[serde(default = "default_buf_size")]
    pub memory_size: usize,
}
impl Default for BufferConfig {
    fn default() -> Self { Self { memory_size: 10_000 } }
}

fn default_data_dir() -> PathBuf { "/var/lib/courier".into() }
fn default_max_line() -> usize { 256 * 1024 }
fn default_mask() -> String { "***REDACTED***".into() }
fn default_prefix() -> String { "logs/{org_id}/{source}/{date}/{hour}/".into() }
fn default_region() -> String { "us-east-1".into() }
fn default_compression() -> String { "zstd".into() }
fn default_batch_bytes() -> usize { 5*1024*1024 }
fn default_batch_events() -> usize { 10_000 }
fn default_batch_secs() -> u64 { 10 }
fn default_buf_size() -> usize { 10_000 }

pub fn load(path: &Path) -> anyhow::Result<Config> {
    let content = std::fs::read_to_string(path)?;
    let config: Config = serde_yaml::from_str(&content)?;
    tracing::info!(sources = config.sources.len(), "Config loaded");
    Ok(config)
}

pub async fn load_remote(api_url: &str, token: &str, agent_id: &str) -> anyhow::Result<Config> {
    let url = format!(
        "{}/courier/bootstrap/{}?hostname={}&agent_id={}",
        api_url,
        token,
        gethostname::gethostname().to_string_lossy(),
        agent_id,
    );
    let resp = reqwest::get(&url).await?;
    if !resp.status().is_success() {
        anyhow::bail!("Failed to fetch remote config: {}", resp.status());
    }
    let text = resp.text().await?;
    let config: Config = serde_yaml::from_str(&text)?;
    tracing::info!("Remote config loaded from {}", api_url);
    Ok(config)
}

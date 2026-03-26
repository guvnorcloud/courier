use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

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

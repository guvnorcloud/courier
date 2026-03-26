pub mod file_source;

use std::collections::HashMap;
use tokio::sync::mpsc;
use crate::config::SourceConfig;
use crate::state::StateStore;

#[derive(Debug, Clone)]
pub struct LogEvent {
    pub timestamp: i64,
    pub message: String,
    pub source: String,
    pub host: String,
    pub file: Option<String>,
    pub fields: HashMap<String, String>,
}

impl LogEvent {
    pub fn new(message: String, source: &str) -> Self {
        Self {
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            message,
            source: source.to_string(),
            host: gethostname::gethostname().to_string_lossy().to_string(),
            file: None,
            fields: HashMap::new(),
        }
    }
}

pub async fn start_sources(
    sources: &HashMap<String, SourceConfig>,
    tx: mpsc::Sender<LogEvent>,
    state: &StateStore,
) -> anyhow::Result<Vec<tokio::task::JoinHandle<()>>> {
    let mut handles = Vec::new();
    for (name, cfg) in sources {
        match cfg {
            SourceConfig::File(file_cfg) => {
                let h = file_source::start(name.clone(), file_cfg.clone(), tx.clone(), state.clone()).await?;
                handles.push(h);
            }
        }
    }
    Ok(handles)
}

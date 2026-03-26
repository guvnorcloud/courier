use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::info;

#[derive(Clone)]
pub struct StateStore { inner: Arc<Mutex<Inner>> }
struct Inner { path: PathBuf, offsets: HashMap<String, u64> }

impl StateStore {
    pub fn open(data_dir: &Path) -> anyhow::Result<Self> {
        let path = data_dir.join("courier.state.json");
        let offsets = if path.exists() {
            serde_json::from_str(&std::fs::read_to_string(&path)?).unwrap_or_default()
        } else {
            if let Some(p) = path.parent() { std::fs::create_dir_all(p)?; }
            HashMap::new()
        };
        info!(cursors = offsets.len(), "State store opened");
        Ok(Self { inner: Arc::new(Mutex::new(Inner { path, offsets })) })
    }

    pub fn get_offset(&self, file: &Path) -> Option<u64> {
        self.inner.lock().unwrap().offsets.get(&file.to_string_lossy().to_string()).copied()
    }

    pub fn set_offset(&self, file: &Path, offset: u64) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.offsets.insert(file.to_string_lossy().to_string(), offset);
        std::fs::write(&inner.path, serde_json::to_string_pretty(&inner.offsets)?)?;
        Ok(())
    }
}

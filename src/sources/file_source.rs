use crate::config::FileSourceConfig;
use crate::sources::LogEvent;
use crate::state::StateStore;
use notify::{RecommendedWatcher, RecursiveMode, Watcher, EventKind};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
use tokio::sync::mpsc;
use tracing::{info, warn};

pub async fn start(
    name: String, config: FileSourceConfig, tx: mpsc::Sender<LogEvent>, state: StateStore,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    Ok(tokio::spawn(async move {
        if let Err(e) = run(name, config, tx, state).await {
            tracing::error!(error = %e, "File source error");
        }
    }))
}

async fn run(
    name: String, config: FileSourceConfig, tx: mpsc::Sender<LogEvent>, state: StateStore,
) -> anyhow::Result<()> {
    let mut tracked: HashMap<PathBuf, u64> = HashMap::new();
    for pattern in &config.include {
        for entry in glob::glob(pattern)? {
            if let Ok(path) = entry {
                if config.exclude.iter().any(|ex| glob::Pattern::new(ex).map(|p| p.matches_path(&path)).unwrap_or(false)) { continue; }
                let offset = state.get_offset(&path).unwrap_or(0);
                tracked.insert(path, offset);
            }
        }
    }
    info!(source = %name, files = tracked.len(), "File source started");

    for (path, offset) in &tracked {
        if let Ok(new_off) = read_from(path, *offset, &name, &config, &tx, &state).await {
            let _ = new_off;
        }
    }

    let (wtx, mut wrx) = tokio::sync::mpsc::channel(1000);
    let mut watcher = RecommendedWatcher::new(
        move |res: Result<notify::Event, notify::Error>| { if let Ok(ev) = res { let _ = wtx.blocking_send(ev); } },
        notify::Config::default(),
    )?;

    let mut watched = HashSet::new();
    for path in tracked.keys() {
        if let Some(parent) = path.parent() {
            if watched.insert(parent.to_path_buf()) { watcher.watch(parent, RecursiveMode::NonRecursive)?; }
        }
    }

    while let Some(event) = wrx.recv().await {
        if matches!(event.kind, EventKind::Modify(_) | EventKind::Create(_)) {
            for path in &event.paths {
                if !tracked.contains_key(path) {
                    let matches = config.include.iter().any(|p| glob::Pattern::new(p).map(|pat| pat.matches_path(path)).unwrap_or(false));
                    if matches { tracked.insert(path.clone(), 0); }
                }
                if let Some(off) = tracked.get(path).copied() {
                    if let Ok(new_off) = read_from(path, off, &name, &config, &tx, &state).await {
                        tracked.insert(path.clone(), new_off);
                    }
                }
            }
        }
    }
    Ok(())
}

async fn read_from(
    path: &PathBuf, offset: u64, source: &str, config: &FileSourceConfig,
    tx: &mpsc::Sender<LogEvent>, state: &StateStore,
) -> anyhow::Result<u64> {
    let file = tokio::fs::File::open(path).await?;
    let size = file.metadata().await?.len();
    let start = if offset > size { 0 } else { offset };
    let mut file = file;
    file.seek(std::io::SeekFrom::Start(start)).await?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut pos = start;
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 { break; }
        pos += n as u64;
        let trimmed = line.trim_end();
        if trimmed.is_empty() { continue; }
        let msg = if trimmed.len() > config.max_line_length { &trimmed[..config.max_line_length] } else { trimmed };
        let mut ev = LogEvent::new(msg.to_string(), source);
        ev.file = Some(path.to_string_lossy().to_string());
        if tx.send(ev).await.is_err() { break; }
    }
    state.set_offset(path, pos)?;
    Ok(pos)
}

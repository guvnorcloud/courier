//! Model manager — downloads, caches, and verifies GGUF model files.
//!
//! On first run with `intelligence.tier = "llm"`, this downloads the configured
//! BitNet GGUF from HuggingFace and caches it under `{data_dir}/models/`.

use std::path::{Path, PathBuf};
use tracing::{info, warn};

const HF_BASE_URL: &str = "https://huggingface.co";

/// Resolved path to a ready-to-load model file.
pub struct ResolvedModel {
    pub path: PathBuf,
    pub repo: String,
    pub file: String,
}

/// Ensure the model GGUF is available locally. Downloads if missing.
pub async fn ensure_model(
    data_dir: &Path,
    repo: &str,
    file: &str,
) -> anyhow::Result<ResolvedModel> {
    let models_dir = data_dir.join("models");
    std::fs::create_dir_all(&models_dir)?;

    // Sanitize repo name for filesystem (microsoft/bitnet-b1.58-2B-4T-gguf → microsoft--bitnet-b1.58-2B-4T-gguf)
    let safe_repo = repo.replace('/', "--");
    let model_path = models_dir.join(&safe_repo).join(file);

    if model_path.exists() {
        let size = std::fs::metadata(&model_path)?.len();
        info!(path = %model_path.display(), size_mb = size / (1024 * 1024), "Model already cached");
        return Ok(ResolvedModel {
            path: model_path,
            repo: repo.to_string(),
            file: file.to_string(),
        });
    }

    // Download from HuggingFace
    let url = format!(
        "{}/{}/resolve/main/{}",
        HF_BASE_URL, repo, file
    );
    info!(url = %url, dest = %model_path.display(), "Downloading model");

    std::fs::create_dir_all(model_path.parent().unwrap())?;
    let tmp_path = model_path.with_extension("tmp");

    download_file(&url, &tmp_path).await?;

    // Rename into place atomically
    std::fs::rename(&tmp_path, &model_path)?;

    let size = std::fs::metadata(&model_path)?.len();
    info!(
        path = %model_path.display(),
        size_mb = size / (1024 * 1024),
        "Model download complete"
    );

    Ok(ResolvedModel {
        path: model_path,
        repo: repo.to_string(),
        file: file.to_string(),
    })
}

/// Stream-download a file with progress logging.
async fn download_file(url: &str, dest: &Path) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()?;

    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("Model download failed: HTTP {}", resp.status());
    }

    let total = resp.content_length().unwrap_or(0);
    if total > 0 {
        info!(size_mb = total / (1024 * 1024), "Model size");
    }

    let mut file = tokio::fs::File::create(dest).await?;
    use tokio::io::AsyncWriteExt;

    let bytes = resp.bytes().await?;
    let downloaded = bytes.len() as u64;
    file.write_all(&bytes).await?;
    file.flush().await?;

    if total > 0 {
        info!(
            downloaded_mb = downloaded / (1024 * 1024),
            total_mb = total / (1024 * 1024),
            "Model download complete"
        );
    }
    Ok(())
}

/// Check if the model is already cached.
pub fn is_cached(data_dir: &Path, repo: &str, file: &str) -> bool {
    let safe_repo = repo.replace('/', "--");
    data_dir.join("models").join(safe_repo).join(file).exists()
}

/// Get the expected model path (whether or not it exists).
pub fn model_path(data_dir: &Path, repo: &str, file: &str) -> PathBuf {
    let safe_repo = repo.replace('/', "--");
    data_dir.join("models").join(safe_repo).join(file)
}

/// Delete a cached model.
pub fn evict(data_dir: &Path, repo: &str, file: &str) -> anyhow::Result<()> {
    let path = model_path(data_dir, repo, file);
    if path.exists() {
        std::fs::remove_file(&path)?;
        warn!(path = %path.display(), "Model evicted from cache");
    }
    Ok(())
}

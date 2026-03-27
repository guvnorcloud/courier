use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: String,
    pub is_dir: bool,
    pub size: u64,
    pub modified: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub children: Vec<FileEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostDiscovery {
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub processes: Vec<DetectedProcess>,
    pub file_tree: Vec<FileEntry>,
    pub recommendations: Vec<LogRecommendation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectedProcess {
    pub name: String,
    pub pid: u32,
    pub cmdline: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecommendation {
    pub service: String,
    pub description: String,
    pub paths: Vec<String>,
    pub parse_format: String,
    pub confidence: f32,
}

// Known service -> log path mappings: (process_name, description, log_paths, parse_format)
const SERVICE_LOG_MAP: &[(&str, &str, &[&str], &str)] = &[
    ("nginx", "Nginx web server", &["/var/log/nginx/*.log", "/var/log/nginx/access.log", "/var/log/nginx/error.log"], "regex"),
    ("apache2", "Apache web server", &["/var/log/apache2/*.log", "/var/log/httpd/*.log"], "regex"),
    ("httpd", "Apache web server", &["/var/log/httpd/*.log", "/var/log/apache2/*.log"], "regex"),
    ("postgres", "PostgreSQL database", &["/var/log/postgresql/*.log", "/var/lib/pgsql/data/log/*.log"], "logfmt"),
    ("mysqld", "MySQL database", &["/var/log/mysql/*.log", "/var/log/mysqld.log"], "regex"),
    ("mongod", "MongoDB", &["/var/log/mongodb/*.log"], "json"),
    ("redis-server", "Redis", &["/var/log/redis/*.log"], "logfmt"),
    ("docker", "Docker daemon", &["/var/log/docker*.log", "/var/lib/docker/containers/**/*.log"], "json"),
    ("kubelet", "Kubernetes kubelet", &["/var/log/kubelet.log", "/var/log/pods/**/*.log"], "json"),
    ("sshd", "SSH daemon", &["/var/log/auth.log", "/var/log/secure"], "regex"),
    ("node", "Node.js application", &["/var/log/app/*.log", "/var/log/node/*.log"], "json"),
    ("java", "Java application", &["/var/log/app/*.log", "/var/log/java/*.log"], "regex"),
    ("python", "Python application", &["/var/log/app/*.log"], "json"),
    ("haproxy", "HAProxy load balancer", &["/var/log/haproxy*.log"], "regex"),
    ("envoy", "Envoy proxy", &["/var/log/envoy/*.log"], "json"),
    ("caddy", "Caddy web server", &["/var/log/caddy/*.log"], "json"),
    ("systemd-journald", "System journal", &["/var/log/syslog", "/var/log/messages"], "regex"),
];

const SCAN_ROOTS: &[&str] = &[
    "/var/log",
    "/var/log/journal",
    "/opt",
    "/home",
    "/tmp",
];

const MAX_DEPTH: usize = 3;
const MAX_ENTRIES: usize = 500;

pub fn discover() -> HostDiscovery {
    let hostname = gethostname::gethostname().to_string_lossy().to_string();
    let os = std::env::consts::OS.to_string();
    let arch = std::env::consts::ARCH.to_string();

    let processes = detect_processes();
    let file_tree = scan_file_tree();
    let recommendations = generate_recommendations(&processes, &file_tree);

    HostDiscovery {
        hostname,
        os,
        arch,
        processes,
        file_tree,
        recommendations,
    }
}

fn detect_processes() -> Vec<DetectedProcess> {
    let mut procs = Vec::new();

    // Read /proc on Linux
    if let Ok(entries) = std::fs::read_dir("/proc") {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if let Ok(pid) = name.parse::<u32>() {
                let comm_path = format!("/proc/{}/comm", pid);
                let cmdline_path = format!("/proc/{}/cmdline", pid);
                if let Ok(comm) = std::fs::read_to_string(&comm_path) {
                    let comm = comm.trim().to_string();
                    let cmdline = std::fs::read_to_string(&cmdline_path)
                        .unwrap_or_default()
                        .replace('\0', " ")
                        .trim()
                        .to_string();
                    // Only include interesting processes (not kernel threads)
                    if !comm.is_empty() && !cmdline.is_empty() {
                        procs.push(DetectedProcess { name: comm, pid, cmdline });
                    }
                }
            }
        }
    }

    // Deduplicate by name (keep first of each)
    let mut seen = HashSet::new();
    procs.retain(|p| seen.insert(p.name.clone()));

    procs
}

/// Recursively scan directories to build a file tree for the UI file browser.
/// Max depth of 3, max 500 total entries. Skips symlinks and unreadable dirs.
fn scan_file_tree() -> Vec<FileEntry> {
    let mut total_count: usize = 0;
    let mut tree = Vec::new();

    for root in SCAN_ROOTS {
        if total_count >= MAX_ENTRIES {
            break;
        }
        let path = std::path::Path::new(root);
        if !path.exists() || !path.is_dir() {
            continue;
        }
        // Skip symlinks at root level to avoid loops
        if path.symlink_metadata().map_or(true, |m| m.file_type().is_symlink()) {
            continue;
        }
        if let Some(entry) = scan_dir_recursive(path, 0, &mut total_count) {
            tree.push(entry);
        }
    }

    tree
}

fn scan_dir_recursive(
    dir: &std::path::Path,
    depth: usize,
    total_count: &mut usize,
) -> Option<FileEntry> {
    if *total_count >= MAX_ENTRIES {
        return None;
    }
    *total_count += 1;

    let modified = dir
        .metadata()
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| {
            t.duration_since(std::time::UNIX_EPOCH)
                .ok()
                .map(|d| {
                    chrono::DateTime::from_timestamp(d.as_secs() as i64, d.subsec_nanos())
                        .map(|dt| dt.to_rfc3339())
                        .unwrap_or_default()
                })
        });

    let mut children = Vec::new();

    if depth < MAX_DEPTH {
        if let Ok(entries) = std::fs::read_dir(dir) {
            let mut entries_vec: Vec<_> = entries.flatten().collect();
            // Sort for deterministic output
            entries_vec.sort_by_key(|e| e.file_name());

            for entry in entries_vec {
                if *total_count >= MAX_ENTRIES {
                    break;
                }

                let path = entry.path();

                // Skip symlinks to avoid loops
                if let Ok(meta) = entry.metadata() {
                    if meta.file_type().is_symlink() {
                        continue;
                    }
                } else {
                    continue;
                }

                // Also check symlink_metadata for the entry itself
                if let Ok(sym_meta) = std::fs::symlink_metadata(&path) {
                    if sym_meta.file_type().is_symlink() {
                        continue;
                    }
                }

                if path.is_dir() {
                    if let Some(child) = scan_dir_recursive(&path, depth + 1, total_count) {
                        children.push(child);
                    }
                } else if path.is_file() {
                    if *total_count >= MAX_ENTRIES {
                        break;
                    }
                    *total_count += 1;

                    let file_meta = std::fs::metadata(&path).ok();
                    let size = file_meta.as_ref().map_or(0, |m| m.len());
                    let file_modified = file_meta
                        .and_then(|m| m.modified().ok())
                        .and_then(|t| {
                            t.duration_since(std::time::UNIX_EPOCH)
                                .ok()
                                .map(|d| {
                                    chrono::DateTime::from_timestamp(d.as_secs() as i64, d.subsec_nanos())
                                        .map(|dt| dt.to_rfc3339())
                                        .unwrap_or_default()
                                })
                        });

                    children.push(FileEntry {
                        path: path.to_string_lossy().to_string(),
                        is_dir: false,
                        size,
                        modified: file_modified,
                        children: Vec::new(),
                    });
                }
            }
        }
    }

    Some(FileEntry {
        path: dir.to_string_lossy().to_string(),
        is_dir: true,
        size: 0,
        modified,
        children,
    })
}

fn generate_recommendations(
    processes: &[DetectedProcess],
    file_tree: &[FileEntry],
) -> Vec<LogRecommendation> {
    let mut recs = Vec::new();

    // Build a set of all directories found in the file tree
    let mut available_dirs: HashSet<String> = HashSet::new();
    fn collect_dirs(entry: &FileEntry, dirs: &mut HashSet<String>) {
        if entry.is_dir {
            dirs.insert(entry.path.clone());
        }
        for child in &entry.children {
            collect_dirs(child, dirs);
        }
    }
    for root in file_tree {
        collect_dirs(root, &mut available_dirs);
    }

    for (proc_name, desc, paths, format) in SERVICE_LOG_MAP {
        // Check if this process is running
        let is_running = processes.iter().any(|p| p.name.contains(proc_name));
        if !is_running {
            continue;
        }

        // Check which log paths actually exist
        let existing_paths: Vec<String> = paths
            .iter()
            .filter(|p| {
                // Check if the parent directory exists
                let parent = std::path::Path::new(p)
                    .parent()
                    .map(|pa| pa.to_string_lossy().to_string());
                parent.map_or(false, |d| {
                    available_dirs.contains(&d) || std::path::Path::new(&d).exists()
                })
            })
            .map(|p| p.to_string())
            .collect();

        if !existing_paths.is_empty() {
            recs.push(LogRecommendation {
                service: proc_name.to_string(),
                description: desc.to_string(),
                paths: existing_paths,
                parse_format: format.to_string(),
                confidence: 0.9,
            });
        }
    }

    // Always recommend syslog if it exists
    if std::path::Path::new("/var/log/syslog").exists()
        || std::path::Path::new("/var/log/messages").exists()
    {
        let paths = if std::path::Path::new("/var/log/syslog").exists() {
            vec!["/var/log/syslog".to_string()]
        } else {
            vec!["/var/log/messages".to_string()]
        };
        recs.push(LogRecommendation {
            service: "syslog".to_string(),
            description: "System logs".to_string(),
            paths,
            parse_format: "regex".to_string(),
            confidence: 0.8,
        });
    }

    recs
}

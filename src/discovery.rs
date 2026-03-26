use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostDiscovery {
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub processes: Vec<DetectedProcess>,
    pub log_dirs: Vec<DiscoveredLogDir>,
    pub recommendations: Vec<LogRecommendation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectedProcess {
    pub name: String,
    pub pid: u32,
    pub cmdline: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredLogDir {
    pub path: String,
    pub file_count: usize,
    pub total_bytes: u64,
    pub newest_file: Option<String>,
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

pub fn discover() -> HostDiscovery {
    let hostname = gethostname::gethostname().to_string_lossy().to_string();
    let os = std::env::consts::OS.to_string();
    let arch = std::env::consts::ARCH.to_string();

    let processes = detect_processes();
    let log_dirs = scan_log_directories();
    let recommendations = generate_recommendations(&processes, &log_dirs);

    HostDiscovery {
        hostname,
        os,
        arch,
        processes,
        log_dirs,
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

fn scan_log_directories() -> Vec<DiscoveredLogDir> {
    let dirs_to_scan = [
        "/var/log",
        "/var/log/nginx",
        "/var/log/apache2",
        "/var/log/httpd",
        "/var/log/postgresql",
        "/var/log/mysql",
        "/var/log/mongodb",
        "/var/log/redis",
        "/var/log/app",
        "/tmp",
    ];

    let mut results = Vec::new();
    for dir in &dirs_to_scan {
        let path = std::path::Path::new(dir);
        if !path.exists() || !path.is_dir() {
            continue;
        }

        let mut file_count = 0usize;
        let mut total_bytes = 0u64;
        let mut newest: Option<(String, std::time::SystemTime)> = None;

        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    if meta.is_file() {
                        file_count += 1;
                        total_bytes += meta.len();
                        if let Ok(modified) = meta.modified() {
                            if newest.as_ref().map_or(true, |(_, t)| modified > *t) {
                                newest = Some((
                                    entry.path().to_string_lossy().to_string(),
                                    modified,
                                ));
                            }
                        }
                    }
                }
            }
        }

        if file_count > 0 {
            results.push(DiscoveredLogDir {
                path: dir.to_string(),
                file_count,
                total_bytes,
                newest_file: newest.map(|(p, _)| p),
            });
        }
    }

    results
}

fn generate_recommendations(
    processes: &[DetectedProcess],
    log_dirs: &[DiscoveredLogDir],
) -> Vec<LogRecommendation> {
    let mut recs = Vec::new();
    let available_dirs: HashSet<&str> = log_dirs.iter().map(|d| d.path.as_str()).collect();

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
                    available_dirs.contains(d.as_str()) || std::path::Path::new(&d).exists()
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

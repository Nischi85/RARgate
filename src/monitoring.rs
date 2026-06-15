//! Mount-health monitoring and Unraid notifications.
//!
//! Two background tasks live here:
//! - `start_monitor` watches the backend and FUSE mountpoints; if either disappears
//!   or becomes unreadable, it raises an Unraid notification via the `notify` helper.
//! - `start_rar2fs_resource_monitor` follows the rar2fs PID and logs its RSS / FD
//!   count so post-mortem crashes have data to inspect.
//!
//! Notifications no-op gracefully on non-Unraid systems where the helper isn't present.

use anyhow::Result;
use std::path::{Path, PathBuf};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::{sleep, Duration};
use tracing::{info, error, warn};

use crate::config::NotificationConfig;

/// Path to the Unraid notify helper. Absent on non-Unraid systems.
const UNRAID_NOTIFY_BIN: &str = "/usr/local/emhttp/webGui/scripts/notify";

/// True if we've confirmed `UNRAID_NOTIFY_BIN` is usable. Initialised
/// once on first attempt; if the binary is missing or unexecutable we
/// log a single info line and skip future notify calls instead of
/// repeating the error per failure.
static UNRAID_NOTIFY_AVAILABLE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

fn unraid_notify_available() -> bool {
    *UNRAID_NOTIFY_AVAILABLE.get_or_init(|| {
        let p = Path::new(UNRAID_NOTIFY_BIN);
        // We only check existence here; an unexecutable file will surface
        // as an exec error the first time we try, and we'll suppress the
        // SECOND such error via UNRAID_NOTIFY_DEGRADED below.
        let available = p.exists();
        if available {
            info!("Unraid notify available at {}", UNRAID_NOTIFY_BIN);
        } else {
            info!("Unraid notify not found at {} — notifications will be logged only (this is normal on non-Unraid systems)", UNRAID_NOTIFY_BIN);
        }
        available
    })
}

/// Once an Unraid notify call fails at runtime (e.g. binary present but
/// not executable, or the helper script itself errors), we flip this
/// flag so subsequent failures don't spam the log.
static UNRAID_NOTIFY_DEGRADED: AtomicBool = AtomicBool::new(false);

/// Default path to rar2fs resource monitoring log (overridable via logging.rar2fs_resources_log_file).
pub const RAR2FS_RESOURCES_LOG: &str = "/mnt/cache/rargate/rar2fs-resources.log";

pub struct MountMonitor {
    backend_path: PathBuf,
    rargate_path: PathBuf,
    config: NotificationConfig,
    backend_failure_count: u32,
    rargate_failure_count: u32,
}

impl MountMonitor {
    pub fn new(backend_path: PathBuf, rargate_path: PathBuf, config: NotificationConfig) -> Self {
        Self {
            backend_path,
            rargate_path,
            config,
            backend_failure_count: 0,
            rargate_failure_count: 0,
        }
    }
    
    pub async fn start_monitoring(&mut self) -> Result<()> {
        let check_interval = Duration::from_secs(self.config.check_interval.unwrap_or(300));
        let max_errors = self.config.max_consecutive_errors.unwrap_or(3);

        info!("Mount monitoring started (checking every {}s, max {} consecutive error notifications)",
              check_interval.as_secs(), max_errors);

        // Give mounts time to stabilize on startup
        sleep(Duration::from_secs(10)).await;

        loop {
            // Check backend mount
            let backend_mounted = is_mounted(&self.backend_path).await?;

            // Check rargate mount
            let rargate_mounted = is_mounted(&self.rargate_path).await?;

            // Handle backend mount status
            if !backend_mounted {
                self.backend_failure_count += 1;

                let msg = format!(
                    "🚨 ALERT: rar2fs backend mount lost!\n\
                     Path: {}\n\
                     Time: {}\n\
                     Consecutive failures: {}\n\
                     Action: Check rar2fs process and restart if needed",
                    self.backend_path.display(),
                    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
                    self.backend_failure_count
                );

                error!("{}", msg);

                // Send notification only if under threshold
                if self.backend_failure_count <= max_errors {
                    send_unraid_notification(
                        "rar2fs Backend Mount Lost",
                        &format!("Path: {}\nFailure #{}\nAction: Check rar2fs process and restart if needed",
                            self.backend_path.display(), self.backend_failure_count)
                    ).await;
                } else if self.backend_failure_count == max_errors + 1 {
                    info!("Backend mount notification threshold reached ({}), suppressing further notifications until recovery",
                          max_errors);
                }
            } else {
                // Mount recovered
                if self.backend_failure_count > 0 {
                    info!("✅ rar2fs backend mount recovered after {} failures", self.backend_failure_count);
                    send_unraid_notification(
                        "rar2fs Backend Recovered",
                        &format!("Path: {}\nRecovered after {} consecutive failures",
                            self.backend_path.display(), self.backend_failure_count)
                    ).await;
                    self.backend_failure_count = 0;
                }
            }

            // Handle rargate mount status
            if !rargate_mounted {
                self.rargate_failure_count += 1;

                let msg = format!(
                    "🚨 ALERT: RARGate FUSE mount lost!\n\
                     Path: {}\n\
                     Time: {}\n\
                     Consecutive failures: {}\n\
                     Action: Check RARGate process and restart if needed",
                    self.rargate_path.display(),
                    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
                    self.rargate_failure_count
                );

                error!("{}", msg);

                // Send notification only if under threshold
                if self.rargate_failure_count <= max_errors {
                    send_unraid_notification(
                        "RARGate FUSE Mount Lost",
                        &format!("Path: {}\nFailure #{}\nAction: Check RARGate process and restart if needed",
                            self.rargate_path.display(), self.rargate_failure_count)
                    ).await;
                } else if self.rargate_failure_count == max_errors + 1 {
                    info!("RARGate mount notification threshold reached ({}), suppressing further notifications until recovery",
                          max_errors);
                }
            } else {
                // Mount recovered
                if self.rargate_failure_count > 0 {
                    info!("✅ RARGate FUSE mount recovered after {} failures", self.rargate_failure_count);
                    send_unraid_notification(
                        "RARGate Mount Recovered",
                        &format!("Path: {}\nRecovered after {} consecutive failures",
                            self.rargate_path.display(), self.rargate_failure_count)
                    ).await;
                    self.rargate_failure_count = 0;
                }
            }

            sleep(check_interval).await;
        }
    }
}

pub async fn start_monitor(
    backend_path: PathBuf,
    rargate_path: PathBuf,
    config: NotificationConfig,
) -> Result<tokio::task::JoinHandle<()>> {
    let mut monitor = MountMonitor::new(backend_path, rargate_path, config);

    let handle = tokio::spawn(async move {
        if let Err(e) = monitor.start_monitoring().await {
            error!("Mount monitoring failed: {}", e);
        }
    });

    Ok(handle)
}

async fn is_mounted(path: &PathBuf) -> Result<bool> {
    let output = tokio::process::Command::new("mountpoint")
        .arg("-q")
        .arg(path)
        .output()
        .await?;

    Ok(output.status.success())
}

/// Send Unraid notification using the built-in notify script. Silently
/// skips on non-Unraid systems (and on Unraid systems where the helper
/// has previously failed) — the alert details are already in the log.
async fn send_unraid_notification(subject: &str, description: &str) {
    if !unraid_notify_available() {
        return;
    }
    if UNRAID_NOTIFY_DEGRADED.load(Ordering::Relaxed) {
        return;
    }

    let result = tokio::process::Command::new(UNRAID_NOTIFY_BIN)
        .arg("-e")
        .arg("RARGate")
        .arg("-s")
        .arg(subject)
        .arg("-d")
        .arg(description)
        .arg("-i")
        .arg("alert")
        .output()
        .await;

    match result {
        Ok(output) => {
            if output.status.success() {
                info!("Unraid notification sent successfully");
            } else {
                error!("Failed to send Unraid notification: {:?}",
                    String::from_utf8_lossy(&output.stderr));
                UNRAID_NOTIFY_DEGRADED.store(true, Ordering::Relaxed);
                warn!("Disabling further Unraid notify attempts this run");
            }
        }
        Err(e) => {
            error!("Failed to execute Unraid notify command: {}", e);
            UNRAID_NOTIFY_DEGRADED.store(true, Ordering::Relaxed);
            warn!("Disabling further Unraid notify attempts this run");
        }
    }
}

/// Start rar2fs resource monitoring task.
/// Logs FD count, memory usage, and I/O stats to `log_path` every `interval_secs` seconds.
pub fn start_rar2fs_resource_monitor(
    rar2fs_pid: Option<u32>,
    log_path: PathBuf,
    interval_secs: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval = Duration::from_secs(interval_secs);

        // Initial delay to let things stabilize
        sleep(Duration::from_secs(60)).await;

        info!("rar2fs resource monitoring started (logging every {}s to {})", interval_secs, log_path.display());

        loop {
            let pid = match rar2fs_pid {
                Some(p) => p,
                None => {
                    // Try to find rar2fs PID dynamically
                    match find_rar2fs_pid_dynamic().await {
                        Some(p) => p,
                        None => {
                            warn!("rar2fs resource monitor: cannot find rar2fs process");
                            sleep(interval).await;
                            continue;
                        }
                    }
                }
            };

            // Collect resource stats
            if let Some(stats) = collect_rar2fs_stats(pid).await {
                log_rar2fs_stats(&stats, &log_path);
            }

            sleep(interval).await;
        }
    })
}

/// Resource statistics for rar2fs process
#[derive(Debug)]
struct Rar2fsStats {
    timestamp: String,
    pid: u32,
    fd_count: usize,
    vm_rss_kb: u64,
    vm_size_kb: u64,
    threads: u32,
    read_bytes: u64,
    write_bytes: u64,
    state: String,
}

async fn find_rar2fs_pid_dynamic() -> Option<u32> {
    let output = tokio::process::Command::new("pgrep")
        .arg("-x")
        .arg("rar2fs")
        .output()
        .await
        .ok()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        stdout.trim().lines().next()?.parse().ok()
    } else {
        None
    }
}

async fn collect_rar2fs_stats(pid: u32) -> Option<Rar2fsStats> {
    let proc_path = format!("/proc/{}", pid);

    // Check if process still exists
    if !std::path::Path::new(&proc_path).exists() {
        warn!("rar2fs process {} no longer exists", pid);
        return None;
    }

    // Count file descriptors
    let fd_path = format!("{}/fd", proc_path);
    let fd_count = std::fs::read_dir(&fd_path)
        .map(|entries| entries.count())
        .unwrap_or(0);

    // Read status file
    let status_path = format!("{}/status", proc_path);
    let status = std::fs::read_to_string(&status_path).unwrap_or_default();

    let mut vm_rss_kb = 0u64;
    let mut vm_size_kb = 0u64;
    let mut threads = 0u32;
    let mut state = String::from("?");

    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            vm_rss_kb = parse_kb_value(line);
        } else if line.starts_with("VmSize:") {
            vm_size_kb = parse_kb_value(line);
        } else if line.starts_with("Threads:") {
            threads = line.split_whitespace().nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        } else if line.starts_with("State:") {
            state = line.split_whitespace().nth(1)
                .map(|s| s.to_string())
                .unwrap_or_else(|| "?".to_string());
        }
    }

    // Read I/O stats
    let io_path = format!("{}/io", proc_path);
    let io_content = std::fs::read_to_string(&io_path).unwrap_or_default();

    let mut read_bytes = 0u64;
    let mut write_bytes = 0u64;

    for line in io_content.lines() {
        if line.starts_with("read_bytes:") {
            read_bytes = line.split_whitespace().nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        } else if line.starts_with("write_bytes:") {
            write_bytes = line.split_whitespace().nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        }
    }

    Some(Rar2fsStats {
        timestamp: chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        pid,
        fd_count,
        vm_rss_kb,
        vm_size_kb,
        threads,
        read_bytes,
        write_bytes,
        state,
    })
}

fn parse_kb_value(line: &str) -> u64 {
    // Format: "VmRSS:     12345 kB"
    line.split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

fn log_rar2fs_stats(stats: &Rar2fsStats, log_path: &std::path::Path) {
    let log_line = format!(
        "[{}] PID={} State={} FDs={} VmRSS={}KB VmSize={}KB Threads={} ReadBytes={} WriteBytes={}\n",
        stats.timestamp,
        stats.pid,
        stats.state,
        stats.fd_count,
        stats.vm_rss_kb,
        stats.vm_size_kb,
        stats.threads,
        stats.read_bytes,
        stats.write_bytes,
    );

    // Log to file
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    {
        Ok(mut file) => {
            if let Err(e) = file.write_all(log_line.as_bytes()) {
                error!("Failed to write to rar2fs resources log: {}", e);
            }
        }
        Err(e) => {
            error!("Failed to open rar2fs resources log: {}", e);
        }
    }

    // Also log to tracing at debug level
    info!("rar2fs resources: {}", log_line.trim());
}

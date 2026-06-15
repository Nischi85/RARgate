//! `Rar2fsBackend` — manages the rar2fs child process lifecycle.
//!
//! Spawns rar2fs against the configured source path, waits for the mount to appear,
//! and exposes the resulting backend mountpoint (which `RarGateFs` reads through).
//! On drop, the backend unmounts cleanly so the next start finds a fresh tree —
//! stale mounts are the most common failure mode and are deliberately avoided here.
//!
//! `pid()` is published so `monitoring::start_rar2fs_resource_monitor` can watch it.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use anyhow::Result;
use tracing::{info, error, warn};

use crate::config::Rar2fsConfig;
use crate::metrics::MetricsCollector;

/// Counting semaphore that caps concurrent rar2fs backend `read_dir` calls.
///
/// rar2fs has known hang conditions under bursty concurrent indexing of multi-volume
/// archives (upstream issues #11, #116). This limiter caps how many threads can be
/// walking an archive listing at once, so a sudden burst (e.g., Emby scanning a season
/// directory that just gained 8 new episodes) can't saturate rar2fs's worker pool.
///
/// Single-file metadata/open ops are NOT gated — they're high-frequency and individually
/// cheap. Only directory walks, which fan out across an archive's index, go through here.
pub struct RarFsLimiter {
    available: Mutex<usize>,
    cv: Condvar,
    metrics: Mutex<Option<Arc<MetricsCollector>>>,
}

impl RarFsLimiter {
    pub fn new(permits: usize) -> Arc<Self> {
        Arc::new(Self {
            available: Mutex::new(permits.max(1)),
            cv: Condvar::new(),
            metrics: Mutex::new(None),
        })
    }

    /// Attach a metrics collector. Call this once at startup; later `acquire()` calls
    /// will record wait times and a blocked-acquire counter so operators can tell
    /// whether `max_concurrent_reads` is throttling real load.
    pub fn set_metrics(self: &Arc<Self>, metrics: Arc<MetricsCollector>) {
        *self.metrics.lock().unwrap() = Some(metrics);
    }

    /// Block until a permit is available, then return a guard that releases on drop.
    pub fn acquire(self: &Arc<Self>) -> RarFsPermit {
        let mut available = self.available.lock().unwrap();
        let was_blocked = *available == 0;
        let wait_start = if was_blocked { Some(Instant::now()) } else { None };
        while *available == 0 {
            available = self.cv.wait(available).unwrap();
        }
        *available -= 1;
        drop(available);

        if let Some(metrics) = self.metrics.lock().unwrap().as_ref() {
            metrics.rar2fs_limiter_acquires_total.fetch_add(1, Ordering::Relaxed);
            if let Some(start) = wait_start {
                let waited_ms = start.elapsed().as_millis() as u64;
                metrics.rar2fs_limiter_blocked_acquires.fetch_add(1, Ordering::Relaxed);
                metrics.rar2fs_limiter_total_wait_ms.fetch_add(waited_ms, Ordering::Relaxed);
            }
        }

        RarFsPermit { limiter: Arc::clone(self) }
    }
}

pub struct RarFsPermit {
    limiter: Arc<RarFsLimiter>,
}

impl Drop for RarFsPermit {
    fn drop(&mut self) {
        let mut available = self.limiter.available.lock().unwrap();
        *available += 1;
        self.limiter.cv.notify_one();
    }
}

/// Default path to rar2fs stderr log file (overridable via logging.rar2fs_log_file).
pub const RAR2FS_LOG_PATH: &str = "/mnt/cache/rargate/rar2fs.log";

pub struct Rar2fsBackend {
    backend_path: PathBuf,
    process: Option<Child>,
    /// PID of rar2fs process (if started by us)
    rar2fs_pid: Option<u32>,
}

impl Rar2fsBackend {
    /// Start rar2fs backend and return backend path.
    /// `log_path` is where rar2fs stderr is appended; `None` disables stderr capture entirely
    /// (the kernel discards it — no file opened, no I/O).
    pub fn start(source: &Path, config: &Rar2fsConfig, log_path: Option<&Path>) -> Result<Self> {
        // Determine backend mount point
        let backend_path = if let Some(ref path) = config.backend_mount {
            path.clone()
        } else {
            PathBuf::from(format!("/tmp/rargate-rar2fs-backend-{}", std::process::id()))
        };

        // Create backend mount point
        std::fs::create_dir_all(&backend_path)?;

        // Check if already mounted
        if is_mounted(&backend_path) {
            info!("Backend already mounted at: {:?}", backend_path);
            // Try to find existing rar2fs PID
            let existing_pid = find_rar2fs_pid(&backend_path);
            if let Some(pid) = existing_pid {
                info!("Found existing rar2fs process with PID: {}", pid);
            }
            return Ok(Self {
                backend_path,
                process: None,
                rar2fs_pid: existing_pid,
            });
        }

        // Build rar2fs command - ALL options from config only
        let binary_path = config.binary_path.as_deref().unwrap_or("/usr/local/bin/rar2fs");
        let mut cmd = Command::new(binary_path);

        // Add options from config (no hardcoded defaults)
        if let Some(ref extra_opts) = config.extra_options {
            if !extra_opts.is_empty() {
                for opt in extra_opts.split_whitespace() {
                    cmd.arg(opt);
                }
            }
        }

        // Open log file for rar2fs stderr (append mode to preserve across restarts).
        // When disabled (log_path = None) we never open a file — stderr goes to /dev/null
        // at the kernel, so there is no per-write I/O at all.
        let stderr_file = match log_path {
            Some(path) => {
                // Ensure the parent dir exists (cache path may not be created yet on first run).
                if let Some(parent) = path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                match File::options().create(true).append(true).open(path) {
                    Ok(f) => {
                        info!("rar2fs stderr will be logged to: {}", path.display());
                        Stdio::from(f)
                    }
                    Err(e) => {
                        warn!("Failed to open rar2fs log file {}: {}. Stderr will be discarded.", path.display(), e);
                        Stdio::null()
                    }
                }
            }
            None => {
                info!("rar2fs stderr logging disabled (logging.rar2fs_diagnostics_enabled=false)");
                Stdio::null()
            }
        };

        cmd.arg(source)
           .arg(&backend_path)
           .stdin(Stdio::null())
           .stdout(Stdio::null())
           .stderr(stderr_file);

        info!("Starting rar2fs: {:?}", cmd);

        // Start rar2fs process
        let process = cmd.spawn()?;
        let pid = process.id();

        // Wait for mount to be ready (rar2fs typically mounts in ~200ms)
        let mut attempts = 0;
        while !is_mounted(&backend_path) && attempts < 50 {
            thread::sleep(Duration::from_millis(100));
            attempts += 1;
        }

        if !is_mounted(&backend_path) {
            error!("rar2fs failed to mount after 5 seconds - check logs and ensure backend directory is clean");
            return Err(anyhow::anyhow!("rar2fs mount timeout - backend directory may not be empty or rar2fs failed"));
        }

        // rar2fs daemonizes, so the original process becomes zombie while child does work.
        // Find the actual worker PID by searching /proc for the running rar2fs process.
        let actual_pid = find_rar2fs_pid(&backend_path).unwrap_or(pid);
        if actual_pid != pid {
            info!("rar2fs daemonized: original PID {} → worker PID {}", pid, actual_pid);
        }

        info!("✅ rar2fs backend mounted at: {:?} (PID: {})", backend_path, actual_pid);

        Ok(Self {
            backend_path,
            process: Some(process),
            rar2fs_pid: Some(actual_pid),
        })
    }

    /// Get backend mount path
    pub fn path(&self) -> &Path {
        &self.backend_path
    }

    /// Get rar2fs process ID (if known)
    pub fn pid(&self) -> Option<u32> {
        self.rar2fs_pid
    }

    /// Stop rar2fs backend
    pub fn stop(&mut self) {
        if let Some(ref mut process) = self.process {
            info!("Stopping rar2fs backend...");
            let _ = process.kill();
            let _ = process.wait();
        }

        // Unmount if still mounted
        if is_mounted(&self.backend_path) {
            let _ = Command::new("fusermount3")
                .arg("-u")
                .arg(&self.backend_path)
                .status();
        }
    }
}

impl Drop for Rar2fsBackend {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Check if a path is a mount point
fn is_mounted(path: &Path) -> bool {
    // Try to read /proc/mounts
    if let Ok(content) = std::fs::read_to_string("/proc/mounts") {
        let path_str = path.to_string_lossy();
        for line in content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 && parts[1] == path_str.as_ref() {
                return true;
            }
        }
    }
    false
}

/// Find rar2fs process ID by looking for the process that has the mount point
fn find_rar2fs_pid(backend_path: &Path) -> Option<u32> {
    // Search through /proc for rar2fs processes
    let proc_dir = match std::fs::read_dir("/proc") {
        Ok(d) => d,
        Err(_) => return None,
    };

    let backend_str = backend_path.to_string_lossy();

    for entry in proc_dir.flatten() {
        let pid_str = entry.file_name();
        let pid_str = pid_str.to_string_lossy();

        // Skip non-numeric entries
        if !pid_str.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }

        // Skip zombie processes (State: Z)
        let status_path = format!("/proc/{}/status", pid_str);
        if let Ok(status) = std::fs::read_to_string(&status_path) {
            if status.lines().any(|line| line.starts_with("State:") && line.contains('Z')) {
                continue;
            }
        }

        // Check if this process is the rar2fs binary (not fusermount with rar2fs in options)
        let cmdline_path = format!("/proc/{}/cmdline", pid_str);
        if let Ok(cmdline) = std::fs::read_to_string(&cmdline_path) {
            // cmdline uses null bytes as separators
            let cmdline = cmdline.replace('\0', " ");
            // Match actual rar2fs binary path (not fusermount with fsname=rar2fs)
            if (cmdline.starts_with("/usr/local/bin/rar2fs") || cmdline.starts_with("rar2fs "))
               && cmdline.contains(&*backend_str) {
                if let Ok(pid) = pid_str.parse::<u32>() {
                    return Some(pid);
                }
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering as AtomicOrdering;

    #[test]
    fn limiter_grants_permits_up_to_capacity() {
        let limiter = RarFsLimiter::new(3);
        let _p1 = limiter.acquire();
        let _p2 = limiter.acquire();
        let _p3 = limiter.acquire();
        // All three permits are held simultaneously without blocking.
    }

    #[test]
    fn limiter_releases_permit_on_drop() {
        let limiter = RarFsLimiter::new(1);
        {
            let _p = limiter.acquire();
        }
        // Permit returned to the pool — second acquire should not block.
        let _p2 = limiter.acquire();
    }

    #[test]
    fn limiter_blocks_when_exhausted_until_release() {
        let limiter = RarFsLimiter::new(1);
        let held = limiter.acquire();

        let stage = Arc::new(AtomicUsize::new(0));
        let stage_t = Arc::clone(&stage);
        let limiter_t = Arc::clone(&limiter);

        let handle = thread::spawn(move || {
            stage_t.store(1, AtomicOrdering::SeqCst); // about to acquire
            let _p = limiter_t.acquire();
            stage_t.store(2, AtomicOrdering::SeqCst); // got the permit
        });

        // Give the thread time to reach the blocking acquire.
        thread::sleep(Duration::from_millis(50));
        assert_eq!(stage.load(AtomicOrdering::SeqCst), 1, "thread should be blocked on acquire");

        drop(held);
        handle.join().unwrap();
        assert_eq!(stage.load(AtomicOrdering::SeqCst), 2, "thread should have acquired after release");
    }

    #[test]
    fn limiter_records_metrics_only_when_blocked() {
        let limiter = RarFsLimiter::new(1);
        let metrics = MetricsCollector::new();
        limiter.set_metrics(Arc::clone(&metrics));

        // First acquire: no contention.
        let held = limiter.acquire();
        assert_eq!(metrics.rar2fs_limiter_acquires_total.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.rar2fs_limiter_blocked_acquires.load(Ordering::Relaxed), 0);

        // Spawn a thread that will block, then release from this thread.
        let limiter_t = Arc::clone(&limiter);
        let handle = thread::spawn(move || {
            let _p = limiter_t.acquire();
        });
        thread::sleep(Duration::from_millis(30));
        drop(held);
        handle.join().unwrap();

        assert_eq!(metrics.rar2fs_limiter_acquires_total.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.rar2fs_limiter_blocked_acquires.load(Ordering::Relaxed), 1);
        assert!(metrics.rar2fs_limiter_total_wait_ms.load(Ordering::Relaxed) > 0,
                "blocked acquire should record a non-zero wait");
    }

    #[test]
    fn limiter_floor_of_zero_permits_is_one() {
        // Defensive: a zero-permit limiter would deadlock; constructor clamps to 1.
        let limiter = RarFsLimiter::new(0);
        let _p = limiter.acquire();
    }
}

//! Cross-cutting helpers: signal handling, logging setup, and shell-config emission.
//!
//! - `setup_signal_handlers` wires SIGTERM (graceful shutdown via a oneshot) and
//!   SIGHUP (config reload via an mpsc receiver) so the rest of `main` can `await`
//!   them in a `tokio::select!`.
//! - `setup_logging` configures tracing-subscriber with size-based log rotation.
//! - `write_monitor_conf` / `write_paths_conf` emit `.env`-style files that the
//!   deploy bash scripts source — keeping the Rust and shell sides in sync without
//!   re-parsing `config.yaml` from bash.

use anyhow::Result;
use signal_hook::consts::{SIGHUP, SIGTERM};
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use tokio::sync::oneshot;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use futures::stream::StreamExt;

/// Default log file path
const DEFAULT_LOG_FILE_PATH: &str = "/var/log/rargate.log";

/// Configured log file path (set once at startup)
static LOG_FILE_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Check interval for log rotation (1 minute - more frequent due to debug logging volume)
const LOG_CHECK_INTERVAL_SECS: u64 = 60;

/// Maximum log file size in bytes (configurable via logging.max_size_mb; default 500 MB)
static MAX_LOG_SIZE: AtomicU64 = AtomicU64::new(500 * 1024 * 1024);

/// Extra files (besides rargate.log) that the rotation task keeps under a byte cap.
/// Each entry is (path, max_size_bytes). Populated at startup for rar2fs.log and the
/// rar2fs resource log, which are appended to by rar2fs/the monitor rather than tracing.
static CAPPED_FILES: OnceLock<Mutex<Vec<(PathBuf, u64)>>> = OnceLock::new();

/// Register a file to be size-capped by the rotation task. `max_size_mb` of 0 disables
/// capping for that file (it is simply not registered).
pub fn register_capped_file(path: PathBuf, max_size_mb: u64) {
    if max_size_mb == 0 {
        return;
    }
    let max_bytes = max_size_mb * 1024 * 1024;
    let reg = CAPPED_FILES.get_or_init(|| Mutex::new(Vec::new()));
    if let Ok(mut files) = reg.lock() {
        if !files.iter().any(|(p, _)| p == &path) {
            files.push((path, max_bytes));
        }
    }
}

/// Get the configured log file path
fn get_log_path() -> &'static PathBuf {
    LOG_FILE_PATH.get_or_init(|| PathBuf::from(DEFAULT_LOG_FILE_PATH))
}

/// Truncate log file in-place to keep only the most recent lines (FIFO style)
///
/// CRITICAL: Opens existing file and truncates in-place to preserve the inode.
/// This ensures any open file descriptors (like the tracing appender) continue
/// writing to the same file. Using File::create() or rename() would create a
/// new inode, orphaning the appender's file descriptor.
fn truncate_log_if_needed() {
    let log_path = get_log_path();
    let max_size = MAX_LOG_SIZE.load(Ordering::Relaxed);
    truncate_file_if_needed(log_path, max_size);
}

/// Truncate an arbitrary append-mode log file in-place (FIFO, preserves inode).
/// Shared by the main rargate.log and every file registered via [`register_capped_file`].
fn truncate_file_if_needed(log_path: &Path, max_size: u64) {
    let target_size = max_size * 80 / 100; // Keep 80% after rotation

    let metadata = match std::fs::metadata(log_path) {
        Ok(m) => m,
        Err(_) => return, // File doesn't exist, nothing to do
    };

    if metadata.len() <= max_size {
        return; // File is within limits
    }

    // Read file content
    let content = match std::fs::read_to_string(log_path) {
        Ok(c) => c,
        Err(_) => return,
    };

    // Keep last N lines to stay under target_size
    let lines: Vec<&str> = content.lines().collect();
    if lines.is_empty() {
        return;
    }

    let bytes_per_line = content.len() / lines.len();
    let lines_to_keep = (target_size as usize / bytes_per_line).min(lines.len());
    let kept_lines = &lines[lines.len().saturating_sub(lines_to_keep)..];

    // Build new content
    let new_content: String = kept_lines.iter()
        .map(|line| format!("{}\n", line))
        .collect();

    // Open existing file (don't create new), write, then truncate to exact size
    // This preserves the inode so open file descriptors remain valid
    let result = OpenOptions::new()
        .write(true)
        .open(log_path)
        .and_then(|mut file| {
            file.seek(SeekFrom::Start(0))?;
            file.write_all(new_content.as_bytes())?;
            file.set_len(new_content.len() as u64)?;
            file.flush()
        });

    if result.is_err() {
        // Silent failure - logging would be recursive
    }
}

/// Start background task that periodically checks and truncates log file
pub fn start_log_rotation_task() {
    std::thread::spawn(|| {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(LOG_CHECK_INTERVAL_SECS));
            truncate_log_if_needed();
            if let Some(reg) = CAPPED_FILES.get() {
                // Snapshot under the lock, then truncate without holding it.
                let files: Vec<(PathBuf, u64)> = match reg.lock() {
                    Ok(f) => f.clone(),
                    Err(_) => continue,
                };
                for (path, max_size) in files {
                    truncate_file_if_needed(&path, max_size);
                }
            }
        }
    });
}

pub fn setup_logging(debug: bool, max_log_size_mb: u64, log_file: Option<PathBuf>) -> Result<WorkerGuard> {
    let level = if debug { "debug" } else { "info" };

    // Set the log file path (use provided path or default)
    let log_path = log_file.unwrap_or_else(|| PathBuf::from(DEFAULT_LOG_FILE_PATH));
    let _ = LOG_FILE_PATH.set(log_path.clone());

    // Set the maximum log size from config (convert MB to bytes)
    MAX_LOG_SIZE.store(max_log_size_mb * 1024 * 1024, Ordering::Relaxed);

    // Create parent directory if it doesn't exist
    if let Some(parent) = log_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Truncate log if needed before opening (handles logs that grew while stopped)
    truncate_log_if_needed();

    // Extract directory and filename for the appender
    let log_dir = log_path.parent().unwrap_or_else(|| std::path::Path::new("/var/log"));
    let log_filename = log_path.file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("rargate.log");

    // Create file appender
    let file_appender = tracing_appender::rolling::never(log_dir, log_filename);
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("rargate={}", level).into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(non_blocking))
        .init();

    // Start background log rotation task
    start_log_rotation_task();

    Ok(guard)
}

/// Sets up signal handlers for SIGTERM, SIGINT (shutdown), and SIGHUP (config reload).
/// Returns a receiver that yields `()` on each SIGHUP, which main uses to trigger hot reload.
pub fn setup_signal_handlers(
    shutdown_tx: oneshot::Sender<()>,
) -> Result<tokio::sync::mpsc::UnboundedReceiver<()>> {
    use signal_hook_tokio::Signals;
    use tokio::sync::mpsc;

    let (reload_tx, reload_rx) = mpsc::unbounded_channel::<()>();

    let signals = Signals::new([SIGTERM, signal_hook::consts::SIGINT, SIGHUP])?;
    let _handle = signals.handle();

    tokio::spawn(async move {
        let mut signals = signals;
        while let Some(signal) = signals.next().await {
            match signal {
                SIGTERM | signal_hook::consts::SIGINT => {
                    tracing::info!("Received signal {}, shutting down", signal);
                    let _ = shutdown_tx.send(());
                    break;
                }
                SIGHUP => {
                    tracing::info!("Received SIGHUP — triggering config reload");
                    let _ = reload_tx.send(());
                }
                _ => {}
            }
        }
    });

    // Register cleanup handler
    std::panic::set_hook(Box::new(|panic_info| {
        tracing::error!("Panic occurred: {}", panic_info);
    }));

    Ok(reload_rx)
}

/// Write the bash monitor's config file at /var/run/rargate-monitor.conf.
/// This is sourced by rargate-monitor.sh at startup so the bash monitor
/// respects the same config.yaml settings as the Rust side.
pub fn write_monitor_conf(config: &crate::config::Config) -> Result<()> {
    use std::io::Write;

    let cr = config.crash_recovery.as_ref();
    let enabled = cr.and_then(|c| c.enabled).unwrap_or(true);
    let restart_after = cr.and_then(|c| c.restart_after_failures).unwrap_or(3);
    let max_attempts = cr.and_then(|c| c.max_restart_attempts).unwrap_or(5);
    let reset_after = cr.and_then(|c| c.reset_after_minutes).unwrap_or(30);
    let max_notifications = config.notifications.as_ref()
        .and_then(|n| n.max_consecutive_errors)
        .unwrap_or(3);

    let contents = format!(
        "# Generated by rargate at startup — DO NOT EDIT.\n\
         # Source of truth is /etc/rargate/config.yaml.\n\
         ENABLE_AUTO_RESTART={}\n\
         RESTART_AFTER_FAILURES={}\n\
         MAX_RESTART_ATTEMPTS={}\n\
         RESET_AFTER_MINUTES={}\n\
         MAX_NOTIFICATIONS={}\n",
        if enabled { 1 } else { 0 },
        restart_after,
        max_attempts,
        reset_after,
        max_notifications,
    );

    let path = "/var/run/rargate-monitor.conf";
    match std::fs::OpenOptions::new().create(true).write(true).truncate(true).open(path) {
        Ok(mut f) => {
            f.write_all(contents.as_bytes())?;
            tracing::info!("Wrote monitor config to {}", path);
            Ok(())
        }
        Err(e) => {
            // Non-fatal — the bash monitor falls back to its own defaults.
            tracing::warn!("Could not write {}: {} (bash monitor will use defaults)", path, e);
            Ok(())
        }
    }
}

/// Write the paths file at /var/run/rargate-paths.conf. Sourced by all
/// deploy bash scripts (startup, shutdown, monitor) so they read mount
/// paths from config.yaml instead of duplicating them. Single source of
/// truth lives in the YAML.
pub fn write_paths_conf(config: &crate::config::Config) -> Result<()> {
    use std::io::Write;

    fn pb(p: &std::path::Path) -> String { p.to_string_lossy().into_owned() }
    fn opt_pb(o: Option<&std::path::PathBuf>) -> String {
        o.map(|p| pb(p)).unwrap_or_default()
    }

    let rargate_mount = pb(&config.mountpoint);
    let source = pb(&config.source);
    let backend_mount = opt_pb(config.rar2fs.backend_mount.as_ref());
    let log_file = opt_pb(config.logging.as_ref().and_then(|l| l.log_file.as_ref()));
    let sfv_log_file = opt_pb(
        config.sfv_validation.as_ref().and_then(|s| s.log_file.as_ref()),
    );
    let overlay_lower = opt_pb(
        config.overlay_reference.as_ref().map(|o| &o.unverified_share_path),
    );
    let overlay_upper = opt_pb(
        config.overlay_reference.as_ref().map(|o| &o.verified_share_path),
    );
    let overlay_merged = opt_pb(
        config.overlay_reference.as_ref().and_then(|o| o.merged_directory.as_ref()),
    );
    let core_dump_dir = config.core_dumps.as_ref()
        .and_then(|c| c.directory.as_ref())
        .map(|p| pb(p))
        .unwrap_or_else(|| "/mnt/cache/rargate/cores".to_string());
    let core_dump_keep = config.core_dumps.as_ref()
        .and_then(|c| c.keep)
        .unwrap_or(5);

    let contents = format!(
        "# Generated by rargate at startup — DO NOT EDIT.\n\
         # Source of truth is /etc/rargate/config.yaml.\n\
         # Sourced by userscripts-startup.sh, userscripts-shutdown.sh, rargate-monitor.sh.\n\
         RARGATE_MOUNT={rargate_mount}\n\
         SOURCE={source}\n\
         BACKEND_MOUNT={backend_mount}\n\
         RARGATE_LOG_FILE={log_file}\n\
         SFV_LOG_FILE={sfv_log_file}\n\
         OVERLAY_LOWER={overlay_lower}\n\
         OVERLAY_UPPER={overlay_upper}\n\
         OVERLAY_MERGED={overlay_merged}\n\
         CORE_DUMP_DIR={core_dump_dir}\n\
         CORE_DUMP_KEEP={core_dump_keep}\n",
    );

    let path = "/var/run/rargate-paths.conf";
    match std::fs::OpenOptions::new().create(true).write(true).truncate(true).open(path) {
        Ok(mut f) => {
            f.write_all(contents.as_bytes())?;
            tracing::info!("Wrote paths config to {}", path);
            Ok(())
        }
        Err(e) => {
            // Non-fatal — the bash scripts fall back to their own defaults.
            tracing::warn!("Could not write {}: {} (bash scripts will use their own defaults)", path, e);
            Ok(())
        }
    }
}

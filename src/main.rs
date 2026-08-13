//! RARGate — high-performance FUSE filesystem exposing RAR archives as plain media files.
//!
//! Architecture (top-down — see `run()` for the wire-up):
//!
//! ```text
//!     config.yaml ──▶ config.rs ──▶ Rar2fsBackend (backend.rs)
//!                                   │
//!                                   ▼
//!                              RarGateFs (filesystem.rs) ──▶ FilterEngine (filtering.rs)
//!                                   │                              │
//!                                   ▼                              ▼
//!                            FUSE mountpoint                  sfv.rs (SFV validation)
//!                                   │
//!                                   ▼
//!                            InotifyWatcher (inotify_watcher.rs)
//!                                   │
//!                                   ▼  MediaEvent (created/deleted)
//!                       Emby / Jellyfin / Plex notifiers
//!                       (emby_/jellyfin_/plex_notifier.rs, shared via media_notifier_common.rs)
//! ```
//!
//! Side channels: `monitoring.rs` watches mount health and emits Unraid notifications;
//! `metrics.rs` exposes a JSON status file; `utils.rs` writes shell-sourceable configs
//! that the deploy scripts read.

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use tracing::{info, warn, error, debug};

mod config;
mod filesystem;
mod backend;
mod filtering;
mod metrics;
mod monitoring;
mod inotify_watcher;
mod media_notifier_common;
mod emby_notifier;
mod jellyfin_notifier;
mod plex_notifier;
mod arr_notifier;
mod sfv;
mod utils;

use config::Config;
use filesystem::RarGateFs;
use backend::Rar2fsBackend;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const BUILD_DATE: &str = env!("BUILD_DATE");
const LONG_VERSION: &str = const_format::formatcp!(
    "RARGate {version}
High-Performance FUSE Filesystem with RAR Archive Support

Copyright (C) 2026 Nischi
Developed with: Claude Code
License: MIT

Build Information:
  Build Date: {build_date}
  Rust Version: {rustc}
  Target: {target}
  Profile: release

Features:
  • RAR extraction via rar2fs backend
  • SFV validation (strict/permissive modes)
  • Intelligent file/directory filtering
  • Overlay/UnionFS integration
  • inotify-based cache invalidation
  • Unraid notifications
  • Write support (RAR-protected)

Dependencies:
  • FUSE 3.x (fuser crate)
  • rar2fs (external backend)
  • Rust async runtime (tokio)

Homepage: https://github.com/Nischi85/RARgate
Report bugs: https://github.com/Nischi85/RARgate/issues",
    version = VERSION,
    build_date = BUILD_DATE,
    rustc = env!("RUSTC_VERSION"),
    target = env!("TARGET"),
);

#[derive(Parser)]
#[command(name = "rargate")]
#[command(version = VERSION)]
#[command(long_version = LONG_VERSION)]
#[command(about = "High-performance FUSE filesystem with RAR archive support and intelligent filtering")]
#[command(long_about = "RARGate - High-Performance FUSE Filesystem for RAR Archives

DESCRIPTION:
    RARGate provides transparent RAR archive extraction with intelligent file filtering,
    SFV validation, and optimal performance for media streaming (Plex/Emby/Jellyfin).

ARCHITECTURE:
    [RAR archives] → [rar2fs backend] → [RARGate filtering] → [Clean mount]

FEATURES:
    • On-the-fly RAR extraction via rar2fs backend
    • SFV validation (strict/permissive modes)
    • Intelligent file/directory filtering
    • Overlay/UnionFS detection
    • Unraid notification support
    • Read-only mount (protects source files)
    • Perfect SMB/Windows compatibility

USAGE:
    rargate --config <config.yaml>

EXAMPLE:
    rargate --config /etc/rargate/config.yaml
    rargate --config /etc/rargate/config.yaml --foreground --debug")]
struct Cli {
    /// Path to YAML configuration file
    #[arg(short, long, value_name = "FILE")]
    config: PathBuf,

    /// Run in foreground (don't daemonize)
    #[arg(short, long)]
    foreground: bool,

    /// Enable debug logging
    #[arg(short, long)]
    debug: bool,
}

/// Validate system dependencies and prerequisites before starting
fn validate_dependencies(config: &Config) -> Result<()> {
    use std::path::Path;
    use anyhow::bail;

    // Check rar2fs binary exists
    let rar2fs_path = config.rar2fs.binary_path.as_deref().unwrap_or("/usr/local/bin/rar2fs");
    if !Path::new(rar2fs_path).exists() {
        eprintln!("❌ ERROR: rar2fs binary not found");
        eprintln!("   Expected at: {}", rar2fs_path);
        eprintln!();
        eprintln!("   Installation:");
        eprintln!("   - Ubuntu/Debian: apt-get install rar2fs");
        eprintln!("   - Or compile from source: https://github.com/hasse69/rar2fs");
        eprintln!();
        eprintln!("   Alternatively, set rar2fs.binary_path in your config to the correct location.");
        bail!("rar2fs not installed");
    }

    // Check FUSE is available
    if !Path::new("/dev/fuse").exists() {
        eprintln!("❌ ERROR: FUSE not available");
        eprintln!("   /dev/fuse device not found");
        eprintln!();
        eprintln!("   Fix:");
        eprintln!("   - Load FUSE module: modprobe fuse");
        eprintln!("   - Check module loaded: lsmod | grep fuse");
        bail!("FUSE module not loaded");
    }

    // Check user_allow_other in /etc/fuse.conf if allow_other is enabled
    // Only warn for non-root users since root can mount without user_allow_other
    let allow_other = config.fuse_options.as_ref()
        .and_then(|opts| opts.allow_other)
        .unwrap_or(true);

    if allow_other && !nix::unistd::Uid::effective().is_root() {
        if let Ok(content) = std::fs::read_to_string("/etc/fuse.conf") {
            if !content.lines().any(|line| {
                let line = line.trim();
                line == "user_allow_other" || line.starts_with("user_allow_other ")
            }) {
                eprintln!("⚠️  WARNING: allow_other enabled but user_allow_other not set in /etc/fuse.conf");
                eprintln!("   This may cause mount failures for non-root users");
                eprintln!();
                eprintln!("   Fix:");
                eprintln!("   echo 'user_allow_other' >> /etc/fuse.conf");
                eprintln!();
                // Don't fail, just warn - root can still mount
            }
        }
    }

    // Verify rar2fs is executable
    if let Err(e) = std::process::Command::new(rar2fs_path)
        .arg("--version")
        .output()
    {
        eprintln!("❌ ERROR: Cannot execute rar2fs");
        eprintln!("   Path: {}", rar2fs_path);
        eprintln!("   Error: {}", e);
        eprintln!();
        eprintln!("   Check file permissions: chmod +x {}", rar2fs_path);
        bail!("rar2fs not executable");
    }

    info!("✅ Dependency validation passed");
    info!("   rar2fs: {}", rar2fs_path);
    info!("   FUSE: /dev/fuse");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Load configuration first (needed for verbose setting)
    let config = Config::load(&cli.config)?;

    // Initialize logging (CLI --debug flag OR config verbose setting)
    // The _log_guard must be kept alive for the duration of the program
    let debug_enabled = cli.debug || config.verbose.unwrap_or(false);
    let max_log_size_mb = config.logging.as_ref().and_then(|l| l.max_size_mb).unwrap_or(500);
    let log_file = config.logging.as_ref().and_then(|l| l.log_file.clone());
    let _log_guard = utils::setup_logging(debug_enabled, max_log_size_mb, log_file)?;

    info!("Loaded configuration from {}", cli.config.display());

    // Resolve rar2fs diagnostics settings (with defaults). Each log is gated independently:
    // when off, no file is opened / no task is spawned / nothing is registered.
    // The master switch overrides both per-log flags.
    let logging = config.logging.as_ref();
    let diagnostics_master = logging.and_then(|l| l.rar2fs_diagnostics_enabled).unwrap_or(true);
    let rar2fs_log_enabled = diagnostics_master
        && logging.and_then(|l| l.rar2fs_log_enabled).unwrap_or(true);
    let rar2fs_resources_enabled = diagnostics_master
        && logging.and_then(|l| l.rar2fs_resources_enabled).unwrap_or(true);
    let rar2fs_log_path = logging
        .and_then(|l| l.rar2fs_log_file.clone())
        .unwrap_or_else(|| std::path::PathBuf::from(backend::RAR2FS_LOG_PATH));
    let rar2fs_log_max_mb = logging.and_then(|l| l.rar2fs_log_max_size_mb).unwrap_or(50);
    let rar2fs_resources_log_path = logging
        .and_then(|l| l.rar2fs_resources_log_file.clone())
        .unwrap_or_else(|| std::path::PathBuf::from(monitoring::RAR2FS_RESOURCES_LOG));
    let rar2fs_resources_max_mb = logging.and_then(|l| l.rar2fs_resources_log_max_size_mb).unwrap_or(20);
    let rar2fs_resource_interval = logging.and_then(|l| l.rar2fs_resource_interval_seconds).unwrap_or(300);
    if rar2fs_log_enabled {
        utils::register_capped_file(rar2fs_log_path.clone(), rar2fs_log_max_mb);
    } else {
        info!("rar2fs stderr log disabled: no file opened, nothing registered for rotation");
    }
    if rar2fs_resources_enabled {
        utils::register_capped_file(rar2fs_resources_log_path.clone(), rar2fs_resources_max_mb);
    } else {
        info!("rar2fs resource sampling disabled: no sampling task spawned");
    }

    // Validate configuration
    config.validate()?;

    // Validate system dependencies and prerequisites
    validate_dependencies(&config)?;

    // Write shell-sourceable config files so the deploy bash scripts
    // respect the same config.yaml values as the Rust side.
    if let Err(e) = utils::write_monitor_conf(&config) {
        warn!("write_monitor_conf failed: {}", e);
    }
    if let Err(e) = utils::write_paths_conf(&config, &cli.config) {
        warn!("write_paths_conf failed: {}", e);
    }

    // Start rar2fs backend
    let mut backend = Rar2fsBackend::start(
        &config.source,
        &config.rar2fs,
        rar2fs_log_enabled.then_some(rar2fs_log_path.as_path()),
    )?;
    info!("Started rar2fs backend at {}", backend.path().display());
    
    // Setup signal handling for graceful shutdown and SIGHUP config reload
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let mut reload_rx = utils::setup_signal_handlers(shutdown_tx)?;
    
    // Start mount monitoring if enabled
    let monitor_handle = if config.notifications.as_ref().is_some_and(|n| n.enabled) {
        Some(monitoring::start_monitor(
            backend.path().to_path_buf(),
            config.mountpoint.clone(),
            config.notifications.clone().unwrap(),
        ).await?)
    } else {
        None
    };

    // Start rar2fs resource monitoring (for debugging crashes), unless disabled.
    let _rar2fs_monitor = if rar2fs_resources_enabled {
        Some(monitoring::start_rar2fs_resource_monitor(
            backend.pid(),
            rar2fs_resources_log_path.clone(),
            rar2fs_resource_interval,
        ))
    } else {
        None
    };

    // Create metrics collector (shared across all modules via Arc)
    let metrics = metrics::MetricsCollector::new();

    // Global cap on concurrent rar2fs `read_dir` calls. Shared between RarGateFs (FUSE
    // readdir handler) and FilterEngine (validation pool) so the two subsystems can't
    // independently saturate rar2fs during a burst.
    let rar2fs_limiter = backend::RarFsLimiter::new(
        config.rar2fs.max_concurrent_reads.unwrap_or(4)
    );
    rar2fs_limiter.set_metrics(metrics.clone());

    // Create and mount filesystem (also returns the hot-reload handle for SIGHUP)
    let (mut fs, reload_handle) = RarGateFs::new(
        backend.path().to_path_buf(),
        config.source.clone(),
        config.clone(),
        rar2fs_limiter,
    )?;
    fs.set_metrics(metrics.clone());

    // Start metrics status-file writer if configured
    if let Some((status_path, interval_secs)) = metrics::metrics_config(&config.metrics) {
        info!("Metrics status file: {} (update every {}s)", status_path.display(), interval_secs);
        metrics::start_metrics_writer(metrics.clone(), status_path, interval_secs, VERSION);
    }

    // Spawn one media-server notifier. Each notifier follows the same shape: pull the
    // Option<*Config> off `Config`, gate on `enabled`, build the notifier via its
    // constructor, attach metrics, spawn `run()`. The constructor differs (struct
    // method vs free fn), so this is a macro rather than a generic fn.
    macro_rules! start_notifier {
        ($name:literal, $cfg:expr, $metrics:expr, $ctor:expr) => {{
            match $cfg {
                Some(cfg) if cfg.enabled => {
                    info!("Starting {} notifier", $name);
                    match $ctor(cfg.clone()) {
                        Ok((mut notifier, handle)) => {
                            notifier.set_metrics($metrics.clone());
                            tokio::spawn(notifier.run());
                            Some(handle)
                        }
                        Err(e) => {
                            error!("Failed to initialize {} notifier: {}", $name, e);
                            None
                        }
                    }
                }
                _ => None,
            }
        }};
    }

    let emby_handle     = start_notifier!("Emby",     &config.emby,     metrics, emby_notifier::EmbyNotifier::new);
    let jellyfin_handle = start_notifier!("Jellyfin", &config.jellyfin, metrics, jellyfin_notifier::new);
    let plex_handle     = start_notifier!("Plex",     &config.plex,     metrics, plex_notifier::PlexNotifier::new);
    let arr_handle      = start_notifier!("Arr",      &config.arr,      metrics, arr_notifier::ArrNotifier::new);

    // Start inotify watcher for overlay layers if configured
    let inotify_handle = if let Some(overlay_config) = &config.overlay_reference {
        if overlay_config.use_for_filtering.unwrap_or(false) {
            info!("Starting inotify monitoring for overlay layers");
            let exclude_dirs = config.filters.as_ref().and_then(|f| f.exclude_dirs.clone());
            let validation_backoff = config.sfv_validation.as_ref().and_then(|s| s.validation_backoff.as_ref());
            let watcher_config = config.watcher.as_ref();
            let mut watcher = inotify_watcher::InotifyWatcher::new(overlay_config, backend.path().to_path_buf(), exclude_dirs, validation_backoff, watcher_config);
            watcher.set_metrics(metrics.clone());

            // Get cache handle for invalidation
            let dir_cache = fs.get_cache_handle();

            // Create cache invalidation channel
            let (cache_tx, mut cache_rx) = tokio::sync::mpsc::unbounded_channel();
            watcher.set_cache_channel(cache_tx);

            // Connect Emby notifier to inotify watcher if enabled
            if let Some(emby_handle) = emby_handle.clone() {
                watcher.set_emby_handle(emby_handle);
            }

            // Connect Jellyfin notifier to inotify watcher if enabled
            if let Some(jellyfin_handle) = jellyfin_handle.clone() {
                watcher.set_jellyfin_handle(jellyfin_handle);
            }

            // Connect Plex notifier to inotify watcher if enabled
            if let Some(plex_handle) = plex_handle.clone() {
                watcher.set_plex_handle(plex_handle);
            }

            // Connect Sonarr/Radarr rescan notifier to inotify watcher if enabled
            if let Some(arr_handle) = arr_handle.clone() {
                watcher.set_arr_handle(arr_handle);
            }

            // Spawn task to handle cache invalidations
            tokio::spawn(async move {
                while let Some(path) = cache_rx.recv().await {
                    debug!("Cache invalidated for: {}", path.display());
                    // Clear cache entry for this path
                    dir_cache.remove(&path);
                    // Also clear parent directory cache (path changes affect parent listings)
                    if let Some(parent) = path.parent() {
                        dir_cache.remove(&parent.to_path_buf());
                        debug!("Cache invalidated for parent: {}", parent.display());
                    }
                }
            });

            Some(watcher.start_watching().await?)
        } else {
            None
        }
    } else {
        None
    };

    info!("Starting RARGate filesystem:");
    info!("  Source: {}", config.source.display());
    info!("  Backend: {}", backend.path().display());
    info!("  Mountpoint: {}", config.mountpoint.display());
    info!("  Mode: {}", if cli.foreground || !config.daemon { "Foreground" } else { "Background" });
    info!("  Write support: Enabled (RAR-extracted content protected)");

    // Mount filesystem (RW mode, RAR content automatically protected)
    let mut mount_handle = tokio::task::spawn_blocking({
        let mountpoint = config.mountpoint.clone();
        let fuse_options = config.fuse_options.clone();
        move || {
            // Build mount options from config - ALL options from config only
            let mut options = vec![];

            // Add options from config (no hardcoded defaults)
            if let Some(ref fuse_opts) = fuse_options {
                // FSName
                if let Some(ref fsname) = fuse_opts.fsname {
                    options.push(fuser::MountOption::FSName(fsname.clone()));
                }

                // AutoUnmount
                if fuse_opts.auto_unmount.unwrap_or(false) {
                    options.push(fuser::MountOption::AutoUnmount);
                }

                // AllowOther
                if fuse_opts.allow_other.unwrap_or(false) {
                    options.push(fuser::MountOption::AllowOther);
                }

                // AllowRoot
                if fuse_opts.allow_root.unwrap_or(false) {
                    options.push(fuser::MountOption::AllowRoot);
                }

                // DefaultPermissions
                if fuse_opts.default_permissions.unwrap_or(false) {
                    options.push(fuser::MountOption::DefaultPermissions);
                }

                // RW/RO mode
                if fuse_opts.ro.unwrap_or(false) {
                    options.push(fuser::MountOption::RO);
                } else {
                    options.push(fuser::MountOption::RW);
                }

                // Nonempty
                if fuse_opts.nonempty.unwrap_or(false) {
                    options.push(fuser::MountOption::CUSTOM("nonempty".to_string()));
                }

                // Custom options
                if let Some(ref custom) = fuse_opts.custom_options {
                    for opt in custom {
                        options.push(fuser::MountOption::CUSTOM(opt.clone()));
                    }
                }
            }

            fuser::mount2(fs, &mountpoint, &options)
        }
    });
    
    // Wait for shutdown signal, SIGHUP reload, or mount failure
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                info!("Received shutdown signal");
                break;
            }
            Some(()) = reload_rx.recv() => {
                // SIGHUP: reload config and apply hot-reloadable fields
                match Config::load(&cli.config) {
                    Ok(new_config) => {
                        reload_handle.reload(&new_config);
                    }
                    Err(e) => {
                        error!("SIGHUP: Failed to reload config from {}: {}", cli.config.display(), e);
                    }
                }
            }
            result = &mut mount_handle => {
                match result {
                    Ok(Ok(())) => info!("Filesystem unmounted cleanly"),
                    Ok(Err(e)) => error!("Filesystem mount failed: {}", e),
                    Err(e) => error!("Mount task panicked: {}", e),
                }
                break;
            }
        }
    }
    
    // Cleanup
    info!("Shutting down...");

    // Stop monitoring tasks
    if let Some(handle) = monitor_handle {
        handle.abort();
    }
    if let Some(handle) = inotify_handle {
        handle.abort();
    }

    backend.stop();
    info!("Shutdown complete");

    Ok(())
}

//! `InotifyWatcher` — watches the source-overlay layers for filesystem activity and
//! drives two downstream effects:
//!
//! 1. **Cache invalidation** — when a directory finishes uploading (debounced),
//!    invalidates the matching entry in `RarGateFs`'s validated-directory cache so
//!    the next `readdir` re-checks SFV.
//! 2. **Media-server notifications** — emits `MediaEvent::Created` / `Deleted` to the
//!    Emby / Jellyfin / Plex notifiers via their `NotifierHandle`s.
//!
//! Three bounded LRUs prevent unbounded memory growth: `pending_dirs` (awaiting SFV
//! check), `failed_validations` (backoff tracking), `temp_path_last_logged`
//! (rate-limits debug spam).

use anyhow::Result;
use inotify::{Inotify, WatchMask, EventMask};
use lru::LruCache;
use nix::poll::{poll, PollFd, PollFlags, PollTimeout};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::os::fd::AsFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::{info, debug, warn, error};

use crate::config::{OverlayConfig, ValidationBackoffConfig, WatcherConfig};
use crate::media_notifier_common::{MediaEvent, NotifierHandle};
use crate::metrics::MetricsCollector;
use crate::sfv::{is_sfv_file, validate_sfv_with_completeness};

/// Default debounce time in seconds for SFV validation after file activity settles
const DEFAULT_SFV_DEBOUNCE_SECONDS: u64 = 30;

/// Default time in seconds after which notified directories can be removed from pending_dirs
/// This prevents memory growth from accumulated old entries
const DEFAULT_PENDING_CLEANUP_SECONDS: u64 = 300; // 5 minutes

/// Default time in seconds after which a notified directory can be re-validated
/// if new structural events arrive. This allows retrying failed notifications.
const DEFAULT_REVALIDATION_THRESHOLD_SECONDS: u64 = 30;

/// Directory pending SFV validation after file activity
#[derive(Clone)]
struct PendingDirectory {
    /// Last file event timestamp (for debounce)
    last_event: Instant,
    /// Already notified media servers for this directory
    notified: bool,
    /// Timestamp when notification was sent (for cleanup and re-validation)
    notified_at: Option<Instant>,
}

/// Companion/sidecar file extensions (subtitles, metadata, scratch). These are written and
/// churned by tools like bazarr alongside the media; their deletion is transient and must not
/// trigger a media-server "deleted" notification or disturb a directory's validated state.
const SIDECAR_EXTENSIONS: &[&str] = &[
    "srt", "ass", "ssa", "sub", "idx", "vtt", "smi", "nfo", "tmp", "part",
];

/// True if `path` ends in a known sidecar/companion extension (case-insensitive).
fn is_sidecar_file(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| SIDECAR_EXTENSIONS.contains(&e.to_lowercase().as_str()))
        .unwrap_or(false)
}

/// Minimum gap between re-notifications for an already-announced directory. A single
/// subtitle add/sync produces a burst of file events spread over a minute or more; this
/// collapses that burst into one log line + one media-server refresh instead of repeating.
const RE_NOTIFY_MIN_INTERVAL_SECS: u64 = 120;

/// Directory that failed SFV validation - track for backoff
struct FailedValidation {
    /// Last validation attempt time
    last_attempt: Instant,
    /// Number of consecutive failures
    failure_count: u32,
    /// Wall-clock time of the first failure, for "stuck for N" reporting
    first_seen: std::time::SystemTime,
    /// Highest milestone already logged at WARN (throttles per-cooldown spam)
    last_milestone_logged: u32,
    /// Whether the one-time escalation ERROR has already been emitted
    escalated: bool,
}

/// Default cooldown duration before retrying failed validations (5 minutes)
const DEFAULT_VALIDATION_COOLDOWN_SECS: u64 = 300;

/// Default maximum failures before requiring cooldown to retry
const DEFAULT_MAX_VALIDATION_FAILURES: u32 = 3;

/// Default consecutive failures after which a release is escalated: one loud ERROR
/// plus a flag in the status file. Past this, a stuck release is almost certainly an
/// incomplete or abandoned download rather than a still-in-progress one.
const DEFAULT_SFV_ESCALATION_THRESHOLD: u32 = 25;

/// Failure counts at which a throttled WARN is logged. Without this, a permanently
/// broken release re-WARNs every cooldown cycle (hundreds of identical lines); we
/// instead log only when crossing one of these milestones.
const SFV_FAILURE_MILESTONES: &[u32] = &[3, 10, 50, 100, 500, 1000, 5000];

/// Highest failure milestone reached, for throttled WARN logging.
fn sfv_failure_milestone(count: u32) -> u32 {
    let mut m = 0;
    for &x in SFV_FAILURE_MILESTONES {
        if count >= x { m = x; } else { break; }
    }
    m
}

/// Human-friendly elapsed duration, e.g. "4d 3h", "5h 12m", "45m", "30s".
fn humanize_duration(d: std::time::Duration) -> String {
    let s = d.as_secs();
    let (days, hours, mins) = (s / 86_400, (s % 86_400) / 3_600, (s % 3_600) / 60);
    if days > 0 { format!("{days}d {hours}h") }
    else if hours > 0 { format!("{hours}h {mins}m") }
    else if mins > 0 { format!("{mins}m") }
    else { format!("{s}s") }
}

/// Cleanup threshold for old failure records (1 hour) - not configurable
const FAILURE_CLEANUP_THRESHOLD_SECS: u64 = 3600;

/// LRU cache sizes for bounded memory usage
const LRU_PENDING_DIRS_SIZE: usize = 10_000;
const LRU_FAILED_VALIDATIONS_SIZE: usize = 5_000;
const LRU_TEMP_PATH_LOG_SIZE: usize = 1_000;

/// Cleanup interval for periodic cache maintenance (60 seconds)
const CLEANUP_INTERVAL_SECS: u64 = 60;

/// Check if a path is a temporary file/directory from download clients
/// These should be ignored to avoid stale path caching
fn is_temporary_path(path: &Path) -> bool {
    let path_str = path.to_string_lossy();

    // SABnzbd/NZBGet temporary naming pattern
    // Example: .::TMPNAME:D:29359%15528680822634716288:MovieName
    if path_str.contains(".::TMPNAME:") {
        return true;
    }

    // Common unpack temporary directory pattern
    if path_str.contains("_UNPACK_") {
        return true;
    }

    // Check filename-specific patterns
    if let Some(name) = path.file_name() {
        let name_str = name.to_string_lossy();

        // qBittorrent incomplete files
        if name_str.ends_with(".!qB") {
            return true;
        }

        // macOS temporary files
        if name_str.starts_with("._") {
            return true;
        }

        // Common partial download extensions
        if name_str.ends_with(".part") || name_str.ends_with(".tmp") || name_str.ends_with(".dctmp") {
            return true;
        }
    }

    false
}

/// Check if a directory matches an exclude pattern (case-insensitive)
fn is_excluded_dir(exclude_dirs: &[String], path: &Path) -> bool {
    if let Some(name) = path.file_name() {
        let name_lower = name.to_string_lossy().to_lowercase();
        return exclude_dirs.iter().any(|p| p == &name_lower);
    }
    false
}

/// Inotify watcher for overlay layers (verified and unverified).
/// Monitors the underlying filesystem layers and invalidates caches when files change.
pub struct InotifyWatcher {
    verified_path: PathBuf,
    unverified_path: PathBuf,
    /// Overlay merged directory (e.g., /path/to/overlay/merged).
    /// When files change in the verified layer, we stat the corresponding overlay path to refresh the cache.
    overlay_merged_path: Option<PathBuf>,
    rar2fs_backend_path: PathBuf,
    /// Wrapped in Arc so start_watching() clones the pointer, not the Vec.
    exclude_dirs: Arc<Vec<String>>,
    /// Optional metrics collector — incremented on inotify events, overflows, and watch count.
    metrics: Option<Arc<MetricsCollector>>,
    /// Maximum consecutive validation failures before entering cooldown
    max_validation_failures: u32,
    /// Cooldown duration in seconds after max failures
    validation_cooldown_secs: u64,
    /// Consecutive failures after which a release is escalated (loud ERROR + status flag)
    escalation_threshold: u32,
    /// Debounce time in seconds for SFV validation
    sfv_debounce_secs: u64,
    /// Time in seconds after which notified entries are cleaned up
    pending_cleanup_secs: u64,
    /// Time in seconds after which re-validation is allowed
    revalidation_threshold_secs: u64,
    // INVARIANT: each of these is set exactly once via the corresponding set_*() method,
    // which must be called before start_watching(). OnceLock is used instead of RwLock
    // because the value never changes after initialization — eliminating lock overhead
    // on the inotify hot path.
    cache_invalidation_tx: Arc<OnceLock<tokio::sync::mpsc::UnboundedSender<PathBuf>>>,
    emby_tx: Arc<OnceLock<NotifierHandle>>,
    jellyfin_tx: Arc<OnceLock<NotifierHandle>>,
    plex_tx: Arc<OnceLock<NotifierHandle>>,
    arr_tx: Arc<OnceLock<NotifierHandle>>,
}

/// Runtime context passed into the inotify watch loop.
/// Groups all configuration parameters into a single struct so watch_loop() avoids
/// a 13-parameter signature. All fields are owned so the struct can be moved into
/// spawn_blocking (which requires 'static bounds).
struct WatchLoopContext {
    verified_path: PathBuf,
    unverified_path: PathBuf,
    overlay_merged_path: Option<PathBuf>,
    rar2fs_backend_path: PathBuf,
    /// Arc pointer: clone is a pointer bump, not a Vec copy.
    exclude_dirs: Arc<Vec<String>>,
    max_validation_failures: u32,
    validation_cooldown_secs: u64,
    escalation_threshold: u32,
    sfv_debounce_secs: u64,
    pending_cleanup_secs: u64,
    revalidation_threshold_secs: u64,
    metrics: Option<Arc<MetricsCollector>>,
}

/// Invariant limits for the recursive watch-adding walk, grouped so the recursion
/// threads one context instead of three unchanging arguments.
struct WatchLimits<'a> {
    max_depth: usize,
    max_watches: usize,
    exclude_dirs: &'a [String],
}

impl InotifyWatcher {
    pub fn new(
        overlay_config: &OverlayConfig,
        rar2fs_backend_path: PathBuf,
        exclude_dirs: Option<Vec<String>>,
        validation_backoff: Option<&ValidationBackoffConfig>,
        watcher_config: Option<&WatcherConfig>,
    ) -> Self {
        let max_validation_failures = validation_backoff
            .and_then(|b| b.max_failures)
            .unwrap_or(DEFAULT_MAX_VALIDATION_FAILURES);
        let validation_cooldown_secs = validation_backoff
            .and_then(|b| b.cooldown_seconds)
            .unwrap_or(DEFAULT_VALIDATION_COOLDOWN_SECS);
        // Escalation must sit at or above max_failures (which is when WARNs begin).
        let escalation_threshold = validation_backoff
            .and_then(|b| b.escalate_after)
            .unwrap_or(DEFAULT_SFV_ESCALATION_THRESHOLD)
            .max(max_validation_failures);

        // Get watcher timing config with defaults
        let sfv_debounce_secs = watcher_config
            .and_then(|w| w.sfv_debounce_seconds)
            .unwrap_or(DEFAULT_SFV_DEBOUNCE_SECONDS);
        let pending_cleanup_secs = watcher_config
            .and_then(|w| w.pending_cleanup_minutes)
            .map(|m| m * 60)  // Convert minutes to seconds
            .unwrap_or(DEFAULT_PENDING_CLEANUP_SECONDS);
        let revalidation_threshold_secs = watcher_config
            .and_then(|w| w.revalidation_threshold_seconds)
            .unwrap_or(DEFAULT_REVALIDATION_THRESHOLD_SECONDS);

        // Log overlay refresh configuration
        if let Some(ref merged) = overlay_config.merged_directory {
            info!("Overlay refresh enabled: changes in {} will refresh {}",
                  overlay_config.verified_share_path.display(), merged.display());
        }

        Self {
            verified_path: overlay_config.verified_share_path.clone(),
            unverified_path: overlay_config.unverified_share_path.clone(),
            overlay_merged_path: overlay_config.merged_directory.clone(),
            rar2fs_backend_path,
            exclude_dirs: Arc::new(
                exclude_dirs
                    .unwrap_or_default()
                    .iter()
                    .map(|s| s.to_lowercase())
                    .collect(),
            ),
            max_validation_failures,
            validation_cooldown_secs,
            escalation_threshold,
            sfv_debounce_secs,
            pending_cleanup_secs,
            revalidation_threshold_secs,
            metrics: None,
            cache_invalidation_tx: Arc::new(OnceLock::new()),
            emby_tx: Arc::new(OnceLock::new()),
            jellyfin_tx: Arc::new(OnceLock::new()),
            plex_tx: Arc::new(OnceLock::new()),
            arr_tx: Arc::new(OnceLock::new()),
        }
    }

    /// Attach a metrics collector. Must be called before start_watching().
    pub fn set_metrics(&mut self, metrics: Arc<MetricsCollector>) {
        self.metrics = Some(metrics);
    }

    /// Registers the cache invalidation channel. Must be called before start_watching().
    pub fn set_cache_channel(&self, tx: tokio::sync::mpsc::UnboundedSender<PathBuf>) {
        let _ = self.cache_invalidation_tx.set(tx);
    }

    /// Registers the Emby handle. Must be called before start_watching().
    pub fn set_emby_handle(&self, handle: NotifierHandle) {
        let _ = self.emby_tx.set(handle);
        info!("Emby notifier connected to inotify watcher");
    }

    /// Registers the Jellyfin handle. Must be called before start_watching().
    pub fn set_jellyfin_handle(&self, handle: NotifierHandle) {
        let _ = self.jellyfin_tx.set(handle);
        info!("Jellyfin notifier connected to inotify watcher");
    }

    /// Registers the Plex handle. Must be called before start_watching().
    pub fn set_plex_handle(&self, handle: NotifierHandle) {
        let _ = self.plex_tx.set(handle);
        info!("Plex notifier connected to inotify watcher");
    }

    /// Registers the Sonarr/Radarr rescan handle. Must be called before start_watching().
    pub fn set_arr_handle(&self, handle: NotifierHandle) {
        let _ = self.arr_tx.set(handle);
        info!("Arr (Sonarr/Radarr) notifier connected to inotify watcher");
    }

    /// Main watch loop (runs in blocking thread)
    /// Get maximum number of watches allowed based on system limit
    /// Reads from /proc/sys/fs/inotify/max_user_watches and uses 80% of it
    /// Returns (max_watches, system_limit)
    fn get_max_watches() -> (usize, usize) {
        let system_limit = std::fs::read_to_string("/proc/sys/fs/inotify/max_user_watches")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(8192); // Conservative fallback if unable to read system limit

        // Use 80% of system limit (leave 20% headroom for other applications)
        let max_watches = (system_limit * 80) / 100;

        info!("System inotify limit: {} watches", system_limit);
        info!("RARGate will use up to {} watches (80% of system limit)", max_watches);

        (max_watches, system_limit)
    }

    /// Start watching both overlay layers
    pub async fn start_watching(&self) -> Result<tokio::task::JoinHandle<()>> {
        info!("Starting inotify watcher for overlay layers");
        info!("  Verified layer: {}", self.verified_path.display());
        info!("  Unverified layer: {}", self.unverified_path.display());
        if let Some(ref merged) = self.overlay_merged_path {
            info!("  Overlay merged: {} (auto-refresh enabled)", merged.display());
        }
        info!("  SFV debounce: {}s", self.sfv_debounce_secs);
        info!("  Pending cleanup: {}s ({}m)", self.pending_cleanup_secs, self.pending_cleanup_secs / 60);
        info!("  Re-validation threshold: {}s", self.revalidation_threshold_secs);
        info!("  Validation backoff: {} failures, {}s cooldown",
              self.max_validation_failures, self.validation_cooldown_secs);
        info!("  SFV escalation: loud ERROR + status flag after {} failures",
              self.escalation_threshold);

        let ctx = WatchLoopContext {
            verified_path: self.verified_path.clone(),
            unverified_path: self.unverified_path.clone(),
            overlay_merged_path: self.overlay_merged_path.clone(),
            rar2fs_backend_path: self.rar2fs_backend_path.clone(),
            exclude_dirs: self.exclude_dirs.clone(),
            max_validation_failures: self.max_validation_failures,
            validation_cooldown_secs: self.validation_cooldown_secs,
            escalation_threshold: self.escalation_threshold,
            sfv_debounce_secs: self.sfv_debounce_secs,
            pending_cleanup_secs: self.pending_cleanup_secs,
            revalidation_threshold_secs: self.revalidation_threshold_secs,
            metrics: self.metrics.clone(),
        };
        let cache_tx = self.cache_invalidation_tx.clone();
        let emby_tx = self.emby_tx.clone();
        let jellyfin_tx = self.jellyfin_tx.clone();
        let plex_tx = self.plex_tx.clone();
        let arr_tx = self.arr_tx.clone();

        let handle = tokio::task::spawn_blocking(move || {
            if let Err(e) = Self::watch_loop(ctx, cache_tx, emby_tx, jellyfin_tx, plex_tx, arr_tx) {
                error!("Inotify watch loop failed: {}", e);
            }
        });

        Ok(handle)
    }

    /// Main watch loop (runs in blocking thread).
    ///
    /// Three concurrent concerns are interleaved in a single thread to keep the
    /// inotify file descriptor's locality tight:
    ///
    ///   (A) **Event polling** — `poll()` on the inotify FD with a short timeout;
    ///       when events arrive, classify each path (verified vs unverified layer,
    ///       directory vs file) and update the relevant pending-directory state.
    ///   (B) **Debounce-ready dirs** — every loop iteration, walk `pending_dirs` and
    ///       trigger SFV validation for any directory whose debounce window has
    ///       expired. Successful validations emit `MediaEvent::Created` to the
    ///       notifiers and an invalidation message to the FUSE cache; failures move
    ///       to `failed_validations` with exponential backoff.
    ///   (C) **Failed-validation retry** — directories in `failed_validations` are
    ///       re-checked once their cooldown elapses, up to `max_validation_failures`,
    ///       at which point they're dropped (loud failure, logged for investigation).
    ///
    /// All three caches are bounded `LruCache`s — see the constants above for sizing.
    fn watch_loop(
        ctx: WatchLoopContext,
        cache_tx: Arc<OnceLock<tokio::sync::mpsc::UnboundedSender<PathBuf>>>,
        emby_tx: Arc<OnceLock<NotifierHandle>>,
        jellyfin_tx: Arc<OnceLock<NotifierHandle>>,
        plex_tx: Arc<OnceLock<NotifierHandle>>,
        arr_tx: Arc<OnceLock<NotifierHandle>>,
    ) -> Result<()> {
        // Unpack context fields for convenient access
        let verified_path = ctx.verified_path.as_path();
        let unverified_path = ctx.unverified_path.as_path();
        let overlay_merged_path = ctx.overlay_merged_path.as_deref();
        let rar2fs_backend_path = ctx.rar2fs_backend_path.as_path();
        let exclude_dirs: &[String] = &ctx.exclude_dirs;
        let metrics = ctx.metrics.as_ref();
        let max_validation_failures = ctx.max_validation_failures;
        let validation_cooldown_secs = ctx.validation_cooldown_secs;
        let escalation_threshold = ctx.escalation_threshold;
        let sfv_debounce_secs = ctx.sfv_debounce_secs;
        let pending_cleanup_secs = ctx.pending_cleanup_secs;
        let revalidation_threshold_secs = ctx.revalidation_threshold_secs;
        let mut inotify = Inotify::init()?;
        let mut watch_descriptors = HashMap::new();

        // Pending directories waiting for SFV validation after debounce
        // LRU cache bounds memory growth (10,000 entries max)
        let mut pending_dirs: LruCache<PathBuf, PendingDirectory> = LruCache::new(
            NonZeroUsize::new(LRU_PENDING_DIRS_SIZE).unwrap()
        );

        // Track directories that have failed SFV validation for backoff
        // LRU cache bounds memory growth (5,000 entries max)
        let mut failed_validations: LruCache<PathBuf, FailedValidation> = LruCache::new(
            NonZeroUsize::new(LRU_FAILED_VALIDATIONS_SIZE).unwrap()
        );

        // Rate-limit debug logging for temporary paths (once per 5s per path)
        // LRU cache bounds memory growth (1,000 entries max)
        let mut temp_path_last_logged: LruCache<PathBuf, Instant> = LruCache::new(
            NonZeroUsize::new(LRU_TEMP_PATH_LOG_SIZE).unwrap()
        );
        const TEMP_PATH_LOG_INTERVAL_SECS: u64 = 5;

        // Coalesce rapid duplicate overlay-cache refreshes for the same directory. A
        // multi-file release landing fires one inotify event per file, each of which
        // would otherwise re-readdir rar2fs for the same parent dir — amplifying load
        // on every SFV pass. Keyed on the directory; short TTL so genuine later updates
        // still refresh (worst-case visibility lag = OVERLAY_REFRESH_DEDUPE_SECS).
        let mut overlay_refresh_last: LruCache<PathBuf, Instant> = LruCache::new(
            NonZeroUsize::new(LRU_TEMP_PATH_LOG_SIZE).unwrap()
        );

        // Directories already announced as media to the servers. Survives the pending_dirs
        // churn so that a re-validation (e.g. after a subtitle is added) is logged quietly as
        // a re-notification instead of re-announcing "New media dir".
        let mut announced_dirs: LruCache<PathBuf, Instant> = LruCache::new(
            NonZeroUsize::new(LRU_PENDING_DIRS_SIZE).unwrap()
        );

        // Track last cleanup time - cleanup runs every CLEANUP_INTERVAL_SECS
        let mut last_cleanup = Instant::now();

        // Watch both paths recursively with limits to prevent exhausting system resources
        let mut watch_count = 0;
        let (max_watches, system_limit) = Self::get_max_watches();
        const MAX_DEPTH: usize = 10; // Limit depth to prevent excessive recursion
        let limits = WatchLimits { max_depth: MAX_DEPTH, max_watches, exclude_dirs };

        Self::add_watch_recursive_limited(&mut inotify, &mut watch_descriptors, verified_path, 0, &mut watch_count, &limits)?;
        Self::add_watch_recursive_limited(&mut inotify, &mut watch_descriptors, unverified_path, 0, &mut watch_count, &limits)?;

        if watch_count >= max_watches {
            let percentage = (max_watches * 100) / system_limit;
            warn!(
                "Reached watch limit ({}/{}, {}% of system limit {}), some directories not monitored",
                watch_count, max_watches, percentage, system_limit
            );
        }
        info!("Inotify watching {} paths ({} watches used, {} available)",
              watch_descriptors.len(), watch_count, max_watches - watch_count);
        if let Some(m) = metrics {
            m.inotify_watches.store(watch_count as u64, Ordering::Relaxed);
        }

        let mut buffer = [0; 4096];
        let debounce_duration = std::time::Duration::from_secs(sfv_debounce_secs);

        // Poll timeout: 1 second for responsive debounce checking
        const POLL_TIMEOUT_MS: u16 = 1000;

        loop {
            // Use poll() with timeout to periodically check pending directories
            // This ensures debounce checks happen even when no inotify events arrive
            let mut poll_fds = [PollFd::new(inotify.as_fd(), PollFlags::POLLIN)];
            let poll_result = poll(&mut poll_fds, PollTimeout::from(POLL_TIMEOUT_MS));

            // Read events only if data is available (poll returned POLLIN)
            let events = match poll_result {
                Ok(n) if n > 0 => {
                    // Data available, read events
                    inotify.read_events(&mut buffer).ok()
                }
                Ok(_) => {
                    // Timeout - no events, but we still check pending dirs below
                    None
                }
                Err(e) => {
                    warn!("Poll error on inotify fd: {}", e);
                    None
                }
            };

            // Check pending directories that have passed debounce time
            let now = Instant::now();
            let ready_dirs: Vec<PathBuf> = pending_dirs
                .iter()
                .filter(|(_, pending)| !pending.notified && now.duration_since(pending.last_event) >= debounce_duration)
                .map(|(path, _)| path.clone())
                .collect();

            for dir_path in ready_dirs {
                // Check if directory is in backoff due to previous failures.
                //
                // Two gates apply, in order:
                //   1. Once `failure_count >= max_validation_failures` we enter the
                //      long `validation_cooldown_secs` cooldown (existing behaviour).
                //   2. Below that ceiling, apply an exponential per-attempt backoff
                //      (5s → 15s → 45s, capped at the cooldown duration) so a
                //      directory whose first SFV attempt fails doesn't get re-checked
                //      every loop iteration. Without this, observed behaviour during
                //      yesterday's incident was 3 failed validations within 14ms.
                let should_skip = if let Some(failed) = failed_validations.get(&dir_path) {
                    if failed.failure_count >= max_validation_failures {
                        now.duration_since(failed.last_attempt) < std::time::Duration::from_secs(validation_cooldown_secs)
                    } else {
                        let attempt_backoff = 5u64
                            .saturating_mul(3u64.saturating_pow(failed.failure_count.saturating_sub(1)))
                            .min(validation_cooldown_secs);
                        now.duration_since(failed.last_attempt) < std::time::Duration::from_secs(attempt_backoff)
                    }
                } else {
                    false
                };

                if should_skip {
                    continue;  // Skip this directory, still in backoff
                }

                // Find SFV file in this directory
                if let Some(sfv_path) = Self::find_sfv_file(&dir_path) {
                    let validation_start = Instant::now();
                    let validation_result = validate_sfv_with_completeness(&sfv_path);
                    let validation_duration = validation_start.elapsed();
                    debug!("SFV validation for {} took {:.1}ms (result: {})",
                           dir_path.display(), validation_duration.as_secs_f64() * 1000.0, validation_result);

                    if validation_result {
                        // SFV valid - clear any previous failure state
                        failed_validations.pop(&dir_path);
                        if let Some(m) = metrics { m.clear_stuck(&dir_path); }

                        // Refresh overlay cache so rar2fs sees new files on the next access.
                        Self::refresh_overlay_cache(&dir_path, verified_path, overlay_merged_path, &mut overlay_refresh_last);

                        // A passing SFV implies a valid RAR set — the "media directory" signal.
                        // First sighting announces new media. A later change to an already-known
                        // dir (e.g. a subtitle added) logs one clear "changed" line and refreshes
                        // the servers, but is rate-limited so the burst of events from a single
                        // subtitle add/sync collapses into one line instead of spamming.
                        let last_announce = announced_dirs.get(&dir_path).copied();
                        let should_notify = match last_announce {
                            None => {
                                info!("SFV validation passed for {} (took {:.1}ms)",
                                      dir_path.display(), validation_duration.as_secs_f64() * 1000.0);
                                info!("New media dir: {}", dir_path.display());
                                true
                            }
                            Some(t) if now.duration_since(t) >= std::time::Duration::from_secs(RE_NOTIFY_MIN_INTERVAL_SECS) => {
                                info!("Media dir changed, re-notifying servers: {}", dir_path.display());
                                true
                            }
                            Some(_) => {
                                // Within the rate-limit window — same operation's churn, skip.
                                debug!("Skipping duplicate re-notification (recent) for {}", dir_path.display());
                                false
                            }
                        };

                        if should_notify {
                            announced_dirs.put(dir_path.clone(), now);
                            Self::notify_server(&emby_tx, &dir_path, false, "Emby");
                            Self::notify_server(&jellyfin_tx, &dir_path, false, "Jellyfin");
                            Self::notify_server(&plex_tx, &dir_path, false, "Plex");
                            // SFV passed → the release is now visible in the mount. This is the
                            // authoritative moment to import: fire an in-place Sonarr/Radarr
                            // rescan so the grab lands without depending on dc-bridge's nudge.
                            Self::notify_server(&arr_tx, &dir_path, false, "Arr");
                        }

                        // Mark as notified with timestamp for cleanup/re-validation
                        if let Some(pending) = pending_dirs.get_mut(&dir_path) {
                            pending.notified = true;
                            pending.notified_at = Some(Instant::now());
                        }
                    } else {
                        // Validation failed - track failure for backoff
                        let now_wall = std::time::SystemTime::now();
                        let (failure_count, first_seen) =
                            if let Some(existing) = failed_validations.get_mut(&dir_path) {
                                existing.failure_count += 1;
                                existing.last_attempt = now;
                                (existing.failure_count, existing.first_seen)
                            } else {
                                failed_validations.put(dir_path.clone(), FailedValidation {
                                    last_attempt: now,
                                    failure_count: 1,
                                    first_seen: now_wall,
                                    last_milestone_logged: 0,
                                    escalated: false,
                                });
                                (1, now_wall)
                            };

                        if failure_count >= max_validation_failures {
                            let stuck_for = humanize_duration(first_seen.elapsed().unwrap_or_default());
                            // Read prior throttle state without bumping LRU recency.
                            let (prev_milestone, already_escalated) = failed_validations
                                .peek(&dir_path)
                                .map(|f| (f.last_milestone_logged, f.escalated))
                                .unwrap_or((0, false));

                            if failure_count >= escalation_threshold && !already_escalated {
                                // One loud, distinct line — easy to spot in the log — then go
                                // quiet (the status file carries it from here).
                                error!("🛑 PERSISTENT SFV FAILURE: {} — failed {} times over {}; likely an incomplete or abandoned download. Re-grab or remove it.",
                                       dir_path.display(), failure_count, stuck_for);
                                if let Some(f) = failed_validations.get_mut(&dir_path) {
                                    f.escalated = true;
                                    f.last_milestone_logged = failure_count;
                                }
                            } else if !already_escalated {
                                // Throttle WARNs to milestone crossings (3, 10, 50, 100, ...)
                                // instead of one per cooldown cycle.
                                let milestone = sfv_failure_milestone(failure_count);
                                if milestone > prev_milestone {
                                    warn!("SFV validation failed {} times for {} (stuck {}), entering cooldown ({}s) - check for missing or incomplete files",
                                          failure_count, dir_path.display(), stuck_for, validation_cooldown_secs);
                                    if let Some(f) = failed_validations.get_mut(&dir_path) {
                                        f.last_milestone_logged = milestone;
                                    }
                                } else {
                                    debug!("SFV validation still failing for {} ({} times, stuck {}), in cooldown",
                                           dir_path.display(), failure_count, stuck_for);
                                }
                            } else {
                                debug!("SFV validation still failing (already escalated) for {} ({} times)",
                                       dir_path.display(), failure_count);
                            }

                            // Surface to the status file so operators have one place to look.
                            if let Some(m) = metrics {
                                m.record_stuck(&dir_path, failure_count, first_seen);
                            }
                        } else {
                            debug!("SFV validation failed for {} (attempt {}/{}, took {:.1}ms), will retry after next file event",
                                   dir_path.display(), failure_count, max_validation_failures,
                                   validation_duration.as_secs_f64() * 1000.0);
                        }
                    }
                }
            }

            // Process inotify events if available
            if let Some(events) = events {
                for event in events {
                    // Handle Q_OVERFLOW: kernel inotify queue overflowed, events were lost
                    if event.mask.contains(EventMask::Q_OVERFLOW) {
                        if let Some(m) = metrics {
                            m.inotify_overflows.fetch_add(1, Ordering::Relaxed);
                        }
                        // Suggest increasing max_queued_events if this happens repeatedly
                        let max_queued = std::fs::read_to_string("/proc/sys/fs/inotify/max_queued_events")
                            .ok()
                            .and_then(|s| s.trim().parse::<u64>().ok())
                            .unwrap_or(16384);
                        warn!("inotify queue overflow detected - events may have been lost \
                               (current max_queued_events={}; consider raising it via \
                               sysctl fs.inotify.max_queued_events={})", max_queued, max_queued * 2);
                        info!("Clearing pending_dirs and triggering full re-scan of watched verified paths");

                        // Clear pending_dirs since we don't know what events were lost
                        pending_dirs.clear();
                        failed_validations.clear();

                        // Re-scan all watched verified paths for SFV files
                        for (_, watch_path) in watch_descriptors.iter() {
                            if watch_path.starts_with(verified_path)
                                && Self::find_sfv_file(watch_path).is_some() {
                                    pending_dirs.put(watch_path.clone(), PendingDirectory {
                                        last_event: Instant::now(),
                                        notified: false,
                                        notified_at: None,
                                    });
                                    debug!("Q_OVERFLOW recovery: queued {} for validation", watch_path.display());
                                }
                        }

                        info!("Q_OVERFLOW recovery: queued {} directories for re-validation", pending_dirs.len());
                        continue; // Q_OVERFLOW has no associated path, skip normal processing
                    }

                    if let Some(m) = metrics {
                        m.inotify_events.fetch_add(1, Ordering::Relaxed);
                    }

                    // Get path from watch descriptor
                    let path = match watch_descriptors.get(&event.wd) {
                        Some(p) => p.clone(),
                        None => continue,
                    };

                    // Build full path with event name
                    let full_path = if let Some(name) = event.name {
                        path.join(name)
                    } else {
                        path.clone()
                    };

                    // Skip temporary files (with rate-limited debug logging)
                    if is_temporary_path(&full_path) {
                        // Rate-limit debug logging to once per TEMP_PATH_LOG_INTERVAL_SECS per path
                        let should_log = temp_path_last_logged
                            .get(&full_path)
                            .map(|last| last.elapsed().as_secs() >= TEMP_PATH_LOG_INTERVAL_SECS)
                            .unwrap_or(true);

                        if should_log {
                            debug!("Skipping temporary path: {}", full_path.display());
                            temp_path_last_logged.put(full_path.clone(), Instant::now());
                        }
                        continue;
                    }

                    debug!("inotify event: {:?} on {:?}", event.mask, full_path);

                    // Translate source path (verified/unverified) to backend path for cache invalidation
                    // The dir_cache uses backend paths as keys
                    let backend_path = Self::translate_to_backend_path(
                        &full_path,
                        verified_path,
                        unverified_path,
                        rar2fs_backend_path,
                    );

                    // Always invalidate cache for any file change
                    Self::invalidate_cache(&cache_tx, &backend_path);

                    // Refresh overlay cache when files change in the verified path
                    // This forces the overlay to see files written directly to upperdir
                    if full_path.starts_with(verified_path) {
                        // Refresh the containing directory (not the file) so a release's
                        // per-file events coalesce onto a single dedupe key.
                        if let Some(parent) = full_path.parent() {
                            Self::refresh_overlay_cache(parent, verified_path, overlay_merged_path, &mut overlay_refresh_last);
                        }
                    }

                    // Get the directory containing this file (use event mask instead of syscall)
                    let event_dir = if event.mask.contains(EventMask::ISDIR) {
                        full_path.clone()
                    } else {
                        full_path.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| full_path.clone())
                    };

                    // Determine if this path is in the verified or unverified layer
                    let is_verified_path = full_path.starts_with(verified_path);

                    // Only notify on structural changes (create, delete, move), not modifications
                    let is_structural_change = event.mask.contains(EventMask::CREATE)
                        || event.mask.contains(EventMask::DELETE)
                        || event.mask.contains(EventMask::MOVED_FROM)
                        || event.mask.contains(EventMask::MOVED_TO);

                    // Classify structural event as deletion or creation
                    let is_deletion_event = event.mask.contains(EventMask::DELETE)
                        || event.mask.contains(EventMask::MOVED_FROM)
                        || event.mask.contains(EventMask::DELETE_SELF);

                    if is_verified_path {
                        // VERIFIED PATH: creations require SFV validation; deletions bypass it
                        if is_structural_change {
                            if is_deletion_event {
                                if is_sidecar_file(&full_path) && !event.mask.contains(EventMask::ISDIR) {
                                    // Transient subtitle/companion churn (e.g. bazarr writing,
                                    // syncing and renaming a .srt): don't notify servers of the
                                    // deletion and don't clear the parent dir's validated state.
                                    debug!("Sidecar file deletion, ignoring: {}", full_path.display());
                                } else {
                                    // Files are already gone — bypass SFV, notify directly.
                                    // ISDIR deletions are handled (and notified) in the dedicated
                                    // directory-deletion block below to avoid duplicate notifications.
                                    if !event.mask.contains(EventMask::ISDIR) {
                                        debug!("Verified path file deletion, notifying directly: {}", full_path.display());
                                        Self::notify_server(&emby_tx, &full_path, true, "Emby");
                                        Self::notify_server(&jellyfin_tx, &full_path, true, "Jellyfin");
                                        Self::notify_server(&plex_tx, &full_path, true, "Plex");
                                    }
                                    // Clean up pending/failure state for the deleted dir
                                    pending_dirs.pop(&event_dir);
                                    failed_validations.pop(&event_dir);
                                }
                            } else {
                                // CREATE / MOVED_TO: existing SFV-validation pending logic
                                // Reset failure state - new file activity should trigger fresh validation attempt
                                if failed_validations.pop(&event_dir).is_some() {
                                    debug!("Reset validation backoff for {} due to new file activity", event_dir.display());
                                }

                                if let Some(pending) = pending_dirs.get_mut(&event_dir) {
                                    if !pending.notified {
                                        // Reset debounce timer if not yet notified
                                        pending.last_event = Instant::now();
                                        debug!("Debounce reset for verified path: {}", event_dir.display());
                                    } else if let Some(notified_at) = pending.notified_at {
                                        // Allow re-validation if notified more than threshold ago
                                        if now.duration_since(notified_at) >= std::time::Duration::from_secs(revalidation_threshold_secs) {
                                            pending.notified = false;
                                            pending.notified_at = None;
                                            pending.last_event = Instant::now();
                                            info!("Re-validation triggered for {} (new event {}s after notification)",
                                                  event_dir.display(), now.duration_since(notified_at).as_secs());
                                        }
                                    }
                                } else {
                                    // New pending directory
                                    pending_dirs.put(event_dir.clone(), PendingDirectory {
                                        last_event: Instant::now(),
                                        notified: false,
                                        notified_at: None,
                                    });
                                    debug!("Added pending directory (verified): {}", event_dir.display());
                                }
                            }
                        }
                    } else if is_structural_change {
                        // UNVERIFIED PATH: Notify immediately (no SFV required)
                        debug!("Unverified path, notifying immediately (is_deletion={}) for: {}",
                               is_deletion_event, full_path.display());
                        Self::notify_server(&emby_tx, &full_path, is_deletion_event, "Emby");
                        Self::notify_server(&jellyfin_tx, &full_path, is_deletion_event, "Jellyfin");
                        Self::notify_server(&plex_tx, &full_path, is_deletion_event, "Plex");
                    }

                    // Handle directory creation or move - watch new directories
                    if event.mask.contains(EventMask::ISDIR) &&
                       (event.mask.contains(EventMask::CREATE) || event.mask.contains(EventMask::MOVED_TO)) {
                        // Skip excluded directories
                        if is_excluded_dir(exclude_dirs, &full_path) {
                            debug!("Skipping excluded directory: {}", full_path.display());
                            continue;
                        }

                        let watch_count_before = watch_descriptors.len();
                        info!("New directory detected: {} (adding watches...)", full_path.display());
                        match Self::add_watch_recursive(&mut inotify, &mut watch_descriptors, &full_path, max_watches, exclude_dirs) {
                            Ok(()) => {
                                let watches_added = watch_descriptors.len() - watch_count_before;
                                info!("  Added {} watches for new directory (total: {})", watches_added, watch_descriptors.len());
                                if let Some(m) = metrics {
                                    m.inotify_watches.store(watch_descriptors.len() as u64, Ordering::Relaxed);
                                }

                                // Race condition fix: check if directory already has content
                                // Files may have been written before watch was added
                                if full_path.starts_with(verified_path)
                                    && Self::find_sfv_file(&full_path).is_some()
                                        && !pending_dirs.contains(&full_path) {
                                            pending_dirs.put(full_path.clone(), PendingDirectory {
                                                last_event: Instant::now(),
                                                notified: false,
                                                notified_at: None,
                                            });
                                            info!("  Queued for validation (has SFV): {}", full_path.display());
                                        }
                            }
                            Err(e) => {
                                warn!("Failed to watch new directory {}: {}", full_path.display(), e);
                            }
                        }
                    }

                    // Handle directory deletion - clean up watches and notify media servers
                    if event.mask.contains(EventMask::DELETE_SELF) {
                        // The watched directory itself was deleted - remove its watch
                        watch_descriptors.remove(&event.wd);
                        pending_dirs.pop(&full_path);
                        failed_validations.pop(&full_path);
                        // Notify media servers that this directory is gone
                        Self::notify_server(&emby_tx, &full_path, true, "Emby");
                        Self::notify_server(&jellyfin_tx, &full_path, true, "Jellyfin");
                        Self::notify_server(&plex_tx, &full_path, true, "Plex");
                    } else if event.mask.contains(EventMask::DELETE) && event.mask.contains(EventMask::ISDIR) {
                        // A subdirectory was deleted - only clean up pending_dirs
                        // DON'T remove from watch_descriptors (event.wd is the parent's watch!)
                        // The deleted directory's watch will be cleaned up by its own DELETE_SELF event
                        pending_dirs.pop(&full_path);
                        failed_validations.pop(&full_path);
                        // Notify media servers that this subdirectory is gone
                        Self::notify_server(&emby_tx, &full_path, true, "Emby");
                        Self::notify_server(&jellyfin_tx, &full_path, true, "Jellyfin");
                        Self::notify_server(&plex_tx, &full_path, true, "Plex");
                    }
                }
            }

            // Periodic cleanup - runs every CLEANUP_INTERVAL_SECS (60s)
            // This reduces CPU overhead from O(n) per second to O(n) per minute
            // while still allowing timely removal of stale entries
            if last_cleanup.elapsed().as_secs() >= CLEANUP_INTERVAL_SECS {
                last_cleanup = Instant::now();

                // Clean up old failure records to prevent memory growth
                // LruCache doesn't have retain, so collect keys to remove
                let failure_keys_to_remove: Vec<PathBuf> = failed_validations
                    .iter()
                    .filter(|(_, failed)| {
                        now.duration_since(failed.last_attempt) >= std::time::Duration::from_secs(FAILURE_CLEANUP_THRESHOLD_SECS)
                    })
                    .map(|(path, _)| path.clone())
                    .collect();
                for key in failure_keys_to_remove {
                    failed_validations.pop(&key);
                    if let Some(m) = metrics { m.clear_stuck(&key); }
                }

                // Clean up old notified entries from pending_dirs to prevent memory growth
                let cleanup_threshold = std::time::Duration::from_secs(pending_cleanup_secs);
                let before_cleanup = pending_dirs.len();
                let pending_keys_to_remove: Vec<PathBuf> = pending_dirs
                    .iter()
                    .filter(|(path, pending)| {
                        if pending.notified {
                            if let Some(notified_at) = pending.notified_at {
                                let should_remove = now.duration_since(notified_at) >= cleanup_threshold;
                                if should_remove {
                                    debug!("Cleaning up old notified entry: {}", path.display());
                                }
                                should_remove
                            } else {
                                false // Keep if notified but no timestamp (shouldn't happen)
                            }
                        } else {
                            false // Keep entries that haven't been notified yet
                        }
                    })
                    .map(|(path, _)| path.clone())
                    .collect();
                for key in pending_keys_to_remove {
                    pending_dirs.pop(&key);
                }
                let cleaned = before_cleanup - pending_dirs.len();
                if cleaned > 0 {
                    debug!("Cleaned up {} old pending_dirs entries (remaining: {})", cleaned, pending_dirs.len());
                }

                // Clean up old temp path log entries (paths not seen for 60+ seconds)
                let temp_keys_to_remove: Vec<PathBuf> = temp_path_last_logged
                    .iter()
                    .filter(|(_, last)| last.elapsed().as_secs() >= 60)
                    .map(|(path, _)| path.clone())
                    .collect();
                for key in temp_keys_to_remove {
                    temp_path_last_logged.pop(&key);
                }
            }

            // No sleep needed - poll() already has 1000ms timeout
        }
    }

    /// Translate a source layer path (verified/unverified) to the rar2fs backend path
    /// This is needed because the dir_cache uses backend paths as keys
    fn translate_to_backend_path(
        source_path: &Path,
        verified_path: &Path,
        unverified_path: &Path,
        backend_path: &Path,
    ) -> PathBuf {
        // Try to strip verified path prefix first
        if let Ok(relative) = source_path.strip_prefix(verified_path) {
            return backend_path.join(relative);
        }
        // Try unverified path prefix
        if let Ok(relative) = source_path.strip_prefix(unverified_path) {
            return backend_path.join(relative);
        }
        // Fallback - return as-is (shouldn't happen in normal operation)
        source_path.to_path_buf()
    }

    /// Find SFV file in a directory
    fn find_sfv_file(dir_path: &Path) -> Option<PathBuf> {
        if let Ok(entries) = std::fs::read_dir(dir_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if is_sfv_file(&path) {
                    return Some(path);
                }
            }
        }
        None
    }

    /// Recursively add inotify watches for a directory tree with depth and count limits
    fn add_watch_recursive_limited(
        inotify: &mut Inotify,
        watch_map: &mut HashMap<inotify::WatchDescriptor, PathBuf>,
        path: &Path,
        current_depth: usize,
        watch_count: &mut usize,
        limits: &WatchLimits,
    ) -> Result<()> {
        if !path.exists() {
            return Ok(());
        }

        // Check depth limit
        if current_depth >= limits.max_depth {
            debug!("Reached max depth {} at path {}", limits.max_depth, path.display());
            return Ok(());
        }

        // Check watch count limit
        if *watch_count >= limits.max_watches {
            debug!("Reached max watch count {} at path {}", limits.max_watches, path.display());
            return Ok(());
        }

        // Watch this directory
        let wd = inotify.watches().add(
            path,
            WatchMask::CREATE
                | WatchMask::DELETE
                | WatchMask::DELETE_SELF
                | WatchMask::MODIFY
                | WatchMask::MOVED_FROM
                | WatchMask::MOVED_TO
                | WatchMask::ATTRIB,
        )?;

        watch_map.insert(wd, path.to_path_buf());
        *watch_count += 1;

        // Recursively watch subdirectories
        if path.is_dir() {
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries.flatten() {
                    if let Ok(metadata) = entry.metadata() {
                        if metadata.is_dir() {
                            // Skip excluded directories
                            if is_excluded_dir(limits.exclude_dirs, &entry.path()) {
                                continue;
                            }
                            Self::add_watch_recursive_limited(
                                inotify,
                                watch_map,
                                &entry.path(),
                                current_depth + 1,
                                watch_count,
                                limits,
                            )?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Wrapper for add_watch_recursive (for newly created directories)
    fn add_watch_recursive(
        inotify: &mut Inotify,
        watch_map: &mut HashMap<inotify::WatchDescriptor, PathBuf>,
        path: &Path,
        max_watches: usize,
        exclude_dirs: &[String],
    ) -> Result<()> {
        let mut watch_count = watch_map.len();
        const MAX_DEPTH: usize = 10;
        let limits = WatchLimits { max_depth: MAX_DEPTH, max_watches, exclude_dirs };
        Self::add_watch_recursive_limited(inotify, watch_map, path, 0, &mut watch_count, &limits)
    }

    /// Send cache invalidation notification
    fn invalidate_cache(
        cache_tx: &Arc<OnceLock<tokio::sync::mpsc::UnboundedSender<PathBuf>>>,
        path: &Path,
    ) {
        // OnceLock: no lock needed — sender is immutable after startup
        if let Some(tx) = cache_tx.get() {
            if let Err(e) = tx.send(path.to_path_buf()) {
                warn!("Failed to send cache invalidation for {:?}: {}", path, e);
            }
        }
    }

    /// Refresh overlay cache by stat'ing the corresponding path in the merged directory
    /// This forces the kernel to re-read the directory from the underlying layers,
    /// making files written directly to the verified upperdir visible in the overlay
    fn refresh_overlay_cache(
        source_path: &Path,
        verified_path: &Path,
        overlay_merged_path: Option<&Path>,
        refresh_last: &mut LruCache<PathBuf, Instant>,
    ) {
        // Coalesce bursts: skip if this directory was already refreshed very recently.
        const OVERLAY_REFRESH_DEDUPE_SECS: u64 = 3;

        let Some(merged_path) = overlay_merged_path else {
            return; // No overlay configured
        };

        // Only refresh for paths in the verified layer
        let Ok(relative) = source_path.strip_prefix(verified_path) else {
            return;
        };

        // Drop redundant re-readdirs of the same dir within the dedupe window.
        let recently = refresh_last
            .get(source_path)
            .map(|last| last.elapsed().as_secs() < OVERLAY_REFRESH_DEDUPE_SECS)
            .unwrap_or(false);
        if recently {
            return;
        }
        refresh_last.put(source_path.to_path_buf(), Instant::now());

        let overlay_path = merged_path.join(relative);

        // Stat + readdir the overlay path to force kernel dentry cache refresh
        // metadata() refreshes the inode, read_dir() forces re-enumeration of contents
        match std::fs::metadata(&overlay_path) {
            Ok(meta) => {
                if meta.is_dir() {
                    // Force directory re-read by iterating entries
                    if let Ok(entries) = std::fs::read_dir(&overlay_path) {
                        let count = entries.count();
                        debug!("Refreshed overlay cache for: {} ({} entries)", overlay_path.display(), count);
                    }
                } else {
                    debug!("Refreshed overlay cache for: {}", overlay_path.display());
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Path doesn't exist in overlay yet, try parent directory
                if let Some(parent) = overlay_path.parent() {
                    if let Ok(meta) = std::fs::metadata(parent) {
                        if meta.is_dir() {
                            if let Ok(entries) = std::fs::read_dir(parent) {
                                let count = entries.count();
                                debug!("Refreshed overlay cache for parent: {} ({} entries)", parent.display(), count);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                debug!("Could not refresh overlay cache for {}: {}", overlay_path.display(), e);
            }
        }
    }

    /// Notify a media server of a file change (creation or deletion).
    /// Skips temporary download-client paths. Uses OnceLock — no lock needed after startup.
    fn notify_server(
        handle: &Arc<OnceLock<NotifierHandle>>,
        path: &Path,
        is_deletion: bool,
        server_name: &'static str,
    ) {
        if is_temporary_path(path) {
            debug!("Skipping {} notification for temporary path: {}", server_name, path.display());
            return;
        }
        let Some(handle) = handle.get() else { return };
        // OnceLock: no lock needed — handle is immutable after startup
        let event = if is_deletion {
            MediaEvent::Deleted(path.to_path_buf())
        } else {
            MediaEvent::Created(path.to_path_buf())
        };
        debug!("Sending {} notification (is_deletion={}) for: {}", server_name, is_deletion, event.path().display());
        let handle = handle.clone();
        tokio::spawn(async move { handle.notify(event).await; });
    }
}

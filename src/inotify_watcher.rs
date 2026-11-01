//! `InotifyWatcher` — watches the source-overlay layers for filesystem activity and
//! drives two downstream effects:
//!
//! 1. **Cache invalidation** — when a directory finishes uploading (debounced),
//!    invalidates the matching entry in `RarGateFs`'s validated-directory cache so
//!    the next `readdir` re-checks SFV.
//! 2. **Media-server notifications** — emits `MediaEvent::Created` / `Deleted` to the
//!    Emby / Jellyfin / Plex notifiers via their `NotifierHandle`s.
//!
//! The loop itself is `WatchLoop`; its per-directory state lives in bounded LRUs so
//! memory can't grow without limit.

use anyhow::Result;
use inotify::{Event, EventMask, Events, Inotify, WatchDescriptor, WatchMask};
use lru::LruCache;
use nix::poll::{poll, PollFd, PollFlags, PollTimeout};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::num::NonZeroUsize;
use std::os::fd::AsFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
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

impl PendingDirectory {
    fn new() -> Self {
        Self { last_event: Instant::now(), notified: false, notified_at: None }
    }
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

/// Default consecutive failures before escalating a REGRESSION — a directory that was
/// already a confirmed, announced media dir and has now started failing validation
/// (files deleted/moved out from under it, not a normal still-downloading release).
/// Escalates as soon as it enters cooldown at all, rather than waiting through many
/// cooldown cycles like a fresh/never-seen directory does.
const DEFAULT_REGRESSION_ESCALATION_THRESHOLD: u32 = 3;

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

/// How recently a partial-download file (`.dctmp`, `.part`, `.!qB`, ...) must have
/// been written for its release to count as still downloading. A long multi-volume
/// grab can leave one volume idle for hours while another is being written, so this
/// keys off the NEWEST partial file in the directory. Past it, the download has
/// genuinely stalled and the release is reported as stuck like any other.
const ACTIVE_DOWNLOAD_GRACE_SECS: u64 = 6 * 3600;

/// True if `dir` holds a partial-download file written within `grace` — i.e. the
/// release is still being downloaded, so failing SFV validation is expected (most
/// volumes simply aren't there yet) and must not be reported as stuck.
fn has_active_download(dir: &Path, grace: std::time::Duration) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else { return false };
    entries.flatten().any(|entry| {
        is_temporary_path(&entry.path())
            && entry
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.elapsed().ok())
                .is_some_and(|age| age < grace)
    })
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
    /// Same as escalation_threshold, but for a directory that regressed from an
    /// already-confirmed media dir (see DEFAULT_REGRESSION_ESCALATION_THRESHOLD).
    regression_escalation_threshold: u32,
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
    notifiers: Notifiers,
}

/// The notifier handles the watch loop sends to. Each is set at most once,
/// before start_watching(), so reads need no lock.
#[derive(Clone, Default)]
struct Notifiers {
    emby: Arc<OnceLock<NotifierHandle>>,
    jellyfin: Arc<OnceLock<NotifierHandle>>,
    plex: Arc<OnceLock<NotifierHandle>>,
    arr: Arc<OnceLock<NotifierHandle>>,
}

impl Notifiers {
    /// Tell Emby, Jellyfin and Plex that `path` appeared or went away.
    fn media_servers(&self, path: &Path, is_deletion: bool) {
        notify(&self.emby, path, is_deletion, "Emby");
        notify(&self.jellyfin, path, is_deletion, "Jellyfin");
        notify(&self.plex, path, is_deletion, "Plex");
    }

    /// Ask Sonarr/Radarr to rescan a newly validated release.
    fn arr(&self, path: &Path) {
        notify(&self.arr, path, false, "Arr");
    }
}

/// Send one notifier a creation or deletion event, unless it isn't configured or
/// `path` is a download client's temporary file.
fn notify(handle: &OnceLock<NotifierHandle>, path: &Path, is_deletion: bool, server_name: &'static str) {
    if is_temporary_path(path) {
        debug!("Skipping {} notification for temporary path: {}", server_name, path.display());
        return;
    }
    let Some(handle) = handle.get() else { return };
    let event = if is_deletion {
        MediaEvent::Deleted(path.to_path_buf())
    } else {
        MediaEvent::Created(path.to_path_buf())
    };
    debug!("Sending {} notification (is_deletion={}) for: {}", server_name, is_deletion, event.path().display());
    let handle = handle.clone();
    tokio::spawn(async move { handle.notify(event).await; });
}

/// The watch loop's configuration. All fields are owned so it can be moved into
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
    regression_escalation_threshold: u32,
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
        let regression_escalation_threshold = validation_backoff
            .and_then(|b| b.regression_escalate_after)
            .unwrap_or(DEFAULT_REGRESSION_ESCALATION_THRESHOLD)
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
            regression_escalation_threshold,
            sfv_debounce_secs,
            pending_cleanup_secs,
            revalidation_threshold_secs,
            metrics: None,
            cache_invalidation_tx: Arc::new(OnceLock::new()),
            notifiers: Notifiers::default(),
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
        let _ = self.notifiers.emby.set(handle);
        info!("Emby notifier connected to inotify watcher");
    }

    /// Registers the Jellyfin handle. Must be called before start_watching().
    pub fn set_jellyfin_handle(&self, handle: NotifierHandle) {
        let _ = self.notifiers.jellyfin.set(handle);
        info!("Jellyfin notifier connected to inotify watcher");
    }

    /// Registers the Plex handle. Must be called before start_watching().
    pub fn set_plex_handle(&self, handle: NotifierHandle) {
        let _ = self.notifiers.plex.set(handle);
        info!("Plex notifier connected to inotify watcher");
    }

    /// Registers the Sonarr/Radarr rescan handle. Must be called before start_watching().
    pub fn set_arr_handle(&self, handle: NotifierHandle) {
        let _ = self.notifiers.arr.set(handle);
        info!("Arr (Sonarr/Radarr) notifier connected to inotify watcher");
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
        info!("  SFV regression escalation: loud ERROR + status flag after {} failures \
               for a previously-confirmed media dir", self.regression_escalation_threshold);

        let ctx = WatchLoopContext {
            verified_path: self.verified_path.clone(),
            unverified_path: self.unverified_path.clone(),
            overlay_merged_path: self.overlay_merged_path.clone(),
            rar2fs_backend_path: self.rar2fs_backend_path.clone(),
            exclude_dirs: self.exclude_dirs.clone(),
            max_validation_failures: self.max_validation_failures,
            validation_cooldown_secs: self.validation_cooldown_secs,
            escalation_threshold: self.escalation_threshold,
            regression_escalation_threshold: self.regression_escalation_threshold,
            sfv_debounce_secs: self.sfv_debounce_secs,
            pending_cleanup_secs: self.pending_cleanup_secs,
            revalidation_threshold_secs: self.revalidation_threshold_secs,
            metrics: self.metrics.clone(),
        };
        let cache_tx = self.cache_invalidation_tx.clone();
        let notifiers = self.notifiers.clone();

        let handle = tokio::task::spawn_blocking(move || {
            match WatchLoop::new(ctx, cache_tx, notifiers) {
                Ok(watch_loop) => watch_loop.run(),
                Err(e) => error!("Inotify watch loop failed: {}", e),
            }
        });

        Ok(handle)
    }
}

/// Interval for the "skipping temporary path" debug line, per path.
const TEMP_PATH_LOG_INTERVAL_SECS: u64 = 5;

/// How long one wait for inotify events lasts; the debounce checks run at least this often.
const POLL_TIMEOUT_MS: u16 = 1000;

/// Directory depth below each watched layer that gets inotify watches.
const MAX_WATCH_DEPTH: usize = 10;

fn lru<V>(size: usize) -> LruCache<PathBuf, V> {
    LruCache::new(NonZeroUsize::new(size).unwrap())
}

/// The watch loop and the state it keeps between iterations. Runs on a blocking thread.
///
/// Each iteration:
///   1. waits up to a second for inotify events;
///   2. SFV-validates every pending release directory whose files have been quiet
///      for the debounce window — a pass announces it to the media servers, a
///      failure backs off and, if it persists, reports the release as stuck;
///   3. handles the events: cache invalidation, overlay refresh, queueing release
///      directories for validation, watching new directories, forgetting deleted ones;
///   4. once a minute, prunes the bounded caches.
struct WatchLoop {
    ctx: WatchLoopContext,
    cache_tx: Arc<OnceLock<tokio::sync::mpsc::UnboundedSender<PathBuf>>>,
    notifiers: Notifiers,
    inotify: Inotify,
    /// Watched directory for each watch descriptor.
    watch_descriptors: HashMap<WatchDescriptor, PathBuf>,
    max_watches: usize,
    /// Release directories waiting for SFV validation once their files settle.
    pending_dirs: LruCache<PathBuf, PendingDirectory>,
    /// Directories failing SFV validation, for backoff and stuck reporting.
    failed_validations: LruCache<PathBuf, FailedValidation>,
    /// When the "skipping temporary path" debug line was last logged, per path.
    temp_path_last_logged: LruCache<PathBuf, Instant>,
    /// Coalesces overlay-cache refreshes of the same directory. A release landing
    /// fires one event per file, each of which would otherwise re-read the same
    /// directory through rar2fs (see `refresh_overlay_cache`).
    overlay_refresh_last: LruCache<PathBuf, Instant>,
    /// Directories already announced as media. Outlives `pending_dirs`, so a later
    /// change (a subtitle added) re-notifies quietly instead of re-announcing
    /// "New media dir", and a validation failure here counts as a regression.
    announced_dirs: LruCache<PathBuf, Instant>,
    last_cleanup: Instant,
}

impl WatchLoop {
    /// Set up inotify and watch both overlay layers recursively, within the system's watch limit.
    fn new(
        ctx: WatchLoopContext,
        cache_tx: Arc<OnceLock<tokio::sync::mpsc::UnboundedSender<PathBuf>>>,
        notifiers: Notifiers,
    ) -> Result<Self> {
        let mut inotify = Inotify::init()?;
        let mut watch_descriptors = HashMap::new();
        let (max_watches, system_limit) = Self::get_max_watches();
        let limits = WatchLimits { max_depth: MAX_WATCH_DEPTH, max_watches, exclude_dirs: &ctx.exclude_dirs };
        let mut watch_count = 0;
        Self::add_watch_recursive_limited(&mut inotify, &mut watch_descriptors, &ctx.verified_path, 0, &mut watch_count, &limits)?;
        Self::add_watch_recursive_limited(&mut inotify, &mut watch_descriptors, &ctx.unverified_path, 0, &mut watch_count, &limits)?;

        if watch_count >= max_watches {
            let percentage = (max_watches * 100) / system_limit;
            warn!(
                "Reached watch limit ({}/{}, {}% of system limit {}), some directories not monitored",
                watch_count, max_watches, percentage, system_limit
            );
        }
        info!("Inotify watching {} paths ({} watches used, {} available)",
              watch_descriptors.len(), watch_count, max_watches - watch_count);
        if let Some(m) = &ctx.metrics {
            m.inotify_watches.store(watch_count as u64, Ordering::Relaxed);
        }

        Ok(Self {
            ctx,
            cache_tx,
            notifiers,
            inotify,
            watch_descriptors,
            max_watches,
            pending_dirs: lru(LRU_PENDING_DIRS_SIZE),
            failed_validations: lru(LRU_FAILED_VALIDATIONS_SIZE),
            temp_path_last_logged: lru(LRU_TEMP_PATH_LOG_SIZE),
            overlay_refresh_last: lru(LRU_TEMP_PATH_LOG_SIZE),
            announced_dirs: lru(LRU_PENDING_DIRS_SIZE),
            last_cleanup: Instant::now(),
        })
    }

    fn run(mut self) -> ! {
        let mut buffer = [0; 4096];
        loop {
            let events = self.wait_for_events(&mut buffer);
            let now = Instant::now();
            self.validate_ready_dirs(now);
            for event in events.into_iter().flatten() {
                self.handle_event(event, now);
            }
            if self.last_cleanup.elapsed().as_secs() >= CLEANUP_INTERVAL_SECS {
                self.last_cleanup = Instant::now();
                self.periodic_cleanup(now);
            }
        }
    }

    /// Wait up to POLL_TIMEOUT_MS for inotify events. Returning on the timeout
    /// keeps the debounce checks running while nothing happens on disk.
    fn wait_for_events<'a>(&mut self, buffer: &'a mut [u8]) -> Option<Events<'a>> {
        let mut poll_fds = [PollFd::new(self.inotify.as_fd(), PollFlags::POLLIN)];
        match poll(&mut poll_fds, PollTimeout::from(POLL_TIMEOUT_MS)) {
            Ok(n) if n > 0 => self.inotify.read_events(buffer).ok(),
            Ok(_) => None,
            Err(e) => {
                warn!("Poll error on inotify fd: {}", e);
                None
            }
        }
    }

    // ── SFV validation of settled release directories ──────────────────────

    /// Validate each pending directory whose files have been quiet for the
    /// debounce window and that isn't backing off after earlier failures.
    fn validate_ready_dirs(&mut self, now: Instant) {
        let debounce = Duration::from_secs(self.ctx.sfv_debounce_secs);
        let ready: Vec<PathBuf> = self.pending_dirs
            .iter()
            .filter(|(_, pending)| !pending.notified && now.duration_since(pending.last_event) >= debounce)
            .map(|(path, _)| path.clone())
            .collect();

        for dir_path in ready {
            if self.in_backoff(&dir_path, now) {
                continue;
            }
            let Some(sfv_path) = Self::find_sfv_file(&dir_path) else { continue };
            let started = Instant::now();
            let valid = validate_sfv_with_completeness(&sfv_path);
            let took = started.elapsed();
            debug!("SFV validation for {} took {:.1}ms (result: {})",
                   dir_path.display(), took.as_secs_f64() * 1000.0, valid);
            if valid {
                self.on_validation_passed(&dir_path, took, now);
            } else {
                self.on_validation_failed(&dir_path, took, now);
            }
        }
    }

    /// Still backing off after failed validations? Below `max_validation_failures`
    /// retries back off exponentially (5s → 15s → 45s, capped at the cooldown), so a
    /// fresh failure isn't retried on every loop iteration; from there on, at most
    /// one retry per `validation_cooldown_secs`.
    fn in_backoff(&mut self, dir_path: &Path, now: Instant) -> bool {
        let cooldown = self.ctx.validation_cooldown_secs;
        let max_failures = self.ctx.max_validation_failures;
        let Some(failed) = self.failed_validations.get(dir_path) else { return false };
        let wait_secs = if failed.failure_count >= max_failures {
            cooldown
        } else {
            5u64.saturating_mul(3u64.saturating_pow(failed.failure_count.saturating_sub(1)))
                .min(cooldown)
        };
        now.duration_since(failed.last_attempt) < Duration::from_secs(wait_secs)
    }

    /// The SFV checks out: clear any failure state, refresh the overlay so the
    /// files are visible, and announce the release.
    fn on_validation_passed(&mut self, dir_path: &Path, took: Duration, now: Instant) {
        self.failed_validations.pop(dir_path);
        if let Some(m) = &self.ctx.metrics { m.clear_stuck(dir_path); }

        Self::refresh_overlay_cache(dir_path, &self.ctx.verified_path, self.ctx.overlay_merged_path.as_deref(), &mut self.overlay_refresh_last);

        // A passing SFV implies a valid RAR set — the "media directory" signal.
        // First sighting announces new media. A later change to an already-known
        // dir (e.g. a subtitle added) logs one clear "changed" line and refreshes
        // the servers, but is rate-limited so the burst of events from a single
        // subtitle add/sync collapses into one line instead of spamming.
        let (should_notify, is_first_announce) = match self.announced_dirs.get(dir_path).copied() {
            None => {
                info!("SFV validation passed for {} (took {:.1}ms)",
                      dir_path.display(), took.as_secs_f64() * 1000.0);
                info!("New media dir: {}", dir_path.display());
                (true, true)
            }
            Some(t) if now.duration_since(t) >= Duration::from_secs(RE_NOTIFY_MIN_INTERVAL_SECS) => {
                info!("Media dir changed, re-notifying media servers: {}", dir_path.display());
                (true, false)
            }
            Some(_) => {
                // Within the rate-limit window — same operation's churn, skip.
                debug!("Skipping duplicate re-notification (recent) for {}", dir_path.display());
                (false, false)
            }
        };

        if should_notify {
            self.announced_dirs.put(dir_path.to_path_buf(), now);
            self.notifiers.media_servers(dir_path, false);
            // Sonarr/Radarr get a rescan ONLY on the first announce — that's the
            // authoritative "the release is now visible, import it in place"
            // moment. A later change to an already-imported release (a subtitle
            // or metadata sidecar landing, courtesy of bazarr et al.) has no
            // *arr relevance: they don't manage external subs, and the rescan
            // races the overlay-cache refresh — Sonarr's scan can briefly not
            // see the media file and mark it MissingFromDisk. Media servers
            // still get poked so external subs show up promptly.
            if is_first_announce {
                self.notifiers.arr(dir_path);
            }
        }

        // Mark as notified with timestamp for cleanup/re-validation
        if let Some(pending) = self.pending_dirs.get_mut(dir_path) {
            pending.notified = true;
            pending.notified_at = Some(Instant::now());
        }
    }

    /// The SFV doesn't check out: count the failure for backoff. Past
    /// `max_validation_failures` the release is reported as stuck (throttled WARNs,
    /// then one escalation ERROR) unless it's evidently still downloading.
    fn on_validation_failed(&mut self, dir_path: &Path, took: Duration, now: Instant) {
        let max_failures = self.ctx.max_validation_failures;
        let now_wall = std::time::SystemTime::now();
        let (failure_count, first_seen) =
            if let Some(existing) = self.failed_validations.get_mut(dir_path) {
                existing.failure_count += 1;
                existing.last_attempt = now;
                (existing.failure_count, existing.first_seen)
            } else {
                self.failed_validations.put(dir_path.to_path_buf(), FailedValidation {
                    last_attempt: now,
                    failure_count: 1,
                    first_seen: now_wall,
                    last_milestone_logged: 0,
                    escalated: false,
                });
                (1, now_wall)
            };

        if failure_count < max_failures {
            debug!("SFV validation failed for {} (attempt {}/{}, took {:.1}ms), will retry after next file event",
                   dir_path.display(), failure_count, max_failures, took.as_secs_f64() * 1000.0);
            return;
        }

        if has_active_download(dir_path, Duration::from_secs(ACTIVE_DOWNLOAD_GRACE_SECS)) {
            // Still being downloaded — missing volumes are expected, not a
            // stuck release. Backoff/cooldown above still applies; only the
            // WARN/ERROR escalation and the status-file flag are skipped.
            debug!("SFV validation failing for {} ({} times) but a download is still in progress, not flagging as stuck",
                   dir_path.display(), failure_count);
            if let Some(m) = &self.ctx.metrics { m.clear_stuck(dir_path); }
            return;
        }

        self.report_stuck(dir_path, failure_count, first_seen);
    }

    /// Log a persistently failing release — WARN at milestone failure counts, one
    /// escalation ERROR past the threshold — and list it in the status file.
    fn report_stuck(&mut self, dir_path: &Path, failure_count: u32, first_seen: std::time::SystemTime) {
        let stuck_for = humanize_duration(first_seen.elapsed().unwrap_or_default());
        // Read prior throttle state without bumping LRU recency.
        let (prev_milestone, already_escalated) = self.failed_validations
            .peek(dir_path)
            .map(|f| (f.last_milestone_logged, f.escalated))
            .unwrap_or((0, false));

        // A directory already in announced_dirs passed validation once — now
        // failing means its files were very likely deleted/moved out from under
        // it (not a normal still-downloading release), so it escalates on a much
        // shorter fuse. peek() to avoid bumping LRU recency for a status check.
        let is_regression = self.announced_dirs.peek(dir_path).is_some();
        let escalate_at = if is_regression {
            self.ctx.regression_escalation_threshold
        } else {
            self.ctx.escalation_threshold
        };

        if already_escalated {
            debug!("SFV validation still failing (already escalated) for {} ({} times)",
                   dir_path.display(), failure_count);
        } else if failure_count >= escalate_at {
            // One loud, distinct line — easy to spot in the log — then go quiet
            // (the status file carries it from here).
            if is_regression {
                error!("🛑 REGRESSED MEDIA DIR: {} — was previously valid, now failing SFV validation {} times over {}; files were very likely deleted or moved. Re-grab or remove it.",
                       dir_path.display(), failure_count, stuck_for);
            } else {
                error!("🛑 PERSISTENT SFV FAILURE: {} — failed {} times over {}; likely an incomplete or abandoned download. Re-grab or remove it.",
                       dir_path.display(), failure_count, stuck_for);
            }
            if let Some(f) = self.failed_validations.get_mut(dir_path) {
                f.escalated = true;
                f.last_milestone_logged = failure_count;
            }
        } else {
            // Throttle WARNs to milestone crossings (3, 10, 50, 100, ...)
            // instead of one per cooldown cycle.
            let milestone = sfv_failure_milestone(failure_count);
            if milestone > prev_milestone {
                warn!("SFV validation failed {} times for {} (stuck {}), entering cooldown ({}s) - check for missing or incomplete files",
                      failure_count, dir_path.display(), stuck_for, self.ctx.validation_cooldown_secs);
                if let Some(f) = self.failed_validations.get_mut(dir_path) {
                    f.last_milestone_logged = milestone;
                }
            } else {
                debug!("SFV validation still failing for {} ({} times, stuck {}), in cooldown",
                       dir_path.display(), failure_count, stuck_for);
            }
        }

        // Surface to the status file so operators have one place to look.
        if let Some(m) = &self.ctx.metrics {
            m.record_stuck(dir_path, failure_count, first_seen);
        }
    }

    // ── inotify events ──────────────────────────────────────────────────────

    fn handle_event(&mut self, event: Event<&OsStr>, now: Instant) {
        if event.mask.contains(EventMask::Q_OVERFLOW) {
            self.recover_from_overflow();
            return;
        }
        if let Some(m) = &self.ctx.metrics {
            m.inotify_events.fetch_add(1, Ordering::Relaxed);
        }

        let Some(watched_dir) = self.watch_descriptors.get(&event.wd) else { return };
        let full_path = match event.name {
            Some(name) => watched_dir.join(name),
            None => watched_dir.clone(),
        };
        if is_temporary_path(&full_path) {
            self.log_skipped_temporary(&full_path);
            return;
        }
        debug!("inotify event: {:?} on {:?}", event.mask, full_path);

        // The FUSE directory cache is keyed by backend path; invalidate on any change.
        let backend_path = Self::translate_to_backend_path(
            &full_path,
            &self.ctx.verified_path,
            &self.ctx.unverified_path,
            &self.ctx.rar2fs_backend_path,
        );
        Self::invalidate_cache(&self.cache_tx, &backend_path);

        let is_verified_path = full_path.starts_with(&self.ctx.verified_path);
        if is_verified_path {
            // Files written straight into the verified upperdir only show in the overlay
            // once it re-reads the directory. Refresh the containing directory (not the
            // file) so a release's per-file events coalesce onto one dedupe key.
            if let Some(parent) = full_path.parent() {
                Self::refresh_overlay_cache(parent, &self.ctx.verified_path, self.ctx.overlay_merged_path.as_deref(), &mut self.overlay_refresh_last);
            }
        }

        let is_dir = event.mask.contains(EventMask::ISDIR);
        let event_dir = if is_dir {
            full_path.clone()
        } else {
            full_path.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| full_path.clone())
        };
        // Only structural changes (create, delete, move) notify, not modifications.
        let is_structural_change = event.mask.contains(EventMask::CREATE)
            || event.mask.contains(EventMask::DELETE)
            || event.mask.contains(EventMask::MOVED_FROM)
            || event.mask.contains(EventMask::MOVED_TO);
        let is_deletion = event.mask.contains(EventMask::DELETE)
            || event.mask.contains(EventMask::MOVED_FROM)
            || event.mask.contains(EventMask::DELETE_SELF);

        if is_structural_change {
            if !is_verified_path {
                // Unverified layer: no SFV required, notify immediately.
                debug!("Unverified path, notifying immediately (is_deletion={}) for: {}",
                       is_deletion, full_path.display());
                self.notifiers.media_servers(&full_path, is_deletion);
            } else if is_deletion {
                self.on_verified_deletion(&full_path, &event_dir, is_dir);
            } else {
                // Verified layer: creations wait for SFV validation.
                self.mark_pending(&event_dir, now);
            }
        }

        if is_dir && (event.mask.contains(EventMask::CREATE) || event.mask.contains(EventMask::MOVED_TO)) {
            self.watch_new_dir(&full_path);
        }

        if event.mask.contains(EventMask::DELETE_SELF) {
            // The watched directory itself was deleted - remove its watch
            self.watch_descriptors.remove(&event.wd);
            self.forget_dir(&full_path);
        } else if event.mask.contains(EventMask::DELETE) && is_dir {
            // A subdirectory was deleted. Its watch is NOT removed here (event.wd is
            // the parent's watch); the subdirectory's own DELETE_SELF does that.
            self.forget_dir(&full_path);
        }
    }

    /// The kernel's inotify queue overflowed and events were lost. Start over:
    /// drop pending and failure state and queue every watched verified directory
    /// that has an SFV for validation.
    fn recover_from_overflow(&mut self) {
        if let Some(m) = &self.ctx.metrics {
            m.inotify_overflows.fetch_add(1, Ordering::Relaxed);
        }
        let max_queued = std::fs::read_to_string("/proc/sys/fs/inotify/max_queued_events")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(16384);
        warn!("inotify queue overflow detected - events may have been lost \
               (current max_queued_events={}; consider raising it via \
               sysctl fs.inotify.max_queued_events={})", max_queued, max_queued * 2);
        info!("Clearing pending_dirs and triggering full re-scan of watched verified paths");

        self.pending_dirs.clear();
        self.failed_validations.clear();
        for watch_path in self.watch_descriptors.values() {
            if watch_path.starts_with(&self.ctx.verified_path) && Self::find_sfv_file(watch_path).is_some() {
                self.pending_dirs.put(watch_path.clone(), PendingDirectory::new());
                debug!("Q_OVERFLOW recovery: queued {} for validation", watch_path.display());
            }
        }
        info!("Q_OVERFLOW recovery: queued {} directories for re-validation", self.pending_dirs.len());
    }

    /// Debug-log a skipped temporary path, at most once per TEMP_PATH_LOG_INTERVAL_SECS per path.
    fn log_skipped_temporary(&mut self, path: &Path) {
        let should_log = self.temp_path_last_logged
            .get(path)
            .map(|last| last.elapsed().as_secs() >= TEMP_PATH_LOG_INTERVAL_SECS)
            .unwrap_or(true);
        if should_log {
            debug!("Skipping temporary path: {}", path.display());
            self.temp_path_last_logged.put(path.to_path_buf(), Instant::now());
        }
    }

    /// A file or directory in the verified layer was deleted or moved away.
    fn on_verified_deletion(&mut self, full_path: &Path, event_dir: &Path, is_dir: bool) {
        if is_sidecar_file(full_path) && !is_dir {
            // Transient subtitle/companion churn (e.g. bazarr writing, syncing and
            // renaming a .srt): don't notify servers of the deletion and don't clear
            // the parent dir's validated state.
            debug!("Sidecar file deletion, ignoring: {}", full_path.display());
            return;
        }
        // The files are already gone, so skip SFV and notify directly. Directory
        // deletions are notified by forget_dir, to avoid notifying twice.
        if !is_dir {
            debug!("Verified path file deletion, notifying directly: {}", full_path.display());
            self.notifiers.media_servers(full_path, true);
        }
        self.pending_dirs.pop(event_dir);
        self.failed_validations.pop(event_dir);
    }

    /// New file activity in a verified directory: (re)start its debounce window,
    /// and clear any failure backoff so it gets a fresh validation attempt.
    fn mark_pending(&mut self, event_dir: &Path, now: Instant) {
        if self.failed_validations.pop(event_dir).is_some() {
            debug!("Reset validation backoff for {} due to new file activity", event_dir.display());
        }

        let Some(pending) = self.pending_dirs.get_mut(event_dir) else {
            self.pending_dirs.put(event_dir.to_path_buf(), PendingDirectory::new());
            debug!("Added pending directory (verified): {}", event_dir.display());
            return;
        };
        if !pending.notified {
            pending.last_event = Instant::now();
            debug!("Debounce reset for verified path: {}", event_dir.display());
        } else if let Some(notified_at) = pending.notified_at {
            // Already announced: allow re-validation once the threshold has passed,
            // which retries a notification that may not have landed.
            if now.duration_since(notified_at) >= Duration::from_secs(self.ctx.revalidation_threshold_secs) {
                pending.notified = false;
                pending.notified_at = None;
                pending.last_event = Instant::now();
                info!("Re-validation triggered for {} (new event {}s after notification)",
                      event_dir.display(), now.duration_since(notified_at).as_secs());
            }
        }
    }

    /// Watch a newly created or moved-in directory tree, and queue it for
    /// validation if its files (and SFV) landed before the watch was added.
    fn watch_new_dir(&mut self, dir_path: &Path) {
        if is_excluded_dir(&self.ctx.exclude_dirs, dir_path) {
            debug!("Skipping excluded directory: {}", dir_path.display());
            return;
        }

        let watch_count_before = self.watch_descriptors.len();
        info!("New directory detected: {} (adding watches...)", dir_path.display());
        if let Err(e) = Self::add_watch_recursive(&mut self.inotify, &mut self.watch_descriptors, dir_path, self.max_watches, &self.ctx.exclude_dirs) {
            warn!("Failed to watch new directory {}: {}", dir_path.display(), e);
            return;
        }
        let watches_added = self.watch_descriptors.len() - watch_count_before;
        info!("  Added {} watches for new directory (total: {})", watches_added, self.watch_descriptors.len());
        if let Some(m) = &self.ctx.metrics {
            m.inotify_watches.store(self.watch_descriptors.len() as u64, Ordering::Relaxed);
        }

        if dir_path.starts_with(&self.ctx.verified_path)
            && Self::find_sfv_file(dir_path).is_some()
            && !self.pending_dirs.contains(dir_path)
        {
            self.pending_dirs.put(dir_path.to_path_buf(), PendingDirectory::new());
            info!("  Queued for validation (has SFV): {}", dir_path.display());
        }
    }

    /// A watched directory is gone: drop its pending, failure and stuck state and
    /// tell the media servers.
    fn forget_dir(&mut self, dir_path: &Path) {
        self.pending_dirs.pop(dir_path);
        self.failed_validations.pop(dir_path);
        if let Some(m) = &self.ctx.metrics { m.clear_stuck(dir_path); }
        self.notifiers.media_servers(dir_path, true);
    }

    // ── periodic cleanup ────────────────────────────────────────────────────

    /// Prune entries the caches no longer need. Runs every CLEANUP_INTERVAL_SECS
    /// rather than every iteration, since each pass walks every cache.
    fn periodic_cleanup(&mut self, now: Instant) {
        // Failure records not retried in an hour. LruCache has no retain, so collect keys first.
        let stale_failures: Vec<PathBuf> = self.failed_validations
            .iter()
            .filter(|(_, failed)| now.duration_since(failed.last_attempt) >= Duration::from_secs(FAILURE_CLEANUP_THRESHOLD_SECS))
            .map(|(path, _)| path.clone())
            .collect();
        for key in stale_failures {
            self.failed_validations.pop(&key);
            if let Some(m) = &self.ctx.metrics { m.clear_stuck(&key); }
        }
        // A stuck release deleted without a delete event reaching us
        // (e.g. its parent removed first) would otherwise stay listed forever.
        if let Some(m) = &self.ctx.metrics {
            for gone in m.prune_missing_stuck() {
                self.failed_validations.pop(&gone);
            }
        }

        // Announced directories, once past the cleanup window.
        let cleanup_threshold = Duration::from_secs(self.ctx.pending_cleanup_secs);
        let before_cleanup = self.pending_dirs.len();
        let done: Vec<PathBuf> = self.pending_dirs
            .iter()
            .filter(|(_, pending)| {
                pending.notified
                    && pending.notified_at.is_some_and(|at| now.duration_since(at) >= cleanup_threshold)
            })
            .map(|(path, _)| path.clone())
            .collect();
        for key in done {
            debug!("Cleaning up old notified entry: {}", key.display());
            self.pending_dirs.pop(&key);
        }
        let cleaned = before_cleanup - self.pending_dirs.len();
        if cleaned > 0 {
            debug!("Cleaned up {} old pending_dirs entries (remaining: {})", cleaned, self.pending_dirs.len());
        }

        // Temporary-path log entries not seen for a minute.
        let quiet: Vec<PathBuf> = self.temp_path_last_logged
            .iter()
            .filter(|(_, last)| last.elapsed().as_secs() >= 60)
            .map(|(path, _)| path.clone())
            .collect();
        for key in quiet {
            self.temp_path_last_logged.pop(&key);
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    /// The inotify watch budget: 80% of /proc/sys/fs/inotify/max_user_watches,
    /// leaving headroom for other applications. Returns (max_watches, system_limit).
    fn get_max_watches() -> (usize, usize) {
        let system_limit = std::fs::read_to_string("/proc/sys/fs/inotify/max_user_watches")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(8192); // Conservative fallback if unable to read system limit
        let max_watches = (system_limit * 80) / 100;

        info!("System inotify limit: {} watches", system_limit);
        info!("RARGate will use up to {} watches (80% of system limit)", max_watches);

        (max_watches, system_limit)
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
        watch_map: &mut HashMap<WatchDescriptor, PathBuf>,
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
        watch_map: &mut HashMap<WatchDescriptor, PathBuf>,
        path: &Path,
        max_watches: usize,
        exclude_dirs: &[String],
    ) -> Result<()> {
        let mut watch_count = watch_map.len();
        let limits = WatchLimits { max_depth: MAX_WATCH_DEPTH, max_watches, exclude_dirs };
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

        // Walk from the overlay root DOWN the relative path, re-reading each level
        // that exists. AirDC++ writes straight into the upperdir (`fin`) beneath a
        // live overlay, so a brand-new directory only shows up in the merged view
        // when its parent is re-enumerated. `read_dir` on an EXISTING directory does
        // that and surfaces the new child; reading top-down means each level's read
        // reveals the next, so an entire new series/season/release tree appears in
        // one pass.
        //
        // Crucially we never `stat`/`metadata` a path we expect to be missing: a
        // failed lookup caches a sticky NEGATIVE dentry that a later `read_dir`
        // cannot evict, which is exactly what left freshly-downloaded series
        // invisible until a manual cache drop. We only ever `read_dir` (which does
        // not create negative entries for the dir's children) and detect presence
        // by scanning the parent's listing for the next component's name.
        let mut cur = merged_path.to_path_buf();
        // Root always exists; re-enumerate it first.
        if let Ok(entries) = std::fs::read_dir(&cur) {
            let _ = entries.count();
        }
        let mut fully_present = true;
        for comp in relative.components() {
            let name = comp.as_os_str();
            // Is `name` present in the (freshly re-read) parent `cur`? Check by
            // scanning entries rather than stat'ing the child, to avoid poisoning.
            let present = std::fs::read_dir(&cur)
                .map(|entries| entries.filter_map(Result::ok).any(|e| e.file_name() == name))
                .unwrap_or(false);
            cur.push(comp);
            if !present {
                fully_present = false;
                break;
            }
            // Re-enumerate this level so the NEXT component becomes visible.
            if let Ok(entries) = std::fs::read_dir(&cur) {
                let _ = entries.count();
            }
        }

        // `fully_present` means every path COMPONENT resolved — but overlayfs can
        // still serve a stale listing of the LEAF's CONTENTS: a release dir is created
        // in the upperdir (empty) and cached, then its files are written, and a re-read
        // of an already-cached dir doesn't reliably evict that empty snapshot. So also
        // require the merged leaf to hold every entry the verified source has; the
        // overlay is a superset of the upperdir (barring whiteouts, which fresh
        // downloads never have), so a missing entry means the listing is stale.
        // (`cur` is the merged leaf when `fully_present`; short-circuit so we don't
        // read a partial path when an ancestor never resolved.)
        let leaf_reflects_source = fully_present && {
            let overlay: std::collections::HashSet<std::ffi::OsString> = std::fs::read_dir(&cur)
                .map(|e| e.filter_map(Result::ok).map(|d| d.file_name()).collect())
                .unwrap_or_default();
            std::fs::read_dir(source_path)
                .map(|src| src.filter_map(Result::ok).all(|d| overlay.contains(&d.file_name())))
                .unwrap_or(true) // source unreadable → don't force a drop
        };

        if leaf_reflects_source {
            debug!("Refreshed overlay cache along path: {}", overlay_path.display());
            return;
        }

        // Either an ancestor never resolved, or the leaf's contents are stale — the
        // overlay's directory cache is poisoned (a negative or empty listing was cached
        // while the content was still absent) and a re-read can't evict it. Drop the
        // VFS dentry/inode caches as a last resort. Rare (only genuinely-stuck brand-new
        // trees or a release dir written after its empty parent was cached) and coalesced
        // by the dedupe window above.
        warn!("Overlay still stale for {} after re-read — dropping VFS caches to surface it", overlay_path.display());
        Self::drop_vfs_caches();
    }

    /// Drop the kernel's dentry/inode caches (`vm.drop_caches=2`) to evict a stale
    /// overlay directory listing. Best-effort: logged and ignored on failure. Used
    /// only as the overlay-refresh fallback when a re-read can't surface new content.
    fn drop_vfs_caches() {
        use std::io::Write as _;
        // sync first so nothing is lost, then drop dentries + inodes.
        let _ = std::process::Command::new("sync").status();
        match std::fs::OpenOptions::new().write(true).open("/proc/sys/vm/drop_caches") {
            Ok(mut f) => {
                if let Err(e) = f.write_all(b"2\n") {
                    warn!("Overlay fallback: failed to write drop_caches: {}", e);
                }
            }
            Err(e) => warn!("Overlay fallback: cannot open drop_caches ({}); is rargate root?", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    fn touch(path: &Path, age: Duration) {
        let f = std::fs::File::create(path).unwrap();
        f.set_modified(SystemTime::now() - age).unwrap();
    }

    const GRACE: Duration = Duration::from_secs(ACTIVE_DOWNLOAD_GRACE_SECS);

    #[test]
    fn active_download_recent_partial_file() {
        let dir = tempfile::tempdir().unwrap();
        touch(&dir.path().join("rel.rar"), Duration::from_secs(10 * 3600));
        touch(&dir.path().join("rel.r00.dctmp"), Duration::from_secs(60));
        assert!(has_active_download(dir.path(), GRACE));
    }

    #[test]
    fn active_download_newest_partial_wins() {
        // One volume idle for hours while another is being written — still downloading.
        let dir = tempfile::tempdir().unwrap();
        touch(&dir.path().join("rel.r01.dctmp"), Duration::from_secs(11 * 3600));
        touch(&dir.path().join("rel.r00.dctmp"), Duration::from_secs(30));
        assert!(has_active_download(dir.path(), GRACE));
    }

    #[test]
    fn stalled_download_is_not_active() {
        let dir = tempfile::tempdir().unwrap();
        touch(&dir.path().join("rel.r00.dctmp"), Duration::from_secs(7 * 3600));
        assert!(!has_active_download(dir.path(), GRACE));
    }

    #[test]
    fn no_partial_files_is_not_active() {
        // Missing volumes with nothing downloading: genuinely incomplete/deleted.
        let dir = tempfile::tempdir().unwrap();
        touch(&dir.path().join("rel.rar"), Duration::from_secs(5));
        touch(&dir.path().join("rel.sfv"), Duration::from_secs(5));
        assert!(!has_active_download(dir.path(), GRACE));
    }

    #[test]
    fn missing_dir_is_not_active() {
        assert!(!has_active_download(Path::new("/nonexistent/rargate-test"), GRACE));
    }
}

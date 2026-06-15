//! Runtime metrics collection and status file writer.
//!
//! Maintains atomic counters that any module can increment without locking.
//! A background task periodically serialises them to a JSON status file so
//! operators can check RARGate health without tailing logs.
//!
//! Enable in config:
//! ```yaml
//! metrics:
//!   enabled: true
//!   status_file: /mnt/cache/rargate/rargate-status.json
//!   update_interval_seconds: 60
//! ```

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

const DEFAULT_STATUS_FILE: &str = "/mnt/cache/rargate/rargate-status.json";
const DEFAULT_UPDATE_INTERVAL_SECS: u64 = 60;

/// Shared, lock-free metrics counters.  All increments use `Relaxed` ordering —
/// exact consistency between counters is not required; approximate totals are enough.
pub struct MetricsCollector {
    /// Monotonic start time (for uptime calculation)
    started_at: Instant,

    // Inotify
    pub inotify_events: AtomicU64,
    pub inotify_overflows: AtomicU64,
    pub inotify_watches: AtomicU64,      // current watch count (set, not incremented)

    // SFV validation
    pub sfv_validations_passed: AtomicU64,
    pub sfv_validations_failed: AtomicU64,

    // Media server notifications
    pub emby_notifications: AtomicU64,
    pub jellyfin_notifications: AtomicU64,
    pub plex_notifications: AtomicU64,
    /// Sonarr/Radarr rescan batches fired by the arr notifier.
    pub arr_notifications: AtomicU64,
    pub emby_full_refreshes: AtomicU64,
    pub jellyfin_full_refreshes: AtomicU64,
    pub plex_full_refreshes: AtomicU64,
    pub full_refresh_cooldown_skips: AtomicU64,

    // Filter cache
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,

    // rar2fs concurrency limiter (RarFsLimiter)
    /// Number of `acquire()` calls that had to block waiting for a permit.
    /// Compare against `rar2fs_limiter_acquires_total` to see how often the cap bites.
    pub rar2fs_limiter_blocked_acquires: AtomicU64,
    /// Total `acquire()` calls (blocked + non-blocked). Use with the above for a rate.
    pub rar2fs_limiter_acquires_total: AtomicU64,
    /// Cumulative wait time across all blocked acquires (milliseconds). Divide by
    /// `rar2fs_limiter_blocked_acquires` to get average wait when the cap bites.
    pub rar2fs_limiter_total_wait_ms: AtomicU64,
}

impl MetricsCollector {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            started_at: Instant::now(),
            inotify_events: AtomicU64::new(0),
            inotify_overflows: AtomicU64::new(0),
            inotify_watches: AtomicU64::new(0),
            sfv_validations_passed: AtomicU64::new(0),
            sfv_validations_failed: AtomicU64::new(0),
            emby_notifications: AtomicU64::new(0),
            jellyfin_notifications: AtomicU64::new(0),
            plex_notifications: AtomicU64::new(0),
            arr_notifications: AtomicU64::new(0),
            emby_full_refreshes: AtomicU64::new(0),
            jellyfin_full_refreshes: AtomicU64::new(0),
            plex_full_refreshes: AtomicU64::new(0),
            full_refresh_cooldown_skips: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            rar2fs_limiter_blocked_acquires: AtomicU64::new(0),
            rar2fs_limiter_acquires_total: AtomicU64::new(0),
            rar2fs_limiter_total_wait_ms: AtomicU64::new(0),
        })
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    /// Write current counters to a JSON status file.
    pub fn write_status_file(&self, path: &Path, version: &str) {
        let uptime = self.uptime_seconds();
        let passes = self.sfv_validations_passed.load(Ordering::Relaxed);
        let fails  = self.sfv_validations_failed.load(Ordering::Relaxed);
        let total_sfv = passes + fails;
        let sfv_pass_rate = if total_sfv > 0 {
            (passes as f64 / total_sfv as f64) * 100.0
        } else {
            100.0
        };
        let hits   = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total_cache = hits + misses;
        let cache_hit_rate = if total_cache > 0 {
            (hits as f64 / total_cache as f64) * 100.0
        } else {
            100.0
        };

        let lim_total = self.rar2fs_limiter_acquires_total.load(Ordering::Relaxed);
        let lim_blocked = self.rar2fs_limiter_blocked_acquires.load(Ordering::Relaxed);
        let lim_wait_ms = self.rar2fs_limiter_total_wait_ms.load(Ordering::Relaxed);
        let lim_block_rate = if lim_total > 0 {
            (lim_blocked as f64 / lim_total as f64) * 100.0
        } else {
            0.0
        };
        let lim_avg_wait_ms = if lim_blocked > 0 {
            lim_wait_ms as f64 / lim_blocked as f64
        } else {
            0.0
        };

        // Build JSON manually to avoid pulling in serde_json for just this use case
        // (serde_json is already a dependency, so we use it)
        let now = chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%z");
        let json = format!(
            r#"{{
  "version": "{version}",
  "timestamp": "{now}",
  "uptime_seconds": {uptime},
  "inotify": {{
    "events_processed": {events},
    "queue_overflows": {overflows},
    "current_watches": {watches}
  }},
  "sfv": {{
    "validations_passed": {passes},
    "validations_failed": {fails},
    "pass_rate_pct": {sfv_pass_rate:.1}
  }},
  "notifications": {{
    "emby_sent": {emby},
    "jellyfin_sent": {jellyfin},
    "plex_sent": {plex},
    "arr_sent": {arr},
    "emby_full_refreshes": {emby_fr},
    "jellyfin_full_refreshes": {jf_fr},
    "plex_full_refreshes": {plex_fr},
    "full_refresh_cooldown_skips": {cooldown_skips}
  }},
  "filter_cache": {{
    "hits": {hits},
    "misses": {misses},
    "hit_rate_pct": {cache_hit_rate:.1}
  }},
  "rar2fs_limiter": {{
    "acquires_total": {lim_total},
    "blocked_acquires": {lim_blocked},
    "block_rate_pct": {lim_block_rate:.1},
    "total_wait_ms": {lim_wait_ms},
    "avg_wait_ms_when_blocked": {lim_avg_wait_ms:.1}
  }}
}}"#,
            version = version,
            now = now,
            uptime = uptime,
            events = self.inotify_events.load(Ordering::Relaxed),
            overflows = self.inotify_overflows.load(Ordering::Relaxed),
            watches = self.inotify_watches.load(Ordering::Relaxed),
            passes = passes,
            fails = fails,
            sfv_pass_rate = sfv_pass_rate,
            emby = self.emby_notifications.load(Ordering::Relaxed),
            jellyfin = self.jellyfin_notifications.load(Ordering::Relaxed),
            plex = self.plex_notifications.load(Ordering::Relaxed),
            arr = self.arr_notifications.load(Ordering::Relaxed),
            emby_fr = self.emby_full_refreshes.load(Ordering::Relaxed),
            jf_fr = self.jellyfin_full_refreshes.load(Ordering::Relaxed),
            plex_fr = self.plex_full_refreshes.load(Ordering::Relaxed),
            cooldown_skips = self.full_refresh_cooldown_skips.load(Ordering::Relaxed),
            hits = hits,
            misses = misses,
            cache_hit_rate = cache_hit_rate,
            lim_total = lim_total,
            lim_blocked = lim_blocked,
            lim_block_rate = lim_block_rate,
            lim_wait_ms = lim_wait_ms,
            lim_avg_wait_ms = lim_avg_wait_ms,
        );

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        if let Err(e) = std::fs::write(path, json.as_bytes()) {
            warn!("Failed to write metrics status file {}: {}", path.display(), e);
        } else {
            debug!("Metrics: status file updated at {}", path.display());
        }
    }
}

/// Start a background task that writes the status file periodically.
pub fn start_metrics_writer(
    metrics: Arc<MetricsCollector>,
    status_file: PathBuf,
    interval_secs: u64,
    version: &'static str,
) {
    let interval = Duration::from_secs(interval_secs);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            metrics.write_status_file(&status_file, version);
        }
    });
}

/// Build status-file path and interval from optional config, returning `None` if disabled.
pub fn metrics_config(
    cfg: &Option<crate::config::MetricsConfig>,
) -> Option<(PathBuf, u64)> {
    let cfg = cfg.as_ref()?;
    if !cfg.enabled {
        return None;
    }
    let path = cfg.status_file
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_STATUS_FILE));
    let interval = cfg.update_interval_seconds.unwrap_or(DEFAULT_UPDATE_INTERVAL_SECS);
    Some((path, interval))
}

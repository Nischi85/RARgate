//! YAML configuration parsing and runtime validation.
//!
//! `Config::load` reads `config.yaml`, applies defaults, and validates invariants
//! (paths exist, ports parse, mutually-exclusive options aren't both set). All other
//! modules consume immutable slices of the resulting `Config` tree — there is no
//! global config singleton.
//!
//! Custom deserializers live alongside the structs they're used by (e.g.
//! `deserialize_permissions` for octal/decimal mode strings).

use anyhow::{Context, Result};
use serde::{Deserialize, Deserializer};
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub source: PathBuf,
    pub mountpoint: PathBuf,
    pub verbose: Option<bool>,
    pub daemon: bool,
    pub read_only: Option<bool>,
    pub no_delete: Option<bool>,
    pub rar2fs: Rar2fsConfig,
    pub sfv_validation: Option<SfvConfig>,
    pub filters: Option<FiltersConfig>,
    pub overlay_reference: Option<OverlayConfig>,
    pub unionfs_reference: Option<UnionfsConfig>,
    pub notifications: Option<NotificationConfig>,
    pub crash_recovery: Option<CrashRecoveryConfig>,
    pub watcher: Option<WatcherConfig>,
    pub emby: Option<EmbyConfig>,
    pub jellyfin: Option<JellyfinConfig>,
    pub plex: Option<PlexConfig>,
    pub arr: Option<ArrConfig>,
    pub fuse_options: Option<FuseOptions>,
    pub logging: Option<LogConfig>,
    pub metrics: Option<MetricsConfig>,
    pub core_dumps: Option<CoreDumpConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Rar2fsConfig {
    pub binary_path: Option<String>,
    pub backend_mount: Option<PathBuf>,
    pub seek_length: Option<i32>,
    pub extra_options: Option<String>,
    /// Maximum concurrent reads from rar2fs backend. Default: 4
    /// Lower values prevent rar2fs crashes under heavy load.
    /// rar2fs is single-threaded and can be overwhelmed by concurrent access.
    pub max_concurrent_reads: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SfvConfig {
    pub enabled: bool,
    pub verified_share_indicators: Vec<String>,
    pub unverified_share_indicators: Vec<String>,
    pub default_mode: String,
    pub log_failures: Option<bool>,
    pub log_file: Option<PathBuf>,
    /// Lazy mode: Show all directories during listing, validate only on access
    /// true = Instant directory listing (validation happens when entering directory)
    /// false = Full validation during listing (slower but hides invalid dirs immediately)
    pub lazy_mode: Option<bool>,
    /// Backoff settings for failed SFV validations (prevents log spam)
    pub validation_backoff: Option<ValidationBackoffConfig>,
    /// Time threshold (seconds) for detecting active downloads.
    /// Files modified within this time are considered "in progress".
    /// SFV failures won't be logged while downloads are active.
    /// Default: 60
    pub download_threshold_seconds: Option<u64>,
    /// When true, permissive directories with SFV files are still validated.
    /// Directories without SFV files are always shown in permissive mode.
    /// Default: false (permissive always shows regardless of SFV)
    pub permissive_validates_sfv: Option<bool>,
}

/// Configuration for SFV validation backoff (prevents log spam from repeated failures)
#[derive(Debug, Clone, Deserialize)]
pub struct ValidationBackoffConfig {
    /// Maximum consecutive failures before entering cooldown. Default: 3
    pub max_failures: Option<u32>,
    /// Cooldown duration in seconds before retrying after max failures. Default: 300 (5 minutes)
    pub cooldown_seconds: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FiltersConfig {
    pub exclude_dirs: Option<Vec<String>>,
    pub exclude_files: Option<Vec<String>>,
    /// Valid media file extensions (without dots) for RAR extraction and Emby notifications
    /// Default: mkv, mp4, avi, m4v, mov, wmv, flv, mpg, mpeg, mp3, flac, m4a, wav, aac, ogg, wma, iso, img, bin, cue
    pub media_extensions: Option<Vec<String>>,
    pub rar_archives: Option<RarArchiveConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RarArchiveConfig {
    pub enabled: bool,
    pub hide_archives: Option<bool>,
    pub cache_timeout: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OverlayConfig {
    pub verified_share_path: PathBuf,
    pub unverified_share_path: PathBuf,
    /// Merged directory where overlay is mounted (e.g., /path/to/overlay/merged)
    /// Used to refresh overlay cache when files are written directly to the verified upperdir
    pub merged_directory: Option<PathBuf>,
    pub use_for_filtering: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UnionfsConfig {
    pub verified_share_path: PathBuf,
    pub unverified_share_path: PathBuf,
    pub merged_directory: PathBuf,
    pub use_for_filtering: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NotificationConfig {
    pub enabled: bool,
    pub check_interval: Option<u64>,
    pub max_consecutive_errors: Option<u32>,
}

/// Auto-recovery settings for the bash monitor script. Rargate writes these
/// out to /var/run/rargate-monitor.conf on startup so the bash side respects
/// the same config.yaml everyone else reads.
#[derive(Debug, Clone, Deserialize)]
pub struct CrashRecoveryConfig {
    pub enabled: Option<bool>,
    pub restart_after_failures: Option<u32>,
    pub max_restart_attempts: Option<u32>,
    pub reset_after_minutes: Option<u32>,
}

/// Inotify watcher configuration for file change monitoring
#[derive(Debug, Clone, Deserialize)]
pub struct WatcherConfig {
    /// Time to wait after last file event before SFV validation (seconds). Default: 30
    pub sfv_debounce_seconds: Option<u64>,
    /// How long to keep notified directories in pending_dirs (minutes). Default: 5
    pub pending_cleanup_minutes: Option<u64>,
    /// Time after notification before allowing re-validation on new events (seconds). Default: 30
    pub revalidation_threshold_seconds: Option<u64>,
}

/// Emby media server integration configuration
#[derive(Debug, Clone, Deserialize)]
pub struct EmbyConfig {
    pub enabled: bool,
    pub url: String,
    pub api_token: String,
    /// Debounce time for file change notifications (seconds). Default: 5
    pub debounce_seconds: Option<u64>,
    /// Cache duration for Emby library items (minutes). Default: 15
    /// Higher = fewer API calls, slower detection of new Emby items
    /// Lower = more API calls, faster detection of library changes
    /// Recommended: Match your Emby scheduled scan interval
    pub cache_minutes: Option<u64>,
    /// Fallback to full library refresh if no matching item found. Default: false
    /// When true: Triggers /Library/Refresh if targeted refresh can't find the item
    /// When false: Only logs a warning and waits for Emby's scheduled scan
    /// Use case: Enable for new content that Emby hasn't indexed yet
    pub fallback_full_refresh: Option<bool>,
    /// Maximum number of retry attempts for API calls. Default: 3
    pub max_retries: Option<u32>,
    /// Initial delay in milliseconds between retry attempts (doubles each retry). Default: 1000
    pub retry_delay_ms: Option<u64>,
    /// Minimum seconds between full library scans (fallback mode). Default: 600 (10 min)
    /// Prevents hammering the server when new content arrives faster than Emby indexes it.
    pub full_refresh_cooldown_seconds: Option<u64>,
    pub path_mapping: Option<PathMappingConfig>,
}

/// Path mapping for translating host paths to Emby/Jellyfin/Plex container paths
#[derive(Debug, Clone, Deserialize)]
pub struct PathMappingConfig {
    /// Single host path (for backwards compatibility)
    pub host_path: Option<PathBuf>,
    /// Multiple host paths that all map to the same emby_path/container_path
    /// Useful for overlay filesystems with verified/unverified layers
    pub host_paths: Option<Vec<PathBuf>>,
    /// Container path (works for Emby, Jellyfin, and Plex)
    /// Field name is "emby_path" for backwards compatibility
    pub emby_path: PathBuf,
}

/// Jellyfin media server integration configuration
#[derive(Debug, Clone, Deserialize)]
pub struct JellyfinConfig {
    pub enabled: bool,
    pub url: String,
    pub api_token: String,
    /// Debounce time for file change notifications (seconds). Default: 5
    pub debounce_seconds: Option<u64>,
    /// Cache duration for Jellyfin library items (minutes). Default: 15
    /// Higher = fewer API calls, slower detection of new items
    /// Lower = more API calls, faster detection of library changes
    pub cache_minutes: Option<u64>,
    /// Fallback to full library refresh if no matching item found. Default: false
    /// When true: Triggers /Library/Refresh if targeted refresh can't find the item
    /// When false: Only logs a warning and waits for scheduled scan
    pub fallback_full_refresh: Option<bool>,
    /// Maximum number of retry attempts for API calls. Default: 3
    pub max_retries: Option<u32>,
    /// Initial delay in milliseconds between retry attempts (doubles each retry). Default: 1000
    pub retry_delay_ms: Option<u64>,
    /// Minimum seconds between full library scans (fallback mode). Default: 600 (10 min)
    pub full_refresh_cooldown_seconds: Option<u64>,
    pub path_mapping: Option<PathMappingConfig>,
}

/// Plex media server integration configuration
#[derive(Debug, Clone, Deserialize)]
pub struct PlexConfig {
    pub enabled: bool,
    pub url: String,
    /// Plex authentication token (X-Plex-Token)
    pub token: String,
    /// Debounce time for file change notifications (seconds). Default: 5
    pub debounce_seconds: Option<u64>,
    /// Cache duration for Plex library sections (minutes). Default: 15
    /// Higher = fewer API calls
    /// Lower = more API calls, faster section discovery
    pub cache_minutes: Option<u64>,
    /// Fallback to full library refresh if path-specific refresh fails. Default: false
    /// When true: Triggers full library scan if cannot determine section
    /// When false: Only logs a warning and waits for scheduled scan
    pub fallback_full_refresh: Option<bool>,
    /// Maximum number of retry attempts for API calls. Default: 3
    pub max_retries: Option<u32>,
    /// Initial delay in milliseconds between retry attempts (doubles each retry). Default: 1000
    pub retry_delay_ms: Option<u64>,
    /// Minimum seconds between full library scans (fallback mode). Default: 600 (10 min)
    pub full_refresh_cooldown_seconds: Option<u64>,
    pub path_mapping: Option<PathMappingConfig>,
}

/// Sonarr/Radarr rescan-on-exposure integration.
///
/// rargate fires an in-place `RescanSeries`/`RescanMovie` the moment a release's SFV
/// validates, so imports no longer depend on the downloader (dc-bridge) winning a race
/// against rargate's gate. Read-only: rargate never moves/copies files, it only POSTs
/// rescan commands, so it is safe over the read-only mount.
#[derive(Debug, Clone, Deserialize)]
pub struct ArrConfig {
    pub enabled: bool,
    /// Sonarr instance — handles TV series (RescanSeries).
    pub sonarr: Option<ArrInstanceConfig>,
    /// Radarr instance — handles movies (RescanMovie). Best-effort: only fires once
    /// Radarr's movie path matches the scene folder (dc-bridge repoints it).
    pub radarr: Option<ArrInstanceConfig>,
    /// Debounce time for file change notifications (seconds). Default: 5
    pub debounce_seconds: Option<u64>,
    /// Cache duration for the *arr series/movie list (minutes). Default: 5
    pub cache_minutes: Option<u64>,
    /// Maximum number of retry attempts for API calls. Default: 3
    pub max_retries: Option<u32>,
    /// Initial delay in milliseconds between retry attempts (doubles each retry). Default: 1000
    pub retry_delay_ms: Option<u64>,
}

/// One Sonarr or Radarr instance.
#[derive(Debug, Clone, Deserialize)]
pub struct ArrInstanceConfig {
    pub url: String,
    pub api_key: String,
    /// Maps the rargate verified host path to the path this *arr instance sees, used to
    /// match a validated release folder against the instance's series/movie paths.
    pub path_mapping: Option<PathMappingConfig>,
}

/// Metrics and status file configuration
#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    /// Whether to write the status file. Default: false
    pub enabled: bool,
    /// Path to write the JSON status file (default: /mnt/cache/rargate/rargate-status.json)
    pub status_file: Option<PathBuf>,
    /// How often to update the status file in seconds. Default: 60
    pub update_interval_seconds: Option<u64>,
}

/// Logging configuration
#[derive(Debug, Clone, Deserialize)]
pub struct LogConfig {
    /// Path to the log file (default: /var/log/rargate.log)
    pub log_file: Option<PathBuf>,
    /// Maximum log file size in MB before rotation (default: 10)
    pub max_size_mb: Option<u64>,
    /// Master switch for rar2fs diagnostics (stderr log + resource sampling). Default: true.
    /// When false, rargate opens no rar2fs stderr file (stderr → /dev/null at the kernel),
    /// spawns no resource-sampling task, and registers nothing for rotation.
    /// Acts as an override: either per-log flag below is only honored when this is true.
    pub rar2fs_diagnostics_enabled: Option<bool>,
    /// Per-log switch for the rar2fs stderr log. Default: true. When false (or the master is
    /// off), no stderr file is opened and nothing is registered for rotation.
    pub rar2fs_log_enabled: Option<bool>,
    /// Per-log switch for rar2fs resource sampling. Default: true. When false (or the master
    /// is off), the sampling task is never spawned and its log is not registered.
    pub rar2fs_resources_enabled: Option<bool>,
    /// Path for rar2fs stderr log (default: /mnt/cache/rargate/rar2fs.log)
    pub rar2fs_log_file: Option<PathBuf>,
    /// Cap for the rar2fs stderr log in MB (default: 50)
    pub rar2fs_log_max_size_mb: Option<u64>,
    /// Path for the rar2fs resource-sampling log (default: /mnt/cache/rargate/rar2fs-resources.log)
    pub rar2fs_resources_log_file: Option<PathBuf>,
    /// Cap for the rar2fs resource log in MB (default: 20)
    pub rar2fs_resources_log_max_size_mb: Option<u64>,
    /// How often to sample rar2fs resource stats, in seconds (default: 300)
    pub rar2fs_resource_interval_seconds: Option<u64>,
}

/// Core-dump capture configuration (consumed by userscripts-startup.sh via rargate-paths.conf).
#[derive(Debug, Clone, Deserialize)]
pub struct CoreDumpConfig {
    /// Directory where core dumps are written (default: /mnt/cache/rargate/cores)
    pub directory: Option<PathBuf>,
    /// How many of the most recent core dumps to retain (default: 5)
    pub keep: Option<u32>,
}

/// Custom deserializer for file permissions
/// Accepts both octal strings ("0o777", "0755") and decimal integers (511)
fn deserialize_permissions<'de, D>(deserializer: D) -> Result<Option<u16>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum PermValue {
        String(String),
        Number(u16),
    }

    match Option::<PermValue>::deserialize(deserializer)? {
        None => Ok(None),
        Some(PermValue::Number(n)) => Ok(Some(n)),
        Some(PermValue::String(s)) => {
            // Try parsing as octal with 0o prefix
            if let Some(octal_str) = s.strip_prefix("0o") {
                u16::from_str_radix(octal_str, 8)
                    .map(Some)
                    .map_err(|_| Error::custom(format!("Invalid octal permission: 0o{}", octal_str)))
            }
            // Try parsing as octal without prefix (e.g., "755")
            else if s.chars().all(|c| c.is_ascii_digit()) {
                u16::from_str_radix(&s, 8)
                    .map(Some)
                    .map_err(|_| Error::custom(format!("Invalid octal permission: {}", s)))
            }
            else {
                Err(Error::custom(format!(
                    "Invalid permission format: {}. Use octal (0o755, 755) or decimal (493)",
                    s
                )))
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FuseOptions {
    // Basic FUSE options
    pub fsname: Option<String>,
    pub auto_unmount: Option<bool>,
    pub allow_other: Option<bool>,
    pub allow_root: Option<bool>,
    pub default_permissions: Option<bool>,
    pub ro: Option<bool>,                // Read-only mount
    pub nonempty: Option<bool>,          // Allow mount on non-empty directory

    // Cache timeouts
    pub attr_timeout: Option<f64>,
    pub entry_timeout: Option<f64>,

    // File permissions
    #[serde(default, deserialize_with = "deserialize_permissions")]
    pub permissions: Option<u16>,

    // Custom options (for any FUSE option not explicitly supported)
    pub custom_options: Option<Vec<String>>,
}

impl Config {
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        
        let config: Config = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;
        
        Ok(config)
    }
    
    pub fn validate(&self) -> Result<()> {
        use tracing::{warn, info};

        // Validate source directory exists
        if !self.source.exists() {
            anyhow::bail!("Source directory does not exist: {}", self.source.display());
        }

        // Validate mountpoint exists and is empty
        if !self.mountpoint.exists() {
            anyhow::bail!("Mountpoint does not exist: {}", self.mountpoint.display());
        }

        // Check for common mistake: source == mountpoint
        if self.source.canonicalize().ok() == self.mountpoint.canonicalize().ok() {
            anyhow::bail!(
                "Source and mountpoint cannot be the same directory!\n   \
                Source: {}\n   \
                Mountpoint: {}",
                self.source.display(),
                self.mountpoint.display()
            );
        }

        // Check if already mounted
        if is_mounted(&self.mountpoint)? {
            anyhow::bail!("Mountpoint already in use: {}", self.mountpoint.display());
        }

        // Validate SFV configuration
        if let Some(sfv) = &self.sfv_validation {
            if sfv.enabled {
                // Check default_mode is valid
                if !["strict", "permissive"].contains(&sfv.default_mode.as_str()) {
                    anyhow::bail!(
                        "Invalid SFV default_mode: '{}'. Must be 'strict' or 'permissive'",
                        sfv.default_mode
                    );
                }

                // Warn if no share indicators configured (probably a mistake)
                if sfv.verified_share_indicators.is_empty()
                    && sfv.unverified_share_indicators.is_empty() {
                    warn!("⚠️  SFV validation enabled but no share indicators configured");
                    warn!("   All directories will use default_mode: {}", sfv.default_mode);
                    warn!("   Consider adding verified_share_indicators or unverified_share_indicators");
                }

                // Warn if verified/unverified indicators overlap — they categorise
                // the same path differently, leading to undefined behaviour.
                for vi in &sfv.verified_share_indicators {
                    for ui in &sfv.unverified_share_indicators {
                        if vi == ui || vi.contains(ui.as_str()) || ui.contains(vi.as_str()) {
                            warn!(
                                "⚠️  SFV indicators overlap: verified={:?} unverified={:?} — \
                                 paths matching both will be classified non-deterministically",
                                vi, ui
                            );
                        }
                    }
                }

                // Info about active mode
                info!("SFV validation enabled (default: {})", sfv.default_mode);
                if sfv.lazy_mode.unwrap_or(false) {
                    info!("  Lazy mode: Validation deferred until directory access (faster listings)");
                } else {
                    info!("  Eager mode: Validation during directory listing (slower but immediate)");
                }
            } else if sfv.lazy_mode.unwrap_or(false) {
                warn!("⚠️  lazy_mode=true has no effect when sfv_validation.enabled=false");
            }
        }

        // Validate overlay configuration
        if let Some(overlay) = &self.overlay_reference {
            if overlay.use_for_filtering.unwrap_or(false) {
                if !overlay.verified_share_path.exists() {
                    warn!(
                        "⚠️  Overlay verified_share_path does not exist: {}",
                        overlay.verified_share_path.display()
                    );
                    warn!("   Overlay-based filtering may not work correctly");
                }

                if !overlay.unverified_share_path.exists() {
                    warn!(
                        "⚠️  Overlay unverified_share_path does not exist: {}",
                        overlay.unverified_share_path.display()
                    );
                    warn!("   Overlay-based filtering may not work correctly");
                }

                info!("Overlay filtering enabled:");
                info!("  Verified: {}", overlay.verified_share_path.display());
                info!("  Unverified: {}", overlay.unverified_share_path.display());
            }
        }

        // Validate UnionFS configuration
        if let Some(unionfs) = &self.unionfs_reference {
            if unionfs.use_for_filtering.unwrap_or(false) {
                if !unionfs.verified_share_path.exists() {
                    warn!(
                        "⚠️  UnionFS verified_share_path does not exist: {}",
                        unionfs.verified_share_path.display()
                    );
                }

                if !unionfs.unverified_share_path.exists() {
                    warn!(
                        "⚠️  UnionFS unverified_share_path does not exist: {}",
                        unionfs.unverified_share_path.display()
                    );
                }

                if !unionfs.merged_directory.exists() {
                    warn!(
                        "⚠️  UnionFS merged_directory does not exist: {}",
                        unionfs.merged_directory.display()
                    );
                }

                info!("UnionFS filtering enabled:");
                info!("  Verified: {}", unionfs.verified_share_path.display());
                info!("  Unverified: {}", unionfs.unverified_share_path.display());
            }
        }

        // Validate rar2fs configuration
        if let Some(seek_length) = self.rar2fs.seek_length {
            if seek_length != 0 && seek_length != 1 {
                warn!("⚠️  rar2fs seek_length={} is unusual (typically 0 or 1)", seek_length);
                warn!("   0 = Scan all volumes (slow but comprehensive)");
                warn!("   1 = Scan first volume only (50x faster, recommended)");
            }
        }

        if let Some(backend_mount) = &self.rar2fs.backend_mount {
            if !backend_mount.exists() {
                anyhow::bail!(
                    "rar2fs backend_mount directory does not exist: {}",
                    backend_mount.display()
                );
            }
        }

        if let Some(n) = self.rar2fs.max_concurrent_reads {
            if n == 0 {
                anyhow::bail!(
                    "rar2fs.max_concurrent_reads must be > 0 (got 0). \
                     Omit the field to use the default of 4."
                );
            }
        }

        // FUSE attribute/entry cache timeouts — sanity bounds. 0 disables
        // the cache (legitimate but rarely intended); >24h is almost
        // certainly a config error.
        if let Some(fuse_opts) = &self.fuse_options {
            const MAX_TIMEOUT_SECS: f64 = 86_400.0;
            if let Some(t) = fuse_opts.attr_timeout {
                if t < 0.0 {
                    anyhow::bail!("fuse_options.attr_timeout cannot be negative (got {})", t);
                }
                if t > MAX_TIMEOUT_SECS {
                    warn!("⚠️  fuse_options.attr_timeout={}s is > 24h — probably a typo", t);
                }
            }
            if let Some(t) = fuse_opts.entry_timeout {
                if t < 0.0 {
                    anyhow::bail!("fuse_options.entry_timeout cannot be negative (got {})", t);
                }
                if t > MAX_TIMEOUT_SECS {
                    warn!("⚠️  fuse_options.entry_timeout={}s is > 24h — probably a typo", t);
                }
            }
        }

        // Validate RAR archive filtering
        if let Some(filters) = &self.filters {
            if let Some(rar) = &filters.rar_archives {
                if !rar.enabled && rar.hide_archives.unwrap_or(false) {
                    warn!("⚠️  rar_archives.hide_archives=true has no effect when enabled=false");
                }

                if rar.enabled && rar.hide_archives.unwrap_or(true) {
                    info!("RAR archive hiding: Enabled (.rar, .r00, etc. will be hidden)");
                }
            }
        }

        // Validate notification settings
        if let Some(notif) = &self.notifications {
            if notif.enabled {
                let interval = notif.check_interval.unwrap_or(60);
                if interval < 30 {
                    warn!("⚠️  notifications.check_interval={} is very low", interval);
                    warn!("   Values < 30 seconds may cause excessive system load");
                    warn!("   Recommended: 60-300 seconds");
                }

                info!("Mount monitoring enabled (check every {}s)", interval);
            }
        }

        // Validate Emby configuration
        if let Some(emby) = &self.emby {
            if emby.enabled {
                if emby.url.is_empty() {
                    anyhow::bail!("Emby URL cannot be empty when enabled");
                }
                if emby.api_token.is_empty() {
                    anyhow::bail!("Emby API token cannot be empty when enabled");
                }
                if let Err(e) = validate_http_url(&emby.url) {
                    anyhow::bail!("Invalid Emby URL '{}': {}", emby.url, e);
                }

                let debounce = emby.debounce_seconds.unwrap_or(5);
                if debounce < 1 {
                    warn!("⚠️  emby.debounce_seconds={} is very low", debounce);
                    warn!("   May cause excessive API calls. Recommended: 5+");
                }

                let cache_mins = emby.cache_minutes.unwrap_or(15);
                info!("Emby integration enabled:");
                info!("  URL: {}", emby.url);
                info!("  Debounce: {}s", debounce);
                info!("  Item cache: {} minutes", cache_mins);

                if let Some(mapping) = &emby.path_mapping {
                    if let Some(host_paths) = &mapping.host_paths {
                        info!("  Path mapping ({} sources) -> {}",
                              host_paths.len(),
                              mapping.emby_path.display());
                        for path in host_paths {
                            info!("    - {}", path.display());
                        }
                    } else if let Some(host_path) = &mapping.host_path {
                        info!("  Path mapping: {} -> {}",
                              host_path.display(),
                              mapping.emby_path.display());
                    }
                } else {
                    warn!("⚠️  No path_mapping configured for Emby");
                    warn!("   Host paths will be sent to Emby as-is");
                    warn!("   This usually won't work with Docker containers");
                }
            }
        }

        // Validate Jellyfin configuration (URL parse only — full validation
        // is similar to Emby and lives in the notifier when used).
        if let Some(jf) = &self.jellyfin {
            if jf.enabled {
                if jf.url.is_empty() {
                    anyhow::bail!("Jellyfin URL cannot be empty when enabled");
                }
                if jf.api_token.is_empty() {
                    anyhow::bail!("Jellyfin API token cannot be empty when enabled");
                }
                if let Err(e) = validate_http_url(&jf.url) {
                    anyhow::bail!("Invalid Jellyfin URL '{}': {}", jf.url, e);
                }
                info!("Jellyfin integration enabled (URL: {})", jf.url);
            }
        }

        // Validate Plex configuration
        if let Some(plex) = &self.plex {
            if plex.enabled {
                if plex.url.is_empty() {
                    anyhow::bail!("Plex URL cannot be empty when enabled");
                }
                if plex.token.is_empty() {
                    anyhow::bail!("Plex token cannot be empty when enabled");
                }
                if let Err(e) = validate_http_url(&plex.url) {
                    anyhow::bail!("Invalid Plex URL '{}': {}", plex.url, e);
                }
                info!("Plex integration enabled (URL: {})", plex.url);
            }
        }

        // Validate FUSE options
        if let Some(fuse_opts) = &self.fuse_options {
            if let Some(perms) = fuse_opts.permissions {
                // Validate permissions are reasonable (not world-writable, etc.)
                if perms & 0o002 != 0 {
                    warn!("⚠️  FUSE permissions {:#o} allow world-write", perms);
                    warn!("   This may be a security risk");
                }
                info!("FUSE permissions: {:#o}", perms);
            }

            if fuse_opts.allow_other.unwrap_or(true) {
                info!("FUSE allow_other: Enabled (other users can access mount)");
            }
        }

        // Info about daemon mode
        if self.daemon {
            info!("Daemon mode: Enabled (will run in background)");
        } else {
            info!("Daemon mode: Disabled (will run in foreground)");
        }

        Ok(())
    }
}

fn is_mounted(path: &std::path::Path) -> Result<bool> {
    let output = std::process::Command::new("mountpoint")
        .arg("-q")
        .arg(path)
        .output()
        .context("Failed to check if path is mounted")?;

    Ok(output.status.success())
}

/// Lightweight HTTP(S) URL sanity check for media-server config values.
/// Catches the common "forgot the http:// prefix" / "trailing slash typo"
/// mistakes without pulling in a full URL-parsing crate.
fn validate_http_url(url: &str) -> std::result::Result<(), &'static str> {
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .ok_or("must start with http:// or https://")?;
    if rest.is_empty() {
        return Err("missing host after scheme");
    }
    let host_part = rest.split('/').next().unwrap_or("");
    if host_part.is_empty() {
        return Err("missing host (path-only URL)");
    }
    // If a port is present, it must be numeric.
    if let Some((host, port)) = host_part.rsplit_once(':') {
        if host.is_empty() {
            return Err("empty host before port");
        }
        if port.parse::<u16>().is_err() {
            return Err("port must be a number 0–65535");
        }
    }
    Ok(())
}

//! `FilterEngine` — decides which files and directories `RarGateFs` exposes.
//!
//! For each candidate path the engine answers two questions: (1) does the directory
//! contain media files (by extension), and (2) does the matching `.sfv` validate? Only
//! directories that pass both checks are shown. SFV validation modes (`strict` vs
//! `permissive`) and exclude-dir patterns are driven from `config.yaml`.
//!
//! Heavy lifting uses `rayon` for parallel directory probes and `DashMap` for the
//! validated-directory cache. The engine is hot-reloadable via `FilterReloadHandle`.

use anyhow::Result;
use dashmap::DashMap;
use rayon::prelude::*;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, Duration};
use tracing::{debug, info, warn};
use wildmatch::WildMatch;

use crate::backend::RarFsLimiter;
use crate::config::{Config, SfvConfig, FiltersConfig, OverlayConfig, UnionfsConfig};
use crate::media_notifier_common::DEFAULT_MEDIA_EXTENSIONS;
use crate::metrics::MetricsCollector;
use crate::sfv::validate_sfv_content_with_map;

#[derive(Debug, Clone, PartialEq)]
pub enum SfvMode {
    Strict,
    Permissive,
}

#[derive(Debug, Clone)]
pub struct DirectoryInfo {
    pub should_show: bool,
    pub timestamp: SystemTime,
}

/// Default threshold (seconds) for detecting active downloads
const DEFAULT_DOWNLOAD_THRESHOLD_SECS: u64 = 60;

/// Fields that can be live-reloaded via SIGHUP without restarting the process.
/// Stored under a `RwLock` so the main thread can swap them while FUSE threads read.
pub struct HotConfig {
    pub sfv_config: Option<Arc<SfvConfig>>,
    pub filters_config: Option<Arc<FiltersConfig>>,
    pub download_threshold_secs: u64,
    /// Media extensions as a HashSet (with dot prefix, e.g. ".mkv") for O(1) lookup.
    pub media_extensions: Arc<HashSet<String>>,
    pub exclude_file_matchers: Arc<Vec<WildMatch>>,
    pub exclude_dir_patterns_lower: Arc<HashSet<String>>,
}

/// Handle returned by `FilterEngine::new()` for reloading hot configuration on SIGHUP.
pub struct FilterReloadHandle(pub Arc<RwLock<HotConfig>>);

impl FilterReloadHandle {
    /// Replace hot-reloadable fields from a freshly parsed config.
    /// Logs what changed. Non-hot fields (overlay paths, read_only, etc.) are ignored.
    pub fn reload(&self, config: &Config) {
        let new_hot = HotConfig::from_config(config);
        match self.0.write() {
            Ok(mut guard) => {
                *guard = new_hot;
                info!("FilterEngine: hot configuration reloaded");
                info!("  SFV enabled: {}", config.sfv_validation.as_ref().map(|s| s.enabled).unwrap_or(false));
                info!("  Download threshold: {}s", config.sfv_validation.as_ref()
                    .and_then(|s| s.download_threshold_seconds)
                    .unwrap_or(DEFAULT_DOWNLOAD_THRESHOLD_SECS));
            }
            Err(e) => {
                warn!("FilterEngine: config reload failed (lock poisoned): {}", e);
            }
        }
    }
}

pub struct FilterEngine {
    /// Hot-reloadable fields (sfv config, media extensions, exclude patterns).
    /// Wrapped in RwLock so SIGHUP can swap them while FUSE threads read.
    hot: Arc<RwLock<HotConfig>>,
    // Immutable after startup — changing these requires a restart
    overlay_config: Option<Arc<OverlayConfig>>,
    unionfs_config: Option<Arc<UnionfsConfig>>,
    read_only: bool,
    no_delete: bool,
    backend_path: PathBuf,
    // Cache for SFV validation results (path -> DirectoryInfo)
    // Arc allows sharing with inotify invalidation task
    dir_cache: Arc<DashMap<PathBuf, DirectoryInfo>>,
    cache_ttl: Duration,
    // THROTTLING: Custom thread pool to limit concurrent rar2fs backend access
    // rar2fs is single-threaded and can crash under heavy concurrent load
    validation_pool: rayon::ThreadPool,
    /// Shared with `RarGateFs` so total concurrent rar2fs `read_dir` calls (FUSE
    /// readdir + validation pool) are bounded globally, not per-subsystem.
    rar2fs_limiter: Arc<RarFsLimiter>,
    // Optional metrics collector for SFV pass/fail and cache hit tracking
    metrics: Option<Arc<MetricsCollector>>,
}

impl HotConfig {
    /// Build a `HotConfig` from a parsed `Config`. Called on startup and on SIGHUP.
    pub fn from_config(config: &Config) -> Self {
        // Media extensions stored as ".mkv", ".mp4" etc. (dot-prefixed) in a HashSet
        // so hot paths can do O(1) extension lookup by slicing the filename after the last '.'.
        let media_extensions: HashSet<String> = config.filters
            .as_ref()
            .and_then(|f| f.media_extensions.as_ref())
            .map(|exts| exts.iter().map(|e| format!(".{}", e.to_lowercase())).collect())
            .unwrap_or_else(|| {
                DEFAULT_MEDIA_EXTENSIONS.iter()
                    .map(|e| format!(".{}", e))
                    .collect()
            });

        // Pre-compile WildMatch patterns once (90% speedup vs creating per-file)
        let exclude_file_matchers: Vec<WildMatch> = config.filters
            .as_ref()
            .and_then(|f| f.exclude_files.as_ref())
            .map(|patterns| patterns.iter().map(|p| WildMatch::new(&p.to_lowercase())).collect())
            .unwrap_or_default();

        // Pre-lowercase directory patterns into HashSet for O(1) membership tests
        let exclude_dir_patterns_lower: HashSet<String> = config.filters
            .as_ref()
            .and_then(|f| f.exclude_dirs.as_ref())
            .map(|patterns| patterns.iter().map(|p| p.to_lowercase()).collect())
            .unwrap_or_default();

        let download_threshold_secs = config.sfv_validation
            .as_ref()
            .and_then(|s| s.download_threshold_seconds)
            .unwrap_or(DEFAULT_DOWNLOAD_THRESHOLD_SECS);

        Self {
            sfv_config: config.sfv_validation.clone().map(Arc::new),
            filters_config: config.filters.clone().map(Arc::new),
            download_threshold_secs,
            media_extensions: Arc::new(media_extensions),
            exclude_file_matchers: Arc::new(exclude_file_matchers),
            exclude_dir_patterns_lower: Arc::new(exclude_dir_patterns_lower),
        }
    }
}

impl FilterEngine {
    /// Create a new `FilterEngine` and a `FilterReloadHandle` for SIGHUP hot-reload.
    pub fn new(backend_path: PathBuf, config: Config, rar2fs_limiter: Arc<RarFsLimiter>) -> Result<(Self, FilterReloadHandle)> {
        // Get cache timeout from config (default: 300 seconds = 5 minutes)
        let cache_ttl = Duration::from_secs(
            config.filters
                .as_ref()
                .and_then(|f| f.rar_archives.as_ref())
                .and_then(|r| r.cache_timeout)
                .unwrap_or(300)
        );

        // THROTTLING: Create a limited thread pool for backend access
        // rar2fs is single-threaded and crashes under heavy concurrent load
        let max_backend_threads = config.rar2fs.max_concurrent_reads.unwrap_or(4);
        let validation_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(max_backend_threads)
            .thread_name(|i| format!("sfv-validator-{}", i))
            .build()
            .expect("Failed to create validation thread pool");
        info!("Created validation thread pool with {} threads (max_concurrent_reads)", max_backend_threads);

        let hot = Arc::new(RwLock::new(HotConfig::from_config(&config)));
        let reload_handle = FilterReloadHandle(Arc::clone(&hot));

        let engine = Self {
            hot,
            overlay_config: config.overlay_reference.map(Arc::new),
            unionfs_config: config.unionfs_reference.map(Arc::new),
            read_only: config.read_only.unwrap_or(false),
            no_delete: config.no_delete.unwrap_or(true),
            backend_path,
            dir_cache: Arc::new(DashMap::new()),
            cache_ttl,
            validation_pool,
            rar2fs_limiter,
            metrics: None,
        };

        Ok((engine, reload_handle))
    }

    /// Attach a metrics collector (call before first use).
    pub fn set_metrics(&mut self, metrics: Arc<MetricsCollector>) {
        self.metrics = Some(metrics);
    }
    
    /// Get a clone of the cache for invalidation purposes
    pub fn get_cache_handle(&self) -> Arc<DashMap<PathBuf, DirectoryInfo>> {
        Arc::clone(&self.dir_cache)
    }

    pub fn should_show_file(&self, parent_path: &Path, filename: &str) -> bool {
        let hot = self.hot.read().unwrap();

        // Check RAR archive hiding first
        if let Some(filters) = &hot.filters_config {
            if let Some(rar_config) = &filters.rar_archives {
                if rar_config.enabled && rar_config.hide_archives.unwrap_or(true)
                    && self.is_rar_archive(filename) {
                        debug!("RAR archive hidden: {}", filename);
                        return false;
                    }
            }
        }

        // Check basic file filters using pre-compiled patterns (PERFORMANCE FIX)
        if !hot.exclude_file_matchers.is_empty() {
            let filename_lower = filename.to_lowercase();
            for matcher in hot.exclude_file_matchers.iter() {
                if matcher.matches(&filename_lower) {
                    debug!("File {} excluded by pattern", filename);
                    return false;
                }
            }
        }

        // Check SFV validation if enabled
        if let Some(sfv_config) = &hot.sfv_config {
            if sfv_config.enabled {
                drop(hot); // release lock before the potentially slow SFV path
                return self.is_file_allowed_by_sfv(parent_path, filename);
            }
        }

        true
    }

    /// Check if file is a RAR archive part
    fn is_rar_archive(&self, filename: &str) -> bool {
        let lower = filename.to_lowercase();

        // Check for .rar extension
        if lower.ends_with(".rar") {
            return true;
        }

        // Check for .rXX pattern (where XX is 00-99)
        // PERFORMANCE FIX: Check pattern directly instead of looping 100 times!
        // This reduces complexity from O(100) to O(1) per file
        if lower.len() >= 4 {
            let bytes = lower.as_bytes();
            let len = bytes.len();

            // Check if it ends with ".rXX" where XX are two digits
            if bytes[len - 4] == b'.' &&
               bytes[len - 3] == b'r' &&
               bytes[len - 2].is_ascii_digit() &&
               bytes[len - 1].is_ascii_digit() {
                return true;
            }
        }

        false
    }

    /// **TIER 1: Parallel SFV Validation**
    /// Validate multiple directories concurrently for much faster performance
    /// Returns a map of directory names to should_show status
    pub fn should_show_directories_parallel(&self, parent_path: &Path, dirnames: &[String]) -> std::collections::HashMap<String, bool> {
        use std::collections::HashMap;

        let mut results = HashMap::new();

        // Check if SFV validation is enabled — read once, drop lock
        let (sfv_enabled, lazy_mode) = {
            let hot = self.hot.read().unwrap();
            (
                hot.sfv_config.as_ref().map(|c| c.enabled).unwrap_or(false),
                hot.sfv_config.as_ref().and_then(|c| c.lazy_mode).unwrap_or(false),
            )
        };

        // DEBUG: Log config values
        debug!("should_show_directories_parallel: parent_path={}, sfv_enabled={}, lazy_mode={}, num_dirs={}",
            parent_path.display(), sfv_enabled, lazy_mode, dirnames.len());

        // If lazy mode or SFV disabled, just check basic filters
        if lazy_mode || !sfv_enabled {
            debug!("Using basic filters only (lazy_mode={}, sfv_enabled={})", lazy_mode, sfv_enabled);
            for dirname in dirnames {
                let should_show = self.should_show_directory_basic_filter_hot(dirname);
                debug!("  Directory '{}': should_show={}", dirname, should_show);
                results.insert(dirname.clone(), should_show);
            }
            return results;
        }

        // For small directory counts, sequential validation is faster than thread spawning overhead
        const PARALLEL_THRESHOLD: usize = 100;
        if dirnames.len() < PARALLEL_THRESHOLD {
            debug!("Using sequential validation (only {} dirs, threshold={})", dirnames.len(), PARALLEL_THRESHOLD);
            for dirname in dirnames {
                if !self.should_show_directory_basic_filter_hot(dirname) {
                    results.insert(dirname.clone(), false);
                    continue;
                }
                let full_path = parent_path.join(dirname);
                let dir_info = self.get_directory_info(&full_path);
                results.insert(dirname.clone(), dir_info.should_show);
            }
            return results;
        }

        // **Parallel validation with Rayon** - uses limited thread pool to prevent rar2fs crashes
        debug!("Using parallel validation with {} threads ({}+ dirs)",
               self.validation_pool.current_num_threads(), PARALLEL_THRESHOLD);

        // THROTTLING: Use custom thread pool instead of global Rayon pool
        let results: HashMap<String, bool> = self.validation_pool.install(|| {
            dirnames
                .par_iter()
                .map(|dirname| {
                    let should_show = if !self.should_show_directory_basic_filter_hot(dirname) {
                        false
                    } else {
                        let full_path = parent_path.join(dirname);
                        self.get_directory_info(&full_path).should_show
                    };
                    (dirname.clone(), should_show)
                })
                .collect()
        });

        results
    }

    /// Check only basic directory filters using the hot-reloadable exclude patterns.
    fn should_show_directory_basic_filter_hot(&self, dirname: &str) -> bool {
        let hot = self.hot.read().unwrap();
        if !hot.exclude_dir_patterns_lower.is_empty() {
            let dirname_lower = dirname.to_lowercase();
            if hot.exclude_dir_patterns_lower.contains(&dirname_lower) {
                debug!("FILTERED: Directory '{}' matches exclude pattern", dirname);
                return false;
            }
        }
        debug!("ALLOWED (basic filter): Directory '{}'", dirname);
        true
    }


    pub fn read_only_enabled(&self) -> bool {
        self.read_only
    }

    pub fn no_delete_enabled(&self) -> bool {
        self.no_delete
    }

    /// True if `filename` has a configured media extension (e.g. .mkv, .avi).
    /// Used by the write-protection logic to identify RAR-extracted media.
    pub fn is_media_file(&self, filename: &str) -> bool {
        let lower = filename.to_lowercase();
        match lower.rfind('.') {
            Some(dot) => self.hot.read().unwrap().media_extensions.contains(&lower[dot..]),
            None => false,
        }
    }

    fn is_file_allowed_by_sfv(&self, dir_path: &Path, _filename: &str) -> bool {
        let (permissive_validates, lazy_mode) = {
            let hot = self.hot.read().unwrap();
            (
                hot.sfv_config.as_ref().and_then(|c| c.permissive_validates_sfv).unwrap_or(false),
                hot.sfv_config.as_ref().and_then(|c| c.lazy_mode).unwrap_or(false),
            )
        };
        let sfv_mode = self.get_sfv_mode(dir_path);
        match sfv_mode {
            SfvMode::Permissive if !permissive_validates => true,
            SfvMode::Permissive | SfvMode::Strict => {
                if lazy_mode {
                    // Lazy mode: validate directory when file is accessed
                    self.get_directory_info(dir_path).should_show
                } else {
                    // Non-lazy mode: directory was already validated during listing
                    true
                }
            }
        }
    }
    
    fn get_sfv_mode(&self, backend_path: &Path) -> SfvMode {
        let sfv_config = {
            let hot = self.hot.read().unwrap();
            hot.sfv_config.clone()
        };
        let sfv_config = match sfv_config {
            Some(config) => config,
            None => return SfvMode::Permissive,
        };

        // Try overlay/unionfs layer resolution first
        if let Some((_, layer_type)) = self.get_source_layer_path(backend_path) {
            return match layer_type.as_str() {
                "verified" => SfvMode::Strict,
                "unverified" => SfvMode::Permissive,
                _ => SfvMode::Permissive,
            };
        }

        // Fallback to path indicators
        let path_str = backend_path.to_string_lossy();

        // Check for verified indicators (strict mode)
        for indicator in &sfv_config.verified_share_indicators {
            if path_str.contains(indicator) {
                debug!("Path {} matches verified indicator: {} -> strict mode", path_str, indicator);
                return SfvMode::Strict;
            }
        }

        // Check for unverified indicators (permissive mode)
        for indicator in &sfv_config.unverified_share_indicators {
            if path_str.contains(indicator) {
                debug!("Path {} matches unverified indicator: {} -> permissive mode", path_str, indicator);
                return SfvMode::Permissive;
            }
        }

        // Default mode
        match sfv_config.default_mode.as_str() {
            "strict" => SfvMode::Strict,
            _ => SfvMode::Permissive,
        }
    }

    /// Get the actual source layer path (verified or unverified) for a backend path
    /// Returns: (layer_path, layer_type) where layer_type is "verified" or "unverified"
    fn get_source_layer_path(&self, backend_path: &Path) -> Option<(PathBuf, String)> {
        // Convert backend path to relative path
        let relative_path = backend_path.strip_prefix(&self.backend_path).ok()?;

        // Check overlay configuration
        if let Some(overlay_ref) = &self.overlay_config {
            if overlay_ref.use_for_filtering.unwrap_or(false) {
                // Check unverified layer FIRST - if content exists here, treat as unverified
                let unverified_path = overlay_ref.unverified_share_path.join(relative_path);
                if unverified_path.exists() {
                    return Some((unverified_path, "unverified".to_string()));
                }

                // Check verified layer only if not in unverified
                let verified_path = overlay_ref.verified_share_path.join(relative_path);
                if verified_path.exists() {
                    return Some((verified_path, "verified".to_string()));
                }
            }
        }

        // Check unionfs configuration
        if let Some(unionfs_ref) = &self.unionfs_config {
            if unionfs_ref.use_for_filtering.unwrap_or(false) {
                // Check unverified layer FIRST
                let unverified_path = unionfs_ref.unverified_share_path.join(relative_path);
                if unverified_path.exists() {
                    return Some((unverified_path, "unverified".to_string()));
                }

                // Check verified layer only if not in unverified
                let verified_path = unionfs_ref.verified_share_path.join(relative_path);
                if verified_path.exists() {
                    return Some((verified_path, "verified".to_string()));
                }
            }
        }

        None
    }

    /// Get directory information with SFV validation caching.
    ///
    /// Phases:
    ///   1. **Cache lookup** — return immediately on a fresh `dir_cache` hit.
    ///   2. **Backend probe** — scan the rar2fs-extracted listing for files with media
    ///      extensions. No media → directory is hidden (returns "not visible").
    ///   3. **Source-layer SFV read** — for the same path on the source overlay,
    ///      locate `.sfv` files and validate per the configured mode
    ///      (`strict`/`permissive`). SFV lives on the source, not the backend, because
    ///      rar2fs doesn't surface non-archived files.
    ///   4. **Decision & cache write** — combine the media probe and SFV result into a
    ///      `DirectoryInfo`, store it in `dir_cache`, return.
    ///
    /// The cache TTL and the strict/permissive split are read from `self.hot` under a
    /// short-lived `RwLock` so SIGHUP reloads pick up without blocking ongoing reads.
    fn get_directory_info(&self, backend_dir_path: &Path) -> DirectoryInfo {
        let now = SystemTime::now();

        // Check cache first. Also remember the prior decision (even if the entry is stale)
        // so we can log the hidden -> visible transition once when a directory becomes OK.
        let mut prev_should_show: Option<bool> = None;
        if let Some(entry) = self.dir_cache.get(backend_dir_path) {
            prev_should_show = Some(entry.should_show);
            if now.duration_since(entry.timestamp).unwrap_or(Duration::MAX) < self.cache_ttl {
                if let Some(metrics) = &self.metrics {
                    metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                }
                return entry.clone();
            }
        }

        if let Some(metrics) = &self.metrics {
            metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
        }

        // Snapshot hot-reloadable config once — holds lock only for the clone, not I/O
        let (media_extensions, sfv_config_snap) = {
            let hot = self.hot.read().unwrap();
            (Arc::clone(&hot.media_extensions), hot.sfv_config.clone())
        };

        // Get the actual source layer path (bypasses overlay cache)
        let (source_path, layer_type) = self.get_source_layer_path(backend_dir_path)
            .unwrap_or_else(|| (backend_dir_path.to_path_buf(), String::new()));

        // For SFV validation, we MUST read from the SOURCE layer (verified/unverified), NOT the rar2fs backend!
        // The rar2fs backend shows extracted media files, but SFV lists RAR archive parts.
        // We check media files from backend (for has_media detection), but RAR parts from source.

        // First, read backend for media file detection — gate concurrent walks via the
        // shared rar2fs limiter to keep parallel validations from saturating rar2fs.
        let mut has_media = false;
        {
            let _permit = self.rar2fs_limiter.acquire();
            let backend_entries = match std::fs::read_dir(backend_dir_path) {
                Ok(entries) => entries,
                Err(e) => {
                    warn!("Failed to read backend directory {}: {}", backend_dir_path.display(), e);
                    let info = DirectoryInfo {
                        should_show: true,
                        timestamp: now,
                    };
                    self.dir_cache.insert(backend_dir_path.to_path_buf(), info.clone());
                    return info;
                }
            };

            for entry in backend_entries.flatten() {
                let entry_path = entry.path();
                if entry_path.is_file() {
                    // O(1) HashSet lookup: find the dot, slice extension, check membership
                    let lower_name = entry.file_name().to_string_lossy().to_lowercase();
                    if let Some(dot_pos) = lower_name.rfind('.') {
                        if media_extensions.contains(&lower_name[dot_pos..]) {
                            has_media = true;
                            break;
                        }
                    }
                }
            }
        }

        // OPTIMIZATION: If no media files found, this is a parent directory
        // Skip source layer read entirely - no SFV validation needed
        if !has_media {
            debug!("Directory {} has no media files (parent dir), skipping source read", source_path.display());
            let info = DirectoryInfo {
                should_show: true,
                timestamp: now,
            };
            self.dir_cache.insert(backend_dir_path.to_path_buf(), info.clone());
            return info;
        }

        // Now read SOURCE layer (verified/unverified) for SFV validation - this has the actual RAR files
        // Only done for directories with media files (need SFV validation)
        let entries = match std::fs::read_dir(&source_path) {
            Ok(entries) => entries,
            Err(e) => {
                warn!("Failed to read directory {}: {}", backend_dir_path.display(), e);
                let info = DirectoryInfo {
                    should_show: true,
                    timestamp: now,
                };
                self.dir_cache.insert(backend_dir_path.to_path_buf(), info.clone());
                return info;
            }
        };

        // Build file list from SOURCE layer for SFV validation
        let mut has_sfv = false;
        let mut sfv_file_path = None;
        let mut actual_files = std::collections::HashMap::new(); // Build once for SFV validation

        for entry in entries.flatten() {
            let entry_path = entry.path();
            if entry_path.is_file() {
                let filename = entry.file_name().to_string_lossy().to_string();
                let lower_name = filename.to_lowercase();

                // Build case-insensitive filename map for SFV validation (RAR parts, etc.)
                actual_files.insert(lower_name.clone(), filename);

                if lower_name.ends_with(".sfv") {
                    has_sfv = true;
                    sfv_file_path = Some(entry_path);
                    debug!("SFV file found in {}: {}", source_path.display(), lower_name);
                }
            } else if entry_path.is_dir() {
                // Scene releases place samples/proofs in a subfolder that the
                // SFV references with a Windows path (e.g. 'Sample\foo.mkv').
                // Index those files by basename so such SFV entries validate.
                if let Ok(sub_entries) = std::fs::read_dir(&entry_path) {
                    for sub in sub_entries.flatten() {
                        if sub.path().is_file() {
                            let sub_name = sub.file_name().to_string_lossy().to_string();
                            actual_files.entry(sub_name.to_lowercase()).or_insert(sub_name);
                        }
                    }
                }
            }
        }

        // Determine if should show (only for directories with media files)
        let should_show = {
            // Determine mode
            let sfv_mode = if !layer_type.is_empty() {
                match layer_type.as_str() {
                    "verified" => SfvMode::Strict,
                    "unverified" => SfvMode::Permissive,
                    _ => self.get_sfv_mode(backend_dir_path),
                }
            } else {
                self.get_sfv_mode(backend_dir_path)
            };

            debug!("SFV validation for {}: mode={:?}, layer={}, hasSFV={}",
                  source_path.display(), sfv_mode, layer_type, has_sfv);

            let permissive_validates = sfv_config_snap.as_ref()
                .and_then(|c| c.permissive_validates_sfv)
                .unwrap_or(false);

            match sfv_mode {
                SfvMode::Permissive if !permissive_validates => true,
                SfvMode::Permissive => {
                    // permissive_validates_sfv enabled: validate if SFV exists, show if absent
                    if !has_sfv {
                        true
                    } else if let Some(ref sfv_path) = sfv_file_path {
                        let sfv_valid = self.validate_sfv_file_with_files(sfv_path, &actual_files);
                        if !sfv_valid {
                            debug!("Hiding directory {} (permissive mode, SFV present but validation failed)", backend_dir_path.display());
                            self.log_sfv_failure(backend_dir_path, "permissive mode, SFV validation failed", true, has_sfv);
                            false
                        } else {
                            true
                        }
                    } else {
                        true
                    }
                }
                SfvMode::Strict => {
                    if !has_sfv {
                        debug!("Hiding directory {} (strict mode, no SFV file)", backend_dir_path.display());
                        self.log_sfv_failure(backend_dir_path, "strict mode, no SFV file", true, has_sfv);
                        false
                    } else if let Some(ref sfv_path) = sfv_file_path {
                        // Validate SFV contents in strict mode using pre-built file list (PERFORMANCE FIX)
                        let sfv_valid = self.validate_sfv_file_with_files(sfv_path, &actual_files);
                        if !sfv_valid {
                            debug!("Hiding directory {} (strict mode, SFV validation failed)", backend_dir_path.display());
                            self.log_sfv_failure(backend_dir_path, "strict mode, SFV validation failed", true, has_sfv);
                            false
                        } else {
                            true
                        }
                    } else {
                        true
                    }
                }
            }
        };

        // Log the transition from hidden to visible once, so it is clear in the log when a
        // directory that was previously failing validation has become OK to show. Only fires
        // on a false -> true flip (not on first sight), so it does not spam during scans.
        if should_show && prev_should_show == Some(false) {
            info!("Directory now passes validation, showing: {}", backend_dir_path.display());
        }

        // Track SFV validation outcome (only when media files were present and evaluated)
        if has_media {
            if let Some(metrics) = &self.metrics {
                if should_show {
                    metrics.sfv_validations_passed.fetch_add(1, Ordering::Relaxed);
                } else {
                    metrics.sfv_validations_failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        let info = DirectoryInfo {
            should_show,
            timestamp: now,
        };

        self.dir_cache.insert(backend_dir_path.to_path_buf(), info.clone());
        info
    }

    /// Validate SFV file contents - check if all listed files exist
    /// PERFORMANCE: Accepts pre-built file list to avoid double directory read (50% I/O reduction)
    fn validate_sfv_file_with_files(&self, sfv_file_path: &Path, actual_files: &std::collections::HashMap<String, String>) -> bool {
        // Validate SFV entries against pre-built file list using shared module
        // Use lossy UTF-8 to handle legacy encodings (ISO-8859-1, Windows-1252)
        match std::fs::read(sfv_file_path) {
            Ok(bytes) => {
                let content = String::from_utf8_lossy(&bytes);
                let valid = validate_sfv_content_with_map(&content, actual_files);
                if valid {
                    debug!("SFV validation passed for {}", sfv_file_path.display());
                } else {
                    debug!("SFV validation failed for {}", sfv_file_path.display());
                }
                valid
            }
            Err(e) => {
                warn!("Failed to read SFV file {}: {}", sfv_file_path.display(), e);
                true // Assume valid on error (permissive)
            }
        }
    }

    /// Check if a directory has any files that are still being written (recent mtime)
    /// Returns true if download appears to be in progress
    fn is_download_in_progress(&self, dir_path: &Path) -> bool {
        let threshold = {
            let hot = self.hot.read().unwrap();
            hot.download_threshold_secs
        };
        if let Ok(entries) = std::fs::read_dir(dir_path) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    if metadata.is_file() {
                        if let Ok(modified) = metadata.modified() {
                            if let Ok(elapsed) = modified.elapsed() {
                                if elapsed.as_secs() < threshold {
                                    debug!("Download in progress: {} modified {}s ago (threshold: {}s)",
                                           entry.path().display(), elapsed.as_secs(), threshold);
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }
        false
    }

    /// Log SFV validation failures (but not for active downloads)
    fn log_sfv_failure(&self, path: &Path, reason: &str, has_media: bool, has_sfv: bool) {
        let sfv_config = {
            let hot = self.hot.read().unwrap();
            hot.sfv_config.clone()
        };
        if let Some(sfv_config) = sfv_config {
            if !sfv_config.log_failures.unwrap_or(true) {
                return;
            }

            // Check if this is an active download - don't log as failure
            if self.is_download_in_progress(path) {
                debug!("SFV validation deferred for {} (download in progress)", path.display());
                return;
            }

            if let Some(log_file) = &sfv_config.log_file {
                let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
                let log_msg = format!(
                    "[{}] HIDDEN: {} (reason: {}, hasMedia: {}, hasSFV: {})\n",
                    timestamp, path.display(), reason, has_media, has_sfv
                );

                if let Err(e) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(log_file)
                    .and_then(|mut f| std::io::Write::write_all(&mut f, log_msg.as_bytes()))
                {
                    warn!("Failed to write SFV failure log: {}", e);
                }
            }
        }
    }
    
}

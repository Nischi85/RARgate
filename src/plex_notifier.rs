//! Plex media server integration
//!
//! This module provides integration with Plex media servers, supporting:
//! - Partial library scan via `/library/sections/{id}/refresh?path=...`
//! - Full library scan via `/library/sections/all/refresh`
//! - Path mapping for Docker deployments
//! - Library section discovery and caching
//! - Debouncing for batching rapid file changes
//!
//! Authentication uses X-Plex-Token header or token query parameter

use crate::config::{PathMappingConfig, PlexConfig};
use crate::media_notifier_common::{
    build_http_client, filter_existing_paths, get_parent_dirs, post_with_retry,
    run_debounce_loop, translate_path, BatchProcessor, ItemCache, MediaEvent, NotifierHandle,
    DEFAULT_MAX_RETRIES, DEFAULT_RETRY_DELAY_MS, MEDIA_EVENT_CHANNEL_CAPACITY,
};
use crate::metrics::MetricsCollector;
use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Re-export NotifierHandle as PlexNotifierHandle for backwards compatibility
pub type PlexNotifierHandle = NotifierHandle;

/// Plex library sections response
#[derive(Debug, Deserialize)]
struct PlexSectionsResponse {
    #[serde(rename = "MediaContainer")]
    media_container: PlexMediaContainer,
}

#[derive(Debug, Deserialize)]
struct PlexMediaContainer {
    #[serde(rename = "Directory")]
    directories: Option<Vec<PlexDirectory>>,
}

#[derive(Debug, Clone, Deserialize)]
struct PlexDirectory {
    #[serde(rename = "key")]
    key: String,
    #[serde(rename = "title")]
    title: String,
    #[serde(rename = "type")]
    section_type: String,
}

/// Configuration needed by the notifier.
///
/// URLs that are stable across all calls are pre-built at construction time so that
/// hot paths (`fetch_sections`, `refresh_library`) avoid repeated `format!()` allocations.
struct NotifierConfig {
    token: String,
    debounce_seconds: u64,
    cache_minutes: u64,
    fallback_full_refresh: bool,
    path_mapping: Option<PathMappingConfig>,
    /// Maximum retry attempts for API calls
    max_retries: u32,
    /// Initial retry delay in milliseconds (doubles each retry)
    retry_delay_ms: u64,
    // Pre-built URLs — computed once in new(), named by purpose
    /// GET all library sections (used to populate the sections cache)
    fetch_sections_url: String,
    /// POST to trigger a full library rescan (fallback only)
    full_library_url: String,
    /// Prefix for per-section path refresh URLs: append `"{section_key}/refresh?path=...&X-Plex-Token={token}"`
    section_refresh_base: String,
    /// Minimum seconds between full library refreshes (None = no cooldown)
    full_refresh_cooldown_secs: Option<u64>,
}

/// Main Plex notifier that handles API calls
pub struct PlexNotifier {
    config: NotifierConfig,
    client: reqwest::Client,
    receiver: mpsc::Receiver<MediaEvent>,
    sections_cache: ItemCache<PlexDirectory>,
    metrics: Option<Arc<MetricsCollector>>,
    /// Tracks when the last full library refresh was triggered (for cooldown enforcement)
    last_full_refresh: Option<Instant>,
}

impl PlexNotifier {
    /// Create a new PlexNotifier and its handle
    pub fn new(config: PlexConfig) -> Result<(Self, PlexNotifierHandle)> {
        let (sender, receiver) = mpsc::channel::<MediaEvent>(MEDIA_EVENT_CHANNEL_CAPACITY);

        let client = build_http_client()?;
        let cache_minutes = config.cache_minutes.unwrap_or(15);

        // Pre-build all stable URLs once so hot paths never call format!() at runtime
        let fetch_sections_url  = format!("{}/library/sections?X-Plex-Token={}", config.url, config.token);
        let full_library_url    = format!("{}/library/sections/all/refresh?X-Plex-Token={}", config.url, config.token);
        let section_refresh_base = format!("{}/library/sections/", config.url);

        info!("Plex: URL: {}", config.url);

        let notifier_config = NotifierConfig {
            token: config.token,
            debounce_seconds: config.debounce_seconds.unwrap_or(5),
            cache_minutes,
            fallback_full_refresh: config.fallback_full_refresh.unwrap_or(false),
            path_mapping: config.path_mapping,
            max_retries: config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
            retry_delay_ms: config.retry_delay_ms.unwrap_or(DEFAULT_RETRY_DELAY_MS),
            fetch_sections_url,
            full_library_url,
            section_refresh_base,
            full_refresh_cooldown_secs: config.full_refresh_cooldown_seconds,
        };

        let notifier = PlexNotifier {
            config: notifier_config,
            client,
            receiver,
            sections_cache: ItemCache::new(cache_minutes),
            metrics: None,
            last_full_refresh: None,
        };

        let handle = NotifierHandle::new(sender);

        Ok((notifier, handle))
    }

    /// Attach a metrics collector (call before run()).
    pub fn set_metrics(&mut self, metrics: Arc<MetricsCollector>) {
        self.metrics = Some(metrics);
    }

    /// Start the notification processing loop.
    ///
    /// The debounce/select loop itself lives in `media_notifier_common::run_debounce_loop`
    /// (shared with Emby); this method just logs startup, extracts the receiver, and hands
    /// ownership to the driver.
    pub async fn run(mut self) {
        info!("Plex notifier started");
        info!("  Debounce: {}s", self.config.debounce_seconds);
        info!("  Section cache: {} minutes", self.config.cache_minutes);
        info!("  Retry: {} attempts, {}ms initial delay", self.config.max_retries, self.config.retry_delay_ms);

        let debounce_duration = std::time::Duration::from_secs(self.config.debounce_seconds);

        // Move the receiver out so we can pass it to the driver separately from `self`.
        // The placeholder channel is dropped immediately when the driver consumes self.
        let (_placeholder_tx, placeholder_rx) = mpsc::channel::<MediaEvent>(1);
        let receiver = std::mem::replace(&mut self.receiver, placeholder_rx);
        run_debounce_loop(self, receiver, debounce_duration).await;
    }

    /// Process a batch of created/updated paths
    async fn process_batch_created(&mut self, paths: &[PathBuf]) -> Result<()> {
        info!("Plex: Processing batch of {} changed paths", paths.len());

        // Filter existing paths and get parent directories using shared utilities
        let existing_paths = filter_existing_paths(paths);
        let parent_dirs = get_parent_dirs(&existing_paths);

        info!("Plex: {} unique directories to refresh", parent_dirs.len());

        if parent_dirs.is_empty() {
            debug!("Plex: All paths were filtered out (no existing paths), skipping API calls");
            return Ok(());
        }

        // Get or refresh the sections cache
        let _sections = self.get_sections_cache().await?;

        // Process each directory
        let mut refreshed_sections: HashSet<String> = HashSet::new();

        for dir in &parent_dirs {
            // Translate host path to Plex path
            let plex_path = translate_path(dir, &self.config.path_mapping, "Plex");
            debug!("Plex: Translated {} -> {}", dir.display(), plex_path.display());

            // Find which section contains this path
            match self.find_section_for_path(&plex_path).await? {
                Some(section_key) => {
                    // Only refresh each section once per batch
                    if !refreshed_sections.contains(&section_key) {
                        if let Err(e) = self.refresh_path_in_section(&section_key, &plex_path).await {
                            error!("Plex: Failed to refresh path in section {}: {}", section_key, e);
                        } else {
                            refreshed_sections.insert(section_key);
                        }
                    }
                }
                None => {
                    warn!("Plex: Could not determine section for path: {}", plex_path.display());

                    // Fallback to full library refresh if enabled, subject to optional cooldown
                    if self.config.fallback_full_refresh {
                        let cooldown_ok = self.config.full_refresh_cooldown_secs.is_none_or(|secs| {
                            self.last_full_refresh
                                .map(|t| t.elapsed().as_secs() >= secs)
                                .unwrap_or(true)
                        });
                        if cooldown_ok {
                            warn!("Plex: [FALLBACK] Triggering full library scan for path: {}", plex_path.display());
                            if let Err(e) = self.refresh_library().await {
                                error!("Plex: [FALLBACK] Full library refresh failed: {}", e);
                            } else {
                                self.last_full_refresh = Some(Instant::now());
                                if let Some(m) = &self.metrics {
                                    m.plex_full_refreshes.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            return Ok(()); // Don't try other paths after full scan
                        } else {
                            warn!("Plex: [FALLBACK] Full library refresh suppressed by cooldown ({}s remaining)",
                                  self.config.full_refresh_cooldown_secs.unwrap_or(0)
                                      .saturating_sub(self.last_full_refresh.map(|t| t.elapsed().as_secs()).unwrap_or(0)));
                            if let Some(m) = &self.metrics {
                                m.full_refresh_cooldown_skips.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    } else {
                        warn!("Plex: [SKIPPED] No section match for \"{}\" - waiting for scheduled scan",
                              plex_path.display());
                    }
                }
            }
        }

        if refreshed_sections.is_empty() {
            info!("Plex: No sections were refreshed (no matching paths)");
        } else {
            info!("Plex: Refreshed {} sections", refreshed_sections.len());
        }

        Ok(())
    }

    /// Process a batch of deleted paths — re-scan parent directories so Plex notices missing items
    async fn process_batch_deleted(&mut self, paths: &[PathBuf]) -> Result<()> {
        info!("Plex: Processing deletion batch of {} path(s)", paths.len());

        // Deleted items are gone; use their parent directories (which still exist)
        // so Plex re-scans and reconciles its library against what's on disk.
        let parent_dirs = get_parent_dirs(paths);

        // If a parent itself was also deleted (e.g. whole show folder removed),
        // escalate to grandparent — at least one ancestor should still exist.
        let scan_dirs: Vec<PathBuf> = parent_dirs
            .iter()
            .map(|p| {
                if p.exists() {
                    p.clone()
                } else {
                    p.parent()
                        .map(|gp| gp.to_path_buf())
                        .unwrap_or_else(|| p.clone())
                }
            })
            .collect();

        info!("Plex: {} parent dir(s) to re-scan after deletion", scan_dirs.len());

        if scan_dirs.is_empty() {
            debug!("Plex: [DELETE] No parent dirs derived from deleted paths, skipping");
            return Ok(());
        }

        let _sections = self.get_sections_cache().await?;
        let mut refreshed_sections: HashSet<String> = HashSet::new();

        for dir in &scan_dirs {
            let plex_path = translate_path(dir, &self.config.path_mapping, "Plex");
            debug!("Plex: [DELETE] Scanning parent path: {}", plex_path.display());

            match self.find_section_for_path(&plex_path).await? {
                Some(section_key) => {
                    if !refreshed_sections.contains(&section_key) {
                        if let Err(e) = self.refresh_path_in_section(&section_key, &plex_path).await {
                            error!("Plex: [DELETE] Failed to refresh section {} for path {}: {}",
                                   section_key, plex_path.display(), e);
                        } else {
                            refreshed_sections.insert(section_key);
                        }
                    }
                }
                None => {
                    warn!("Plex: [DELETE] Could not find section for parent path: {}", plex_path.display());
                    if self.config.fallback_full_refresh {
                        let cooldown_ok = self.config.full_refresh_cooldown_secs.is_none_or(|secs| {
                            self.last_full_refresh
                                .map(|t| t.elapsed().as_secs() >= secs)
                                .unwrap_or(true)
                        });
                        if cooldown_ok {
                            warn!("Plex: [DELETE][FALLBACK] Triggering full library scan");
                            if let Err(e) = self.refresh_library().await {
                                error!("Plex: [DELETE][FALLBACK] Full library refresh failed: {}", e);
                            } else {
                                self.last_full_refresh = Some(Instant::now());
                                if let Some(m) = &self.metrics {
                                    m.plex_full_refreshes.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            return Ok(());
                        } else {
                            warn!("Plex: [DELETE][FALLBACK] Full library refresh suppressed by cooldown");
                            if let Some(m) = &self.metrics {
                                m.full_refresh_cooldown_skips.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        }

        if refreshed_sections.is_empty() {
            info!("Plex: [DELETE] No sections refreshed after deletion");
        } else {
            info!("Plex: [DELETE] Refreshed {} section(s) after deletion", refreshed_sections.len());
        }

        Ok(())
    }

    /// Get cached sections or fetch from API
    async fn get_sections_cache(&self) -> Result<Vec<PlexDirectory>> {
        info!("Plex: Getting sections from cache or API...");
        self.sections_cache.get_or_fetch(|| async {
            self.fetch_sections().await
        }).await
    }

    /// Fetch all library sections from Plex API
    async fn fetch_sections(&self) -> Result<Vec<PlexDirectory>> {
        let url = &self.config.fetch_sections_url;
        debug!("Plex: Fetching library sections");

        let response = self.client
            .get(url)
            .header("Accept", "application/json")
            .send()
            .await
            .context("Failed to fetch library sections from Plex API")?;

        let body = response
            .text()
            .await
            .context("Failed to read Plex API response body")?;

        let response: PlexSectionsResponse = serde_json::from_str(&body)
            .context("Failed to parse Plex sections response")?;

        let sections = response.media_container.directories.unwrap_or_default();

        for section in &sections {
            debug!("Plex: Section '{}' (key={}, type={})", section.title, section.key, section.section_type);
        }

        Ok(sections)
    }

    /// Find which library section contains a given path
    async fn find_section_for_path(&self, path: &Path) -> Result<Option<String>> {
        let sections = self.get_sections_cache().await?;

        let path_str = path.to_string_lossy();
        let components: Vec<&str> = path_str.split('/').collect();

        // Common Plex library names
        let common_sections = ["Movies", "TV Shows", "TV", "Television", "Music", "Photos", "Home Videos"];

        // Check if any path component matches a known section
        for component in &components {
            // Try exact match
            for section in &sections {
                if section.title == *component {
                    debug!("Plex: Matched section '{}' for path {:?}", section.title, path);
                    return Ok(Some(section.key.clone()));
                }
            }

            // Try case-insensitive match against common sections
            for section_name in &common_sections {
                if component.eq_ignore_ascii_case(section_name) {
                    for section in &sections {
                        if section.title.eq_ignore_ascii_case(section_name) {
                            debug!("Plex: Matched section '{}' (case-insensitive) for path {:?}",
                                   section.title, path);
                            return Ok(Some(section.key.clone()));
                        }
                    }
                }
            }
        }

        debug!("Plex: No specific section found for {:?}", path);
        Ok(None)
    }

    /// Refresh a specific path within a library section
    async fn refresh_path_in_section(&self, section_key: &str, path: &Path) -> Result<()> {
        // section_key and path vary per call; base URL and token are pre-built
        let path_str = path.to_string_lossy();
        let url = format!(
            "{}{}refresh?path={}&X-Plex-Token={}",
            self.config.section_refresh_base,
            section_key,
            urlencoding::encode(&path_str),
            self.config.token
        );

        info!("Plex: [TARGETED] Refreshing path {:?} in section {}", path, section_key);

        let description = format!("path refresh in section {}", section_key);
        post_with_retry(
            &self.client,
            &url,
            &description,
            "Plex",
            self.config.max_retries,
            self.config.retry_delay_ms,
        )
        .await
        .context("Failed to refresh path in Plex section")?;

        info!("Plex: [TARGETED] Successfully refreshed path in section {}", section_key);

        Ok(())
    }

    /// Trigger a full library refresh (fallback)
    async fn refresh_library(&self) -> Result<()> {
        info!("Plex: [FALLBACK] Starting full library scan...");

        post_with_retry(
            &self.client,
            &self.config.full_library_url,
            "full library scan",
            "Plex",
            self.config.max_retries,
            self.config.retry_delay_ms,
        )
        .await?;

        info!("Plex: [FALLBACK] Full library scan triggered successfully");

        Ok(())
    }
}

/// Glue to plug `PlexNotifier` into the shared `run_debounce_loop`. The actual batch
/// logic stays in the inherent impl above; this just exposes it through the trait and
/// projects the right per-server metrics counter.
#[async_trait::async_trait]
impl BatchProcessor for PlexNotifier {
    fn server_name(&self) -> &str {
        "Plex"
    }

    fn increment_notification_counter(&self) {
        if let Some(m) = &self.metrics {
            m.plex_notifications.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn process_batch_created(&mut self, paths: &[PathBuf]) -> Result<()> {
        PlexNotifier::process_batch_created(self, paths).await
    }

    async fn process_batch_deleted(&mut self, paths: &[PathBuf]) -> Result<()> {
        PlexNotifier::process_batch_deleted(self, paths).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media_notifier_common::get_parent_dirs;

    #[test]
    fn test_parent_dir_from_deleted_file() {
        // When a file is deleted, its parent directory is used for re-scanning
        let deleted = vec![
            PathBuf::from("/share/Movies/Death.Note.2017/death.note.mkv"),
        ];
        let parents = get_parent_dirs(&deleted);
        assert!(parents.contains(&PathBuf::from("/share/Movies/Death.Note.2017")));
    }

    #[test]
    fn test_parent_dirs_deduplication() {
        // Multiple files deleted from same directory → single parent dir
        let deleted = vec![
            PathBuf::from("/share/Movies/Death.Note.2017/death.note.mkv"),
            PathBuf::from("/share/Movies/Death.Note.2017/death.note.nfo"),
            PathBuf::from("/share/Movies/Death.Note.2017/death.note.srt"),
        ];
        let parents = get_parent_dirs(&deleted);
        assert_eq!(parents.len(), 1);
        assert!(parents.contains(&PathBuf::from("/share/Movies/Death.Note.2017")));
    }

    #[test]
    fn test_parent_dirs_multiple_titles() {
        // Files from different movie folders → separate parent dirs
        let deleted = vec![
            PathBuf::from("/share/Movies/Death.Note.2017/death.note.mkv"),
            PathBuf::from("/share/Movies/Inception.2010/inception.mkv"),
        ];
        let parents = get_parent_dirs(&deleted);
        assert_eq!(parents.len(), 2);
        assert!(parents.contains(&PathBuf::from("/share/Movies/Death.Note.2017")));
        assert!(parents.contains(&PathBuf::from("/share/Movies/Inception.2010")));
    }

    #[test]
    fn test_grandparent_escalation_when_parent_missing() {
        // When the parent itself doesn't exist (whole dir deleted), escalate to grandparent
        // We use a path guaranteed not to exist on disk
        let nonexistent_parent = PathBuf::from("/nonexistent/Movies/Deleted.Show");
        let result = if nonexistent_parent.exists() {
            nonexistent_parent.clone()
        } else {
            nonexistent_parent
                .parent()
                .map(|gp| gp.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("/nonexistent/Movies/Deleted.Show"))
        };
        assert_eq!(result, PathBuf::from("/nonexistent/Movies"));
    }
}

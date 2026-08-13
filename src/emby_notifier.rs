//! Emby/Jellyfin media server notification integration
//!
//! This module provides targeted library refresh for Emby and Jellyfin servers.
//! Both servers use the same API (Jellyfin is Emby-compatible), so this module
//! handles both with a configurable server name for logging.
//!
//! Features:
//! - Targeted item refresh via `/Items/{id}/Refresh`
//! - Full library scan via `/Library/Refresh`
//! - Path mapping for Docker deployments
//! - Item caching to reduce API calls
//! - Debouncing for batching rapid file changes

use crate::config::{EmbyConfig, PathMappingConfig};
use crate::media_notifier_common::{
    build_http_client, post_json_with_retry, post_with_retry, process_batch_emby_style, run_debounce_loop,
    BatchProcessor, ItemCache, MediaEvent, NotifierHandle,
    DEFAULT_MAX_RETRIES, DEFAULT_RETRY_DELAY_MS, MEDIA_EVENT_CHANNEL_CAPACITY,
};
use crate::metrics::MetricsCollector;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Re-export NotifierHandle as EmbyNotifierHandle for backwards compatibility
pub type EmbyNotifierHandle = NotifierHandle;

/// Emby/Jellyfin API response for item queries
#[derive(Debug, Deserialize)]
struct EmbyItemsResponse {
    #[serde(rename = "Items")]
    items: Vec<EmbyItem>,
}

/// Single item from Emby/Jellyfin API
#[derive(Debug, Clone, Deserialize)]
struct EmbyItem {
    #[serde(rename = "Id")]
    id: String,
    #[serde(rename = "Name")]
    name: Option<String>,
    #[serde(rename = "Path")]
    path: Option<String>,
    #[serde(rename = "Type")]
    item_type: Option<String>,
}

/// Emby's unfiltered `/Items?Recursive=true` also returns generic filesystem-browse
/// "Folder" nodes alongside real library items (Movie/Series/Season/CollectionFolder) — a
/// parallel, non-scanning view of the same paths, distinct from the actual library.
/// `POST /Items/{id}/Refresh` against one of these returns success (204) but never triggers
/// real content discovery, so a path match against a "Folder" node silently no-ops instead
/// of surfacing new media. Excluded from the items cache so every match (direct or
/// parent-walk) can only land on an item that actually participates in library scanning.
fn is_scannable_item_type(item_type: Option<&str>) -> bool {
    item_type != Some("Folder")
}

/// The subset of a library's settings we care about from `/Library/VirtualFolders`.
#[derive(Debug, Clone, Deserialize)]
struct LibraryOptions {
    #[serde(rename = "EnableRealtimeMonitor", default)]
    enable_realtime_monitor: bool,
}

/// A library from Emby/Jellyfin's `/Library/VirtualFolders` — the actual CollectionFolder
/// a user configured (Movies, TV Shows, Documentaries, ...), each covering one or more
/// filesystem locations. Distinct from `EmbyItem`: these never appear in a plain
/// `/Items?Recursive=true` listing (that enumerates a library's *children*, not the
/// library roots themselves), so resolving one requires this separate endpoint.
#[derive(Debug, Clone, Deserialize)]
struct VirtualFolder {
    #[serde(rename = "ItemId")]
    item_id: String,
    #[serde(rename = "Locations", default)]
    locations: Vec<String>,
    #[serde(rename = "LibraryOptions", default)]
    library_options: Option<LibraryOptions>,
}

impl VirtualFolder {
    /// Missing/absent options are treated as monitor-off — the cost of a false negative
    /// here is just an extra whole-library refresh, while a false positive would silently
    /// lose the new item (the exact bug this field exists to avoid).
    fn realtime_monitor_enabled(&self) -> bool {
        self.library_options.as_ref().is_some_and(|o| o.enable_realtime_monitor)
    }
}

/// Resolve the library that owns `path` by longest-prefix match against each configured
/// library's locations, along with whether that library has EnableRealtimeMonitor on.
/// A direct, targeted refresh of the real library (unlike a stale "Folder" browse node)
/// reliably discovers new content regardless of that setting — but when it IS on, the much
/// cheaper path-based `/Library/Media/Updated` notification works too, so callers can use
/// the monitor flag to pick the lighter option where it's known to actually work.
fn find_library_for_path(path: &Path, folders: &[VirtualFolder]) -> Option<(String, bool)> {
    folders
        .iter()
        .filter_map(|f| {
            f.locations
                .iter()
                .filter(|loc| path.starts_with(Path::new(loc.as_str())))
                .map(|loc| loc.len())
                .max()
                .map(|best_len| (best_len, &f.item_id, f.realtime_monitor_enabled()))
        })
        .max_by_key(|(len, _, _)| *len)
        .map(|(_, id, monitor)| (id.clone(), monitor))
}

/// Configuration needed by the notifier.
///
/// URLs that are stable across all calls are pre-built at construction time so that
/// hot paths (`fetch_items`, `notify_media_updated`, `refresh_library`) avoid repeated
/// `format!()` allocations.
struct NotifierConfig {
    debounce_seconds: u64,
    cache_minutes: u64,
    fallback_full_refresh: bool,
    path_mapping: Option<PathMappingConfig>,
    /// Server name for logging ("Emby" or "Jellyfin")
    server_name: &'static str,
    /// Maximum retry attempts for API calls
    max_retries: u32,
    /// Initial retry delay in milliseconds (doubles each retry)
    retry_delay_ms: u64,
    // Pre-built URLs — computed once in new_internal(), named by purpose
    /// GET all library items with paths (used to refresh the item cache)
    fetch_items_url: String,
    /// GET configured libraries (Movies, TV Shows, ...) and their filesystem locations
    virtual_folders_url: String,
    /// POST path-based Created/Deleted notifications
    notify_path_url: String,
    /// POST to trigger a full library rescan (fallback only)
    full_library_url: String,
    /// Prefix for per-item refresh URLs: append `"{id}/Refresh?Recursive=true&api_key={token}"`
    item_refresh_base: String,
    /// Suffix for per-item refresh URLs (token is stable, so pre-built too)
    item_refresh_suffix: String,
    /// Minimum seconds between full library refreshes (None = no cooldown)
    full_refresh_cooldown_secs: Option<u64>,
}

/// Main Emby/Jellyfin notifier that handles API calls
pub struct EmbyNotifier {
    config: NotifierConfig,
    client: reqwest::Client,
    receiver: mpsc::Receiver<MediaEvent>,
    items_cache: ItemCache<EmbyItem>,
    virtual_folders_cache: ItemCache<VirtualFolder>,
    metrics: Option<Arc<MetricsCollector>>,
    /// Tracks when the last full library refresh was triggered (for cooldown enforcement)
    last_full_refresh: Option<Instant>,
}

impl EmbyNotifier {
    /// Create a new EmbyNotifier and its handle
    pub fn new(config: EmbyConfig) -> Result<(Self, EmbyNotifierHandle)> {
        Self::new_internal(config, "Emby", "/emby")
    }

    /// Internal constructor with configurable server name
    /// This is also used by Jellyfin notifier since Jellyfin is API-compatible with Emby
    pub fn new_internal(
        config: EmbyConfig,
        server_name: &'static str,
        api_prefix: &'static str,
    ) -> Result<(Self, EmbyNotifierHandle)> {
        let (sender, receiver) = mpsc::channel::<MediaEvent>(MEDIA_EVENT_CHANNEL_CAPACITY);

        let client = build_http_client()?;
        let cache_minutes = config.cache_minutes.unwrap_or(15);

        // Pre-build all stable URLs once so hot paths never call format!() at runtime
        let base = format!("{}{}", config.url, api_prefix);
        let token = &config.api_token;
        let fetch_items_url    = format!("{}/Items?Recursive=true&Fields=Path,Name&api_key={}", base, token);
        let virtual_folders_url = format!("{}/Library/VirtualFolders?api_key={}", base, token);
        let notify_path_url    = format!("{}/Library/Media/Updated?api_key={}", base, token);
        let full_library_url   = format!("{}/Library/Refresh?api_key={}", base, token);
        let item_refresh_base  = format!("{}/Items/", base);
        let item_refresh_suffix = format!("/Refresh?Recursive=true&api_key={}", token);

        info!("{}: URL: {}", server_name, config.url);

        let notifier_config = NotifierConfig {
            debounce_seconds: config.debounce_seconds.unwrap_or(5),
            cache_minutes,
            fallback_full_refresh: config.fallback_full_refresh.unwrap_or(false),
            path_mapping: config.path_mapping,
            server_name,
            max_retries: config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
            retry_delay_ms: config.retry_delay_ms.unwrap_or(DEFAULT_RETRY_DELAY_MS),
            fetch_items_url,
            virtual_folders_url,
            notify_path_url,
            full_library_url,
            item_refresh_base,
            item_refresh_suffix,
            full_refresh_cooldown_secs: config.full_refresh_cooldown_seconds,
        };

        let notifier = EmbyNotifier {
            config: notifier_config,
            client,
            receiver,
            items_cache: ItemCache::new(cache_minutes),
            virtual_folders_cache: ItemCache::new(cache_minutes),
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
    /// (shared with Plex); this method just logs startup, extracts the receiver, and hands
    /// ownership to the driver.
    pub async fn run(mut self) {
        let server_name = self.config.server_name;
        info!("{} notifier started", server_name);
        info!("  Debounce: {}s", self.config.debounce_seconds);
        info!("  Item cache: {} minutes", self.config.cache_minutes);
        info!("  Retry: {} attempts, {}ms initial delay", self.config.max_retries, self.config.retry_delay_ms);

        let debounce_duration = Duration::from_secs(self.config.debounce_seconds);

        // Move the receiver out so we can pass it to the driver separately from `self`.
        // The placeholder channel is dropped immediately when the driver consumes self.
        let (_placeholder_tx, placeholder_rx) = mpsc::channel::<MediaEvent>(1);
        let receiver = std::mem::replace(&mut self.receiver, placeholder_rx);
        run_debounce_loop(self, receiver, debounce_duration).await;
    }

    /// Process a batch of created/updated paths
    async fn process_batch_created(&mut self, paths: &[PathBuf]) -> Result<()> {
        let server_name = self.config.server_name;

        // Get or refresh the items cache
        let items = self.get_items_cache().await?;

        // Find matching items using shared logic
        let items_to_refresh = process_batch_emby_style(
            server_name,
            paths,
            &self.config.path_mapping,
            &items,
            |item| item.path.as_ref(),
            |item| &item.id,
        ).await;

        if items_to_refresh.is_empty() {
            // No matching items found - try parent container refresh, then path-based notification
            use crate::media_notifier_common::{filter_existing_paths, get_target_dirs, translate_path};

            let existing_paths = filter_existing_paths(paths);
            let target_dirs = get_target_dirs(&existing_paths);

            if target_dirs.is_empty() {
                debug!("{}: All paths were filtered out (no existing paths), skipping", server_name);
                return Ok(());
            }

            // Try to find parent containers (Season/Series folders) to refresh
            let parent_items = self.find_parent_items(&target_dirs, &items);

            if !parent_items.is_empty() {
                info!("{}: Refreshing {} parent container(s) for new content",
                      server_name, parent_items.len());
                let mut any_success = false;
                for (item_id, item) in &parent_items {
                    let item_name = item.name.as_deref().unwrap_or("Unknown");
                    let item_type = item.item_type.as_deref().unwrap_or("Unknown");
                    if let Err(e) = self.refresh_item(item_id, item_name, item_type).await {
                        error!("{}: Failed to refresh parent {} ({}): {}",
                               server_name, item_id, item_name, e);
                    } else {
                        any_success = true;
                    }
                }
                if any_success {
                    self.items_cache.invalidate().await;
                }
                return Ok(());
            }

            // No parent item found either (genuinely new content, no existing sibling
            // under this library to match against) — resolve the owning library itself
            // via /Library/VirtualFolders. Libraries with EnableRealtimeMonitor on get
            // the cheap path-based notification, scoped to just the new item's own path
            // (Emby actually picks these up when that setting is on); everything else
            // gets the reliable whole-library refresh, since the path-based notification
            // always reports success but has been observed to silently do nothing for a
            // library where that setting is off.
            let virtual_folders = match self.get_virtual_folders_cache().await {
                Ok(f) => f,
                Err(e) => {
                    warn!("{}: Failed to fetch virtual folders: {}", server_name, e);
                    Vec::new()
                }
            };

            let mut container_paths: Vec<PathBuf> = Vec::new();
            let mut monitor_on_paths: Vec<(PathBuf, String)> = Vec::new();
            let mut libraries_to_refresh: HashSet<String> = HashSet::new();
            for dir in &target_dirs {
                let container_path = translate_path(dir, &self.config.path_mapping, server_name);
                match find_library_for_path(&container_path, &virtual_folders) {
                    Some((library_id, true)) => monitor_on_paths.push((container_path, library_id)),
                    Some((library_id, false)) => { libraries_to_refresh.insert(library_id); }
                    None => container_paths.push(container_path),
                }
            }

            if !monitor_on_paths.is_empty() {
                let paths: Vec<PathBuf> = monitor_on_paths.iter().map(|(p, _)| p.clone()).collect();
                info!("{}: Notifying {} new path(s) directly (owning librar{} has real-time monitoring on)",
                      server_name, paths.len(), if paths.len() == 1 { "y" } else { "ies" });
                if let Err(e) = self.notify_media_updated(&paths, "Created").await {
                    error!("{}: Targeted path notification failed, falling back to library refresh: {}",
                           server_name, e);
                    for (_, library_id) in &monitor_on_paths {
                        libraries_to_refresh.insert(library_id.clone());
                    }
                } else {
                    self.items_cache.invalidate().await;
                    if let Some(m) = &self.metrics {
                        m.emby_targeted_new_content_notifies.fetch_add(paths.len() as u64, Ordering::Relaxed);
                    }
                }
            }

            if !libraries_to_refresh.is_empty() {
                info!("{}: Refreshing {} owning librar{} for new content",
                      server_name, libraries_to_refresh.len(),
                      if libraries_to_refresh.len() == 1 { "y" } else { "ies" });
                for library_id in &libraries_to_refresh {
                    if let Err(e) = self.refresh_item(library_id, "Library", "CollectionFolder").await {
                        error!("{}: Failed to refresh library {}: {}", server_name, library_id, e);
                    }
                }
                self.items_cache.invalidate().await;
            }

            if container_paths.is_empty() {
                return Ok(());
            }

            // Remaining paths matched no known library either — fall back to
            // path-based notification.
            info!("{}: No matching items, parents, or libraries found - using path-based notification for {} paths",
                  server_name, container_paths.len());

            for path in &container_paths {
                debug!("{}:   Notifying path: {}", server_name, path.display());
            }

            // Use path-based notification instead of full library scan
            if let Err(e) = self.notify_media_updated(&container_paths, "Created").await {
                error!("{}: Path-based notification failed: {}", server_name, e);

                // Fall back to full library scan only if path-based notification fails
                // and fallback is enabled, subject to optional cooldown
                if self.config.fallback_full_refresh {
                    let cooldown_ok = self.config.full_refresh_cooldown_secs.is_none_or(|secs| {
                        self.last_full_refresh
                            .map(|t| t.elapsed().as_secs() >= secs)
                            .unwrap_or(true)
                    });
                    if cooldown_ok {
                        warn!("{}: [FALLBACK] Path-based notification failed, triggering full library scan", server_name);
                        if let Err(e) = self.refresh_library().await {
                            error!("{}: [FALLBACK] Full library refresh failed: {}", server_name, e);
                        } else {
                            self.last_full_refresh = Some(Instant::now());
                            if let Some(m) = &self.metrics {
                                m.emby_full_refreshes.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    } else {
                        warn!("{}: [FALLBACK] Full library refresh suppressed by cooldown ({}s remaining)",
                              server_name,
                              self.config.full_refresh_cooldown_secs.unwrap_or(0)
                                  .saturating_sub(self.last_full_refresh.map(|t| t.elapsed().as_secs()).unwrap_or(0)));
                        if let Some(m) = &self.metrics {
                            m.full_refresh_cooldown_skips.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }

            return Ok(());
        }

        info!("{}: Triggering refresh for {} items", server_name, items_to_refresh.len());

        // Trigger refresh for each item
        let mut any_success = false;
        for (item_id, item) in &items_to_refresh {
            let item_name = item.name.as_deref().unwrap_or("Unknown");
            let item_type = item.item_type.as_deref().unwrap_or("Unknown");
            if let Err(e) = self.refresh_item(item_id, item_name, item_type).await {
                error!("{}: Failed to refresh item {} ({}): {}", server_name, item_id, item_name, e);
            } else {
                any_success = true;
            }
        }

        // Invalidate cache after successful refreshes to stay in sync with library changes
        if any_success {
            self.items_cache.invalidate().await;
            debug!("{}: Cache invalidated after successful item refresh", server_name);
        }

        Ok(())
    }

    /// Process a batch of deleted paths — notify Emby/Jellyfin with UpdateType "Deleted"
    async fn process_batch_deleted(&mut self, paths: &[PathBuf]) -> Result<()> {
        let server_name = self.config.server_name;
        info!("{}: Processing deletion batch of {} path(s)", server_name, paths.len());

        // Deleted paths no longer exist on disk — do NOT call filter_existing_paths.
        // Translate host paths to container paths and notify with "Deleted" update type.
        use crate::media_notifier_common::translate_path;

        let container_paths: Vec<PathBuf> = paths
            .iter()
            .map(|p| translate_path(p, &self.config.path_mapping, server_name))
            .collect();

        for path in &container_paths {
            debug!("{}: [DELETE] Notifying deleted path: {}", server_name, path.display());
        }

        if let Err(e) = self.notify_media_updated(&container_paths, "Deleted").await {
            error!("{}: [DELETE] Deletion notification failed: {}", server_name, e);
            // Do not fall back to full library scan — it cannot remove items from the library
        }

        // Invalidate cache so deleted items are not matched in future batches
        self.items_cache.invalidate().await;

        Ok(())
    }

    /// Find parent container items (Season, Series folders) for paths with no direct match.
    /// Walks up the directory tree from each changed path and checks if any cached Emby item
    /// has a matching path, enabling targeted refresh of the parent to discover new children.
    fn find_parent_items(
        &self,
        target_dirs: &HashSet<PathBuf>,
        items: &[EmbyItem],
    ) -> HashMap<String, EmbyItem> {
        use crate::media_notifier_common::translate_path;

        let mut parent_items = HashMap::new();
        let server_name = self.config.server_name;

        for dir in target_dirs {
            let container_path = translate_path(dir, &self.config.path_mapping, server_name);

            // Walk up parent directories (e.g., Season.7 -> Dexter -> TV.Series)
            let mut current = container_path.parent().map(|p| p.to_path_buf());
            while let Some(parent) = current {
                // Don't go above the root media library path
                if parent.components().count() <= 1 {
                    break;
                }

                let mut found = false;
                for item in items {
                    if let Some(item_path_str) = &item.path {
                        let item_path = PathBuf::from(item_path_str);
                        if item_path == parent {
                            let id = item.id.clone();
                            if let std::collections::hash_map::Entry::Vacant(e) =
                                parent_items.entry(id.clone())
                            {
                                info!("{}: [PARENT] Found parent container: {} \"{}\" (ID: {})",
                                    server_name,
                                    item.item_type.as_deref().unwrap_or("Unknown"),
                                    item.name.as_deref().unwrap_or("Unknown"),
                                    id);
                                e.insert(item.clone());
                            }
                            found = true;
                            break;
                        }
                    }
                }

                // If we found a parent for this dir, stop walking up
                if found {
                    break;
                }

                current = parent.parent().map(|p| p.to_path_buf());
            }
        }

        parent_items
    }

    /// Get cached items or fetch from API
    async fn get_items_cache(&self) -> Result<Vec<EmbyItem>> {
        info!("{}: Getting items from cache or API...", self.config.server_name);
        self.items_cache.get_or_fetch(|| async {
            self.fetch_items().await
        }).await
    }

    /// Fetch all items with paths from API
    async fn fetch_items(&self) -> Result<Vec<EmbyItem>> {
        let url = &self.config.fetch_items_url;
        debug!("{}: Fetching library items", self.config.server_name);

        let response = self.client
            .get(url)
            .send()
            .await
            .context(format!("Failed to fetch items from {} API", self.config.server_name))?;

        let body = response
            .text()
            .await
            .context(format!("Failed to read {} API response body", self.config.server_name))?;

        let response: EmbyItemsResponse = serde_json::from_str(&body)
            .context(format!("Failed to parse {} API response", self.config.server_name))?;

        let items: Vec<EmbyItem> = response
            .items
            .into_iter()
            .filter(|item| item.path.is_some())
            .filter(|item| is_scannable_item_type(item.item_type.as_deref()))
            .collect();

        Ok(items)
    }

    async fn get_virtual_folders_cache(&self) -> Result<Vec<VirtualFolder>> {
        self.virtual_folders_cache.get_or_fetch(|| async {
            self.fetch_virtual_folders().await
        }).await
    }

    /// Fetch the server's configured libraries and their filesystem locations.
    /// `/Library/VirtualFolders` returns a bare JSON array (unlike `/Items`, which wraps
    /// results in `{"Items": [...]}`).
    async fn fetch_virtual_folders(&self) -> Result<Vec<VirtualFolder>> {
        let url = &self.config.virtual_folders_url;
        debug!("{}: Fetching virtual folders (libraries)", self.config.server_name);

        let response = self.client
            .get(url)
            .send()
            .await
            .context(format!("Failed to fetch virtual folders from {} API", self.config.server_name))?;

        let body = response
            .text()
            .await
            .context(format!("Failed to read {} virtual folders response body", self.config.server_name))?;

        let folders: Vec<VirtualFolder> = serde_json::from_str(&body)
            .context(format!("Failed to parse {} virtual folders response", self.config.server_name))?;

        Ok(folders)
    }

    /// Trigger a refresh for a specific item
    async fn refresh_item(&self, item_id: &str, item_name: &str, item_type: &str) -> Result<()> {
        // Compose from pre-built base + stable suffix; only `item_id` varies per call
        let url = format!("{}{}{}", self.config.item_refresh_base, item_id, self.config.item_refresh_suffix);

        let server_name = self.config.server_name;
        info!("{}: [TARGETED] Refreshing {} \"{}\" (ID: {})", server_name, item_type, item_name, item_id);

        let description = format!("refresh {} \"{}\"", item_type, item_name);
        match post_with_retry(
            &self.client,
            &url,
            &description,
            server_name,
            self.config.max_retries,
            self.config.retry_delay_ms,
        ).await {
            Ok(resp) => {
                if resp.status().is_success() {
                    info!("{}: [TARGETED] Successfully refreshed {} \"{}\"", server_name, item_type, item_name);
                } else {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    warn!("{}: [TARGETED] Refresh for \"{}\" returned {}: {}", server_name, item_name, status, body);
                }
            }
            Err(e) => {
                error!("{}: [TARGETED] Failed to refresh \"{}\" after retries: {}", server_name, item_name, e);
                // Don't return error - continue with other items
            }
        }

        Ok(())
    }

    /// Notify Emby of media updates via path-based API (for new content)
    async fn notify_media_updated(&self, paths: &[PathBuf], update_type: &str) -> Result<()> {
        #[derive(Serialize)]
        struct MediaUpdate {
            #[serde(rename = "Path")]
            path: String,
            #[serde(rename = "UpdateType")]
            update_type: String,
        }

        #[derive(Serialize)]
        struct MediaUpdatedRequest {
            #[serde(rename = "Updates")]
            updates: Vec<MediaUpdate>,
        }

        let updates: Vec<MediaUpdate> = paths.iter()
            .filter_map(|p| p.to_str())
            .map(|path| MediaUpdate {
                path: path.to_string(),
                update_type: update_type.to_string(),
            })
            .collect();

        if updates.is_empty() {
            return Ok(());
        }

        let url = &self.config.notify_path_url;
        let server_name = self.config.server_name;
        info!("{}: [PATH-BASED] Notifying {} path(s) with UpdateType: {}",
              server_name, updates.len(), update_type);

        for update in &updates {
            debug!("{}: [PATH-BASED]   Path: {}", server_name, update.path);
        }

        let resp = post_json_with_retry(
            &self.client,
            url,
            &MediaUpdatedRequest { updates },
            "path-based notification",
            server_name,
            self.config.max_retries,
            self.config.retry_delay_ms,
        ).await?;

        if resp.status().is_success() {
            info!("{}: [PATH-BASED] Successfully notified path updates", server_name);
            Ok(())
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!("{}: [PATH-BASED] Notification returned {}: {}", server_name, status, body);
            Err(anyhow::anyhow!("Path update failed: {}", status))
        }
    }

    /// Trigger a full library refresh (fallback when no matching item found)
    async fn refresh_library(&self) -> Result<()> {
        let server_name = self.config.server_name;
        info!("{}: [FALLBACK] Starting full library scan...", server_name);

        match post_with_retry(
            &self.client,
            &self.config.full_library_url,
            "full library refresh",
            server_name,
            self.config.max_retries,
            self.config.retry_delay_ms,
        ).await {
            Ok(resp) => {
                if resp.status().is_success() {
                    info!("{}: [FALLBACK] Full library scan triggered successfully", server_name);
                } else {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    warn!("{}: [FALLBACK] Library refresh returned {}: {}", server_name, status, body);
                }
            }
            Err(e) => {
                error!("{}: [FALLBACK] Full library refresh failed after retries: {}", server_name, e);
                return Err(e);
            }
        }

        Ok(())
    }
}

/// Glue to plug `EmbyNotifier` into the shared `run_debounce_loop`. The actual batch
/// logic stays in the inherent impl above; this just exposes it through the trait and
/// projects the right per-server metrics counter.
#[async_trait::async_trait]
impl BatchProcessor for EmbyNotifier {
    fn server_name(&self) -> &str {
        self.config.server_name
    }

    fn increment_notification_counter(&self) {
        if let Some(m) = &self.metrics {
            m.emby_notifications.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn process_batch_created(&mut self, paths: &[PathBuf]) -> Result<()> {
        EmbyNotifier::process_batch_created(self, paths).await
    }

    async fn process_batch_deleted(&mut self, paths: &[PathBuf]) -> Result<()> {
        EmbyNotifier::process_batch_deleted(self, paths).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_translation() {
        use crate::media_notifier_common::translate_path;

        // Test with host_paths (overlay layers)
        let mapping = Some(PathMappingConfig {
            host_path: None,
            host_paths: Some(vec![
                PathBuf::from("/mnt/user/share/verified"),
                PathBuf::from("/mnt/user/share/unverified"),
            ]),
            emby_path: PathBuf::from("/share"),
        });

        // Test translation from verified layer
        let host_path = PathBuf::from("/mnt/user/share/verified/Movies/Test.Movie/movie.mkv");
        let emby_path = translate_path(&host_path, &mapping, "Test");
        assert_eq!(emby_path, PathBuf::from("/share/Movies/Test.Movie/movie.mkv"));

        // Test translation from unverified layer
        let host_path = PathBuf::from("/mnt/user/share/unverified/Movies/Test.Movie/movie.mkv");
        let emby_path = translate_path(&host_path, &mapping, "Test");
        assert_eq!(emby_path, PathBuf::from("/share/Movies/Test.Movie/movie.mkv"));

        // Test path that doesn't match mapping
        let other_path = PathBuf::from("/some/other/path");
        let result = translate_path(&other_path, &mapping, "Test");
        assert_eq!(result, other_path);
    }

    #[test]
    fn test_deletion_path_translation() {
        use crate::media_notifier_common::translate_path;

        // Deleted paths must translate correctly even though the files no longer exist
        let mapping = Some(PathMappingConfig {
            host_path: None,
            host_paths: Some(vec![
                PathBuf::from("/mnt/user/share/verified"),
                PathBuf::from("/mnt/user/share/unverified"),
            ]),
            emby_path: PathBuf::from("/share"),
        });

        // Movie removed from verified layer
        let deleted = PathBuf::from("/mnt/user/share/verified/Movies/Death.Note.2017");
        let translated = translate_path(&deleted, &mapping, "Emby");
        assert_eq!(translated, PathBuf::from("/share/Movies/Death.Note.2017"));

        // TV episode removed from unverified layer
        let deleted = PathBuf::from("/mnt/user/share/unverified/TV Shows/Breaking.Bad/Season.1/ep01.mkv");
        let translated = translate_path(&deleted, &mapping, "Emby");
        assert_eq!(translated, PathBuf::from("/share/TV Shows/Breaking.Bad/Season.1/ep01.mkv"));

        // Path not in any mapped layer returns as-is
        let unmapped = PathBuf::from("/other/location/file.mkv");
        let result = translate_path(&unmapped, &mapping, "Emby");
        assert_eq!(result, unmapped);
    }

    #[test]
    fn scannable_item_type_excludes_generic_folder_nodes() {
        // The exact failure mode: Emby's unfiltered /Items listing includes a "Folder"
        // browse node at the same path as the real library — refreshing it is a silent
        // no-op (200/204) that never discovers new content, so it must never survive
        // into the items cache used for path matching.
        assert!(!is_scannable_item_type(Some("Folder")));
    }

    #[test]
    fn scannable_item_type_keeps_real_library_items() {
        for t in ["Movie", "Series", "Season", "Episode", "CollectionFolder"] {
            assert!(is_scannable_item_type(Some(t)), "{t} should be scannable");
        }
    }

    #[test]
    fn scannable_item_type_keeps_unknown_type() {
        // Missing/unrecognized Type is permissive (only the known-bad "Folder" is denied) —
        // an item with no Type at all should not be silently dropped from matching.
        assert!(is_scannable_item_type(None));
    }

    fn vfolder(item_id: &str, locations: &[&str]) -> VirtualFolder {
        vfolder_with_monitor(item_id, locations, false)
    }

    fn vfolder_with_monitor(item_id: &str, locations: &[&str], realtime_monitor: bool) -> VirtualFolder {
        VirtualFolder {
            item_id: item_id.to_string(),
            locations: locations.iter().map(|s| s.to_string()).collect(),
            library_options: Some(LibraryOptions { enable_realtime_monitor: realtime_monitor }),
        }
    }

    #[test]
    fn library_lookup_matches_the_owning_library() {
        let folders = vec![
            vfolder("7", &["/share/Movies", "/share/Elvis/Movies.starring.Elvis"]),
            vfolder("475592", &["/share/Documentaries/Movies"]),
        ];
        let path = PathBuf::from("/share/Documentaries/Movies/Some.Doc.2026-GROUP");
        assert_eq!(find_library_for_path(&path, &folders), Some(("475592".to_string(), false)));
    }

    #[test]
    fn library_lookup_picks_longest_matching_location() {
        // A library covering multiple locations, one nested under a shallower path from
        // a different library — the more specific (longer) match must win.
        let folders = vec![
            vfolder("1", &["/share"]),
            vfolder("2", &["/share/Movies"]),
        ];
        let path = PathBuf::from("/share/Movies/Some.Movie.2026-GROUP");
        assert_eq!(find_library_for_path(&path, &folders), Some(("2".to_string(), false)));
    }

    #[test]
    fn library_lookup_none_for_unconfigured_path() {
        let folders = vec![vfolder("7", &["/share/Movies"])];
        let path = PathBuf::from("/share/Music/Some.Album");
        assert_eq!(find_library_for_path(&path, &folders), None);
    }

    #[test]
    fn library_lookup_surfaces_realtime_monitor_flag() {
        let folders = vec![
            vfolder_with_monitor("1", &["/share/Movies"], true),
            vfolder("2", &["/share/TV Shows"]),
        ];
        assert_eq!(
            find_library_for_path(&PathBuf::from("/share/Movies/New.Movie.2026"), &folders),
            Some(("1".to_string(), true)),
        );
        assert_eq!(
            find_library_for_path(&PathBuf::from("/share/TV Shows/New.Show"), &folders),
            Some(("2".to_string(), false)),
        );
    }

    #[test]
    fn library_lookup_treats_missing_library_options_as_monitor_off() {
        let folders = vec![VirtualFolder {
            item_id: "3".to_string(),
            locations: vec!["/share/Movies".to_string()],
            library_options: None,
        }];
        assert_eq!(
            find_library_for_path(&PathBuf::from("/share/Movies/New.Movie.2026"), &folders),
            Some(("3".to_string(), false)),
        );
    }

    #[tokio::test]
    async fn test_media_event_routing_in_channel() {
        // Verify that Created and Deleted events are distinguishable through the channel
        use crate::media_notifier_common::MediaEvent;

        let (sender, mut receiver) = tokio::sync::mpsc::channel::<MediaEvent>(10);
        let handle = crate::media_notifier_common::NotifierHandle::new(sender);

        let created_path = PathBuf::from("/mnt/user/share/verified/Movies/New.Movie.2024");
        let deleted_path = PathBuf::from("/mnt/user/share/verified/Movies/Death.Note.2017");

        handle.notify(MediaEvent::Created(created_path.clone())).await;
        handle.notify(MediaEvent::Deleted(deleted_path.clone())).await;

        let ev1 = receiver.recv().await.unwrap();
        assert!(!ev1.is_deletion(), "first event should be a creation");
        assert_eq!(ev1.path(), &created_path);

        let ev2 = receiver.recv().await.unwrap();
        assert!(ev2.is_deletion(), "second event should be a deletion");
        assert_eq!(ev2.path(), &deleted_path);
    }
}

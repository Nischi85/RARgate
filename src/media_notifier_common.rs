//! Common utilities for media server notifiers
//!
//! This module provides shared functionality used by all media server integrations:
//! - Generic item/section caching with TTL
//! - Path mapping for Docker deployments
//! - HTTP client builder with standard settings
//! - Path filtering utilities
//! - Default media extensions (video, audio, disc images)

use anyhow::{Context, Result, anyhow};
use reqwest::Response;

/// Default media extensions for content detection
/// Used by both filtering.rs and inotify_watcher.rs
pub const DEFAULT_MEDIA_EXTENSIONS: &[&str] = &[
    "mkv", "mp4", "avi", "m4v", "mov", "wmv", "flv", "mpg", "mpeg",
    "mp3", "flac", "m4a", "wav", "aac", "ogg", "wma",
    "iso", "img", "bin", "cue",
];

/// Default maximum retry attempts for media-server API calls (Emby/Jellyfin/Plex).
pub const DEFAULT_MAX_RETRIES: u32 = 3;

/// Default initial retry delay in milliseconds (doubles each attempt).
pub const DEFAULT_RETRY_DELAY_MS: u64 = 1000;

/// Bound on the inotify-watcher → notifier mpsc channel. Sized generously so a burst
/// of `MediaEvent`s during a large upload never blocks the watcher; the debounce loop
/// drains opportunistically.
pub const MEDIA_EVENT_CHANNEL_CAPACITY: usize = 1000;
use reqwest::Client;
use serde::Serialize;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::config::PathMappingConfig;

/// Holds a fetched snapshot of the item list together with its age.
/// Grouping these lets ItemCache use a single lock rather than two separate ones.
struct CacheState<T> {
    items: Vec<T>,
    fetched_at: Instant,
}

/// Generic cache with TTL for items or sections.
///
/// A single `RwLock<Option<CacheState<T>>>` covers both the data and its timestamp,
/// halving lock acquisitions compared to the previous two-lock design.
pub struct ItemCache<T: Clone> {
    state: Arc<RwLock<Option<CacheState<T>>>>,
    cache_ttl: Duration,
}

impl<T: Clone> ItemCache<T> {
    pub fn new(cache_minutes: u64) -> Self {
        Self {
            state: Arc::new(RwLock::new(None)),
            cache_ttl: Duration::from_secs(cache_minutes * 60),
        }
    }

    /// Return cached items if still fresh, otherwise call `fetch_fn` to refresh.
    pub async fn get_or_fetch<F, Fut>(&self, fetch_fn: F) -> Result<Vec<T>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Vec<T>>>,
    {
        // Single read lock: check freshness and return a clone if still valid
        {
            let state = self.state.read().await;
            if let Some(s) = state.as_ref() {
                if s.fetched_at.elapsed() < self.cache_ttl {
                    debug!("Using cached items (age: {:.1}s)", s.fetched_at.elapsed().as_secs_f64());
                    return Ok(s.items.clone());
                }
            }
        }

        // Cache miss or TTL expired — fetch fresh data, then store under a single write lock
        let items = fetch_fn().await?;
        {
            let mut state = self.state.write().await;
            *state = Some(CacheState { items: items.clone(), fetched_at: Instant::now() });
        }

        Ok(items)
    }

    /// Discard cached data, forcing a fresh fetch on the next access.
    pub async fn invalidate(&self) {
        let mut state = self.state.write().await;
        *state = None;
        debug!("Cache invalidated - will refresh on next access");
    }
}

/// Build HTTP client with standard settings for media servers
pub fn build_http_client() -> Result<Client> {
    Client::builder()
        .timeout(Duration::from_secs(30))
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(2)
        .build()
        .context("Failed to build HTTP client")
}

/// Execute an HTTP POST request with exponential backoff retry logic
///
/// # Arguments
/// * `client` - HTTP client to use
/// * `url` - URL to POST to
/// * `description` - Human-readable description for logging
/// * `server_name` - Server name for logging (e.g., "Emby", "Jellyfin")
/// * `max_retries` - Maximum number of retry attempts (0 = no retries)
/// * `initial_delay_ms` - Initial retry delay in milliseconds (doubles each retry)
pub async fn post_with_retry(
    client: &Client,
    url: &str,
    description: &str,
    server_name: &str,
    max_retries: u32,
    initial_delay_ms: u64,
) -> Result<Response> {
    let mut last_error = None;

    for attempt in 0..=max_retries {
        if attempt > 0 {
            // Calculate exponential backoff delay: initial_delay * 2^(attempt-1)
            let delay_ms = initial_delay_ms * (1 << (attempt - 1));
            debug!("{}: Retry attempt {} for {} (waiting {}ms)", server_name, attempt, description, delay_ms);
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }

        match client.post(url).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    return Ok(response);
                } else if response.status().is_server_error() {
                    // Server error - worth retrying
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    last_error = Some(format!("Server error {}: {}", status, body));
                    warn!("{}: {} returned {} (attempt {}/{})", server_name, description, status, attempt + 1, max_retries + 1);
                } else {
                    // Client error (4xx) - don't retry; return the response we already have
                    return Ok(response);
                }
            }
            Err(e) => {
                last_error = Some(format!("Request failed: {}", e));
                warn!("{}: {} failed: {} (attempt {}/{})", server_name, description, e, attempt + 1, max_retries + 1);
            }
        }
    }

    Err(anyhow!("All {} retry attempts failed for {}: {}", max_retries + 1, description, last_error.unwrap_or_default()))
}

/// POST a JSON body with retry/backoff. Same retry policy as `post_with_retry`
/// (retry on transport error + 5xx, no retry on 4xx) but attaches `body` as JSON
/// on every attempt. Used for body-carrying calls like Emby's `Library/Media/Updated`.
pub async fn post_json_with_retry<T: Serialize>(
    client: &Client,
    url: &str,
    body: &T,
    description: &str,
    server_name: &str,
    max_retries: u32,
    initial_delay_ms: u64,
) -> Result<Response> {
    let mut last_error = None;

    for attempt in 0..=max_retries {
        if attempt > 0 {
            // Exponential backoff: initial_delay * 2^(attempt-1)
            let delay_ms = initial_delay_ms * (1 << (attempt - 1));
            debug!("{}: Retry attempt {} for {} (waiting {}ms)", server_name, attempt, description, delay_ms);
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }

        match client.post(url).json(body).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    return Ok(response);
                } else if response.status().is_server_error() {
                    let status = response.status();
                    let resp_body = response.text().await.unwrap_or_default();
                    last_error = Some(format!("Server error {}: {}", status, resp_body));
                    warn!("{}: {} returned {} (attempt {}/{})", server_name, description, status, attempt + 1, max_retries + 1);
                } else {
                    // Client error (4xx) - don't retry; return the response we already have
                    return Ok(response);
                }
            }
            Err(e) => {
                last_error = Some(format!("Request failed: {}", e));
                warn!("{}: {} failed: {} (attempt {}/{})", server_name, description, e, attempt + 1, max_retries + 1);
            }
        }
    }

    Err(anyhow!("All {} retry attempts failed for {}: {}", max_retries + 1, description, last_error.unwrap_or_default()))
}

/// Filter paths to only those that exist (or whose parent exists)
pub fn filter_existing_paths(paths: &[PathBuf]) -> Vec<PathBuf> {
    paths
        .iter()
        .filter(|p| {
            let exists = p.exists() || p.parent().map(|parent| parent.exists()).unwrap_or(false);
            if !exists {
                debug!("Skipping non-existent path: {}", p.display());
            }
            exists
        })
        .cloned()
        .collect()
}

/// Get unique parent directories from a list of paths
pub fn get_parent_dirs(paths: &[PathBuf]) -> HashSet<PathBuf> {
    paths
        .iter()
        .filter_map(|p| p.parent().map(|parent| parent.to_path_buf()))
        .collect()
}

/// Get target directories for notification - handles both file and directory paths
/// For FILE paths: returns parent directory (the folder containing the file)
/// For DIRECTORY paths: returns as-is (already the folder we want to notify)
pub fn get_target_dirs(paths: &[PathBuf]) -> HashSet<PathBuf> {
    paths
        .iter()
        .map(|p| {
            if p.is_dir() {
                // Directory path (e.g., SFV-validated episode folder) - use as-is
                debug!("get_target_dirs: keeping directory as-is: {}", p.display());
                p.clone()
            } else {
                // File path (e.g., subtitle file) - get parent directory
                let parent = p.parent().map(|parent| parent.to_path_buf()).unwrap_or_else(|| p.clone());
                debug!("get_target_dirs: using parent for file: {} -> {}", p.display(), parent.display());
                parent
            }
        })
        .collect()
}

/// Translate host path to container path using mapping configuration
pub fn translate_path(host_path: &Path, mapping: &Option<PathMappingConfig>, server_name: &str) -> PathBuf {
    if let Some(mapping) = mapping {
        // Try multiple host_paths first (overlay layers)
        if let Some(host_paths) = &mapping.host_paths {
            for prefix in host_paths {
                if let Ok(relative) = host_path.strip_prefix(prefix) {
                    let translated = mapping.emby_path.join(relative);
                    debug!("{}: Path translation: {} -> {}", server_name, host_path.display(), translated.display());
                    return translated;
                }
            }
        }

        // Fall back to single host_path (backwards compatibility)
        if let Some(host_path_prefix) = &mapping.host_path {
            if let Ok(relative) = host_path.strip_prefix(host_path_prefix) {
                let translated = mapping.emby_path.join(relative);
                debug!("{}: Path translation: {} -> {}", server_name, host_path.display(), translated.display());
                return translated;
            }
        }
    }

    // No mapping or path doesn't match - return as-is
    debug!("{}: No path translation for: {}", server_name, host_path.display());
    host_path.to_path_buf()
}

/// Represents a media library change event with intent (created or deleted)
#[derive(Debug, Clone)]
pub enum MediaEvent {
    Created(PathBuf),
    Deleted(PathBuf),
}

impl MediaEvent {
    pub fn path(&self) -> &PathBuf {
        match self {
            MediaEvent::Created(p) | MediaEvent::Deleted(p) => p,
        }
    }

    #[allow(dead_code)]
    pub fn is_deletion(&self) -> bool {
        matches!(self, MediaEvent::Deleted(_))
    }
}

/// Implemented by each per-server notifier to plug into `run_debounce_loop`.
///
/// The driver loop is identical across Emby/Jellyfin/Plex; only the per-batch
/// processing and the metrics counter differ. Implementors hold their own state
/// (HTTP client, item/section cache, etc.) and perform the actual API calls in the
/// two `process_batch_*` methods.
#[async_trait::async_trait]
pub trait BatchProcessor: Send {
    /// Display name for log messages ("Emby", "Jellyfin", "Plex").
    fn server_name(&self) -> &str;

    /// Increment the per-server "notifications sent" counter on the metrics collector.
    /// Implementors project to the appropriate `AtomicU64` (emby/plex/jellyfin).
    fn increment_notification_counter(&self);

    async fn process_batch_created(&mut self, paths: &[PathBuf]) -> anyhow::Result<()>;
    async fn process_batch_deleted(&mut self, paths: &[PathBuf]) -> anyhow::Result<()>;
}

/// Drive the standard "receive events → debounce → batch-process" loop shared by all
/// media-server notifiers. Runs forever; returns only if the receiver closes.
///
/// Both `pending_created` and `pending_deleted` are de-duplicating `HashSet`s — repeat
/// events for the same path within a debounce window collapse to one API call. The
/// debounce deadline resets on every incoming event, so a sustained burst delays
/// processing until the channel goes quiet for `debounce_duration`.
pub async fn run_debounce_loop<P: BatchProcessor>(
    mut processor: P,
    mut receiver: mpsc::Receiver<MediaEvent>,
    debounce_duration: Duration,
) {
    let server_name = processor.server_name().to_string();

    let mut pending_created: HashSet<PathBuf> = HashSet::new();
    let mut pending_deleted: HashSet<PathBuf> = HashSet::new();
    let mut debounce_deadline: Option<tokio::time::Instant> = None;

    loop {
        tokio::select! {
            // Receive new file change events
            Some(event) = receiver.recv() => {
                match event {
                    MediaEvent::Created(path) => {
                        debug!("{}: Received creation notification: {}", server_name, path.display());
                        pending_created.insert(path);
                    }
                    MediaEvent::Deleted(path) => {
                        debug!("{}: Received deletion notification: {}", server_name, path.display());
                        pending_deleted.insert(path);
                    }
                }
                debounce_deadline = Some(tokio::time::Instant::now() + debounce_duration);
            }

            // Debounce timer expired — process the accumulated batch
            _ = async {
                if let Some(deadline) = debounce_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    // No deadline set; wait forever (next branch will fire on event arrival)
                    std::future::pending::<()>().await;
                }
            } => {
                if pending_created.is_empty() && pending_deleted.is_empty() {
                    continue;
                }
                let created: Vec<PathBuf> = pending_created.drain().collect();
                let deleted: Vec<PathBuf> = pending_deleted.drain().collect();
                debounce_deadline = None;

                if !created.is_empty() {
                    processor.increment_notification_counter();
                    if let Err(e) = processor.process_batch_created(&created).await {
                        error!("{}: Failed to process creation batch: {}", server_name, e);
                    }
                }
                if !deleted.is_empty() {
                    processor.increment_notification_counter();
                    if let Err(e) = processor.process_batch_deleted(&deleted).await {
                        error!("{}: Failed to process deletion batch: {}", server_name, e);
                    }
                }
            }
        }
    }
}

/// Generic notifier handle for sending file change events
#[derive(Clone)]
pub struct NotifierHandle {
    sender: mpsc::Sender<MediaEvent>,
}

impl NotifierHandle {
    pub fn new(sender: mpsc::Sender<MediaEvent>) -> Self {
        Self { sender }
    }

    /// Notify about a file change event
    pub async fn notify(&self, event: MediaEvent) {
        if let Err(e) = self.sender.send(event).await {
            debug!("Failed to send notification (channel closed): {}", e);
        }
    }
}

/// Common batch processing logic for Emby-compatible servers (Emby/Jellyfin)
/// Returns the items that need to be refreshed
pub async fn process_batch_emby_style<T: Clone>(
    server_name: &str,
    paths: &[PathBuf],
    path_mapping: &Option<PathMappingConfig>,
    items: &[T],
    get_item_path: impl Fn(&T) -> Option<&String>,
    get_item_id: impl Fn(&T) -> &str,
) -> std::collections::HashMap<String, T> {
    info!("{}: Processing batch of {} changed paths", server_name, paths.len());

    // Filter existing paths and get target directories
    let existing_paths = filter_existing_paths(paths);
    let target_dirs = get_target_dirs(&existing_paths);

    info!("{}: {} unique directories to refresh", server_name, target_dirs.len());

    // Pre-size to avoid rehashing — one slot per changed directory is a reasonable lower bound
    let mut items_to_refresh: std::collections::HashMap<String, T> = std::collections::HashMap::with_capacity(target_dirs.len());

    if target_dirs.is_empty() {
        debug!("{}: All paths were filtered out (no existing paths), skipping API calls", server_name);
        return items_to_refresh;
    }

    // Find matching items as follows.
    // For each changed directory, find Emby items that:
    // 1. Are INSIDE the changed directory (item_path.starts_with(container_path))
    // 2. Are a direct child of the changed directory (item_path.parent() == container_path)
    // This finds items WITHIN the changed folder, not parent containers.
    // For NEW content, Emby won't have the item yet, so fallback will trigger.
    for dir in &target_dirs {
        // Translate host path to container path
        let container_path = translate_path(dir, path_mapping, server_name);
        debug!("{}: Looking for items in directory: {}", server_name, container_path.display());

        // Find matching items - items WITHIN this directory
        for item in items {
            if let Some(item_path_str) = get_item_path(item) {
                let item_path = PathBuf::from(item_path_str);

                // Item is inside the changed directory
                let item_inside_dir = item_path.starts_with(&container_path);
                // Item's parent is the changed directory (direct child)
                let direct_child = item_path.parent().map(|p| p == container_path).unwrap_or(false);

                if item_inside_dir || direct_child {
                    let id = get_item_id(item).to_string();
                    items_to_refresh.insert(id.clone(), item.clone());
                    debug!("{}: Found match - Item {} for path {}", server_name, id, item_path.display());
                }
            }
        }
    }

    items_to_refresh
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_media_event_created() {
        let path = PathBuf::from("/share/Movies/Test.Movie.2024");
        let event = MediaEvent::Created(path.clone());
        assert_eq!(event.path(), &path);
        assert!(!event.is_deletion());
    }

    #[test]
    fn test_media_event_deleted() {
        let path = PathBuf::from("/share/Movies/Death.Note.2017");
        let event = MediaEvent::Deleted(path.clone());
        assert_eq!(event.path(), &path);
        assert!(event.is_deletion());
    }

    #[tokio::test]
    async fn test_notifier_handle_sends_created() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<MediaEvent>(10);
        let handle = NotifierHandle::new(sender);

        let path = PathBuf::from("/share/Movies/New.Movie.2024");
        handle.notify(MediaEvent::Created(path.clone())).await;

        let received = receiver.recv().await.expect("should receive event");
        assert!(!received.is_deletion());
        assert_eq!(received.path(), &path);
    }

    #[tokio::test]
    async fn test_notifier_handle_sends_deleted() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<MediaEvent>(10);
        let handle = NotifierHandle::new(sender);

        let path = PathBuf::from("/share/Movies/Death.Note.2017");
        handle.notify(MediaEvent::Deleted(path.clone())).await;

        let received = receiver.recv().await.expect("should receive event");
        assert!(received.is_deletion());
        assert_eq!(received.path(), &path);
    }

    #[test]
    fn test_filter_existing_paths_skips_nonexistent() {
        // Non-existent paths should be filtered out for creations
        let paths = vec![
            PathBuf::from("/nonexistent/path/movie.mkv"),
            PathBuf::from("/tmp"), // /tmp always exists
        ];
        let existing = filter_existing_paths(&paths);
        assert_eq!(existing.len(), 1);
        assert_eq!(existing[0], PathBuf::from("/tmp"));
    }

    #[test]
    fn test_get_parent_dirs_from_deleted_paths() {
        // For deletions, we derive parent dirs from the deleted paths
        let deleted_paths = vec![
            PathBuf::from("/share/Movies/Death.Note.2017/death.note.mkv"),
            PathBuf::from("/share/Movies/Death.Note.2017/death.note.nfo"),
        ];
        let parents = get_parent_dirs(&deleted_paths);
        assert_eq!(parents.len(), 1);
        assert!(parents.contains(&PathBuf::from("/share/Movies/Death.Note.2017")));
    }

    #[test]
    fn test_deletion_path_translation() {
        use crate::config::PathMappingConfig;

        let mapping = Some(PathMappingConfig {
            host_path: None,
            host_paths: Some(vec![
                PathBuf::from("/mnt/user/share/verified"),
                PathBuf::from("/mnt/user/share/unverified"),
            ]),
            emby_path: PathBuf::from("/share"),
        });

        // Deleted path in verified layer translates correctly even though file no longer exists
        let deleted = PathBuf::from("/mnt/user/share/verified/Movies/Death.Note.2017");
        let translated = translate_path(&deleted, &mapping, "Emby");
        assert_eq!(translated, PathBuf::from("/share/Movies/Death.Note.2017"));

        // Deleted path in unverified layer
        let deleted = PathBuf::from("/mnt/user/share/unverified/TV/Breaking.Bad/Season.1");
        let translated = translate_path(&deleted, &mapping, "Emby");
        assert_eq!(translated, PathBuf::from("/share/TV/Breaking.Bad/Season.1"));
    }
}

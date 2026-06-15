//! Sonarr/Radarr rescan-on-exposure integration
//!
//! When a release's SFV validates, rargate exposes it through the FUSE mount. This notifier
//! fires an *in-place* `RescanSeries`/`RescanMovie` at that exact moment so Sonarr/Radarr
//! import the freshly-visible files immediately — instead of relying on the downloader
//! (dc-bridge) to nudge a rescan that may race against rargate's gate.
//!
//! It is the same trait-based notifier shape as Emby/Jellyfin/Plex (`BatchProcessor` +
//! `run_debounce_loop`), but instead of refreshing a media server it:
//!   1. translates the validated host path (the verified layer) to the path each *arr instance sees,
//!   2. resolves the owning series/movie by ancestor-path match against the cached *arr list,
//!   3. POSTs the in-place rescan command (no file move/copy — safe over the read-only mount).
//!
//! Deletions are ignored: a rescan against a removed folder is pointless, and pruning is
//! *arr's own scheduled-scan job.

use crate::config::{ArrConfig, ArrInstanceConfig, PathMappingConfig};
use crate::media_notifier_common::{
    build_http_client, filter_existing_paths, post_json_with_retry, run_debounce_loop,
    translate_path, BatchProcessor, ItemCache, MediaEvent, NotifierHandle,
    DEFAULT_MAX_RETRIES, DEFAULT_MEDIA_EXTENSIONS, DEFAULT_RETRY_DELAY_MS,
    MEDIA_EVENT_CHANNEL_CAPACITY,
};
use crate::metrics::MetricsCollector;
use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Re-export NotifierHandle for symmetry with the other notifiers.
pub type ArrNotifierHandle = NotifierHandle;

/// Whether an instance manages TV series (Sonarr) or movies (Radarr). Determines both the
/// list endpoint and the rescan command/id-field.
#[derive(Clone, Copy)]
enum ArrKind {
    Series,
    Movie,
}

impl ArrKind {
    /// (command name, id field name) for the `/api/v3/command` body.
    fn rescan_command(self) -> (&'static str, &'static str) {
        match self {
            ArrKind::Series => ("RescanSeries", "seriesId"),
            ArrKind::Movie => ("RescanMovie", "movieId"),
        }
    }
}

/// A single record from a Sonarr/Radarr list — only the fields we match on. Unknown JSON
/// fields are ignored by serde.
#[derive(Clone, Deserialize)]
struct ArrRecord {
    id: i64,
    path: String,
    /// Radarr movie list carries `hasFile`; Sonarr's series list does not, so it stays
    /// `None`. Used to decide whether a matched movie still needs a force-import.
    #[serde(rename = "hasFile", default)]
    has_file: Option<bool>,
}

/// One importable file from `GET /api/v3/manualimport`. Radarr still parses `quality` and
/// `languages` from the folder name even when the movie itself is unmatched, so we hand
/// these straight back in the `ManualImport` command body. Unknown fields are ignored.
#[derive(Clone, Deserialize)]
struct ManualImportCandidate {
    path: String,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    quality: serde_json::Value,
    #[serde(default)]
    languages: serde_json::Value,
    #[serde(rename = "releaseGroup", default)]
    release_group: Option<String>,
}

/// One configured Sonarr or Radarr instance, with its own list cache and path mapping.
struct ArrInstance {
    name: &'static str,
    kind: ArrKind,
    /// GET list URL (series/movie) with the api key as a query param.
    list_url: String,
    /// POST command URL with the api key as a query param.
    command_url: String,
    /// GET manual-import URL (no query) — folder/filter/apikey are added per request.
    manualimport_url: String,
    api_key: String,
    /// Force-import the in-place file for matched movies that still have no file. Only
    /// ever true for Radarr (movies); Sonarr's per-episode import isn't supported here.
    force_import: bool,
    path_mapping: Option<PathMappingConfig>,
    cache: ItemCache<ArrRecord>,
}

impl ArrInstance {
    fn new(name: &'static str, kind: ArrKind, cfg: &ArrInstanceConfig, cache_minutes: u64) -> Self {
        let base = cfg.url.trim_end_matches('/');
        let endpoint = match kind {
            ArrKind::Series => "series",
            ArrKind::Movie => "movie",
        };
        // Sonarr/Radarr accept the api key as `?apikey=` — keeps it off custom headers so
        // the shared `post_json_with_retry` (which sets no headers) can carry the command.
        let list_url = format!("{base}/api/v3/{endpoint}?apikey={}", cfg.api_key);
        let command_url = format!("{base}/api/v3/command?apikey={}", cfg.api_key);
        let manualimport_url = format!("{base}/api/v3/manualimport");
        // Force-import is a movie-only concept; Sonarr needs per-episode ids that
        // obfuscated names can't yield, so it stays on RescanSeries regardless of config.
        let force_import = matches!(kind, ArrKind::Movie) && cfg.force_import.unwrap_or(true);
        info!("Arr: {} instance at {} (force_import={})", name, base, force_import);
        Self {
            name,
            kind,
            list_url,
            command_url,
            manualimport_url,
            api_key: cfg.api_key.clone(),
            force_import,
            path_mapping: cfg.path_mapping.clone(),
            cache: ItemCache::new(cache_minutes),
        }
    }
}

/// Sonarr/Radarr rescan notifier.
pub struct ArrNotifier {
    client: Client,
    receiver: mpsc::Receiver<MediaEvent>,
    instances: Vec<ArrInstance>,
    debounce_seconds: u64,
    max_retries: u32,
    retry_delay_ms: u64,
    metrics: Option<Arc<MetricsCollector>>,
}

impl ArrNotifier {
    /// Create a new ArrNotifier and its handle.
    pub fn new(config: ArrConfig) -> Result<(Self, ArrNotifierHandle)> {
        let (sender, receiver) = mpsc::channel::<MediaEvent>(MEDIA_EVENT_CHANNEL_CAPACITY);
        let client = build_http_client()?;
        let cache_minutes = config.cache_minutes.unwrap_or(5);

        let mut instances = Vec::new();
        if let Some(s) = &config.sonarr {
            instances.push(ArrInstance::new("Sonarr", ArrKind::Series, s, cache_minutes));
        }
        if let Some(r) = &config.radarr {
            instances.push(ArrInstance::new("Radarr", ArrKind::Movie, r, cache_minutes));
        }
        if instances.is_empty() {
            warn!("Arr notifier enabled but no sonarr/radarr instance configured — no-op");
        }

        let notifier = ArrNotifier {
            client,
            receiver,
            instances,
            debounce_seconds: config.debounce_seconds.unwrap_or(5),
            max_retries: config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
            retry_delay_ms: config.retry_delay_ms.unwrap_or(DEFAULT_RETRY_DELAY_MS),
            metrics: None,
        };

        Ok((notifier, NotifierHandle::new(sender)))
    }

    /// Attach a metrics collector (call before run()).
    pub fn set_metrics(&mut self, metrics: Arc<MetricsCollector>) {
        self.metrics = Some(metrics);
    }

    /// Start the notification processing loop (shared debounce driver).
    pub async fn run(mut self) {
        info!("Arr notifier started");
        info!("  Debounce: {}s", self.debounce_seconds);
        info!("  Instances: {}", self.instances.len());
        info!("  Retry: {} attempts, {}ms initial delay", self.max_retries, self.retry_delay_ms);

        let debounce_duration = Duration::from_secs(self.debounce_seconds);
        let (_placeholder_tx, placeholder_rx) = mpsc::channel::<MediaEvent>(1);
        let receiver = std::mem::replace(&mut self.receiver, placeholder_rx);
        run_debounce_loop(self, receiver, debounce_duration).await;
    }

    /// Fetch the full series/movie list from a *arr instance.
    async fn fetch_list(client: &Client, url: &str, name: &str) -> Result<Vec<ArrRecord>> {
        let response = client
            .get(url)
            .header("Accept", "application/json")
            .send()
            .await
            .with_context(|| format!("{name}: failed to fetch list"))?;
        let body = response
            .text()
            .await
            .with_context(|| format!("{name}: failed to read list response"))?;
        let records: Vec<ArrRecord> = serde_json::from_str(&body)
            .with_context(|| format!("{name}: failed to parse list response"))?;
        Ok(records)
    }

    /// Process a batch of validated (created/changed) release directories: for each *arr
    /// instance, resolve the owning records and fire one in-place rescan per record.
    async fn process_created(&self, paths: &[PathBuf]) -> Result<()> {
        let dirs = filter_existing_paths(paths);
        if dirs.is_empty() {
            debug!("Arr: no existing paths in batch, skipping");
            return Ok(());
        }

        for inst in &self.instances {
            let records = match inst
                .cache
                .get_or_fetch(|| Self::fetch_list(&self.client, &inst.list_url, inst.name))
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    warn!("Arr: {} list fetch failed: {}", inst.name, e);
                    continue;
                }
            };

            // Resolve each validated dir to its owning record; dedupe so one series/movie is
            // rescanned at most once per batch.
            let mut to_rescan: HashSet<i64> = HashSet::new();
            for dir in &dirs {
                let translated = translate_path(dir, &inst.path_mapping, inst.name);
                if let Some(id) = best_match(&records, &translated) {
                    to_rescan.insert(id);
                } else {
                    debug!("Arr: {} no match for {}", inst.name, translated.display());
                }
            }

            for id in to_rescan {
                self.fire_rescan(inst, id).await;
                // A plain RescanMovie can't import scene releases whose extracted file
                // carries an obfuscated internal name (Radarr parses the filename and
                // rejects it as "Unknown Movie"). For matched movies that still have no
                // file, force the in-place import by movieId, which bypasses that parse.
                if inst.force_import {
                    if let Some(rec) = records.iter().find(|r| r.id == id) {
                        if rec.has_file == Some(false) {
                            self.force_import(inst, &rec.path, id).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Force an in-place import of a matched movie's file via the manual-import API.
    ///
    /// `GET /manualimport` returns the candidate file(s) in the movie's own folder with
    /// `quality`/`languages` already parsed (`filterExistingFiles=true` skips anything
    /// already imported, making this idempotent). We pick the largest media file and POST
    /// a `ManualImport` command carrying the `movieId`, which tells Radarr exactly which
    /// movie the file belongs to — no filename guessing. Import mode `auto` keeps the file
    /// in place (no move/copy), so it is safe over the read-only mount.
    async fn force_import(&self, inst: &ArrInstance, folder: &str, movie_id: i64) {
        let candidates = match self
            .client
            .get(&inst.manualimport_url)
            .query(&[
                ("folder", folder),
                ("filterExistingFiles", "true"),
                ("apikey", inst.api_key.as_str()),
            ])
            .header("Accept", "application/json")
            .send()
            .await
            .and_then(|r| r.error_for_status())
        {
            Ok(resp) => match resp.json::<Vec<ManualImportCandidate>>().await {
                Ok(c) => c,
                Err(e) => {
                    warn!("Arr: {} manualimport parse failed for {}: {}", inst.name, folder, e);
                    return;
                }
            },
            Err(e) => {
                warn!("Arr: {} manualimport fetch failed for {}: {}", inst.name, folder, e);
                return;
            }
        };

        // Largest media file in the folder — samples/extras are smaller, and a single
        // feature is the common case. (Split CD1/CD2 releases import only one part; that
        // is a rare DVDRip edge case Radarr models poorly anyway.)
        let pick = candidates
            .into_iter()
            .filter(|c| is_media_path(&c.path))
            .max_by_key(|c| c.size);
        let Some(file) = pick else {
            debug!("Arr: {} no media candidate to import in {}", inst.name, folder);
            return;
        };

        let body = serde_json::json!({
            "name": "ManualImport",
            "importMode": "auto",
            "files": [{
                "path": file.path,
                "movieId": movie_id,
                "quality": file.quality,
                "languages": file.languages,
                "releaseGroup": file.release_group,
            }],
        });
        let description = format!("ManualImport movieId {movie_id}");
        info!("Arr: {} force-importing {} for movieId {}", inst.name, file.path, movie_id);
        match post_json_with_retry(
            &self.client,
            &inst.command_url,
            &body,
            &description,
            inst.name,
            self.max_retries,
            self.retry_delay_ms,
        )
        .await
        {
            Ok(_) => debug!("Arr: {} {} accepted", inst.name, description),
            Err(e) => warn!("Arr: {} {} failed: {}", inst.name, description, e),
        }
    }

    /// POST an in-place rescan command for one resolved series/movie id.
    async fn fire_rescan(&self, inst: &ArrInstance, id: i64) {
        let (cmd, id_field) = inst.kind.rescan_command();
        // Build the body explicitly so the id field name can be dynamic (seriesId/movieId).
        let mut map = serde_json::Map::new();
        map.insert("name".to_string(), serde_json::Value::from(cmd));
        map.insert(id_field.to_string(), serde_json::Value::from(id));
        let body = serde_json::Value::Object(map);
        let description = format!("{cmd} {id}");
        info!("Arr: {} triggering {} for id {}", inst.name, cmd, id);
        match post_json_with_retry(
            &self.client,
            &inst.command_url,
            &body,
            &description,
            inst.name,
            self.max_retries,
            self.retry_delay_ms,
        )
        .await
        {
            Ok(_) => debug!("Arr: {} {} accepted", inst.name, description),
            Err(e) => warn!("Arr: {} {} failed: {}", inst.name, description, e),
        }
    }
}

/// Find the most specific *arr record whose path is an ancestor of (or equal to) the
/// validated release path. Longest matching record path wins so a release nested under a
/// series folder resolves to that series, not a shallower root.
/// True if the path ends in a known media extension (case-insensitive). Used to skip
/// non-media manual-import candidates (.nfo/.sub/etc.) when choosing the file to import.
fn is_media_path(path: &str) -> bool {
    match path.rsplit('.').next() {
        Some(ext) => {
            let ext = ext.to_ascii_lowercase();
            DEFAULT_MEDIA_EXTENSIONS.contains(&ext.as_str())
        }
        None => false,
    }
}

fn best_match(records: &[ArrRecord], path: &Path) -> Option<i64> {
    records
        .iter()
        .filter(|r| {
            let rec_path = Path::new(r.path.trim_end_matches('/'));
            path.starts_with(rec_path)
        })
        .max_by_key(|r| r.path.len())
        .map(|r| r.id)
}

/// Glue into the shared debounce loop. Created batches drive rescans; deletions are ignored.
#[async_trait::async_trait]
impl BatchProcessor for ArrNotifier {
    fn server_name(&self) -> &str {
        "Arr"
    }

    fn increment_notification_counter(&self) {
        if let Some(m) = &self.metrics {
            m.arr_notifications.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn process_batch_created(&mut self, paths: &[PathBuf]) -> Result<()> {
        ArrNotifier::process_created(self, paths).await
    }

    async fn process_batch_deleted(&mut self, _paths: &[PathBuf]) -> Result<()> {
        debug!("Arr: ignoring deletion batch (rescan-on-create only)");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: i64, path: &str) -> ArrRecord {
        ArrRecord { id, path: path.to_string(), has_file: None }
    }

    #[test]
    fn matches_episode_nested_under_series_folder() {
        let records = vec![
            rec(7, "/share/TV.Series/Star.City"),
            rec(9, "/share/TV.Series/Bad.Judge"),
        ];
        let path = PathBuf::from("/share/TV.Series/Star.City/Season.1/Star.City.S01E01.RELEASE");
        assert_eq!(best_match(&records, &path), Some(7));
    }

    #[test]
    fn picks_most_specific_when_roots_nest() {
        // A shallow root and a deeper series path both prefix the release; deeper wins.
        let records = vec![
            rec(1, "/share/TV.Series"),
            rec(2, "/share/TV.Series/Star.City"),
        ];
        let path = PathBuf::from("/share/TV.Series/Star.City/Season.1/ep");
        assert_eq!(best_match(&records, &path), Some(2));
    }

    #[test]
    fn no_match_for_sibling_scene_folder() {
        // Movie not yet path-reconciled: radarr folder name differs from the scene folder.
        let records = vec![rec(3, "/share/Movies/The Odyssey (2026)")];
        let path = PathBuf::from("/share/Movies/The.Odyssey.2026.1080p.WEB.x264-GROUP");
        assert_eq!(best_match(&records, &path), None);
    }

    #[test]
    fn exact_path_match_for_reconciled_movie() {
        let records = vec![rec(5, "/share/Movies/The.Odyssey.2026.1080p.WEB.x264-GROUP")];
        let path = PathBuf::from("/share/Movies/The.Odyssey.2026.1080p.WEB.x264-GROUP");
        assert_eq!(best_match(&records, &path), Some(5));
    }

    #[test]
    fn no_false_prefix_across_similar_names() {
        // /share/Movies/Star should NOT match a release under /share/Movies/Star.City.
        let records = vec![rec(8, "/share/Movies/Star")];
        let path = PathBuf::from("/share/Movies/Star.City/release");
        assert_eq!(best_match(&records, &path), None);
    }

    #[test]
    fn trailing_slash_on_record_path_is_tolerated() {
        let records = vec![rec(4, "/share/TV.Series/Star.City/")];
        let path = PathBuf::from("/share/TV.Series/Star.City/Season.1/ep");
        assert_eq!(best_match(&records, &path), Some(4));
    }

    #[test]
    fn is_media_path_matches_known_extensions_case_insensitively() {
        assert!(is_media_path("/x/hbrs-hjntiu.mkv"));
        assert!(is_media_path("/x/CD1/exvid.AVI"));
        assert!(is_media_path("/x/movie.iso"));
        assert!(!is_media_path("/x/release.nfo"));
        assert!(!is_media_path("/x/release.sfv"));
        assert!(!is_media_path("/x/no_extension"));
    }

    #[test]
    fn has_file_absent_deserializes_to_none() {
        // Sonarr's series list omits hasFile entirely.
        let series: ArrRecord = serde_json::from_str(r#"{"id":7,"path":"/share/TV/X"}"#).unwrap();
        assert_eq!(series.has_file, None);
        // Radarr carries it.
        let movie: ArrRecord =
            serde_json::from_str(r#"{"id":3,"path":"/share/Movies/X","hasFile":false}"#).unwrap();
        assert_eq!(movie.has_file, Some(false));
    }
}

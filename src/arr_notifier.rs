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
//!   2. resolves the owning series/movie by ancestor-path match against the cached *arr list —
//!      with a title+year fallback for movies whose record points at a different release
//!      variant (that path is then repointed at the validated dir before the rescan),
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
    /// Title + year back the movie fallback match for release dirs whose path matches no
    /// record (e.g. a manually-queued variant of a release the record never pointed at).
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    year: Option<i32>,
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
    /// Per-item endpoint base (`{base}/api/v3/{series|movie}`) for GET/PUT by id, used by
    /// the movie-path repoint in the title+year fallback.
    item_url_base: String,
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
        let item_url_base = format!("{base}/api/v3/{endpoint}");
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
            item_url_base,
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
            // Movies matched by title+year instead of path: the record's stored path points
            // somewhere else (a different release variant, or a grab that never landed), so
            // a plain rescan of it would import nothing — these need a path repoint first.
            let mut fallback: Vec<(i64, PathBuf)> = Vec::new();
            let mut fallback_ids: HashSet<i64> = HashSet::new();
            for dir in &dirs {
                let translated = translate_path(dir, &inst.path_mapping, inst.name);
                if let Some(id) = best_match(&records, &translated) {
                    to_rescan.insert(id);
                } else if matches!(inst.kind, ArrKind::Movie) {
                    if let Some(id) = title_year_match(&records, &translated) {
                        if !to_rescan.contains(&id) && fallback_ids.insert(id) {
                            info!(
                                "Arr: {} matched {} to movieId {} by title+year (record path differs)",
                                inst.name, translated.display(), id
                            );
                            fallback.push((id, translated.clone()));
                        }
                    } else {
                        debug!("Arr: {} no match for {}", inst.name, translated.display());
                    }
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

            // Fallback-matched movies: mirror dc-bridge's reconcile — repoint the movie
            // folder at the validated release dir (moveFiles=false, no data touched),
            // then rescan; force-import remains the safety net for obfuscated internal
            // filenames. title_year_match only ever returns fileless records, so the
            // repoint can never yank a folder away from an already-imported file.
            for (id, folder) in fallback {
                let folder = folder.to_string_lossy();
                if self.repoint_movie_path(inst, id, &folder).await {
                    self.fire_rescan(inst, id).await;
                    if inst.force_import {
                        self.force_import(inst, &folder, id).await;
                    }
                }
            }
        }

        Ok(())
    }

    /// Point a movie's folder at the validated release dir (`moveFiles=false` — metadata
    /// only, nothing on disk is touched). Returns false on any failure; the caller then
    /// skips the rescan so we never rescan a half-repointed record.
    async fn repoint_movie_path(&self, inst: &ArrInstance, movie_id: i64, folder: &str) -> bool {
        let get_url = format!("{}/{}?apikey={}", inst.item_url_base, movie_id, inst.api_key);
        let mut movie: serde_json::Value = match self
            .client
            .get(&get_url)
            .header("Accept", "application/json")
            .send()
            .await
            .and_then(|r| r.error_for_status())
        {
            Ok(resp) => match resp.json().await {
                Ok(v) => v,
                Err(e) => {
                    warn!("Arr: {} movie {} fetch parse failed: {}", inst.name, movie_id, e);
                    return false;
                }
            },
            Err(e) => {
                warn!("Arr: {} movie {} fetch failed: {}", inst.name, movie_id, e);
                return false;
            }
        };

        movie["path"] = serde_json::Value::from(folder);
        let put_url = format!(
            "{}/{}?moveFiles=false&apikey={}",
            inst.item_url_base, movie_id, inst.api_key
        );
        info!("Arr: {} repointing movieId {} path -> {}", inst.name, movie_id, folder);
        match self
            .client
            .put(&put_url)
            .json(&movie)
            .send()
            .await
            .and_then(|r| r.error_for_status())
        {
            Ok(_) => true,
            Err(e) => {
                warn!("Arr: {} movie {} path repoint failed: {}", inst.name, movie_id, e);
                false
            }
        }
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

/// Lowercased alphanumerics only — release-name dots/dashes and title punctuation
/// ("2001: A Space Odyssey" vs "2001.A.Space.Odyssey") normalize to the same string.
fn norm_title(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// Movie fallback for release dirs whose path matches no record: parse `<Title>.<Year>.`
/// out of the folder name and match on title+year against the movie list. Grabs of a
/// different release variant than the record points at (or manual AirDC++ grabs) resolve
/// this way. Every year-looking token is tried as the title/year split, so titles that
/// start with one ("2001.A.Space.Odyssey.1968.…") still parse. Year tolerates ±1 (scene
/// premiere-year vs *arr year drift). Only fileless records are considered — a movie with
/// an imported file must never be repointed away from it — and the match must be unique.
fn title_year_match(records: &[ArrRecord], path: &Path) -> Option<i64> {
    let dir_name = path.file_name()?.to_str()?;
    let toks: Vec<&str> = dir_name.split(['.', ' ', '_']).collect();

    for pos in 1..toks.len() {
        let year: i32 = match toks[pos].parse() {
            Ok(y) if (1900..=2099).contains(&y) && toks[pos].len() == 4 => y,
            _ => continue,
        };
        let folder_title = norm_title(&toks[..pos].join(""));
        if folder_title.is_empty() {
            continue;
        }
        let hits: Vec<&ArrRecord> = records
            .iter()
            .filter(|r| {
                r.has_file != Some(true)
                    && r.year.is_some_and(|y| (y - year).abs() <= 1)
                    && r.title.as_deref().is_some_and(|t| norm_title(t) == folder_title)
            })
            .collect();
        if let [only] = hits.as_slice() {
            return Some(only.id);
        }
    }
    None
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
        ArrRecord { id, path: path.to_string(), has_file: None, title: None, year: None }
    }

    fn movie(id: i64, path: &str, title: &str, year: i32, has_file: bool) -> ArrRecord {
        ArrRecord {
            id,
            path: path.to_string(),
            has_file: Some(has_file),
            title: Some(title.to_string()),
            year: Some(year),
        }
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
    fn title_year_fallback_matches_variant_release() {
        // Record points at a 2160p grab that never landed; disk has the 1080p variant.
        let records = vec![movie(
            3906,
            "/share/Movies/2001.A.Space.Odyssey.1968.2160p.UHD.BluRay.X265-IAMABLE",
            "2001: A Space Odyssey",
            1968,
            false,
        )];
        let path =
            PathBuf::from("/share/Movies/2001.A.Space.Odyssey.1968.REMASTERED.1080p.BluRay.X264-AMIABLE");
        assert_eq!(best_match(&records, &path), None); // path match fails...
        assert_eq!(title_year_match(&records, &path), Some(3906)); // ...fallback resolves
    }

    #[test]
    fn title_year_fallback_tolerates_year_drift() {
        // Scene tag 2025 vs Radarr year 2026 (premiere vs release year).
        let records =
            vec![movie(4172, "/share/Movies/Obsession (2026)", "Obsession", 2026, false)];
        let path = PathBuf::from("/share/Movies/Obsession.2025.NORDiC.1080p.WEB-DL.H.264-NORViNE");
        assert_eq!(title_year_match(&records, &path), Some(4172));
    }

    #[test]
    fn title_year_fallback_never_touches_movies_with_files() {
        // A second variant of an already-imported movie must not repoint the record.
        let records =
            vec![movie(2620, "/share/Movies/Deep.Water.2022.1080p.WEB.H264-SLOT", "Deep Water", 2022, true)];
        let path = PathBuf::from("/share/Movies/Deep.Water.2022.720p.BluRay.x264-OTHER");
        assert_eq!(title_year_match(&records, &path), None);
    }

    #[test]
    fn title_year_fallback_requires_unique_match() {
        // Two fileless records with the same title+year (e.g. duplicate entries): ambiguous.
        let records = vec![
            movie(1, "/share/Movies/X (2020)", "Twin", 2020, false),
            movie(2, "/share/Movies/Y (2020)", "Twin", 2020, false),
        ];
        let path = PathBuf::from("/share/Movies/Twin.2020.1080p.WEB.x264-GRP");
        assert_eq!(title_year_match(&records, &path), None);
    }

    #[test]
    fn title_year_fallback_needs_a_year_token() {
        let records = vec![movie(9, "/share/Movies/Z (2019)", "Some Film", 2019, false)];
        let path = PathBuf::from("/share/Movies/Some.Film.1080p.WEB.x264-GRP");
        assert_eq!(title_year_match(&records, &path), None);
    }

    #[test]
    fn mid_title_number_is_not_mistaken_for_the_year() {
        let records = vec![movie(
            11,
            "/share/Movies/Blade Runner 2049 (2017)",
            "Blade Runner 2049",
            2017,
            false,
        )];
        let path = PathBuf::from("/share/Movies/Blade.Runner.2049.2017.1080p.BluRay.x264-GRP");
        assert_eq!(title_year_match(&records, &path), Some(11));
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

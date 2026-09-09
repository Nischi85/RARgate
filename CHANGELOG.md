# Changelog

## Unreleased

- inotify: a Sonarr/Radarr rescan now fires only on a release's **first** announce (the
  "it's valid and visible, import it" moment), not on later re-notifications. A sidecar
  file landing in an already-imported release (a subtitle or `.nfo` written by bazarr &
  co.) still refreshes Emby/Jellyfin/Plex so the subs show up, but no longer pokes the
  *arr apps — that rescan raced the overlay-cache refresh and could make Sonarr briefly
  see the media file as missing and mark it MissingFromDisk.
- SFV validation: strip a leading UTF-8 BOM from `.sfv` lines and resolve subfolder
  entries (`Sample\foo.mkv`) by basename. PowerShell-packed scene releases ship a
  BOM'd `.sfv` whose first line no longer read as a comment, which failed strict
  validation and kept the whole release hidden from the mount.
- SFV validation is now case-insensitive when matching `.sfv` entries to files.
- Filtering: new optional `filters.include_dirs` — a whitelist of top-level directory
  names to expose at the mount root; everything else there is hidden regardless of SFV
  state. Applies only at the root, and is hot-reloadable via SIGHUP like the other
  filter lists.
- SFV failure observability: the per-cooldown WARN is throttled to milestone crossings
  (3/10/50/100/500/...) instead of one line per cycle, each line reports how long the
  release has been stuck, and a single loud ERROR fires once when failures cross
  `validation_backoff.escalate_after` (new knob, default: 25). The status JSON gains a
  `stuck_releases` array (worst-first) and `stuck_releases_count`.
- SFV validation: a directory that was already a confirmed, exposed media dir and then
  starts failing validation now escalates (loud ERROR + status-file flag) after just
  `regression_escalate_after` failures (default: 3) instead of the normal
  `escalate_after` (default: 25) — that failure pattern means its files were very
  likely deleted or moved out from under it, not a normal still-downloading release,
  so it shouldn't have to wait through many quiet cooldown cycles to get flagged.
- Radarr: when a validated movie release can't be matched to a record by path, fall
  back to matching `<Title>.<Year>` from the folder name (unique fileless record, year
  tolerance ±1) and repoint that record at the validated directory before the rescan.
  Movies that already have a file are never touched.
- Emby: new top-level content on libraries with real-time monitoring enabled now uses
  a path-scoped update notification instead of a full-library rescan.
- Deploy: each instance gets its own `paths.conf` derived from the `--config` filename,
  so running two instances no longer makes the deploy scripts poll the wrong mount.

## v1.1.0

- Radarr: force-import releases whose extracted file carries an obfuscated internal name. After a rescan, if a matched movie still has no file, RARGate issues a manual-import request keyed by movie ID, which imports the in-place file without relying on filename parsing (no move or copy). Controlled by `arr.radarr.force_import` (default: `true`); Radarr only.
- Docs: document the `arr:` (Sonarr/Radarr) block in `deploy/config.yaml.example`.
- Add `deploy/scripts/radarr-backfill-import.sh` to force-import existing movies whose file was never imported.

## v1.0.0 — Initial public release

First public release of RARGate.

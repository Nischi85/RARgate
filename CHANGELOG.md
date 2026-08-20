# Changelog

## Unreleased

- SFV validation: a directory that was already a confirmed, exposed media dir and then
  starts failing validation now escalates (loud ERROR + status-file flag) after just
  `regression_escalate_after` failures (default: 3) instead of the normal
  `escalate_after` (default: 25) — that failure pattern means its files were very
  likely deleted or moved out from under it, not a normal still-downloading release,
  so it shouldn't have to wait through many quiet cooldown cycles to get flagged.

## v1.1.0

- Radarr: force-import releases whose extracted file carries an obfuscated internal name. After a rescan, if a matched movie still has no file, RARGate issues a manual-import request keyed by movie ID, which imports the in-place file without relying on filename parsing (no move or copy). Controlled by `arr.radarr.force_import` (default: `true`); Radarr only.
- Docs: document the `arr:` (Sonarr/Radarr) block in `deploy/config.yaml.example`.
- Add `deploy/scripts/radarr-backfill-import.sh` to force-import existing movies whose file was never imported.

## v1.0.0 — Initial public release

First public release of RARGate.

# Changelog

## v1.1.0

- Radarr: force-import releases whose extracted file carries an obfuscated internal name. After a rescan, if a matched movie still has no file, RARGate issues a manual-import request keyed by movie ID, which imports the in-place file without relying on filename parsing (no move or copy). Controlled by `arr.radarr.force_import` (default: `true`); Radarr only.
- Docs: document the `arr:` (Sonarr/Radarr) block in `deploy/config.yaml.example`.
- Add `deploy/scripts/radarr-backfill-import.sh` to force-import existing movies whose file was never imported.

## v1.0.0 — Initial public release

First public release of RARGate.

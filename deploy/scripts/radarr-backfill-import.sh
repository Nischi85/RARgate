#!/usr/bin/env bash
#
# radarr-backfill-import.sh — force-import orphaned movies into Radarr.
#
# Scene RARs expose the media file under an obfuscated internal name (e.g.
# hbrs-hjntiu.mkv). Radarr's automatic scan parses that filename, fails to
# identify it ("Unknown Movie"), and never imports — so the movie record sits
# at hasFile=false even though the file is present and visible via rargate.
#
# This script does what a manual "assign file to movie" does: for every movie
# with hasFile=false whose own folder contains a media file, it issues a
# ManualImport command WITH the movieId, which bypasses the filename parse.
# Radarr imports in place (rename is off), keeping the original filename.
#
# importMode "auto" + a movieId imports the file found in that movie's OWN
# folder, so there is no cross-assignment risk as long as we pair each movie
# with its own path (which we do).
#
# Usage:
#   radarr-backfill-import.sh [--dry-run]
#
# Config: RADARR_URL / RADARR_API_KEY env vars override; otherwise parsed from
# the arr.radarr block of CONFIG (default: ../config.yaml next to this script).

set -euo pipefail

DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="${CONFIG:-$SCRIPT_DIR/../config.yaml}"

# Pull radarr url/api_key from the arr.radarr block unless overridden by env.
if [[ -z "${RADARR_URL:-}" || -z "${RADARR_API_KEY:-}" ]]; then
  if [[ ! -f "$CONFIG" ]]; then
    echo "ERROR: config not found at $CONFIG and RADARR_URL/RADARR_API_KEY not set" >&2
    exit 1
  fi
  # awk: enter the radarr: block, grab the first url:/api_key: lines under it.
  eval "$(awk '
    /^[[:space:]]*radarr:[[:space:]]*$/ {inblk=1; next}
    inblk && /^[[:space:]]*sonarr:[[:space:]]*$/ {inblk=0}
    inblk && match($0, /url:[[:space:]]*"?([^"]+)"?/, m) && !u {printf "RADARR_URL=%s\n", m[1]; u=1}
    inblk && match($0, /api_key:[[:space:]]*"?([^"]+)"?/, m) && !k {printf "RADARR_API_KEY=%s\n", m[1]; k=1}
  ' "$CONFIG")"
fi

if [[ -z "${RADARR_URL:-}" || -z "${RADARR_API_KEY:-}" ]]; then
  echo "ERROR: could not determine Radarr URL/API key" >&2
  exit 1
fi

B="${RADARR_URL%/}"
K="$RADARR_API_KEY"
MEDIA_RE='\.(mkv|mp4|avi|m4v|mov|wmv|flv|mpg|mpeg|iso|img)$'

echo "Radarr: $B  (dry-run=$DRY_RUN)"

# All movies missing a file.
mapfile -t ORPHANS < <(
  curl -fsS "$B/api/v3/movie?apikey=$K" \
  | jq -r '.[] | select(.hasFile==false) | "\(.id)\t\(.path)\t\(.title) (\(.year))"'
)

echo "Movies with hasFile=false: ${#ORPHANS[@]}"
imported=0; skipped=0; failed=0

for row in "${ORPHANS[@]}"; do
  IFS=$'\t' read -r MID MPATH MTITLE <<<"$row"

  cand=$(curl -fsS --get "$B/api/v3/manualimport" \
           --data-urlencode "folder=$MPATH" \
           --data-urlencode "filterExistingFiles=true" \
           --data-urlencode "apikey=$K" 2>/dev/null || echo '[]')

  # Largest media-file candidate in the movie's own folder.
  body=$(echo "$cand" | jq -c --arg re "$MEDIA_RE" --argjson mid "$MID" '
    [ .[] | select(.path | test($re; "i")) ]
    | sort_by(.size) | reverse | .[0:1]
    | { name:"ManualImport", importMode:"auto",
        files: map({path, movieId:$mid, quality, languages, releaseGroup}) }')

  nfiles=$(echo "$body" | jq '.files | length')
  if [[ "$nfiles" -eq 0 ]]; then
    echo "  SKIP  $MTITLE — no media file in folder (unreleased request?)"
    skipped=$((skipped+1)); continue
  fi

  fpath=$(echo "$body" | jq -r '.files[0].path')
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "  WOULD $MTITLE  <=  $fpath"
    imported=$((imported+1)); continue
  fi

  cmdid=$(curl -fsS -X POST "$B/api/v3/command?apikey=$K" \
            -H "Content-Type: application/json" -d "$body" | jq -r '.id // empty')
  if [[ -z "$cmdid" ]]; then
    echo "  FAIL  $MTITLE — command POST rejected"
    failed=$((failed+1)); continue
  fi

  # Poll the command to completion.
  st=""
  for _ in $(seq 1 10); do
    st=$(curl -fsS "$B/api/v3/command/$cmdid?apikey=$K" | jq -r '.status')
    [[ "$st" == "completed" || "$st" == "failed" ]] && break
    sleep 2
  done

  hasfile=$(curl -fsS "$B/api/v3/movie/$MID?apikey=$K" | jq -r '.hasFile')
  if [[ "$hasfile" == "true" ]]; then
    echo "  OK    $MTITLE  <=  $fpath"
    imported=$((imported+1))
  else
    echo "  FAIL  $MTITLE — cmd $st, still hasFile=false"
    failed=$((failed+1))
  fi
done

echo "----"
echo "imported/would-import=$imported  skipped=$skipped  failed=$failed"

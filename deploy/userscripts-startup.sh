#!/bin/bash
#
# RARGate + Overlay Startup Script for Unraid UserScripts
# Schedule: At Startup of Array
#
# 1. Mounts overlay filesystem (unverified + verified layers → merged overlay)
# 2. Installs RARGate (creates /etc/rargate/config.yaml)
# 3. Starts RARGate service
# 4. Starts crash monitor
#

set -uo pipefail   # no -e: tolerates non-zero from is_mounted / pgrep / grep in pipelines

# Require bash 4+ for [[, arrays, process substitution.
if [ -z "${BASH_VERSINFO[0]:-}" ] || [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
    echo "ERROR: userscripts-startup.sh requires bash 4 or newer" >&2
    exit 1
fi

### --- Configuration ---
# BIN_DIR is the directory containing this script, the rargate binary, and
# the install/shutdown/monitor helpers. Adjust to where you keep them.
BIN_DIR="/path/to/rargate/deploy"
CONFIG_FILE="/etc/rargate/config.yaml"
BACKEND="overlay"  # or "unionfs"

# Path defaults — overridden by /var/run/rargate-paths.conf if rargate has
# run at least once (single source of truth = config.yaml).
RARGATE_MOUNT="/mnt/user/rargate"
BACKEND_DIR="/mnt/user/rar2fs-backend"
LOG_FILE="/var/log/rargate.log"
SFV_LOG_FILE="/var/log/rargate-sfv-failures.log"
MONITOR_LOG_FILE="/var/log/rargate-monitor.log"

# Overlay mount points (only used when BACKEND="overlay"). Defaults match
# the live deployment; overridden by /var/run/rargate-paths.conf below.
LOWER="/mnt/user/share/unverified"
UPPER="/mnt/user/share/verified"
WORK="/mnt/user/share/work"              # No equivalent in config.yaml — set here.
MERGED="/mnt/user/share/overlay_merged"

# Pull paths from rargate's written conf file if present. Field names in
# the conf match what utils.rs::write_paths_conf emits; some fields below
# get re-mapped (e.g., BACKEND_MOUNT → BACKEND_DIR).
# shellcheck disable=SC1091
source /var/run/rargate-paths.conf 2>/dev/null || true
BACKEND_DIR="${BACKEND_MOUNT:-$BACKEND_DIR}"
LOG_FILE="${RARGATE_LOG_FILE:-$LOG_FILE}"
LOWER="${OVERLAY_LOWER:-$LOWER}"
UPPER="${OVERLAY_UPPER:-$UPPER}"
MERGED="${OVERLAY_MERGED:-$MERGED}"
# RARGATE_MOUNT and SFV_LOG_FILE are set directly by rargate-paths.conf.
# WORK has no YAML equivalent — auto-derive next to UPPER if user hasn't set it.
if [ -z "${WORK:-}" ]; then WORK="$(dirname "$UPPER")/work"; fi

### --- Helper functions ---

# is_mounted: checks if a given path is currently an active mount point.
# Works by scanning the system mount table for an exact path match.
is_mounted() { mount | awk '{print $3}' | grep -qx "$1"; }

# Enable core dumps so that if RARGate crashes, we get a crash dump file
# we can analyze later for debugging. Without this, crashes leave no trace.
#
# Dumps go to a persistent, size-bounded location. CORE_DUMP_DIR and CORE_DUMP_KEEP
# come from rargate-paths.conf (sourced above) once rargate has run at least once;
# defaults apply on a cold boot before that file exists.
setup_core_dumps() {
    local core_dir="${CORE_DUMP_DIR:-/mnt/cache/rargate/cores}"
    local keep="${CORE_DUMP_KEEP:-5}"
    mkdir -p "$core_dir"
    chmod 1777 "$core_dir"
    # Prune to the newest $keep dumps so accumulated cores can't fill the disk.
    if [ "${keep:-0}" -gt 0 ] 2>/dev/null; then
        # shellcheck disable=SC2012
        ls -1t "$core_dir"/core.* 2>/dev/null | tail -n +"$((keep + 1))" | while read -r old; do
            rm -f -- "$old"
        done
    fi
    echo "$core_dir/core.%e.%p.%t" > /proc/sys/kernel/core_pattern 2>/dev/null || true
    ulimit -c unlimited 2>/dev/null || true
    echo "   Core dumps: $core_dir (keep newest $keep)"
}

# Remove leftover shutdown flags from a previous shutdown. The monitor script
# creates these flags during intentional shutdowns so it knows not to send
# false crash alerts. If we're starting up, any old flags are stale.
clean_shutdown_flags() {
    for flag in /tmp/rargate-intentional-shutdown-*.flag; do
        [ -f "$flag" ] && rm -f "$flag"
    done
}

echo "=================================================="
echo "RARGate + Overlay Startup"
echo "=================================================="
echo ""

setup_core_dumps
clean_shutdown_flags
echo ""

### --- Step 1: Mount Overlay ---
echo "[1/4] Mounting overlay filesystem..."
echo "      Lower (RO): $LOWER"
echo "      Upper (RW): $UPPER"
echo "      Merged:     $MERGED"

# Verify source directories exist
for dir in "$LOWER" "$UPPER"; do
    if [ ! -d "$dir" ]; then
        echo "   ERROR: Directory not found: $dir"
        exit 1
    fi
done

mkdir -p "$WORK" "$MERGED"

if [[ "$BACKEND" == "overlay" ]]; then
    # Check UPPER and WORK are on the same filesystem.
    # GNU stat: `stat -f -c %T <path>`. BSD stat: `stat -f %T <path>`.
    fs_type() {
        stat -f -c %T "$1" 2>/dev/null || stat -f "%T" "$1" 2>/dev/null
    }
    if [ "$(fs_type "$UPPER")" != "$(fs_type "$WORK")" ]; then
        echo "   ERROR: UPPER ($UPPER) and WORK ($WORK) are on different filesystems!"
        exit 1
    fi

    if is_mounted "$MERGED"; then
        echo "      Already mounted: $MERGED"
    elif mount -t overlay overlay \
        -o xino=off,lowerdir="$LOWER",upperdir="$UPPER",workdir="$WORK" \
        "$MERGED"; then
        echo "      Overlay mounted successfully"
    else
        echo "   ERROR: Failed to mount overlay"
        exit 1
    fi
else
    if is_mounted "$MERGED"; then
        echo "      Already mounted: $MERGED"
    elif unionfs-fuse -o cow,nonempty "$UPPER=RW:$LOWER=RO" "$MERGED"; then
        echo "      UnionFS mounted successfully"
    else
        echo "   ERROR: Failed to mount unionfs"
        exit 1
    fi
fi

# Force any pending writes to disk before continuing. This ensures the
# overlay filesystem is fully settled before RARGate starts reading from it.
sync
echo ""

### --- Step 2: Install RARGate ---
echo "[2/4] Installing RARGate..."

if [ ! -d "$BIN_DIR" ]; then
    echo "   ERROR: Bin directory not found: $BIN_DIR"
    exit 1
fi

cd "$BIN_DIR" || exit 1

if [ ! -f "./install-rargate.sh" ]; then
    echo "   ERROR: install-rargate.sh not found in $BIN_DIR"
    exit 1
fi

if ./install-rargate.sh; then
    echo "      RARGate installed"
else
    echo "   ERROR: Installation failed"
    exit 1
fi
echo ""

### --- Step 2.5: Clean stale rar2fs backend ---
# If the previous shutdown didn't clean up properly, there may be a stale
# rar2fs FUSE mount left behind. RARGate creates fresh rar2fs mounts on
# startup, so old ones would conflict. We unmount and clean them here.
echo "   Cleaning up stale rar2fs backend..."

if is_mounted "$BACKEND_DIR"; then
    echo "   Unmounting stale backend: $BACKEND_DIR"
    fusermount3 -uz "$BACKEND_DIR" 2>/dev/null || fusermount -uz "$BACKEND_DIR" 2>/dev/null || umount -fl "$BACKEND_DIR" 2>/dev/null
    sleep 1
fi

# Defensive: only remove inside BACKEND_DIR if it's a non-trivial directory.
# Guards against the "BACKEND_DIR unset → rm -rf /*" footgun under set -u.
if [ -n "${BACKEND_DIR:-}" ] && [ "$BACKEND_DIR" != "/" ] && [ -d "$BACKEND_DIR" ]; then
    file_count=$(ls -A "$BACKEND_DIR" 2>/dev/null | wc -l)
    if [ "$file_count" -gt 0 ]; then
        echo "   Removing $file_count leftover files from $BACKEND_DIR"
        rm -rf -- "$BACKEND_DIR"/*  2>/dev/null || true
        rm -rf -- "$BACKEND_DIR"/.[!.]* 2>/dev/null || true
    fi
fi
echo ""

### --- Step 3: Start RARGate ---
echo "[3/4] Starting RARGate service..."

if [ ! -f "/usr/local/bin/rargate" ]; then
    echo "   ERROR: RARGate not installed at /usr/local/bin/rargate"
    exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo "   ERROR: Config not found: $CONFIG_FILE"
    exit 1
fi

mkdir -p "$RARGATE_MOUNT"
mkdir -p "$(dirname "$LOG_FILE")"

if is_mounted "$RARGATE_MOUNT"; then
    echo "      Already mounted: $RARGATE_MOUNT"
else
    echo "      Config: $CONFIG_FILE"
    echo "      Mount:  $RARGATE_MOUNT"
    echo "      Log:    $LOG_FILE"

    /usr/local/bin/rargate --config "$CONFIG_FILE" >> "$LOG_FILE" 2>&1 &

    MAX_WAIT=15
    WAITED=0
    echo "      Waiting for mount (up to ${MAX_WAIT}s)..."
    while [ $WAITED -lt $MAX_WAIT ]; do
        if is_mounted "$RARGATE_MOUNT"; then
            echo "      Mount detected after ${WAITED}s"
            break
        fi
        sleep 1
        WAITED=$((WAITED + 1))
    done

    if is_mounted "$RARGATE_MOUNT"; then
        echo "      RARGate started successfully"
        echo ""
        echo "      Mount verification:"
        mount | grep rargate || true
        echo ""
        echo "      Memory usage:"
        ps aux | grep "rargate" | grep -v grep | awk '{print "      RSS: " $6 "KB (" $6/1024 "MB)"}'
    else
        echo "   ERROR: RARGate failed to mount"
        echo ""
        echo "Recent log entries:"
        tail -20 "$LOG_FILE"
        exit 1
    fi
fi

echo ""

### --- Step 4: Start Crash Monitor ---
echo "[4/4] Starting external crash monitor..."

MONITOR_SCRIPT="$BIN_DIR/rargate-monitor.sh"
MONITOR_PID_FILE="/var/run/rargate-monitor.pid"

# During an auto-restart the monitor is OUR caller: it ran shutdown.sh then this
# script inline, and resumes its own watch loop once we return (rargate-monitor.sh
# rm's the flag and continues). Starting another monitor here would orphan the
# original and leave two daemons racing to auto-restart. Skip — symmetric with
# shutdown.sh's [1/3] skip on the same flag.
if [ -f /var/run/rargate-intentional-shutdown-auto.flag ] \
   || [ -f /tmp/rargate-intentional-shutdown-auto.flag ]; then
    echo "      Skipped (auto-restart in progress — triggering monitor is still running and resumes its loop)"
elif [ ! -f "$MONITOR_SCRIPT" ]; then
    echo "      Warning: Monitor script not found: $MONITOR_SCRIPT"
else
    # Converge to exactly one monitor. Reap any daemon already running — including
    # untracked orphans a previous restart may have left behind — then start one
    # fresh. The cmdline pattern is specific enough not to hit unrelated processes,
    # and this also obsoletes the old stale-PID-file dance.
    existing=$(pgrep -f 'rargate-monitor\.sh _daemon' 2>/dev/null || true)
    if [ -n "$existing" ]; then
        echo "      Reaping monitor daemon(s) already running before start: $(echo $existing | tr '\n' ' ')"
        # shellcheck disable=SC2086
        kill $existing 2>/dev/null || true
        sleep 1
    fi
    rm -f "$MONITOR_PID_FILE"

    # Try to start. Capture output so we can show it on failure.
    monitor_out=$("$MONITOR_SCRIPT" start 2>&1)
    monitor_rc=$?
    if [ "$monitor_rc" -eq 0 ]; then
        echo "      Crash monitor started"
    else
        echo "      First start attempt failed (rc=$monitor_rc):"
        echo "$monitor_out" | sed 's/^/        /'
        # Retry once after a brief settle, in case it was a transient race.
        sleep 2
        rm -f "$MONITOR_PID_FILE"
        if "$MONITOR_SCRIPT" start; then
            echo "      Crash monitor started on retry"
        else
            echo "      Warning: Failed to start crash monitor (RARGate will still work; start manually with: $MONITOR_SCRIPT start)"
        fi
    fi
fi

echo ""
echo "=================================================="
echo "Startup Complete"
echo "=================================================="
echo ""
echo "Overlay:  $MERGED"
echo "RARGate:  $RARGATE_MOUNT"
echo "Log:      $LOG_FILE"
echo "SFV Log:  $SFV_LOG_FILE"
echo "Monitor:  $MONITOR_LOG_FILE"
echo ""

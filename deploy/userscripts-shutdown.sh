#!/bin/bash
#
# RARGate Shutdown Script for Unraid UserScripts
# Schedule: At Stopping of Array
#
# 1. Stops crash monitor
# 2. Unmounts RARGate + rar2fs backend
# 3. Unmounts overlay filesystem
#
# NOTE: Does NOT handle Docker, VMs, array, or ZFS - Unraid manages those.
# When auto-restart fires from rargate-monitor.sh, Docker IS stopped first
# by the monitor (via $DOCKER_STOP_CMD) before this script runs.

set -uo pipefail   # no -e: shutdown deliberately tolerates non-zero from unmount escalations and pgrep

# Require bash 4+ for [[, arrays, process substitution.
if [ -z "${BASH_VERSINFO[0]:-}" ] || [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
    echo "ERROR: userscripts-shutdown.sh requires bash 4 or newer" >&2
    exit 1
fi

### --- Configuration ---
# BIN_DIR is the directory containing this script and the rargate binary.
BIN_DIR="/path/to/rargate/deploy"
BACKEND="overlay"  # or "unionfs"

# Path defaults — overridden by /var/run/rargate-paths.conf if rargate has
# run at least once (single source of truth = config.yaml).
RARGATE_MOUNT="/mnt/user/rargate"
MERGED="/mnt/user/share/overlay_merged"

# shellcheck disable=SC1091
source /var/run/rargate-paths.conf 2>/dev/null || true
MERGED="${OVERLAY_MERGED:-$MERGED}"
# RARGATE_MOUNT is set directly by rargate-paths.conf when present.

### --- Timeouts ---
MAX_WAIT=30

### --- Helper functions ---

# is_mounted: checks if a given path is currently an active mount point.
is_mounted() { mount | awk '{print $3}' | grep -qx "$1"; }

# do_unmount TARGET LABEL [fuse]
#
# Escalation strategy for unmounting (each step only runs if the previous failed):
#   1. Graceful unmount — asks the filesystem to cleanly disconnect
#   2. Wait up to MAX_WAIT seconds — gives open files time to close
#   3. Lazy unmount (-z/-l) — detaches immediately, cleans up when no longer busy
#   4. Force unmount (-f) — last resort, may leave stale file handles
#
# Pass "fuse" as 3rd argument for FUSE mounts (uses fusermount instead of umount).
do_unmount() {
    local target="$1" label="$2" use_fuse="${3:-}"

    if ! is_mounted "$target"; then
        echo "      $label not mounted"
        return 0
    fi

    echo "      Unmounting $label ($target)..."

    # Step 1: Graceful unmount
    if [ "$use_fuse" = "fuse" ]; then
        fusermount3 -u "$target" 2>/dev/null || fusermount -u "$target" 2>/dev/null
    else
        umount "$target" 2>/dev/null
    fi

    # Step 2: Wait for unmount
    local waited=0
    while is_mounted "$target"; do
        if [ $waited -ge $MAX_WAIT ]; then
            # Step 3: Lazy unmount — detach now, cleanup later
            echo "      Timeout, trying lazy unmount..."
            if [ "$use_fuse" = "fuse" ]; then
                fusermount3 -uz "$target" 2>/dev/null || fusermount -uz "$target" 2>/dev/null
            else
                umount -fl "$target" 2>/dev/null
            fi
            sleep 2
            break
        fi
        sleep 1
        waited=$((waited + 1))
    done

    # Step 4: Force unmount if still mounted
    if is_mounted "$target"; then
        echo "      Still mounted, forcing unmount..."
        umount -fl "$target" 2>/dev/null || umount -f "$target" 2>/dev/null
        sleep 1
    fi

    if is_mounted "$target"; then
        echo "      ERROR: Could not unmount $target (may need manual intervention)"
        return 1
    else
        echo "      $label unmounted"
        return 0
    fi
}

# kill_processes PATTERN LABEL
# Two-step process shutdown:
#   1. SIGTERM — politely asks the process to exit (allows cleanup)
#   2. Wait 2 seconds for it to finish
#   3. SIGKILL (-9) — forces immediate termination if it didn't exit
kill_processes() {
    local pattern="$1" label="$2"
    local pids
    pids=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [ -z "$pids" ]; then
        return 0
    fi
    kill -TERM $pids 2>/dev/null
    sleep 2
    pids=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [ -n "$pids" ]; then
        kill -9 $pids 2>/dev/null
    fi
    echo "        Killed $label"
}

echo "=================================================="
echo "RARGate Clean Shutdown"
echo "=================================================="

# Create shutdown flags BEFORE stopping anything. The crash monitor runs
# independently and checks every 60s — without these flags, it would see
# RARGate disappear and send a false "crash detected" alert. The flags
# tell the monitor "this is an intentional shutdown, don't panic."
if pgrep -f "rargate" > /dev/null; then
    echo "   Creating shutdown flags..."
    for pid in $(pgrep -f "rargate" || true); do
        touch "/tmp/rargate-intentional-shutdown-${pid}.flag" 2>/dev/null || true
    done
    sleep 1
fi
echo ""

### --- Step 1: Stop Crash Monitor ---
echo "[1/3] Stopping crash monitor..."

# Skip when invoked from the monitor's auto-restart path. The monitor creates this
# flag at rargate-monitor.sh:320 before calling us; killing the monitor here would
# orphan the restart sequence so startup.sh never runs.
if [ -f /var/run/rargate-intentional-shutdown-auto.flag ] \
   || [ -f /tmp/rargate-intentional-shutdown-auto.flag ]; then
    echo "      Skipped (auto-restart in progress — monitor IS our caller)"
else
    MONITOR_SCRIPT="$BIN_DIR/rargate-monitor.sh"
    if [ -f "$MONITOR_SCRIPT" ]; then
        if "$MONITOR_SCRIPT" stop; then
            echo "      Crash monitor stopped"
        else
            echo "      Crash monitor was not running"
        fi
    else
        echo "      Monitor script not found"
    fi
    # Reap any monitor daemons not tracked by the PID file (orphans left by an earlier
    # restart). `monitor.sh stop` only kills the PID-file monitor; a stray one left
    # running could fire its own auto-restart during the shutdown->startup window and
    # race the deploy. No-op in the auto path above (we don't reach here).
    orphans=$(pgrep -f 'rargate-monitor\.sh _daemon' || true)
    if [ -n "$orphans" ]; then
        echo "      Reaping orphan monitor daemon(s): $(echo $orphans | tr '\n' ' ')"
        # shellcheck disable=SC2086
        kill $orphans 2>/dev/null || true
    fi
fi
echo ""

### --- Step 2: Stop RARGate + rar2fs ---
# Order matters here: unmount FIRST, then kill processes. If we killed the
# process while it's still mounted, the mount would become "stale" (visible
# but non-functional), which can block Unraid's array stop.
echo "[2/3] Stopping RARGate..."

do_unmount "$RARGATE_MOUNT" "RARGate" "fuse"

# rar2fs backend FUSE mounts sit behind RARGate. Like any FUSE mount,
# if left behind they can block the Unraid array from stopping cleanly.
echo "      Cleaning up rar2fs backend mounts..."
backend_mounts=$(mount | grep "rar2fs-backend" | awk '{print $3}' || true)
if [ -n "$backend_mounts" ]; then
    echo "$backend_mounts" | while read -r backend; do
        echo "        Force unmounting: $backend"
        timeout 3 fusermount3 -uz "$backend" 2>/dev/null || timeout 3 fusermount -uz "$backend" 2>/dev/null || umount -fl "$backend" 2>/dev/null
    done
else
    echo "        No rar2fs-backend mounts found"
fi

# Kill remaining processes
echo "      Killing remaining processes..."
kill_processes "^/usr/local/bin/rargate" "rargate"
kill_processes "rar2fs" "rar2fs"

echo ""

### --- Step 3: Unmount Overlay ---
echo "[3/3] Unmounting overlay filesystem..."

if [[ "$BACKEND" == "overlay" ]]; then
    do_unmount "$MERGED" "Overlay"
else
    do_unmount "$MERGED" "UnionFS" "fuse"
fi

echo ""
echo "=================================================="
echo "RARGate Shutdown Complete"
echo "=================================================="
echo ""
echo "Stopped: monitor, RARGate, rar2fs, overlay"
echo "Unraid will now handle system shutdown..."

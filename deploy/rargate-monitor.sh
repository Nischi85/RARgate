#!/bin/bash
#
# RARGate External Crash Monitor
#
# This script runs independently of RARGate and monitors for crashes.
# It detects when RARGate process dies unexpectedly, sends throttled
# notifications, and (optionally) auto-restarts the stack.
#
# Usage:
#   Start monitoring: ./rargate-monitor.sh start
#   Stop monitoring:  ./rargate-monitor.sh stop
#   Check status:     ./rargate-monitor.sh status
#
# Paths and crash-recovery settings are sourced from /var/run/rargate-*.conf,
# which rargate writes at startup from config.yaml. Built-in defaults below
# apply when those files are missing (e.g., first run before rargate has
# started).
#

# Strict mode (no -e: the monitor loop legitimately tolerates non-zero from
# pgrep / mountpoint / kill -0).
set -uo pipefail

# Require bash 4+ for [[, arrays, process substitution.
if [ -z "${BASH_VERSINFO[0]:-}" ] || [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
    echo "ERROR: rargate-monitor.sh requires bash 4 or newer" >&2
    exit 1
fi

### --- Configuration ---
PID_FILE="/var/run/rargate-monitor.pid"

# Default paths. /var/run/rargate-paths.conf (written by rargate at startup
# from config.yaml) overrides these. Adjust the fallbacks to whatever Unraid
# defaults make sense for first-run-before-rargate-has-ever-started.
RARGATE_MOUNT="/mnt/user/rargate"
BACKEND_MOUNT="/mnt/user/rar2fs-backend"
RARGATE_LOG="/var/log/rargate.log"
# Source paths written by rargate (single source of truth = config.yaml).
# shellcheck disable=SC1091
source /var/run/rargate-paths.conf 2>/dev/null || true
RARGATE_LOG="${RARGATE_LOG_FILE:-$RARGATE_LOG}"  # config.yaml field is RARGATE_LOG_FILE

LOG_FILE="/var/log/rargate-monitor.log"
CRASH_LOG="/var/log/rargate-crash.log"

# The crash log is capped at 500KB and rotated when exceeded so it never grows unbounded.
CRASH_LOG_MAX_SIZE=512000
# Same protection for the monitor's own log — auto-restart events can make
# it grow quickly on a flaky system.
MONITOR_LOG_MAX_SIZE=1048576   # 1 MB
CHECK_INTERVAL=60  # Check every 60 seconds

# Docker control during auto-restart. Defaults match Unraid; override on other distros.
# To opt out entirely (e.g., you don't run Docker, or your media servers aren't dockerized),
# set ENABLE_DOCKER_RESTART=0 in your environment — the stop/start steps will be skipped.
#   export DOCKER_STOP_CMD="systemctl stop docker"
#   export DOCKER_START_CMD="systemctl start docker"
#   export ENABLE_DOCKER_RESTART=0    # to skip docker stop/start entirely
ENABLE_DOCKER_RESTART="${ENABLE_DOCKER_RESTART:-1}"
DOCKER_STOP_CMD="${DOCKER_STOP_CMD:-/etc/rc.d/rc.docker stop}"
DOCKER_START_CMD="${DOCKER_START_CMD:-/etc/rc.d/rc.docker start}"

# Per-step timeouts for the auto-restart sequence (seconds). If one step
# hangs, the monitor recovers instead of blocking forever.
AUTORESTART_DOCKER_TIMEOUT="${AUTORESTART_DOCKER_TIMEOUT:-180}"
AUTORESTART_SHUTDOWN_TIMEOUT="${AUTORESTART_SHUTDOWN_TIMEOUT:-180}"
AUTORESTART_STARTUP_TIMEOUT="${AUTORESTART_STARTUP_TIMEOUT:-300}"

# Unraid notify helper. Absent on non-Unraid systems — the script will
# detect that once and silently skip notify calls thereafter.
UNRAID_NOTIFY_BIN="/usr/local/emhttp/webGui/scripts/notify"
UNRAID_NOTIFY_AVAILABLE=0
[ -x "$UNRAID_NOTIFY_BIN" ] && UNRAID_NOTIFY_AVAILABLE=1

### --- Notification Helper ---
send_notification() {
    local subject="$1"
    local description="$2"
    local importance="${3:-alert}"  # alert, warning, or normal

    if [ "$UNRAID_NOTIFY_AVAILABLE" = "1" ]; then
        "$UNRAID_NOTIFY_BIN" \
            -e "RARGate Monitor" \
            -s "$subject" \
            -d "$description" \
            -i "$importance" 2>/dev/null || true
    fi

    # Always log notification text — survives even when Unraid notify is unavailable.
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] NOTIFICATION: $subject - $description" >> "$LOG_FILE"
}

### --- Logging Helper (with rotation) ---
log() {
    # Rotate the monitor log when it gets large. Keeps the last ~half of
    # content so recent context survives.
    if [ -f "$LOG_FILE" ]; then
        local size
        size=$(stat -c%s "$LOG_FILE" 2>/dev/null || echo 0)
        if [ "$size" -gt "$MONITOR_LOG_MAX_SIZE" ]; then
            local keep=$((MONITOR_LOG_MAX_SIZE / 2))
            {
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] === LOG ROTATED (exceeded ${MONITOR_LOG_MAX_SIZE} bytes) ==="
                tail -c "$keep" "$LOG_FILE"
            } > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
        fi
    fi
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

### --- Crash Logging Helper ---
log_crash() {
    local crash_type="$1"
    local details="$2"

    # Rotate: if log exceeds max size, trim to last 400KB to keep it bounded
    if [ -f "$CRASH_LOG" ]; then
        local size=$(stat -c%s "$CRASH_LOG" 2>/dev/null || echo 0)
        if [ "$size" -gt "$CRASH_LOG_MAX_SIZE" ]; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] === LOG ROTATED (exceeded ${CRASH_LOG_MAX_SIZE} bytes) ===" > "$CRASH_LOG.tmp"
            tail -c 400000 "$CRASH_LOG" >> "$CRASH_LOG.tmp"
            mv "$CRASH_LOG.tmp" "$CRASH_LOG"
        fi
    fi

    # Write crash header
    {
        echo "=================================="
        echo "CRASH DETECTED: $(date '+%Y-%m-%d %H:%M:%S')"
        echo "=================================="
        echo "Type: $crash_type"
        echo "Details: $details"
        echo ""

        # Capture system state
        echo "--- System State ---"
        echo "Uptime: $(uptime)"
        echo "Memory: $(free -h | grep Mem:)"
        echo ""

        # Capture process info
        echo "--- Process Info ---"
        echo "RARGate process: $(pgrep -f 'rargate --config' || echo 'NOT RUNNING')"
        ps aux | grep -E "(rargate|rar2fs)" | grep -v grep || echo "No RARGate processes found"
        echo ""

        # Capture mount state
        echo "--- Mount State ---"
        mount | grep -E "(rargate|rar2fs)" || echo "No RARGate mounts found"
        echo ""

        # Capture last 30 lines of RARGate log
        echo "--- Last 30 Lines of RARGate Log ---"
        tail -30 "$RARGATE_LOG" 2>/dev/null || echo "Log not available"
        echo ""

        # Capture dmesg for kernel issues (last 50 lines with rargate/fuse mentions)
        echo "--- Kernel Messages (FUSE/RARGate) ---"
        dmesg | grep -iE "(rargate|fuse|rar2fs)" | tail -50 || echo "No relevant kernel messages"
        echo ""
        echo "=================================="
        echo ""
    } >> "$CRASH_LOG"

    log "Crash logged to $CRASH_LOG"
}

### --- Check if monitoring is already running ---
# A PID being alive isn't enough: PIDs get recycled, so we also check that
# /proc/<pid>/comm is bash or rargate-monitor to avoid false positives.
is_monitor_running() {
    [ -f "$PID_FILE" ] || return 1
    local pid
    pid=$(cat "$PID_FILE" 2>/dev/null)
    if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
        rm -f "$PID_FILE"
        return 1
    fi
    local comm
    comm=$(cat "/proc/$pid/comm" 2>/dev/null || echo "")
    case "$comm" in
        bash|rargate-monitor|rargate-monitor.sh) return 0 ;;
        *)
            # PID alive but unrelated process — stale PID file with recycled PID.
            rm -f "$PID_FILE"
            return 1
            ;;
    esac
}

### --- Monitor Loop ---
monitor_loop() {
    mkdir -p "$(dirname "$LOG_FILE")" "$(dirname "$CRASH_LOG")"

    # Load settings written by rargate at startup (sourced from config.yaml).
    # Defaults below kick in if the file is missing (e.g., first run before
    # rargate has started).
    # shellcheck disable=SC1091
    source /var/run/rargate-monitor.conf 2>/dev/null || true
    ENABLE_AUTO_RESTART="${ENABLE_AUTO_RESTART:-1}"
    RESTART_AFTER_FAILURES="${RESTART_AFTER_FAILURES:-3}"
    MAX_RESTART_ATTEMPTS="${MAX_RESTART_ATTEMPTS:-5}"
    RESET_AFTER_MINUTES="${RESET_AFTER_MINUTES:-30}"
    MAX_NOTIFICATIONS="${MAX_NOTIFICATIONS:-3}"

    log "RARGate monitor started (PID: $$)"
    log "Monitoring: RARGate=$RARGATE_MOUNT, Backend=$BACKEND_MOUNT"
    log "Settings: auto_restart=$ENABLE_AUTO_RESTART restart_after=$RESTART_AFTER_FAILURES max_attempts=$MAX_RESTART_ATTEMPTS reset_after=${RESET_AFTER_MINUTES}min max_notifications=$MAX_NOTIFICATIONS docker_restart=$ENABLE_DOCKER_RESTART"

    local consecutive_failures=0
    local last_notification=0
    # Per-outage notification cap — resets when the system recovers.
    local notifications_sent=0
    # Auto-restart bookkeeping — restart_attempts decays after RESET_AFTER_MINUTES of health.
    local restart_attempts=0
    local last_restart_ts=0
    local gave_up_notified=0

    while true; do
        # Check if RARGate process is running
        rargate_pid=$(pgrep -f "rargate --config")

        # Check if mounts are healthy
        rargate_mounted=false
        backend_mounted=false

        # We check BOTH existence AND responsiveness of FUSE mounts.
        # A FUSE mount can technically "exist" (shows up in mountpoint -q) but
        # be completely hung/dead if the backing process crashed. The "timeout ls"
        # catches this — if ls hangs for 5 seconds, the mount is unresponsive.
        if mountpoint -q "$RARGATE_MOUNT" 2>/dev/null; then
            if timeout 5 ls "$RARGATE_MOUNT" >/dev/null 2>&1; then
                rargate_mounted=true
            fi
        fi

        if mountpoint -q "$BACKEND_MOUNT" 2>/dev/null; then
            if timeout 5 ls "$BACKEND_MOUNT" >/dev/null 2>&1; then
                backend_mounted=true
            fi
        fi

        # Analyze health
        healthy=true
        failure_reason=""

        if [ -z "$rargate_pid" ]; then
            healthy=false
            failure_reason="RARGate process not found"
        elif [ "$rargate_mounted" = false ]; then
            healthy=false
            failure_reason="RARGate mount not responsive ($RARGATE_MOUNT)"
        elif [ "$backend_mounted" = false ]; then
            healthy=false
            failure_reason="rar2fs backend mount not responsive ($BACKEND_MOUNT)"
        fi

        # Handle failures
        if [ "$healthy" = false ]; then
            consecutive_failures=$((consecutive_failures + 1))
            log "FAILURE DETECTED ($consecutive_failures): $failure_reason"

            # Log crash details on first failure
            if [ $consecutive_failures -eq 1 ]; then
                log_crash "$failure_reason" "Process: ${rargate_pid:-NOT RUNNING}, RARGate mount: $rargate_mounted, Backend mount: $backend_mounted"
            fi

            # Three safeguards against alert spam:
            #   1. Require 2 consecutive failures before alerting (filters out momentary blips)
            #   2. Throttle to one notification every 5 minutes
            #   3. Cap total notifications per outage at MAX_NOTIFICATIONS (from config.yaml)
            current_time=$(date +%s)
            time_since_last=$((current_time - last_notification))

            if [ $consecutive_failures -ge 2 ] \
               && [ $time_since_last -ge 300 ] \
               && [ $notifications_sent -lt $MAX_NOTIFICATIONS ]; then
                send_notification \
                    "RARGate Crash Detected" \
                    "$(cat <<EOF
RARGate has crashed or become unresponsive!

Failure: $failure_reason
Time: $(date '+%Y-%m-%d %H:%M:%S')

Process Status:
- RARGate PID: ${rargate_pid:-NOT RUNNING}
- RARGate mount ($RARGATE_MOUNT): ${rargate_mounted}
- Backend mount ($BACKEND_MOUNT): ${backend_mounted}

Notification $((notifications_sent + 1)) of $MAX_NOTIFICATIONS for this outage.

Action Required:
Check crash log: tail -f $CRASH_LOG
Check main log: tail -f $RARGATE_LOG
Restart: run userscripts-startup.sh from your deploy directory
EOF
)" \
                    "alert"

                last_notification=$current_time
                notifications_sent=$((notifications_sent + 1))
                if [ $notifications_sent -ge $MAX_NOTIFICATIONS ]; then
                    log "Notification cap reached ($MAX_NOTIFICATIONS); suppressing further alerts until recovery"
                fi
            fi

            # Auto-restart: when the failure persists, run the standard
            # shutdown/startup sequence inline. Capped by MAX_RESTART_ATTEMPTS
            # to prevent infinite loops if the failure is something structural.
            if [ "$ENABLE_AUTO_RESTART" = "1" ] \
               && [ $consecutive_failures -ge $RESTART_AFTER_FAILURES ] \
               && [ $restart_attempts -lt $MAX_RESTART_ATTEMPTS ]; then
                restart_attempts=$((restart_attempts + 1))
                log "AUTO-RESTART: attempt $restart_attempts/$MAX_RESTART_ATTEMPTS — $failure_reason"
                send_notification \
                    "RARGate Auto-Restart Attempt $restart_attempts/$MAX_RESTART_ATTEMPTS" \
                    "$failure_reason — running shutdown/startup scripts. If this is the $MAX_RESTART_ATTEMPTS attempt, no further auto-restarts will be tried until the system stays healthy for $RESET_AFTER_MINUTES minutes." \
                    "warning"

                # Signal the existing shutdown script that this is intentional
                touch "/var/run/rargate-intentional-shutdown-auto.flag" 2>/dev/null || \
                    touch "/tmp/rargate-intentional-shutdown-auto.flag"

                # Run the recovery sequence inline, each step wrapped in `timeout`
                # so a hung child can't deadlock the whole monitor loop. Docker steps
                # are gated by ENABLE_DOCKER_RESTART so non-Docker users can opt out.
                if [ "$ENABLE_DOCKER_RESTART" = "1" ]; then
                    timeout "$AUTORESTART_DOCKER_TIMEOUT" bash -c "$DOCKER_STOP_CMD" 2>&1 | tee -a "$LOG_FILE" || log "docker stop step exited non-zero (or timed out)"
                else
                    log "Skipping docker stop (ENABLE_DOCKER_RESTART=0)"
                fi
                timeout "$AUTORESTART_SHUTDOWN_TIMEOUT" "$(dirname "$0")/userscripts-shutdown.sh" 2>&1 | tee -a "$LOG_FILE" || log "shutdown script exited non-zero (or timed out)"
                timeout "$AUTORESTART_STARTUP_TIMEOUT"  "$(dirname "$0")/userscripts-startup.sh"  2>&1 | tee -a "$LOG_FILE" || log "startup script exited non-zero (or timed out)"
                if [ "$ENABLE_DOCKER_RESTART" = "1" ]; then
                    timeout "$AUTORESTART_DOCKER_TIMEOUT" bash -c "$DOCKER_START_CMD" 2>&1 | tee -a "$LOG_FILE" || log "docker start step exited non-zero (or timed out)"
                else
                    log "Skipping docker start (ENABLE_DOCKER_RESTART=0)"
                fi

                rm -f "/var/run/rargate-intentional-shutdown-auto.flag" "/tmp/rargate-intentional-shutdown-auto.flag"

                last_restart_ts=$(date +%s)

                # Verify the restart actually worked before resetting counters.
                # If the backend mount didn't come back, the failure isn't
                # resolved and we want `restart_attempts` to keep climbing
                # toward `MAX_RESTART_ATTEMPTS`.
                sleep 5
                if mountpoint -q "$BACKEND_MOUNT" 2>/dev/null \
                   && timeout 5 ls "$BACKEND_MOUNT" >/dev/null 2>&1; then
                    consecutive_failures=0
                    notifications_sent=0
                    log "AUTO-RESTART: verified healthy (attempt $restart_attempts/$MAX_RESTART_ATTEMPTS)"
                else
                    log "AUTO-RESTART: sequence ran but backend still not responsive — counters NOT reset (attempt $restart_attempts/$MAX_RESTART_ATTEMPTS)"
                fi
            elif [ "$ENABLE_AUTO_RESTART" = "1" ] \
                 && [ $restart_attempts -ge $MAX_RESTART_ATTEMPTS ] \
                 && [ $gave_up_notified -eq 0 ]; then
                send_notification \
                    "RARGate Auto-Restart Gave Up" \
                    "Auto-restart has reached MAX_RESTART_ATTEMPTS=$MAX_RESTART_ATTEMPTS without recovering. Manual intervention required. Monitor will keep watching but will not restart again until the system stays healthy for $RESET_AFTER_MINUTES minutes." \
                    "alert"
                gave_up_notified=1
                log "AUTO-RESTART: gave up after $MAX_RESTART_ATTEMPTS attempts — manual intervention required"
            fi
        else
            # Reset failure counter on success
            if [ $consecutive_failures -gt 0 ]; then
                log "RARGate recovered after $consecutive_failures failures"
                consecutive_failures=0
                notifications_sent=0
            fi

            # Decay the restart-attempt counter if we've been healthy for
            # RESET_AFTER_MINUTES — fresh outages get fresh retries.
            if [ $restart_attempts -gt 0 ] && [ $last_restart_ts -gt 0 ]; then
                current_time=$(date +%s)
                if [ $((current_time - last_restart_ts)) -ge $((RESET_AFTER_MINUTES * 60)) ]; then
                    log "Stable for ${RESET_AFTER_MINUTES} min — resetting restart attempt counter ($restart_attempts → 0)"
                    restart_attempts=0
                    gave_up_notified=0
                fi
            fi
        fi

        sleep $CHECK_INTERVAL
    done
}

### --- Command Handlers ---
cmd_start() {
    if is_monitor_running; then
        echo "❌ Monitor already running (PID: $(cat $PID_FILE))"
        exit 1
    fi

    echo "Starting RARGate crash monitor..."

    # Re-invoke this same script with the "_daemon" argument to run the
    # monitor loop in the background. nohup ensures it survives if the
    # parent shell exits (e.g., when UserScripts finishes the startup script).
    nohup "$0" _daemon >/dev/null 2>&1 &
    echo $! > "$PID_FILE"

    sleep 1

    if is_monitor_running; then
        echo "✅ Monitor started successfully (PID: $(cat $PID_FILE))"
        echo "   Checking every ${CHECK_INTERVAL}s"
        echo "   Log: $LOG_FILE"
    else
        echo "❌ Failed to start monitor"
        exit 1
    fi
}

cmd_stop() {
    if ! is_monitor_running; then
        echo "Monitor not running"
        exit 0
    fi

    local pid=$(cat "$PID_FILE")
    echo "Stopping RARGate monitor (PID: $pid)..."

    kill "$pid" 2>/dev/null
    sleep 2

    if kill -0 "$pid" 2>/dev/null; then
        echo "⚠️  Forcing kill..."
        kill -9 "$pid" 2>/dev/null
    fi

    rm -f "$PID_FILE"
    echo "✅ Monitor stopped"
    log "Monitor stopped by user"
}

cmd_status() {
    if is_monitor_running; then
        local pid=$(cat "$PID_FILE")
        echo "✅ Monitor is running (PID: $pid)"
        echo "   Check interval: ${CHECK_INTERVAL}s"
        echo "   Log file: $LOG_FILE"
        echo ""
        echo "Recent log entries:"
        tail -10 "$LOG_FILE" 2>/dev/null || echo "   (no logs yet)"
    else
        echo "❌ Monitor is not running"
    fi
}

### --- Main ---
case "${1:-}" in
    start)
        cmd_start
        ;;
    stop)
        cmd_stop
        ;;
    status)
        cmd_status
        ;;
    _daemon)
        # Internal command — not meant to be called directly.
        # The "start" command re-invokes this script with "_daemon" to run
        # the monitor loop in a detached background process (see cmd_start).
        monitor_loop
        ;;
    *)
        echo "RARGate External Crash Monitor"
        echo ""
        echo "Usage: $0 {start|stop|status}"
        echo ""
        echo "Commands:"
        echo "  start   - Start monitoring for crashes"
        echo "  stop    - Stop monitoring"
        echo "  status  - Check monitor status"
        echo ""
        echo "The monitor runs independently of RARGate and detects:"
        echo "  • RARGate process crashes"
        echo "  • Unresponsive FUSE mounts"
        echo "  • Backend mount failures"
        echo ""
        exit 1
        ;;
esac

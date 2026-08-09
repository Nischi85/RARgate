#!/bin/bash
#
# RARGate Installation Script for Unraid
#
# Called by userscripts-startup.sh — not intended to be run directly.
# Installs the RARGate binary and config file, after checking that
# all required dependencies are available.
#

set -uo pipefail   # no -e: tolerates non-zero from optional pre-flight checks

if [ -z "${BASH_VERSINFO[0]:-}" ] || [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
    echo "ERROR: install-rargate.sh requires bash 4 or newer" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY_NAME="rargate"
RARGATE_MOUNT="/mnt/user/rargate"
RARGATE_CONFIG="/etc/rargate/config.yaml"
# shellcheck disable=SC1091
source /var/run/rargate-paths.conf 2>/dev/null || true
# RARGATE_MOUNT is overridden by the conf if present.

echo "🔧 Installing RARGate..."
echo ""

### --- Check prerequisites ---
echo "📋 Checking prerequisites..."

# rar2fs extracts files from RAR archives on the fly — without it,
# RARGate can't show extracted content to media servers like Plex/Emby.
if ! command -v rar2fs &> /dev/null; then
    echo "❌ ERROR: rar2fs not found!"
    echo "   RARGate requires rar2fs for RAR extraction"
    echo "   Install: apt-get install rar2fs (in LXC) or use static binary"
    exit 1
fi
echo "   ✅ rar2fs: $(which rar2fs)"

# FUSE (Filesystem in Userspace) lets RARGate present itself as a
# regular folder. Both RARGate and rar2fs need it to work.
if ! lsmod | grep -q fuse; then
    echo "   ℹ️  FUSE kernel module not loaded, loading..."
    if modprobe fuse; then
        echo "   ✅ FUSE: kernel module loaded"
    else
        echo "❌ ERROR: Failed to load FUSE kernel module"
        echo "   Run: modprobe fuse"
        exit 1
    fi
else
    echo "   ✅ FUSE: kernel module already loaded"
fi

# user_allow_other lets FUSE mounts be visible to other users (like the
# Plex user). Without this, only root could see the RARGate mount.
if [ -f /etc/fuse.conf ]; then
    if ! grep -q "^user_allow_other" /etc/fuse.conf; then
        echo "⚠️  WARNING: user_allow_other not enabled in /etc/fuse.conf"
        echo "   This may cause permission issues with media servers"
    else
        echo "   ✅ FUSE: user_allow_other enabled"
    fi
fi

# rar2fs expects the old "fusermount" command, but newer systems only
# have "fusermount3". This symlink bridges the gap.
if [ ! -e /usr/bin/fusermount ] && [ -e /usr/bin/fusermount3 ]; then
    echo "   ℹ️  Creating fusermount → fusermount3 symlink..."
    ln -sf /usr/bin/fusermount3 /usr/bin/fusermount
    echo "   ✅ fusermount symlink created"
elif [ -e /usr/bin/fusermount ]; then
    echo "   ✅ fusermount available"
fi

echo ""
echo "📦 Installing binary..."

# Check source binary exists
if [ ! -f "$SCRIPT_DIR/bin/$BINARY_NAME" ]; then
    echo "❌ ERROR: Binary not found: $SCRIPT_DIR/bin/$BINARY_NAME"
    exit 1
fi

# If the binary is currently running (e.g., from a previous startup that
# didn't shut down cleanly), we need to stop it first. You can't overwrite
# a binary while Linux is executing it — the copy would fail. So we unmount
# and kill the old process before copying the new binary into place.
if [ -f "/usr/local/bin/rargate" ]; then
    echo "   ℹ️  Overwriting existing binary with latest from deploy"

    if lsof "/usr/local/bin/rargate" &> /dev/null || fuser "/usr/local/bin/rargate" &> /dev/null 2>&1; then
        echo "   ⚠️  Binary is in use, stopping RARGate to update..."

        # RARGATE_MOUNT is set in the Configuration section at top of script

        # Try graceful unmount first
        fusermount3 -u "$RARGATE_MOUNT" 2>/dev/null || fusermount -u "$RARGATE_MOUNT" 2>/dev/null || true
        sleep 2

        # Force unmount if still mounted
        if mount | grep -q "$RARGATE_MOUNT"; then
            echo "   ⚠️  Forcing unmount..."
            umount -fl "$RARGATE_MOUNT" 2>/dev/null || true
            sleep 1
        fi

        # Kill any lingering processes. Anchored to this exact invocation
        # (binary + --config path), not just the binary path -- if more than
        # one rargate process is ever running from a different --config (the
        # same binary, just pointed at a different config file), a bare
        # "^/usr/local/bin/rargate" prefix match would kill all of them, not
        # just the one actually being replaced.
        pkill -9 -f "^/usr/local/bin/rargate --config ${RARGATE_CONFIG}\$" 2>/dev/null || true
        sleep 1
    fi
fi

# Copy the binary (should succeed now that we stopped everything above)
cp "$SCRIPT_DIR/bin/$BINARY_NAME" "/usr/local/bin/rargate" || {
    echo "   ❌ ERROR: Failed to copy binary"
    echo "   Check permissions and that $SCRIPT_DIR/bin/$BINARY_NAME exists"
    exit 1
}

chmod +x "/usr/local/bin/rargate"
echo "   ✅ Binary: /usr/local/bin/rargate ($(ls -lh /usr/local/bin/rargate | awk '{print $5}'))"

echo ""
echo "📝 Installing config..."

mkdir -p "/etc/rargate"

if [ -f "/etc/rargate/config.yaml" ]; then
    echo "   ℹ️  Overwriting existing config with latest from deploy"
else
    echo "   ℹ️  Installing fresh config from deploy"
fi

if [ -f "$SCRIPT_DIR/config.yaml" ]; then
    cp "$SCRIPT_DIR/config.yaml" "/etc/rargate/config.yaml" || {
        echo "   ❌ ERROR: Failed to copy config"
        exit 1
    }
    echo "   ✅ Config: /etc/rargate/config.yaml"
else
    echo "   ❌ ERROR: Config not found at $SCRIPT_DIR/config.yaml"
    exit 1
fi

echo ""
echo "✅ Installation complete"
echo "   Binary: /usr/local/bin/rargate"
echo "   Config: /etc/rargate/config.yaml"
echo ""

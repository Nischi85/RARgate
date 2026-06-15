# RARGate

**High-Performance FUSE Filesystem with RAR Archive Support and Intelligent Filtering**

Author: Nischi

## Overview

RARGate is a FUSE filesystem for media servers that provides transparent RAR archive extraction, intelligent file filtering, SFV validation, and automatic media server notifications. It is built on top of [rar2fs](https://github.com/hasse69/rar2fs) by hasse69, which handles the actual RAR extraction. Without rar2fs, RARGate cannot function — it is a required dependency that does the heavy lifting of reading RAR archives as regular files via FUSE.

RARGate adds a filtering and orchestration layer on top of rar2fs: hiding junk files, validating SFV checksums, managing the rar2fs lifecycle, and notifying media servers (Emby/Jellyfin/Plex) when content changes.

**Platforms:** any Linux distribution with FUSE. The `deploy/` folder ships ready-to-use scripts for Unraid (UserScripts), but RARGate is not Unraid-specific — on other distros, run it as a systemd service or under any process supervisor. See [Deployment](#deployment) for both flavours.

## Key Features

### Core
- **Transparent RAR extraction** via [rar2fs](https://github.com/hasse69/rar2fs) backend
- **File filtering** — hide .nfo, .sfv, .exe, sample dirs, etc. with configurable patterns
- **SFV validation** — strict mode (require valid checksums) or permissive mode (show everything)
- **Write support** — create/modify/delete regular files (configurable, RAR content always protected)
- **Overlay/UnionFS support** — automatic layer detection for verified vs unverified content

### Media Server Integration
- **Emby, Jellyfin, and Plex** — targeted library refresh when files change
- **Path mapping** — translates host paths to Docker container paths
- **Debouncing** — batches rapid file changes into efficient API calls
- **Item caching** — reduces API load with configurable TTL

### Monitoring
- **inotify file watcher** — instant cache invalidation when files appear or change
- **External crash monitor** — detects process death and unresponsive mounts. Sends native Unraid notifications when running on Unraid; on other distros the notify call is automatically skipped and the alert is recorded in the monitor's log file.
- **Health checks** — periodic mount and process verification

### Performance
- **Async I/O** with Tokio runtime and reqwest HTTP client
- **Parallel processing** with Rayon work-stealing
- **Smart caching** — configurable FUSE attribute/entry timeouts with inotify invalidation
- **Configurable rar2fs throttling** — limits concurrent reads to prevent backend overload

## Architecture

```
[Media files + RAR archives]
        |
   [rar2fs backend]         ← RAR extraction (github.com/hasse69/rar2fs)
        |
   [RARGate FUSE layer]     ← Filtering, SFV validation, write support
        |
   [Clean mount point]      ← What Emby/Jellyfin/Plex sees
        |
   [Media server API]       ← Automatic targeted library refresh
```

## Installation

### Prerequisites

**rar2fs** (required) — RARGate depends on rar2fs for all RAR archive handling:

```bash
# Ubuntu/Debian
apt-get update && apt-get install -y rar2fs

# Verify
rar2fs --version
```

See the [rar2fs GitHub page](https://github.com/hasse69/rar2fs) for other platforms and build instructions.

**FUSE kernel module** (required):

```bash
modprobe fuse
echo "user_allow_other" >> /etc/fuse.conf
```

### Quick Start

1. Place the binary and config:
   - Binary: `/usr/local/bin/rargate`
   - Config: `/etc/rargate/config.yaml` (see [`deploy/config.yaml.example`](deploy/config.yaml.example))

2. Edit the config — at minimum set `source` and `mountpoint`

3. Run:
   ```bash
   rargate --config /etc/rargate/config.yaml
   ```

## Deployment

The Quick Start above is enough on any Linux box. The two flavours below show how to wire RARGate into the host's normal service lifecycle.

### Generic Linux (systemd)

Drop a unit file at `/etc/systemd/system/rargate.service`:

```ini
[Unit]
Description=RARGate FUSE filesystem
After=network.target local-fs.target
Requires=local-fs.target

[Service]
Type=simple
ExecStart=/usr/local/bin/rargate --config /etc/rargate/config.yaml --foreground
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Then:

```bash
systemctl daemon-reload
systemctl enable --now rargate
journalctl -u rargate -f      # tail logs
```

The `rargate-monitor.sh` script in [`deploy/`](deploy/) also runs on any Linux: it auto-detects the absence of the Unraid `notify` helper and writes alerts to its own log file instead. Docker stop/start commands are overridable via the `DOCKER_STOP_CMD` / `DOCKER_START_CMD` environment variables if you don't use Unraid's `/etc/rc.d/rc.docker`.

### Unraid (UserScripts)

The [`deploy/`](deploy/) folder contains ready-to-use scripts for Unraid's UserScripts plugin:

| Script | Schedule | What it does |
|---|---|---|
| `userscripts-startup.sh` | At Startup of Array | Mounts overlay, installs binary + config, starts RARGate, starts crash monitor |
| `userscripts-shutdown.sh` | At Stopping of Array | Stops monitor, unmounts RARGate + rar2fs + overlay |
| `install-rargate.sh` | Called by startup | Checks prerequisites (rar2fs, FUSE), copies binary + config |
| `rargate-monitor.sh` | Background daemon | Monitors process health and mount responsiveness; sends native Unraid notifications on crash (skipped automatically on non-Unraid hosts) |
| `config.yaml.example` | Reference | Example configuration with all options documented |

## Configuration

See [`deploy/config.yaml.example`](deploy/config.yaml.example) for a complete example with inline documentation.

### Key Sections

**Paths** (generic Linux example; Unraid users typically substitute `/mnt/user/...`):
```yaml
source: /srv/media/overlay_merged    # Your media files
mountpoint: /srv/rargate             # Where filtered view appears
```

**rar2fs Backend:**
```yaml
rar2fs:
  binary_path: /usr/local/bin/rar2fs
  backend_mount: /srv/rar2fs-backend
  max_concurrent_reads: 4
  extra_options: "--seek-length=1 -o allow_other ..."
```

**SFV Validation:**
```yaml
sfv_validation:
  enabled: true
  default_mode: strict       # strict = require .sfv, permissive = show all
  lazy_mode: true            # Validate on access, not upfront
  validation_backoff:
    max_failures: 3
    cooldown_seconds: 300    # Back off after repeated failures
```

**File Filtering:**
```yaml
filters:
  exclude_dirs: [sample, proof, subs]
  exclude_files: ["*.nfo", "*.sfv", "*.exe"]
  rar_archives:
    enabled: true
    hide_archives: true      # Hide .rar/.r00, show extracted content
```

**Media Server Integration** (Emby/Jellyfin/Plex):
```yaml
emby:
  enabled: true
  url: "http://YOUR_EMBY_IP:8096"
  api_token: "YOUR_API_TOKEN"
  path_mapping:
    host_paths: ["/srv/media/verified", "/srv/media/unverified"]
    emby_path: "/share"
```

## Usage

```bash
rargate --version                                          # Show version
rargate --help                                             # Show help
rargate --config /etc/rargate/config.yaml                  # Run as daemon
rargate --config /etc/rargate/config.yaml --foreground     # Run in foreground
```

## Troubleshooting

```bash
# Check if running
mount | grep rargate
ps aux | grep rargate

# View logs (path depends on your config)
tail -f /var/log/rargate.log
```

**"rar2fs not found"** — Install rar2fs: `apt-get install rar2fs` or build from [source](https://github.com/hasse69/rar2fs)

**"FUSE kernel module not loaded"** — Run: `modprobe fuse`

**"Permission denied" on mount** — Add `user_allow_other` to `/etc/fuse.conf`

**Media server not updating** — Check `path_mapping` matches your Docker paths, verify API token, review logs

## Related

- [dc-bridge](https://github.com/Nischi85/dc-bridge) — optional companion that
  fetches scene releases (via AirDC++/Sonarr/Radarr) into the layout RARGate
  serves. Not required; RARGate mounts RAR releases from any source.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for the release history.

## License

MIT License — see [LICENSE](LICENSE)

## Credits

- **Author**: Nischi
- **Development**: Claude Code
- **RAR extraction**: [rar2fs](https://github.com/hasse69/rar2fs) by hasse69 — the essential backend that makes RARGate possible

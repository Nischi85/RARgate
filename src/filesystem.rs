//! `RarGateFs` — the FUSE filesystem that fronts the rar2fs backend.
//!
//! Each FUSE op (`lookup`, `getattr`, `readdir`, `read`, …) is implemented on the
//! `Filesystem` trait. The filesystem holds two roots: the **backend** (where rar2fs
//! mounts extracted archive contents) and the **source** (the raw overlay layer where
//! `.sfv` files live). Reads are gated through `FilterEngine` so that incomplete or
//! corrupt archives are hidden until validation passes.
//!
//! Caches: inode-to-path map, validated-directory LRU. Hot-reload is supported via the
//! handle returned by `RarGateFs::new` (driven by SIGHUP in `main.rs`).

use anyhow::Result;
use fuser::{Filesystem, Request, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyCreate, ReplyOpen, ReplyWrite, FileAttr, FileType};
use lru::LruCache;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH, Instant};
use tracing::{debug, info, warn};

use std::sync::Arc;

use crate::backend::RarFsLimiter;
use crate::config::Config;
use crate::filtering::{FilterEngine, FilterReloadHandle};
use crate::metrics::MetricsCollector;

/// Default maximum number of entries in the inode LRU cache
const DEFAULT_INODE_CACHE_SIZE: usize = 100_000;

/// Default UID/GID for files/directories (nobody:users on unRAID)
const DEFAULT_UID: u32 = 99;
const DEFAULT_GID: u32 = 100;

/// Crash-guard: a source directory untouched for at least this long is assumed a
/// settled (complete) download and skips the `.sfv` completeness scan. An in-flight
/// download keeps bumping its dir mtime, so the active set always falls inside this
/// window; everything older is fast-pathed.
const GUARD_STABLE_SECS: u64 = 120;

/// Return the first `.sfv` file directly inside `dir` (source side), if any.
fn find_sfv_in(dir: &Path) -> Option<PathBuf> {
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let p = entry.path();
        if crate::sfv::is_sfv_file(&p) {
            return Some(p);
        }
    }
    None
}

/// POSIX `struct stat::st_blocks` unit — file size is reported in 512-byte chunks
/// regardless of the actual filesystem block size.
const STAT_BLOCK_SIZE: u64 = 512;

/// Preferred I/O block size reported to FUSE callers. 4 KiB matches the default Linux
/// page size and is what userspace tooling (`stat`, `ls -s`) generally expects.
const FS_BLOCK_SIZE: u32 = 4096;

pub struct RarGateFs {
    backend_path: PathBuf,
    source_path: PathBuf,  // Original source path to detect RAR content
    filter_engine: FilterEngine,
    inode_map: LruCache<u64, PathBuf>,
    path_to_inode: HashMap<PathBuf, u64>,  // Reverse map for O(1) lookups
    next_inode: u64,
    default_permissions: u16,  // Default permissions for files/dirs (e.g., 0o777)
    attr_timeout: Duration,     // FUSE attribute cache timeout
    entry_timeout: Duration,    // FUSE entry cache timeout
    /// Counter for LRU evictions (for monitoring)
    eviction_count: u64,
    /// Open file handles. Holding `std::fs::File` here keeps rar2fs's underlying fd
    /// alive across many `read()` calls — without this, every FUSE read would
    /// re-open the backend file, which destabilises rar2fs under burst load.
    open_files: HashMap<u64, std::fs::File>,
    next_fh: u64,
    /// Caps concurrent rar2fs directory walks shared with the FilterEngine.
    rar2fs_limiter: Arc<RarFsLimiter>,
}

impl RarGateFs {
    pub fn new(backend_path: PathBuf, source_path: PathBuf, config: Config, rar2fs_limiter: Arc<RarFsLimiter>) -> Result<(Self, FilterReloadHandle)> {
        let (filter_engine, reload_handle) = FilterEngine::new(backend_path.clone(), config.clone(), Arc::clone(&rar2fs_limiter))?;

        // Create LRU cache for inode mappings with configurable size
        let cache_size = NonZeroUsize::new(DEFAULT_INODE_CACHE_SIZE)
            .expect("DEFAULT_INODE_CACHE_SIZE must be non-zero");
        let mut inode_map = LruCache::new(cache_size);
        let mut path_to_inode = HashMap::new();

        // Insert root inode
        inode_map.put(1, PathBuf::from("/"));
        path_to_inode.insert(PathBuf::from("/"), 1);

        info!("Inode LRU cache initialized with capacity: {}", DEFAULT_INODE_CACHE_SIZE);

        // Get permissions from config, default to 0o777 for full access
        let default_permissions = config.fuse_options
            .as_ref()
            .and_then(|opts| opts.permissions)
            .unwrap_or(0o777);

        // Get FUSE cache timeouts from config (default: 60 seconds)
        let attr_timeout = Duration::from_secs_f64(
            config.fuse_options
                .as_ref()
                .and_then(|opts| opts.attr_timeout)
                .unwrap_or(60.0)
        );

        let entry_timeout = Duration::from_secs_f64(
            config.fuse_options
                .as_ref()
                .and_then(|opts| opts.entry_timeout)
                .unwrap_or(60.0)
        );

        Ok((Self {
            backend_path,
            source_path,
            filter_engine,
            inode_map,
            path_to_inode,
            next_inode: 2,
            default_permissions,
            attr_timeout,
            entry_timeout,
            eviction_count: 0,
            open_files: HashMap::new(),
            next_fh: 1,
            rar2fs_limiter,
        }, reload_handle))
    }

    /// Get cache handle for invalidation (called before moving into FUSE mount)
    pub fn get_cache_handle(&self) -> std::sync::Arc<dashmap::DashMap<PathBuf, crate::filtering::DirectoryInfo>> {
        self.filter_engine.get_cache_handle()
    }

    /// Attach a metrics collector (call before mounting).
    pub fn set_metrics(&mut self, metrics: std::sync::Arc<MetricsCollector>) {
        self.filter_engine.set_metrics(metrics);
    }

    fn backend_path(&self, path: &str) -> PathBuf {
        let path = path.strip_prefix('/').unwrap_or(path);
        self.backend_path.join(path)
    }

    fn source_path(&self, path: &str) -> PathBuf {
        let path = path.strip_prefix('/').unwrap_or(path);
        self.source_path.join(path)
    }

    /// Crash guard: is the directory at `relative_path` a still-downloading RAR set?
    /// Decided purely from the SOURCE overlay (`.sfv` presence + completeness) — it
    /// NEVER touches the rar2fs backend, so calling it can't make rar2fs parse a
    /// partial archive. Callers use it to avoid issuing any backend stat/readdir for
    /// an incomplete set (rar2fs SIGSEGVs computing a bogus buffer from partial volume
    /// headers). Settled directories are fast-pathed by mtime so the stable library
    /// pays only a single `stat` per entry.
    fn source_set_incomplete(&self, relative_path: &Path) -> bool {
        let source_dir = self.source_path(&relative_path.to_string_lossy());
        if let Ok(md) = std::fs::metadata(&source_dir) {
            if let Ok(modified) = md.modified() {
                if let Ok(elapsed) = modified.elapsed() {
                    if elapsed >= Duration::from_secs(GUARD_STABLE_SECS) {
                        return false;
                    }
                }
            }
        }
        match find_sfv_in(&source_dir) {
            Some(sfv) => !crate::sfv::validate_sfv_with_completeness(&sfv),
            None => false,
        }
    }

    /// Build FileAttr from metadata - reduces code duplication
    /// Use `perm_override` to set specific permissions (e.g., from mkdir mode), or None for defaults.
    fn build_file_attr(&self, ino: u64, metadata: &std::fs::Metadata, kind: FileType, perm_override: Option<u16>) -> FileAttr {
        FileAttr {
            ino,
            size: metadata.len(),
            blocks: metadata.len().div_ceil(STAT_BLOCK_SIZE),
            atime: metadata.accessed().unwrap_or(UNIX_EPOCH),
            mtime: metadata.modified().unwrap_or(UNIX_EPOCH),
            ctime: metadata.modified().unwrap_or(UNIX_EPOCH),
            crtime: metadata.modified().unwrap_or(UNIX_EPOCH),
            kind,
            perm: perm_override.unwrap_or(self.default_permissions),
            nlink: if kind == FileType::Directory { 2 } else { 1 },
            uid: DEFAULT_UID,
            gid: DEFAULT_GID,
            rdev: 0,
            flags: 0,
            blksize: FS_BLOCK_SIZE,
        }
    }

    fn get_path_from_inode(&self, ino: u64) -> Option<&PathBuf> {
        // Use peek() to avoid updating LRU order (allows &self instead of &mut self)
        self.inode_map.peek(&ino)
    }

    fn get_or_create_inode(&mut self, path: PathBuf) -> u64 {
        // O(1) lookup using reverse map instead of O(n) linear search
        if let Some(&ino) = self.path_to_inode.get(&path) {
            // Touch the entry in LRU to mark it as recently used
            let _ = self.inode_map.get(&ino);
            return ino;
        }

        let ino = self.next_inode;
        self.next_inode += 1;

        // Insert into LRU cache and handle potential eviction
        if let Some((evicted_ino, evicted_path)) = self.inode_map.push(ino, path.clone()) {
            // Clean up the reverse map entry for the evicted item
            self.path_to_inode.remove(&evicted_path);
            self.eviction_count += 1;

            // Log eviction periodically (every 1000 evictions)
            if self.eviction_count.is_multiple_of(1000) {
                debug!("Inode LRU cache eviction: {} evictions so far (latest: ino={} path={})",
                       self.eviction_count, evicted_ino, evicted_path.display());
            }
        }

        self.path_to_inode.insert(path, ino);
        ino
    }

    /// Check if deletion is allowed for a file/directory.
    /// Returns Ok(source_path) if allowed, Err(errno) if blocked.
    fn check_delete_allowed(&self, parent: u64, name: &OsStr) -> Result<PathBuf, i32> {
        if self.filter_engine.read_only_enabled() {
            warn!("Blocked deletion (read_only enabled): {:?}", name);
            return Err(libc::EROFS);
        }

        if self.filter_engine.no_delete_enabled() {
            warn!("Blocked deletion (no_delete enabled): {:?}", name);
            return Err(libc::EPERM);
        }

        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => return Err(libc::ENOENT),
        };

        let child_path = parent_path.join(name);

        if self.is_extracted_media(&child_path.to_string_lossy()) {
            warn!("Blocked deletion of RAR-extracted media: {:?}", child_path);
            return Err(libc::EROFS);
        }

        Ok(self.source_path(&child_path.to_string_lossy()))
    }

    /// Check if a path contains RAR-extracted content (read-only protection)
    /// Returns true if the directory contains .rar files (indicating rar2fs extraction)
    fn is_rar_extracted_content(&self, path: &str) -> bool {
        // IMPORTANT: This function determines if a file/directory is RAR-extracted content
        // that should be protected from modification.
        //
        // Strategy:
        // 1. Check if the file exists in the rar2fs backend BUT NOT in source
        // 2. If yes, it's RAR-extracted content (read-only)
        // 3. If it exists in both or only in source, it's a regular file (writable)
        //
        // This allows:
        // - Adding subtitles to movie directories containing RARs ✅
        // - Creating new folders in RAR directories ✅
        // - Modifying regular files alongside RAR content ✅
        //
        // While protecting:
        // - Files extracted from RAR archives (e.g., .mkv from .rar) ❌

        let backend_path = self.backend_path(path);
        let source_path = self.source_path(path);

        // Optimization: Check source first. If it exists, we know it's NOT RAR-extracted.
        // Only check backend if source doesn't exist (saves 1 stat call in common case).
        let source_exists = source_path.exists();
        if source_exists {
            debug!("RAR check for {}: source exists, not RAR-extracted", path);
            return false;
        }

        // Source doesn't exist - check if it exists in backend (RAR-extracted)
        let backend_exists = backend_path.exists();
        let is_rar_extracted = backend_exists;

        debug!(
            "RAR check for {}: backend={}, source=false, is_rar_extracted={}",
            path, backend_exists, is_rar_extracted
        );
        debug!("  backend_path: {:?}", backend_path);
        debug!("  source_path: {:?}", source_path);

        // If the file exists in backend but NOT in source, it's RAR-extracted
        // (rar2fs extracts .mkv from .rar files - the .mkv only exists in backend)
        // If it exists in both, it's a regular file that rar2fs is passing through
        is_rar_extracted
    }

    /// True if `path` is media extracted from a RAR: backend-only content with a media
    /// extension (the .mkv rar2fs produces). The SFV never lists the extracted media by
    /// name, so this is what protects it.
    fn is_extracted_media(&self, path: &str) -> bool {
        if !self.is_rar_extracted_content(path) {
            return false;
        }
        let basename = Path::new(path)
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        self.filter_engine.is_media_file(&basename)
    }

    /// True if the file's basename is listed in a `.sfv` in its own directory (source side).
    /// Release files the SFV names (the .rar/.r00 parts, plus any supplied file it lists)
    /// are protected; files NOT listed — e.g. a subtitle bazarr just added — are not.
    fn is_listed_in_sfv(&self, path: &str) -> bool {
        let p = Path::new(path);
        let (parent, basename) = match (p.parent(), p.file_name()) {
            (Some(par), Some(name)) => (par, name.to_string_lossy().to_lowercase()),
            _ => return false,
        };
        let source_parent = self.source_path(&parent.to_string_lossy());
        let sfv_path = match find_sfv_in(&source_parent) {
            Some(s) => s,
            None => return false,
        };
        let content = match std::fs::read(&sfv_path) {
            Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Err(_) => return false,
        };
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with(';') {
                continue;
            }
            // SFV format: "<filename> <CRC32>" — split on the last space.
            if let Some(sp) = line.rfind(' ') {
                if line[..sp].trim().to_lowercase() == basename {
                    return true;
                }
            }
        }
        false
    }

    /// Write-class protection (write / setattr / rename source). Blocks modifying or
    /// renaming RAR-extracted media and any file the accompanying `.sfv` lists as release
    /// content. New companion files not named in the SFV (subtitles, nfo, artwork) stay
    /// writable. Deletion is governed separately by `is_extracted_media` only.
    fn is_write_protected(&self, path: &str) -> bool {
        self.is_extracted_media(path) || self.is_listed_in_sfv(path)
    }
}

impl Filesystem for RarGateFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("lookup: parent={}, name={:?}", parent, name);
        
        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        
        let child_path = parent_path.join(name);
        let name_str = name.to_string_lossy();

        // Compute backend path for parent directory (for SFV validation)
        let parent_backend_path = self.backend_path(&parent_path.to_string_lossy());

        // Check if file should be shown based on filters
        if !self.filter_engine.should_show_file(&parent_backend_path, &name_str) {
            reply.error(libc::ENOENT);
            return;
        }

        // CRASH GUARD: hide still-downloading RAR sets so the kernel never descends
        // into them (a readdir/stat inside would make rar2fs parse partial volumes and
        // SIGSEGV). Decided from the source `.sfv`, never the backend.
        if self.source_set_incomplete(&child_path) {
            reply.error(libc::ENOENT);
            return;
        }

        let backend_path = self.backend_path(&child_path.to_string_lossy());

        match std::fs::metadata(&backend_path) {
            Ok(metadata) => {
                let ino = self.get_or_create_inode(child_path);
                let file_type = if metadata.is_dir() {
                    FileType::Directory
                } else if metadata.is_symlink() {
                    FileType::Symlink
                } else {
                    FileType::RegularFile
                };
                let attr = self.build_file_attr(ino, &metadata, file_type, None);
                reply.entry(&self.entry_timeout, &attr, 0);
            }
            Err(_) => reply.error(libc::ENOENT),
        }
    }
    
    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        debug!("getattr: ino={}", ino);
        
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        
        // CRASH GUARD: for a still-downloading set, stat the SOURCE rather than the
        // backend so rar2fs isn't asked to parse partial volumes. readdir/lookup
        // already hide these; this covers a cached inode being re-stat'd.
        if self.source_set_incomplete(&path) {
            let source_path = self.source_path(&path.to_string_lossy());
            match std::fs::metadata(&source_path) {
                Ok(metadata) => {
                    let file_type = if metadata.is_dir() {
                        FileType::Directory
                    } else if metadata.is_symlink() {
                        FileType::Symlink
                    } else {
                        FileType::RegularFile
                    };
                    let attr = self.build_file_attr(ino, &metadata, file_type, None);
                    reply.attr(&self.attr_timeout, &attr);
                }
                Err(_) => reply.error(libc::ENOENT),
            }
            return;
        }

        let backend_path = self.backend_path(&path.to_string_lossy());

        match std::fs::metadata(&backend_path) {
            Ok(metadata) => {
                let file_type = if metadata.is_dir() {
                    FileType::Directory
                } else if metadata.is_symlink() {
                    FileType::Symlink
                } else {
                    FileType::RegularFile
                };
                let attr = self.build_file_attr(ino, &metadata, file_type, None);
                reply.attr(&self.attr_timeout, &attr);
            }
            Err(_) => reply.error(libc::ENOENT),
        }
    }

    // readdir runs in three phases:
    //   1. Collect — `std::fs::read_dir` on the backend (rar2fs) path; entries are
    //      gathered into Vecs of (name, kind) tuples.
    //   2. Validate — `FilterEngine::should_show_directories_parallel` checks each
    //      candidate directory against its `.sfv` (read from the source overlay layer
    //      since SFV files don't appear in the backend).
    //   3. Reply — push every visible entry into the FUSE `ReplyDirectory`, allocating
    //      inodes for paths we haven't seen before.
    // Each phase records an `Instant`; the trailing `if total_time > 1s` block emits
    // a per-phase timing breakdown so slow listings can be triaged from logs alone.
    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        let start_time = Instant::now();
        debug!("readdir: ino={}, offset={}", ino, offset);

        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let backend_path = self.backend_path(&path.to_string_lossy());

        let after_path_resolve = Instant::now();

        let mut dir_entries = Vec::new();

        // BUG FIX: Always add . and .. entries to keep indices consistent across readdir() calls
        // If we only add them when offset==0, subsequent calls have different indices!
        // Example: Entry "The.Woman..." is at index 99 in first call (with . and ..)
        //          but at index 97 in second call (without . and ..)
        dir_entries.push((1, FileType::Directory, ".".to_string()));
        dir_entries.push((1, FileType::Directory, "..".to_string()));

        // **TIER 1: Parallel Validation**
        // Collect all directory names first, then validate in parallel
        // OPTIMIZATION: Pre-allocate capacity to avoid reallocations
        let mut all_entries = Vec::with_capacity(128); // Reasonable default for most directories
        let mut dir_names = Vec::with_capacity(32);    // Typically fewer subdirectories

        // Hold the rar2fs limiter permit across the full read+iterate window — the
        // iterator pulls entries lazily, each call costs a backend round-trip.
        let after_read_dir;
        {
            let _permit = self.rar2fs_limiter.acquire();
            let entries = match std::fs::read_dir(&backend_path) {
                Ok(entries) => entries,
                Err(_) => {
                    reply.error(libc::ENOENT);
                    return;
                }
            };
            after_read_dir = Instant::now();

            for entry in entries.flatten() {
                // OPTIMIZATION: Use Cow<str> to avoid allocation for valid UTF-8 filenames
                let name_osstr = entry.file_name();
                let name_cow = name_osstr.to_string_lossy();

                // CRASH GUARD: skip still-downloading RAR sets BEFORE touching the
                // backend. rar2fs gives no d_type, so `entry.file_type()` below
                // `stat`s each child against the backend; stat'ing an incomplete set
                // makes rar2fs parse partial volume headers and SIGSEGV. The check
                // reads only the source `.sfv`, never the backend.
                if self.source_set_incomplete(&path.join(&*name_cow)) {
                    continue;
                }

                // OPTIMIZATION: Use file_type() from DirEntry instead of is_dir()/is_symlink()
                // This avoids additional stat() syscalls - file_type() uses data from readdir()
                let file_type = match entry.file_type() {
                    Ok(ft) => ft,
                    Err(_) => continue, // Skip entries we can't stat
                };
                let is_dir = file_type.is_dir();
                let is_symlink = file_type.is_symlink();

                // Convert to owned String only once at the end
                let name_owned = name_cow.into_owned();
                all_entries.push((name_owned.clone(), is_dir, is_symlink));

                if is_dir {
                    dir_names.push(name_owned);
                }
            }
        }

        let after_collect = Instant::now();

        // Validate all directories in parallel (or instantly with lazy mode)
        let dir_validation_results = self.filter_engine.should_show_directories_parallel(&backend_path, &dir_names);

        let after_validation = Instant::now();
        let total_entries = all_entries.len();

        // Add filtered entries
        for (name, is_dir, is_symlink) in &all_entries {
            // Apply filtering
            let should_show = if *is_dir {
                *dir_validation_results.get(name).unwrap_or(&true)
            } else {
                self.filter_engine.should_show_file(&backend_path, name)
            };

            if should_show {
                let child_path = path.join(name);

                let child_ino = self.get_or_create_inode(child_path);
                let file_type = if *is_dir {
                    FileType::Directory
                } else if *is_symlink {
                    FileType::Symlink
                } else {
                    FileType::RegularFile
                };

                dir_entries.push((child_ino, file_type, name.clone()));
            }
        }

        let after_filtering = Instant::now();

        // Return entries starting from offset
        // BUG FIX: FUSE readdir offset handling with consistent indices
        // Now that we always include "." and ".." in dir_entries, indices are stable across calls
        // Skip entries up to and including the offset (already returned)
        for (i, (ino, file_type, name)) in dir_entries.iter().enumerate().skip(offset as usize) {
            // Use i+1 as the next offset (standard FUSE pattern)
            if reply.add(*ino, (i + 1) as i64, *file_type, name) {
                // Buffer full, entry was NOT added
                break;
            }
        }

        reply.ok();

        let total_time = start_time.elapsed();

        // PERFORMANCE: Only log slow operations (>1s) to reduce hot path overhead
        // Changed from logging all Movies directory reads to only logging slow operations
        if total_time.as_millis() > 1000 {
            info!(
                "SLOW readdir '{}' total={:.3}s (path_resolve={:.3}ms, read_dir={:.3}ms, collect={:.3}ms, validation={:.3}ms, filtering={:.3}ms) entries={}/{}",
                path.display(),
                total_time.as_secs_f64(),
                after_path_resolve.duration_since(start_time).as_secs_f64() * 1000.0,
                after_read_dir.duration_since(after_path_resolve).as_secs_f64() * 1000.0,
                after_collect.duration_since(after_read_dir).as_secs_f64() * 1000.0,
                after_validation.duration_since(after_collect).as_secs_f64() * 1000.0,
                after_filtering.duration_since(after_validation).as_secs_f64() * 1000.0,
                dir_entries.len(),
                total_entries
            );
        } else {
            debug!(
                "readdir '{}' total={:.3}ms entries={}/{}",
                path.display(),
                total_time.as_secs_f64() * 1000.0,
                dir_entries.len(),
                total_entries
            );
        }
    }
    
    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        debug!("open: ino={}", ino);

        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let backend_path = self.backend_path(&path.to_string_lossy());

        match std::fs::File::open(&backend_path) {
            Ok(file) => {
                let fh = self.next_fh;
                self.next_fh = self.next_fh.wrapping_add(1);
                if self.next_fh == 0 { self.next_fh = 1; }  // skip 0 — reserved/sentinel
                self.open_files.insert(fh, file);
                reply.opened(fh, 0);
            }
            Err(_) => reply.error(libc::ENOENT),
        }
    }

    fn read(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64, size: u32, _flags: i32, _lock: Option<u64>, reply: ReplyData) {
        debug!("read: ino={}, fh={}, offset={}, size={}", ino, fh, offset, size);

        use std::io::{Read, Seek, SeekFrom};
        use std::os::unix::fs::FileExt;

        let file = match self.open_files.get(&fh) {
            Some(f) => f,
            None => {
                // Fallback: kernel cached read after our restart, or a caller that
                // skipped open(). Re-open on the fly so we don't return EBADF —
                // but log it because the open/release path is the supported one.
                warn!("read fallback: no cached handle for fh={} ino={} offset={} — re-opening backend file. \
                       If this fires repeatedly, the FUSE handle table has regressed and rar2fs will be under \
                       open/close pressure again.", fh, ino, offset);
                let path = match self.get_path_from_inode(ino) {
                    Some(path) => path.clone(),
                    None => { reply.error(libc::ENOENT); return; }
                };
                let backend_path = self.backend_path(&path.to_string_lossy());
                match std::fs::File::open(&backend_path) {
                    Ok(mut f) => {
                        if f.seek(SeekFrom::Start(offset as u64)).is_err() {
                            reply.error(libc::EIO);
                            return;
                        }
                        let mut buffer = vec![0u8; size as usize];
                        match f.read(&mut buffer) {
                            Ok(n) => { buffer.truncate(n); reply.data(&buffer); }
                            Err(_) => reply.error(libc::EIO),
                        }
                    }
                    Err(_) => reply.error(libc::ENOENT),
                }
                return;
            }
        };

        // Use read_at (pread) to avoid clobbering the file's seek position —
        // FUSE reads can arrive out of order from multiple concurrent readers
        // sharing one file handle.
        let mut buffer = vec![0u8; size as usize];
        match file.read_at(&mut buffer, offset as u64) {
            Ok(bytes_read) => {
                buffer.truncate(bytes_read);
                reply.data(&buffer);
            }
            Err(_) => reply.error(libc::EIO),
        }
    }

    fn release(&mut self, _req: &Request, _ino: u64, fh: u64, _flags: i32, _lock_owner: Option<u64>, _flush: bool, reply: ReplyEmpty) {
        debug!("release: fh={}", fh);
        self.open_files.remove(&fh);
        // HashMap retains its bucket allocation across removals — peak open-file count
        // sticks as the steady-state memory until we explicitly shrink. When the map
        // fully drains, reclaim the buckets so a one-time burst (e.g. a scanner that
        // briefly opened thousands of files) doesn't leave permanent overhead.
        if self.open_files.is_empty() && self.open_files.capacity() > 64 {
            self.open_files.shrink_to_fit();
        }
        reply.ok();
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        match self.check_delete_allowed(parent, name) {
            Ok(source_path) => {
                match std::fs::remove_file(&source_path) {
                    Ok(_) => reply.ok(),
                    Err(_) => reply.error(libc::EIO),
                }
            }
            Err(errno) => reply.error(errno),
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        match self.check_delete_allowed(parent, name) {
            Ok(source_path) => {
                match std::fs::remove_dir(&source_path) {
                    Ok(_) => reply.ok(),
                    Err(_) => reply.error(libc::EIO),
                }
            }
            Err(errno) => reply.error(errno),
        }
    }

    fn readlink(&mut self, _req: &Request, ino: u64, reply: fuser::ReplyData) {
        debug!("readlink: ino={}", ino);

        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let backend_path = self.backend_path(&path.to_string_lossy());

        match std::fs::read_link(&backend_path) {
            Ok(target) => {
                reply.data(target.to_string_lossy().as_bytes());
            }
            Err(_) => reply.error(libc::ENOENT),
        }
    }

    fn statfs(&mut self, _req: &Request, ino: u64, reply: fuser::ReplyStatfs) {
        debug!("statfs: ino={}", ino);

        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let backend_path = self.backend_path(&path.to_string_lossy());

        match nix::sys::statvfs::statvfs(&backend_path) {
            Ok(stat) => {
                reply.statfs(
                    stat.blocks(),
                    stat.blocks_free(),
                    stat.blocks_available(),
                    stat.files(),
                    stat.files_free(),
                    stat.block_size() as u32,
                    stat.name_max() as u32,
                    stat.fragment_size() as u32,
                );
            }
            Err(_) => reply.error(libc::EIO),
        }
    }

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, _umask: u32, reply: fuser::ReplyEntry) {
        debug!("mkdir: parent={}, name={:?}, mode={:o}", parent, name, mode);

        // Check global read_only mode
        if self.filter_engine.read_only_enabled() {
            warn!("Blocked mkdir (read_only enabled): {:?}", name);
            reply.error(libc::EROFS);
            return;
        }

        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let dir_path = parent_path.join(name);

        // Note: We allow creating directories even in RAR-containing directories
        // This allows users to create subtitle folders, etc.
        // Only actual RAR-extracted content is protected (checked in setattr/write)

        // Write to source path (not backend which is read-only rar2fs)
        let source_path = self.source_path(&dir_path.to_string_lossy());

        match std::fs::create_dir(&source_path) {
            Ok(_) => {
                // Set permissions
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = std::fs::set_permissions(&source_path, std::fs::Permissions::from_mode(mode));
                }

                // Return the new directory's attributes
                match std::fs::metadata(&source_path) {
                    Ok(metadata) => {
                        let ino = self.get_or_create_inode(dir_path);
                        let attr = self.build_file_attr(ino, &metadata, FileType::Directory, Some((mode & 0o7777) as u16));
                        reply.entry(&self.entry_timeout, &attr, 0);
                    }
                    Err(_) => reply.error(libc::EIO),
                }
            }
            Err(e) => {
                warn!("mkdir failed: {:?}", e);
                reply.error(libc::EIO);
            }
        }
    }

    fn mknod(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, _umask: u32, _rdev: u32, reply: fuser::ReplyEntry) {
        debug!("mknod: parent={}, name={:?}, mode={:o}", parent, name, mode);

        // Check global read_only mode
        if self.filter_engine.read_only_enabled() {
            warn!("Blocked mknod (read_only enabled): {:?}", name);
            reply.error(libc::EROFS);
            return;
        }

        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let file_path = parent_path.join(name);

        // Note: We allow creating files even in RAR-containing directories
        // This allows users to add subtitles, NFO files, etc.
        // Only actual RAR-extracted content is protected (checked in setattr/write)

        let source_path = self.source_path(&file_path.to_string_lossy());

        // Create empty file
        match std::fs::File::create(&source_path) {
            Ok(_) => {
                // Set permissions
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = std::fs::set_permissions(&source_path, std::fs::Permissions::from_mode(mode));
                }

                // Return the new file's attributes
                match std::fs::metadata(&source_path) {
                    Ok(metadata) => {
                        let ino = self.get_or_create_inode(file_path);
                        let attr = self.build_file_attr(ino, &metadata, FileType::RegularFile, Some((mode & 0o7777) as u16));
                        reply.entry(&self.entry_timeout, &attr, 0);
                    }
                    Err(_) => reply.error(libc::EIO),
                }
            }
            Err(e) => {
                warn!("mknod failed: {:?}", e);
                reply.error(libc::EIO);
            }
        }
    }

    fn create(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, _umask: u32, _flags: i32, reply: ReplyCreate) {
        debug!("create: parent={}, name={:?}, mode={:o}", parent, name, mode);

        // Check global read_only mode
        if self.filter_engine.read_only_enabled() {
            warn!("Blocked create (read_only enabled): {:?}", name);
            reply.error(libc::EROFS);
            return;
        }

        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let file_path = parent_path.join(name);

        // Note: We allow creating files even in RAR-containing directories
        // This allows users to add subtitles, NFO files, etc.
        // Only actual RAR-extracted content is protected (checked in setattr/write)

        let source_path = self.source_path(&file_path.to_string_lossy());

        // Create empty file
        match std::fs::File::create(&source_path) {
            Ok(_) => {
                // Set permissions
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = std::fs::set_permissions(&source_path, std::fs::Permissions::from_mode(mode));
                }

                // Return the new file's attributes
                match std::fs::metadata(&source_path) {
                    Ok(metadata) => {
                        let ino = self.get_or_create_inode(file_path);
                        let attr = self.build_file_attr(ino, &metadata, FileType::RegularFile, Some((mode & 0o7777) as u16));
                        reply.created(&self.entry_timeout, &attr, 0, 0, 0);
                    }
                    Err(_) => reply.error(libc::EIO),
                }
            }
            Err(e) => {
                warn!("create failed: {:?}", e);
                reply.error(libc::EIO);
            }
        }
    }

    fn write(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, data: &[u8], _write_flags: u32, _flags: i32, _lock: Option<u64>, reply: ReplyWrite) {
        debug!("write: ino={}, offset={}, size={}", ino, offset, data.len());

        // Check global read_only mode
        if self.filter_engine.read_only_enabled() {
            warn!("Blocked write (read_only enabled): ino={}", ino);
            reply.error(libc::EROFS);
            return;
        }

        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Check if this is protected content (extracted media or SFV-listed release file)
        if self.is_write_protected(&path.to_string_lossy()) {
            warn!("Blocked write to protected file: {:?}", path);
            reply.error(libc::EROFS);
            return;
        }

        let source_path = self.source_path(&path.to_string_lossy());

        match std::fs::OpenOptions::new().write(true).open(&source_path) {
            Ok(mut file) => {
                use std::io::{Seek, SeekFrom, Write};

                if file.seek(SeekFrom::Start(offset as u64)).is_err() {
                    reply.error(libc::EIO);
                    return;
                }

                match file.write(data) {
                    Ok(bytes_written) => {
                        reply.written(bytes_written as u32);
                    }
                    Err(_) => reply.error(libc::EIO),
                }
            }
            Err(_) => reply.error(libc::ENOENT),
        }
    }

    fn setattr(&mut self, _req: &Request, ino: u64, mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, size: Option<u64>, atime: Option<fuser::TimeOrNow>, mtime: Option<fuser::TimeOrNow>, _ctime: Option<std::time::SystemTime>, _fh: Option<u64>, _crtime: Option<std::time::SystemTime>, _chgtime: Option<std::time::SystemTime>, _bkuptime: Option<std::time::SystemTime>, flags: Option<u32>, reply: ReplyAttr) {
        debug!("setattr: ino={}, mode={:?}, uid={:?}, gid={:?}, size={:?}", ino, mode, uid, gid, size);

        // Check global read_only mode
        if self.filter_engine.read_only_enabled() {
            warn!("Blocked setattr (read_only enabled): ino={}", ino);
            reply.error(libc::EROFS);
            return;
        }

        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Check if this is protected content (extracted media or SFV-listed release file)
        if self.is_write_protected(&path.to_string_lossy()) {
            warn!("Blocked setattr on protected file: {:?}", path);
            reply.error(libc::EROFS);
            return;
        }

        let source_path = self.source_path(&path.to_string_lossy());

        // Apply mode
        #[cfg(unix)]
        if let Some(mode) = mode {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&source_path, std::fs::Permissions::from_mode(mode));
        }

        // Apply uid/gid
        #[cfg(unix)]
        if uid.is_some() || gid.is_some() {
            use nix::unistd::{Uid, Gid};
            let uid = uid.map(Uid::from_raw);
            let gid = gid.map(Gid::from_raw);
            let _ = nix::unistd::chown(&source_path, uid, gid);
        }

        // Apply size (truncate)
        if let Some(size) = size {
            match std::fs::File::open(&source_path) {
                Ok(file) => {
                    let _ = file.set_len(size);
                }
                Err(_) => {
                    reply.error(libc::EIO);
                    return;
                }
            }
        }

        // Apply timestamps
        if atime.is_some() || mtime.is_some() {
            use filetime::{FileTime, set_file_times};

            let metadata = match std::fs::metadata(&source_path) {
                Ok(m) => m,
                Err(_) => {
                    reply.error(libc::EIO);
                    return;
                }
            };

            let atime = match atime {
                Some(fuser::TimeOrNow::SpecificTime(t)) => FileTime::from_system_time(t),
                Some(fuser::TimeOrNow::Now) => FileTime::now(),
                None => FileTime::from_system_time(metadata.accessed().unwrap_or(UNIX_EPOCH)),
            };

            let mtime = match mtime {
                Some(fuser::TimeOrNow::SpecificTime(t)) => FileTime::from_system_time(t),
                Some(fuser::TimeOrNow::Now) => FileTime::now(),
                None => FileTime::from_system_time(metadata.modified().unwrap_or(UNIX_EPOCH)),
            };

            let _ = set_file_times(&source_path, atime, mtime);
        }

        // Suppress unused variable warnings
        let _ = flags;

        // Return updated attributes
        match std::fs::metadata(&source_path) {
            Ok(metadata) => {
                let file_type = if metadata.is_dir() {
                    FileType::Directory
                } else if metadata.is_symlink() {
                    FileType::Symlink
                } else {
                    FileType::RegularFile
                };
                let attr = self.build_file_attr(ino, &metadata, file_type, None);
                reply.attr(&self.attr_timeout, &attr);
            }
            Err(_) => reply.error(libc::EIO),
        }
    }

    fn rename(&mut self, _req: &Request, parent: u64, name: &OsStr, newparent: u64, newname: &OsStr, _flags: u32, reply: fuser::ReplyEmpty) {
        debug!("rename: parent={}, name={:?}, newparent={}, newname={:?}", parent, name, newparent, newname);

        // Check global read_only mode
        if self.filter_engine.read_only_enabled() {
            warn!("Blocked rename (read_only enabled): {:?}", name);
            reply.error(libc::EROFS);
            return;
        }

        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let newparent_path = match self.get_path_from_inode(newparent) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let old_path = parent_path.join(name);
        let new_path = newparent_path.join(newname);

        // Protect extracted media and SFV-listed release files from being renamed.
        // New companion files (subtitles, nfo) not named in the SFV are allowed.
        if self.is_write_protected(&old_path.to_string_lossy()) {
            warn!("Blocked rename of protected content: {:?}", old_path);
            reply.error(libc::EROFS);
            return;
        }

        // Don't check destination - it doesn't exist yet during rename!
        // Only the source file matters for RAR protection.
        // New files (including renamed destinations) are always writable.

        let old_source = self.source_path(&old_path.to_string_lossy());
        let new_source = self.source_path(&new_path.to_string_lossy());

        match std::fs::rename(&old_source, &new_source) {
            Ok(_) => {
                // Update inode mappings
                if let Some(&ino) = self.path_to_inode.get(&old_path) {
                    self.path_to_inode.remove(&old_path);
                    // put() updates the value for an existing key without eviction
                    self.inode_map.put(ino, new_path.clone());
                    self.path_to_inode.insert(new_path, ino);
                }
                reply.ok();
            }
            Err(_) => reply.error(libc::EIO),
        }
    }
}

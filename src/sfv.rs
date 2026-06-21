//! SFV (Simple File Verification) validation module
//!
//! Provides shared SFV validation functionality used by both:
//! - filtering.rs (for deciding whether to show directories)
//! - inotify_watcher.rs (for detecting when downloads are complete)

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tracing::{debug, warn};

/// Minimum age in seconds for a file to be considered complete (not actively being written)
const FILE_COMPLETE_AGE_SECS: u64 = 1;

/// Check if a file is complete (not actively being written)
/// A file is considered complete if its mtime is at least FILE_COMPLETE_AGE_SECS old.
/// This prevents premature SFV validation while files are still being downloaded.
pub fn is_file_complete(path: &Path) -> bool {
    let metadata = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(_) => return false,
    };

    // File must be at least FILE_COMPLETE_AGE_SECS old (not actively being written)
    if let Ok(modified) = metadata.modified() {
        if let Ok(elapsed) = modified.elapsed() {
            return elapsed >= Duration::from_secs(FILE_COMPLETE_AGE_SECS);
        }
    }
    true // If we can't check mtime, assume complete
}

/// Check if a file is an SFV file based on extension
pub fn is_sfv_file(path: &Path) -> bool {
    path.extension()
        .map(|ext| ext.to_string_lossy().to_lowercase() == "sfv")
        .unwrap_or(false)
}

/// Validate SFV file with completeness checking - check if all listed files exist AND are complete
/// Returns true if all files listed in the SFV exist and are no longer being written.
/// This is the function used by inotify_watcher for detecting when downloads are truly complete.
///
/// # Arguments
/// * `sfv_path` - Path to the SFV file
///
/// # Returns
/// * `true` if all files in SFV exist AND are complete (not actively being written)
/// * `false` if any file is missing, incomplete, or SFV cannot be read
pub fn validate_sfv_with_completeness(sfv_path: &Path) -> bool {
    let dir_path = match sfv_path.parent() {
        Some(p) => p,
        None => return false,
    };

    // Read SFV file (use lossy UTF-8 to handle legacy encodings like ISO-8859-1)
    let content = match std::fs::read(sfv_path) {
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(e) => {
            warn!("Failed to read SFV file {}: {}", sfv_path.display(), e);
            return false;
        }
    };

    // Check each file listed in SFV for existence AND completeness
    for line in content.lines() {
        // Strip a leading UTF-8 BOM (PowerShell-generated SFVs are UTF-8-with-BOM,
        // so the first comment line would otherwise read as '\u{feff};' and be
        // mistaken for a filename).
        let line = line.trim_start_matches('\u{feff}').trim();
        if line.is_empty() || line.starts_with(';') {
            continue;
        }

        // SFV format: filename CRC32 (last space separates name from CRC)
        if let Some(space_pos) = line.rfind(' ') {
            let filename = line[..space_pos].trim();
            if !filename.is_empty() {
                // SFV entries may reference a subfolder with a Windows separator
                // (e.g. 'Sample\foo-sample.mkv'); normalise to '/' so the path
                // resolves on Unix.
                let rel = filename.replace('\\', "/");
                let file_path = dir_path.join(&rel);

                // Check if file exists
                if !file_path.exists() {
                    debug!("SFV validation failed: missing file {}", filename);
                    return false;
                }

                // Check if file is complete (not actively being written)
                if !is_file_complete(&file_path) {
                    debug!("SFV validation deferred: file still being written: {}", filename);
                    return false;
                }
            }
        }
    }

    true
}

/// Validate SFV content against a HashMap (used by filtering.rs)
/// The HashMap allows for future extension (e.g., storing CRC values)
///
/// # Arguments
/// * `sfv_content` - Contents of the SFV file
/// * `actual_files` - Map of lowercase filename -> original filename
///
/// # Returns
/// * `true` if all files in SFV exist
/// * `false` if any file is missing
pub fn validate_sfv_content_with_map(sfv_content: &str, actual_files: &HashMap<String, String>) -> bool {
    for line in sfv_content.lines() {
        // Strip a leading UTF-8 BOM (see validate_sfv_with_completeness).
        let line = line.trim_start_matches('\u{feff}').trim();
        if line.is_empty() || line.starts_with(';') {
            continue;
        }

        // SFV format: filename CRC32 (last space separates name from CRC)
        if let Some(space_pos) = line.rfind(' ') {
            let filename = line[..space_pos].trim();
            // SFV entries may carry a subfolder path with Windows or Unix
            // separators (e.g. 'Sample\foo-sample.mkv'); match on the basename,
            // since actual_files is keyed by filename.
            let base = filename
                .rsplit(|c| c == '/' || c == '\\')
                .next()
                .unwrap_or(filename);
            if !base.is_empty()
                && !actual_files.contains_key(&base.to_lowercase()) {
                    debug!("SFV validation failed: missing file {}", filename);
                    return false;
                }
        }
    }

    true
}

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

/// Case-insensitive basename -> (real filename, parent dir) index, built once
/// per validation pass from the SAME two-level scan filtering.rs's own
/// case-insensitive SFV matching already uses (top-level entries, plus one
/// level into subfolders for scene Sample\ layouts) — kept as a sibling
/// function here rather than sharing filtering.rs's HashMap<String, String>
/// shape, since this one also needs to remember WHERE each file actually
/// lives to build a real, stat-able path for the completeness check below.
fn index_actual_files(dir_path: &Path) -> HashMap<String, (String, std::path::PathBuf)> {
    let mut index = HashMap::new();
    let entries = match std::fs::read_dir(dir_path) {
        Ok(e) => e,
        Err(_) => return index,
    };
    for entry in entries.flatten() {
        let entry_path = entry.path();
        if entry_path.is_file() {
            let filename = entry.file_name().to_string_lossy().to_string();
            index.insert(filename.to_lowercase(), (filename, dir_path.to_path_buf()));
        } else if entry_path.is_dir() {
            if let Ok(sub_entries) = std::fs::read_dir(&entry_path) {
                for sub in sub_entries.flatten() {
                    if sub.path().is_file() {
                        let sub_name = sub.file_name().to_string_lossy().to_string();
                        index.entry(sub_name.to_lowercase()).or_insert((sub_name, entry_path.clone()));
                    }
                }
            }
        }
    }
    index
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

    // Built once, matched case-insensitively below — an SFV's own listed
    // filenames are frequently lowercased regardless of the actual release's
    // case (a normal scene convention), so a plain case-sensitive exists()
    // check can report a fully-downloaded release as permanently missing
    // files it actually has. filtering.rs's own SFV check already handles
    // this the same way; this function didn't, which is what let a real,
    // complete download sit stuck re-failing validation forever instead of
    // ever notifying Sonarr/Radarr/Emby.
    let actual_files = index_actual_files(dir_path);

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
                // (e.g. 'Sample\foo-sample.mkv'); the index above already
                // flattens subfolder files by basename, so match on that.
                let base = filename.replace('\\', "/");
                let base = base.rsplit('/').next().unwrap_or(&base);

                let file_path = match actual_files.get(&base.to_lowercase()) {
                    Some((real_name, parent)) => parent.join(real_name),
                    None => {
                        debug!("SFV validation failed: missing file {}", filename);
                        return false;
                    }
                };

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::thread::sleep;
    use tempfile::tempdir;

    fn write_old(path: &Path, contents: &[u8]) {
        fs::write(path, contents).unwrap();
        // is_file_complete requires FILE_COMPLETE_AGE_SECS (1s) of age.
        sleep(Duration::from_millis(1100));
    }

    #[test]
    fn validates_when_sfv_lists_lowercase_but_real_files_are_mixed_case() {
        let dir = tempdir().unwrap();
        write_old(&dir.path().join("Release.Name.r00"), b"x");
        write_old(&dir.path().join("Release.Name.r01"), b"x");
        let sfv_path = dir.path().join("release.name.sfv");
        fs::write(&sfv_path, "release.name.r00 DEADBEEF\nrelease.name.r01 CAFEBABE\n").unwrap();

        assert!(validate_sfv_with_completeness(&sfv_path));
    }

    #[test]
    fn fails_when_a_listed_file_is_genuinely_missing() {
        let dir = tempdir().unwrap();
        write_old(&dir.path().join("Release.Name.r00"), b"x");
        let sfv_path = dir.path().join("release.name.sfv");
        fs::write(&sfv_path, "release.name.r00 DEADBEEF\nrelease.name.r01 CAFEBABE\n").unwrap();

        assert!(!validate_sfv_with_completeness(&sfv_path));
    }

    #[test]
    fn defers_when_a_matched_file_was_just_written() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("Release.Name.r00"), b"x").unwrap(); // fresh, no sleep
        let sfv_path = dir.path().join("release.name.sfv");
        fs::write(&sfv_path, "release.name.r00 DEADBEEF\n").unwrap();

        assert!(!validate_sfv_with_completeness(&sfv_path));
    }

    #[test]
    fn resolves_a_case_mismatched_sample_subfolder_entry() {
        let dir = tempdir().unwrap();
        let sample_dir = dir.path().join("Sample");
        fs::create_dir(&sample_dir).unwrap();
        write_old(&sample_dir.join("Release.Name-sample.mkv"), b"x");
        let sfv_path = dir.path().join("release.name.sfv");
        fs::write(&sfv_path, "sample\\release.name-sample.mkv DEADBEEF\n").unwrap();

        assert!(validate_sfv_with_completeness(&sfv_path));
    }
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
                .rsplit(['/', '\\'])
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

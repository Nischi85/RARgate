//! SFV (Simple File Verification) validation module
//!
//! Provides shared SFV validation functionality used by both:
//! - filtering.rs (for deciding whether to show directories)
//! - inotify_watcher.rs (for detecting when downloads are complete)

use std::collections::HashMap;
use std::path::{Path, PathBuf};
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

/// The files of one release directory, for matching SFV entries against.
///
/// Matching is case-insensitive and by basename: an SFV's listed names are
/// frequently lowercased regardless of the real files' case (a normal scene
/// convention), and scene releases put samples in a subfolder the SFV
/// references with a Windows path (`Sample\foo.mkv`). So top-level files and
/// the files one level down are indexed together by lowercase basename; a
/// top-level file wins over a subfolder file with the same name.
pub struct ReleaseFiles {
    /// lowercase basename -> (real filename, directory it lives in)
    by_name: HashMap<String, (String, PathBuf)>,
    /// The release's own `.sfv` (a top-level one; the last one read if several).
    pub sfv: Option<PathBuf>,
}

impl ReleaseFiles {
    pub fn read(dir_path: &Path) -> std::io::Result<Self> {
        let mut by_name = HashMap::new();
        let mut sfv = None;
        for entry in std::fs::read_dir(dir_path)?.flatten() {
            let entry_path = entry.path();
            if entry_path.is_file() {
                let filename = entry.file_name().to_string_lossy().to_string();
                if is_sfv_file(&entry_path) {
                    sfv = Some(entry_path);
                }
                by_name.insert(filename.to_lowercase(), (filename, dir_path.to_path_buf()));
            } else if entry_path.is_dir() {
                if let Ok(sub_entries) = std::fs::read_dir(&entry_path) {
                    for sub in sub_entries.flatten() {
                        if sub.path().is_file() {
                            let sub_name = sub.file_name().to_string_lossy().to_string();
                            by_name.entry(sub_name.to_lowercase()).or_insert((sub_name, entry_path.clone()));
                        }
                    }
                }
            }
        }
        Ok(Self { by_name, sfv })
    }

    /// Real path of the file an SFV entry names, if the release has it.
    fn find(&self, basename: &str) -> Option<PathBuf> {
        self.by_name
            .get(&basename.to_lowercase())
            .map(|(real_name, parent)| parent.join(real_name))
    }
}

/// The files an SFV lists, as (name as written, basename). Skips comments
/// (`;`) and blank lines, and strips a leading UTF-8 BOM: PowerShell-generated
/// SFVs are UTF-8-with-BOM, so the first comment line would otherwise read as
/// `\u{feff};` and be mistaken for a filename. An entry is `filename CRC32`;
/// the last space separates the two.
pub fn listed_files(sfv_content: &str) -> impl Iterator<Item = (&str, &str)> {
    sfv_content.lines().filter_map(|line| {
        let line = line.trim_start_matches('\u{feff}').trim();
        if line.is_empty() || line.starts_with(';') {
            return None;
        }
        let filename = line[..line.rfind(' ')?].trim();
        let basename = filename.rsplit(['/', '\\']).next().unwrap_or(filename);
        (!basename.is_empty()).then_some((filename, basename))
    })
}

/// Read an SFV file as text (lossy UTF-8, for legacy encodings like ISO-8859-1).
pub fn read_sfv(sfv_path: &Path) -> std::io::Result<String> {
    Ok(String::from_utf8_lossy(&std::fs::read(sfv_path)?).into_owned())
}

/// Every file the SFV lists is present in the release.
pub fn all_listed_files_present(sfv_content: &str, files: &ReleaseFiles) -> bool {
    for (filename, basename) in listed_files(sfv_content) {
        if files.find(basename).is_none() {
            debug!("SFV validation failed: missing file {}", filename);
            return false;
        }
    }
    true
}

/// Every file the SFV lists is present AND no longer being written. This is
/// what inotify_watcher uses to decide a download has finished.
pub fn validate_sfv_with_completeness(sfv_path: &Path) -> bool {
    let Some(dir_path) = sfv_path.parent() else { return false };
    let content = match read_sfv(sfv_path) {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to read SFV file {}: {}", sfv_path.display(), e);
            return false;
        }
    };
    let Ok(files) = ReleaseFiles::read(dir_path) else { return false };

    for (filename, basename) in listed_files(&content) {
        let Some(file_path) = files.find(basename) else {
            debug!("SFV validation failed: missing file {}", filename);
            return false;
        };
        if !is_file_complete(&file_path) {
            debug!("SFV validation deferred: file still being written: {}", filename);
            return false;
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

    #[test]
    fn listed_files_skips_bom_comments_and_blank_lines() {
        let sfv = "\u{feff}; generated by PowerShell\n\nSample\\a-sample.mkv 1234ABCD\nrel.r00 DEADBEEF\n";
        let listed: Vec<_> = listed_files(sfv).collect();
        assert_eq!(listed, vec![("Sample\\a-sample.mkv", "a-sample.mkv"), ("rel.r00", "rel.r00")]);
    }

    #[test]
    fn release_files_finds_the_sfv_and_checks_presence() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("Rel.r00"), b"x").unwrap();
        fs::write(dir.path().join("rel.sfv"), "rel.r00 DEADBEEF\n").unwrap();
        let files = ReleaseFiles::read(dir.path()).unwrap();

        assert_eq!(files.sfv, Some(dir.path().join("rel.sfv")));
        assert!(all_listed_files_present("rel.r00 DEADBEEF\n", &files));
        assert!(!all_listed_files_present("rel.r01 DEADBEEF\n", &files));
    }
}

//! Jellyfin media server notification integration
//!
//! Jellyfin is API-compatible with Emby, so this module reuses the Emby notifier
//! implementation with Jellyfin-specific configuration (no "/emby" API prefix).
//!
//! Features:
//! - Targeted item refresh via `/Items/{id}/Refresh`
//! - Full library scan via `/Library/Refresh`
//! - Path mapping for Docker deployments
//! - Item caching to reduce API calls
//! - Debouncing for batching rapid file changes

use crate::config::JellyfinConfig;
use crate::emby_notifier::EmbyNotifier;
use crate::media_notifier_common::NotifierHandle;
use anyhow::Result;

/// Re-export NotifierHandle as JellyfinNotifierHandle for backwards compatibility
pub type JellyfinNotifierHandle = NotifierHandle;

/// Jellyfin notifier - thin wrapper around EmbyNotifier
///
/// Jellyfin uses the same API as Emby but without the "/emby" prefix.
/// This type alias maintains backwards compatibility while reusing all Emby logic.
pub type JellyfinNotifier = EmbyNotifier;

/// Create a new JellyfinNotifier and its handle
///
/// This is a convenience function that creates an EmbyNotifier configured for Jellyfin.
pub fn new(config: JellyfinConfig) -> Result<(JellyfinNotifier, JellyfinNotifierHandle)> {
    // Convert JellyfinConfig to EmbyConfig-compatible structure
    let emby_config = crate::config::EmbyConfig {
        enabled: config.enabled,
        url: config.url,
        api_token: config.api_token,
        debounce_seconds: config.debounce_seconds,
        cache_minutes: config.cache_minutes,
        fallback_full_refresh: config.fallback_full_refresh,
        path_mapping: config.path_mapping,
        max_retries: config.max_retries,
        retry_delay_ms: config.retry_delay_ms,
        full_refresh_cooldown_seconds: config.full_refresh_cooldown_seconds,
    };

    // Use EmbyNotifier with Jellyfin-specific settings (no "/emby" prefix)
    EmbyNotifier::new_internal(emby_config, "Jellyfin", "")
}


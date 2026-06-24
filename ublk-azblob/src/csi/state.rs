//! On-disk record of every volume this node plugin has published, so a
//! node-local restart of the plugin (a crash, an OOM kill, or a DaemonSet
//! upgrade) can re-attach to the still-live block devices instead of leaving the
//! kubelet's bind-mounts pointing at dead `/dev/ublkbN` / `/dev/nbdN` nodes.
//!
//! ## Why this works
//! The child `ublk-azblob run` processes are spawned with ublk **user-recovery**
//! enabled (`UBLK_F_USER_RECOVERY`). When the node plugin (and with it, the
//! child cgroup) dies abruptly, the kernel keeps `/dev/ublkbN` alive but
//! quiesced rather than tearing it down. On restart the plugin reads these state
//! records and, for each, spawns a fresh `run --recover` child that re-attaches
//! to the same device id and resumes serving I/O — the existing filesystem mount
//! (same `major:minor`) stays valid throughout. NBD has no kernel-side recovery,
//! so its devices are re-served by reconnecting `nbd-client` to the same device
//! node.
//!
//! ## Credentials
//! The full child environment (including any Azure credentials passed as request
//! secrets at publish time) is persisted so the recovered child can re-open the
//! blob. Securing the node-local state directory is the operator's
//! responsibility (it lives under the driver's kubelet plugin directory, which
//! is already root-only); recovery is therefore only as safe as that directory.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::mount::OverlayDirs;

/// Default directory holding the per-volume state files. It lives under the
/// driver's kubelet plugin directory (the same hostPath that backs the CSI
/// socket), so it survives a container restart / DaemonSet upgrade on the node.
pub const DEFAULT_STATE_DIR: &str = "/csi/state";

/// Environment variable overriding [`DEFAULT_STATE_DIR`].
pub const STATE_DIR_ENV: &str = "CSI_STATE_DIR";

/// How a published volume's block device is backed, plus the bits needed to
/// re-establish it on recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum DeviceMode {
    /// A ublk device; `dev_id` is the `/dev/ublkbN` number to recover.
    Ublk { dev_id: i32 },
    /// An NBD device; `device` is the `/dev/nbdN` path and `listen` the
    /// `host:port` the previous server bound (a fresh free port is chosen on
    /// recovery).
    Nbd { device: String, listen: String },
}

/// A persisted, recoverable description of one published volume.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeState {
    /// CSI volume id.
    pub volume_id: String,
    /// Device size in bytes (passed to the recovered `run` child).
    pub size: u64,
    /// CSI target path where the filesystem is mounted (the merged overlay path
    /// for overlay volumes).
    pub target: String,
    /// Filesystem type used at publish time.
    pub fs_type: String,
    /// Mount flags used at publish time (re-applied if a re-mount is needed).
    pub mount_flags: Vec<String>,
    /// Whether the volume was mounted read-only.
    pub readonly: bool,
    /// Backing device mode (ublk vs NBD) and its recovery selectors.
    pub device_mode: DeviceMode,
    /// Ephemeral overlay scratch dirs, when the volume was published with an
    /// overlay (the writable upper is node-local and is *not* recovered across a
    /// full teardown, but the dirs are recorded so unpublish can clean up).
    pub overlay: Option<OverlayDirs>,
    /// The full child environment (storage selectors + credentials) needed to
    /// re-spawn the `run` child on recovery.
    pub env: Vec<(String, String)>,
}

/// Resolve the state directory from `CSI_STATE_DIR` (falling back to
/// [`DEFAULT_STATE_DIR`]).
pub fn state_dir() -> PathBuf {
    std::env::var(STATE_DIR_ENV)
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_STATE_DIR))
}

/// Map a volume id to its state file path inside `dir`.
///
/// The volume id is sanitized to a filesystem-safe component (alphanumerics plus
/// `-`/`_`) so it can't escape the state directory. A short stable hash of the
/// full, unsanitized id is appended so two distinct ids that sanitize to the
/// same string (e.g. `vol/123` and `vol#123`) can't collide onto one file.
fn state_file(dir: &Path, volume_id: &str) -> PathBuf {
    let mut name = String::with_capacity(volume_id.len() + 22);
    for ch in volume_id.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            name.push(ch);
        } else {
            name.push('_');
        }
    }
    // FNV-1a 64-bit over the original bytes: deterministic, dependency-free, and
    // recomputed identically by `remove()`, so the filename is a stable 1:1 map.
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in volume_id.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x00000100000001b3);
    }
    name.push_str(&format!("-{hash:016x}.json"));
    dir.join(name)
}

/// Atomically persist `state` to the state directory (write to a temp file, then
/// rename) so a crash mid-write can never leave a half-written record.
pub fn save(state: &VolumeState) -> anyhow::Result<()> {
    use anyhow::Context as _;
    let dir = state_dir();
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("create state dir {}", dir.display()))?;
    let path = state_file(&dir, &state.volume_id);
    let tmp = path.with_extension("json.tmp");
    let json = serde_json::to_vec_pretty(state).context("serialize volume state")?;
    std::fs::write(&tmp, &json).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, &path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

/// Remove a volume's state file (idempotent — a missing file is not an error).
pub fn remove(volume_id: &str) -> anyhow::Result<()> {
    let path = state_file(&state_dir(), volume_id);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(anyhow::anyhow!("remove {}: {e}", path.display())),
    }
}

/// Load every persisted volume state (skipping unreadable / malformed files).
pub fn load_all() -> Vec<VolumeState> {
    let dir = state_dir();
    let mut out = Vec::new();
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(_) => return out,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        match std::fs::read(&path).map(|b| serde_json::from_slice::<VolumeState>(&b)) {
            Ok(Ok(state)) => out.push(state),
            Ok(Err(e)) => tracing::warn!(path = %path.display(), error = %e, "skipping malformed state file"),
            Err(e) => tracing::warn!(path = %path.display(), error = %e, "skipping unreadable state file"),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_json() {
        let state = VolumeState {
            volume_id: "acct#cont#blob".to_string(),
            size: 1 << 30,
            target: "/var/lib/kubelet/pods/x/vol".to_string(),
            fs_type: "ext4".to_string(),
            mount_flags: vec!["noatime".to_string()],
            readonly: false,
            device_mode: DeviceMode::Ublk { dev_id: 7 },
            overlay: None,
            env: vec![("UBLK_BLOB_URL".to_string(), "http://x/c/b".to_string())],
        };
        let json = serde_json::to_vec(&state).unwrap();
        let back: VolumeState = serde_json::from_slice(&json).unwrap();
        assert_eq!(back.volume_id, state.volume_id);
        assert_eq!(back.size, state.size);
        assert!(matches!(back.device_mode, DeviceMode::Ublk { dev_id: 7 }));
        assert_eq!(back.env, state.env);
    }

    #[test]
    fn nbd_mode_round_trips() {
        let state = VolumeState {
            volume_id: "v".to_string(),
            size: 4096,
            target: "/t".to_string(),
            fs_type: "ext4".to_string(),
            mount_flags: vec![],
            readonly: true,
            device_mode: DeviceMode::Nbd {
                device: "/dev/nbd3".to_string(),
                listen: "127.0.0.1:10809".to_string(),
            },
            overlay: None,
            env: vec![],
        };
        let json = serde_json::to_vec(&state).unwrap();
        let back: VolumeState = serde_json::from_slice(&json).unwrap();
        match back.device_mode {
            DeviceMode::Nbd { device, listen } => {
                assert_eq!(device, "/dev/nbd3");
                assert_eq!(listen, "127.0.0.1:10809");
            }
            _ => panic!("expected NBD mode"),
        }
    }

    #[test]
    fn state_file_sanitizes_volume_id() {
        // The sanitized prefix is filesystem-safe, and a stable hash suffix keeps
        // distinct ids that sanitize alike from colliding onto one file.
        let p = state_file(Path::new("/csi/state"), "a/b#c");
        let name = p.file_name().unwrap().to_str().unwrap();
        assert!(name.starts_with("a_b_c-"), "got {name}");
        assert!(name.ends_with(".json"), "got {name}");
        // Deterministic for a given id.
        assert_eq!(p, state_file(Path::new("/csi/state"), "a/b#c"));
        // Distinct ids that sanitize to the same prefix get distinct files.
        assert_ne!(p, state_file(Path::new("/csi/state"), "a#b/c"));
    }
}

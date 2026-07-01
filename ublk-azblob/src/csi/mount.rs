//! Node-side OS helpers: ublk device discovery, `mkfs`, `fsck`, `mount` / `umount`.
//!
//! These are blocking operations (they shell out to `mkfs`, `fsck`, `mount`,
//! `blkid`, `umount` and poll `/dev`); the node service runs them on a blocking thread.

use std::collections::HashSet;
use std::io::Read;
use std::net::{TcpListener, ToSocketAddrs};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{bail, Context as _};
use tracing::{info, warn};

/// Find a free TCP port on `host` at or after `start`, scanning up to `span`
/// candidates. Used to give every NBD-mode volume its own listen port so a
/// second volume (or a remount that races a still-flushing previous server) on
/// the same node does not fail with "Address already in use". The caller is
/// expected to hold a lock and spawn the NBD server (which binds the port)
/// before releasing it, so the brief window between probe and bind is safe.
pub fn find_free_port(host: &str, start: u16, span: u16) -> anyhow::Result<u16> {
    for offset in 0..span {
        let port = match start.checked_add(offset) {
            Some(p) => p,
            None => break,
        };
        if TcpListener::bind((host, port)).is_ok() {
            return Ok(port);
        }
    }
    bail!(
        "no free NBD port in {host}:{start}..{}",
        start.saturating_add(span)
    );
}

/// Return the set of currently-present `/dev/ublkbN` device nodes.
pub fn list_ublk_devices() -> HashSet<String> {
    let mut set = HashSet::new();
    if let Ok(entries) = std::fs::read_dir("/dev") {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            // ublk block devices are named `ublkbN` (the control node is
            // `ublk-control`, which we must not match).
            if let Some(rest) = name.strip_prefix("ublkb") {
                if !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit()) {
                    set.insert(format!("/dev/{name}"));
                }
            }
        }
    }
    set
}

/// Forward a long-lived child's stdout/stderr to the node plugin's own
/// stdout/stderr in background threads.
///
/// `spawn_device` pipes the child's output so the connect phase can capture an
/// early-exit error message. Once the device is up and the child becomes
/// long-lived, those pipes must keep being drained — otherwise a chatty child
/// fills the ~64 KiB pipe buffer and blocks indefinitely on its next write.
/// Draining here both prevents that deadlock and surfaces the child's logs in
/// `kubectl logs` of the node pod.
pub fn drain_child_output(child: &mut Child) {
    if let Some(mut out) = child.stdout.take() {
        std::thread::spawn(move || {
            let _ = std::io::copy(&mut out, &mut std::io::stdout());
        });
    }
    if let Some(mut err) = child.stderr.take() {
        std::thread::spawn(move || {
            let _ = std::io::copy(&mut err, &mut std::io::stderr());
        });
    }
}

/// Return the set of available (unused) `/dev/nbd*` device nodes for NBD mode.
pub fn list_available_nbd_devices() -> HashSet<String> {
    let mut set = HashSet::new();
    if let Ok(entries) = std::fs::read_dir("/dev") {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(rest) = name.strip_prefix("nbd") {
                if !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit()) {
                    let dev_path = format!("/dev/{name}");
                    // A device is in use once nbd-client connects it: the kernel
                    // then exposes /sys/block/nbdN/pid. (Checking for a `p1`
                    // partition is unreliable — the plugin formats/mounts the
                    // whole device, so a busy device never has a partition node.)
                    let in_use = Path::new(&format!("/sys/block/{name}/pid")).exists();
                    if !in_use {
                        set.insert(dev_path);
                    }
                }
            }
        }
    }
    set
}

/// Spawn `ublk-azblob run --size <size>` (or with `--nbd` for NBD mode) as a child process.
///
/// `env` carries the storage selectors and credentials (`AZURE_STORAGE_*`).
/// `nbd_listen` optionally enables NBD mode with the given listen address (e.g. `127.0.0.1:10809`).
/// The child keeps the device alive until it is signalled.
pub fn spawn_device(
    size: u64,
    env: &[(String, String)],
    nbd_listen: Option<String>,
) -> anyhow::Result<Child> {
    let exe = std::env::current_exe().context("resolve current executable")?;
    let mut cmd = Command::new(exe);
    cmd.arg("run")
        .arg("--size")
        .arg(size.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(ref listen) = nbd_listen {
        info!(listen = %listen, "spawning NBD server");
        cmd.arg("--nbd").arg(listen);
    } else {
        info!("spawning ublk device");
    }

    for (k, v) in env {
        cmd.env(k, v);
    }
    let child = cmd.spawn().context("spawn ublk-azblob run")?;
    info!(
        pid = child.id(),
        nbd_mode = nbd_listen.is_some(),
        "spawned device process"
    );
    Ok(child)
}

/// Wait until a `/dev/ublkbN` node appears that was not in `before`, returning
/// its path.  Fails if the child exits first or the timeout elapses.
pub fn wait_for_new_device(
    before: &HashSet<String>,
    child: &mut Child,
    timeout: Duration,
) -> anyhow::Result<String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(dev) = list_ublk_devices().difference(before).next().cloned() {
            info!(device = %dev, "ublk device appeared");
            return Ok(dev);
        }
        if let Ok(Some(status)) = child.try_wait() {
            bail!("ublk-azblob exited before a device appeared: {status}");
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    bail!("timed out waiting for a new ublk device");
}

/// For NBD mode: wait for the server to start, then connect with nbd-client.
/// Returns the `/dev/nbdN` device path.
pub fn wait_and_connect_nbd(
    nbd_listen: &str,
    child: &mut Child,
    timeout: Duration,
) -> anyhow::Result<String> {
    // Parse host:port from listen address
    let parts: Vec<&str> = nbd_listen.split(':').collect();
    if parts.len() != 2 {
        bail!("invalid NBD listen address: {}", nbd_listen);
    }
    let host = parts[0];
    let port = parts[1];
    let port_num: u16 = port
        .parse()
        .with_context(|| format!("invalid NBD port in listen address: {nbd_listen}"))?;

    // Resolve the listen address once so the readiness probe can use
    // `connect_timeout` without panicking on a non-IP host (the host comes from
    // user-configurable CSI env vars and may be a hostname like `localhost`).
    let addr = (host, port_num)
        .to_socket_addrs()
        .with_context(|| format!("resolve NBD listen address: {nbd_listen}"))?
        .next()
        .with_context(|| format!("no address resolved for NBD listen address: {nbd_listen}"))?;

    let deadline = Instant::now() + timeout;

    // Poll for the child to start listening or exit, honouring `timeout`.
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));

        // Check if child exited
        if let Ok(Some(status)) = child.try_wait() {
            // Try to read any stderr output from the failed child
            let mut stderr = String::new();
            if let Some(ref mut err) = child.stderr {
                let _ = err.read_to_string(&mut stderr);
            }
            let mut stdout = String::new();
            if let Some(ref mut out) = child.stdout {
                let _ = out.read_to_string(&mut stdout);
            }
            bail!(
                "ublk-azblob NBD server exited before connecting: {} stderr: {} stdout: {}",
                status,
                stderr.trim(),
                stdout.trim()
            );
        }

        // Try to connect to see if the server is ready.
        if let Ok(stream) = std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(100))
        {
            drop(stream);
            break; // Server is listening
        }
    }

    // Final check if child is still alive
    if let Ok(Some(status)) = child.try_wait() {
        let mut stderr = String::new();
        if let Some(ref mut err) = child.stderr {
            let _ = err.read_to_string(&mut stderr);
        }
        bail!(
            "ublk-azblob NBD server exited before connecting: {} stderr: {}",
            status,
            stderr
        );
    }

    // Find an available NBD device
    let available = list_available_nbd_devices();
    let nbd_dev = available
        .iter()
        .next()
        .context("no available /dev/nbd* devices")?
        .clone();

    info!(device = %nbd_dev, host = %host, port = %port, "connecting NBD client");

    // Connect with nbd-client
    // Use -L (--nonetlink) for compatibility with older kernels
    let output = Command::new("nbd-client")
        .arg(host)
        .arg(port)
        .arg(&nbd_dev)
        .arg("-L") // Disable netlink, use legacy ioctl interface
        .output()
        .context("failed to run nbd-client")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("nbd-client failed: {stderr}");
    }

    // Verify the device is now connected *and* the kernel has propagated a
    // non-zero size. nbd-client returns as soon as it has handed the socket to
    // the kernel, but `/dev/nbdN`'s size is published a moment later; running
    // mkfs too early fails with "Device size reported to be zero". Poll
    // `blockdev --getsize64` until it reports a non-zero size.
    let mut last_size: u64 = 0;
    while Instant::now() < deadline {
        if Path::new(&nbd_dev).exists() {
            if let Ok(output) = Command::new("blockdev")
                .arg("--getsize64")
                .arg(&nbd_dev)
                .output()
            {
                if output.status.success() {
                    last_size = String::from_utf8_lossy(&output.stdout)
                        .trim()
                        .parse()
                        .unwrap_or(0);
                    if last_size > 0 {
                        info!(device = %nbd_dev, size = last_size, "NBD device connected");
                        return Ok(nbd_dev);
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(250));
    }

    bail!(
        "NBD device {} did not report a non-zero size (last={last_size}) before timeout",
        nbd_dev
    );
}

/// True if `dev` already carries a recognised filesystem (via `blkid`).
pub fn has_filesystem(dev: &str) -> bool {
    // `blkid` exits 0 and prints a TYPE= when a filesystem signature is found,
    // and exits 2 when the device is blank.
    Command::new("blkid")
        .arg(dev)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Built-in `mkfs` and mount options for a supported filesystem type.
///
/// A profile bundles the sensible defaults for a filesystem so a StorageClass
/// only has to pick a type (`newBlobFsType` / `templateBlobFsType`) instead of
/// hand-rolling `mkfs`/mount flags.
pub struct FsProfile {
    /// Options passed to `mkfs.<fs>` before the device argument, or `None` when
    /// the filesystem cannot be created on a live device (mount-only image /
    /// pool filesystems and unknown types).
    pub mkfs_options: Option<&'static [&'static str]>,
    /// Default mount `-o` options applied when mounting the filesystem.
    pub mount_options: &'static [&'static str],
}

/// Return the built-in formatting/mount profile for `fs_type`.
///
/// Supported profiles cover ext2/3/4, xfs, btrfs, squashfs, zfs and ntfs.
/// squashfs, zfs and ntfs are image / pool filesystems that we only ever mount
/// from a template (never freshly format on a live device), so they have no
/// `mkfs` options (`None`). Unknown types also map to `None`, so [`mkfs`]
/// rejects them instead of shelling out to a non-existent `mkfs.<fs>`.
pub fn fs_profile(fs_type: &str) -> FsProfile {
    match fs_type {
        "ext2" | "ext3" | "ext4" => FsProfile {
            // `-F` forces creation without interactive confirmation; `-E
            // nodiscard` avoids a full TRIM pass (faster, and discard maps onto
            // Clear Pages); lazy_itable_init / lazy_journal_init speed up mkfs on
            // large devices (no zeroing of inode tables and journal).
            mkfs_options: Some(&[
                "-F",
                "-E",
                "nodiscard,lazy_itable_init=1,lazy_journal_init=1",
            ]),
            mount_options: &[],
        },
        // `-f` forces formatting even when an old signature is present.
        "xfs" => FsProfile {
            mkfs_options: Some(&["-f"]),
            mount_options: &[],
        },
        "btrfs" => FsProfile {
            mkfs_options: Some(&["-f"]),
            mount_options: &[],
        },
        // Image / pool filesystems: only ever mounted from a template, never
        // formatted on a freshly-provisioned device. squashfs/zfs are read-only
        // images; ntfs is a template-only image too — we do not create NTFS
        // (no reliable read-write NTFS formatting, and `mkfs.ntfs` isn't shipped
        // in the runtime image), so it can only be mounted from a template.
        "squashfs" | "zfs" | "ntfs" => FsProfile {
            mkfs_options: None,
            mount_options: &[],
        },
        _ => FsProfile {
            mkfs_options: None,
            mount_options: &[],
        },
    }
}

/// Create a filesystem of type `fs_type` on `dev` (only call on a blank device).
///
/// The built-in `mkfs` options come from the filesystem [`fs_profile`]. Returns
/// an error for filesystem types that cannot be created on a live device
/// (mount-only image / pool filesystems such as squashfs/zfs, and unknown
/// types).
pub fn mkfs(dev: &str, fs_type: &str) -> anyhow::Result<()> {
    let mkfs_options = fs_profile(fs_type).mkfs_options.ok_or_else(|| {
        anyhow::anyhow!("filesystem type {fs_type:?} does not support mkfs (cannot format {dev})")
    })?;
    let mkfs_bin = format!("mkfs.{fs_type}");
    info!(dev, fs_type, "creating filesystem");
    let mut args: Vec<&str> = mkfs_options.to_vec();
    args.push(dev);
    run(&mkfs_bin, &args)
}

/// How to run `fsck` on a device before mounting it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsckMode {
    /// Don't run `fsck` (default).
    Off,
    /// `fsck -a`: automatically repair (preen) the filesystem; only minor,
    /// safe-to-fix problems are corrected without prompting.
    Preen,
    /// `fsck -f -y`: force a full check even on a clean filesystem and answer
    /// "yes" to every repair prompt.
    Force,
}

impl FsckMode {
    /// Parse a volume-context `fsck` value. Recognised values (case-insensitive):
    /// `""`/`false`/`off`/`no`/`none`/`0` ⇒ [`FsckMode::Off`];
    /// `true`/`auto`/`preen`/`yes`/`on`/`1` ⇒ [`FsckMode::Preen`];
    /// `force`/`full` ⇒ [`FsckMode::Force`].
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "false" | "off" | "no" | "none" | "0" => Ok(FsckMode::Off),
            "true" | "auto" | "preen" | "yes" | "on" | "1" => Ok(FsckMode::Preen),
            "force" | "full" => Ok(FsckMode::Force),
            other => bail!(
                "invalid fsck value {other:?}; expected one of \
                 false/off, true/preen/auto, or force"
            ),
        }
    }
}

/// Run `fsck` on `dev` according to `mode` before mounting.
///
/// Only call this on a writable device that already carries a filesystem.
/// `fsck` repairs in place, so the backing device must be read-write. Exit
/// codes are interpreted per the `fsck(8)` bitmask: `0` (clean) and `1`
/// (errors corrected) are treated as success; anything else — including `2`
/// (corrected, reboot recommended), `4` (errors left uncorrected) and operational
/// failures — is an error.
pub fn fsck(dev: &str, fs_type: &str, mode: FsckMode) -> anyhow::Result<()> {
    let args: Vec<&str> = match mode {
        FsckMode::Off => return Ok(()),
        // `-a` preens (auto-repair without prompting); non-interactive.
        FsckMode::Preen => vec!["-t", fs_type, "-a", dev],
        // `-f` forces a full check, `-y` answers yes to every prompt.
        FsckMode::Force => vec!["-t", fs_type, "-f", "-y", dev],
    };
    info!(dev, fs_type, ?mode, "running fsck");
    let output = Command::new("fsck")
        .args(&args)
        .output()
        .with_context(|| format!("spawn `fsck {}`", args.join(" ")))?;
    // `fsck` returns a bitmask: bit 0 (1) = errors corrected, bit 1 (2) = reboot
    // recommended, bit 2 (4) = errors left uncorrected, bit 3 (8) = operational
    // error, etc. Treat 0 (clean) and 1 (corrected) as success.
    if let Some(code) = output.status.code() {
        if code == 0 || code == 1 {
            if code == 1 {
                warn!(dev, "fsck corrected filesystem errors");
            }
            return Ok(());
        }
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    bail!(
        "`fsck {}` failed ({}): {} {}",
        args.join(" "),
        output.status,
        stdout.trim(),
        stderr.trim()
    );
}

/// Mount `dev` at `target`, creating the mount point if needed.
pub fn mount(
    dev: &str,
    target: &str,
    fs_type: &str,
    mount_flags: &[String],
    readonly: bool,
) -> anyhow::Result<()> {
    std::fs::create_dir_all(target).with_context(|| format!("create mount target {target}"))?;

    let mut args: Vec<String> = vec!["-t".into(), fs_type.into()];
    let mut options: Vec<String> = mount_flags.to_vec();
    if readonly {
        options.push("ro".into());
        // The backing block device is hardware read-only, so the kernel cannot
        // replay an ext journal. For an unclean ext2/3/4 image (e.g. a
        // crash-consistent snapshot) a plain `-o ro` mount fails with "write
        // access unavailable, cannot proceed (try mounting with noload)". Add
        // `noload` so journal recovery is skipped and the image mounts read-only.
        if matches!(fs_type, "ext2" | "ext3" | "ext4") && !options.iter().any(|o| o == "noload") {
            options.push("noload".into());
        }
    }
    if !options.is_empty() {
        args.push("-o".into());
        args.push(options.join(","));
    }
    args.push(dev.into());
    args.push(target.into());

    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    info!(dev, target, fs_type, "mounting");
    run("mount", &arg_refs)
}

/// Node-local scratch directories backing an *ephemeral overlay* mount.
///
/// For a read-only (snapshot) volume the immutable filesystem is mounted at
/// `lower` and an `overlayfs` is stacked on top with a writable `upper` (the
/// pod-local change layer) and an empty `work` dir. The merged, writable view
/// is presented at the CSI target path. The scratch lives on node disk and is
/// discarded on unpublish, so writes never reach the immutable backing blob.
///
/// `lower` is always placed as a hidden sibling of the CSI target (it is only a
/// mount point for the immutable filesystem). The writable `upper`/`work` pair
/// must share one filesystem outside the merged mount (an overlayfs
/// requirement); where they live depends on the layout:
///
///   * Default (`scratch_root = None`): `upper`/`work` are hidden siblings of
///     the target too, on the per-volume kubelet directory's filesystem, and
///     are naturally removed with that directory on unpublish.
///   * Configured scratch base (`scratch_root = Some(_)`): `upper`/`work` live
///     under an operator-chosen base (`overlayScratchDir`), on that base's
///     filesystem — not on the target's — and are pruned explicitly via
///     `scratch_root` on teardown so nothing leaks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OverlayDirs {
    /// Read-only mount point of the immutable lower filesystem.
    pub lower: String,
    /// Writable upper layer (pod-local changes land here).
    pub upper: String,
    /// overlayfs work directory (must be empty and on the same fs as `upper`).
    pub work: String,
    /// Per-volume scratch root holding `upper`/`work` when an operator-chosen
    /// scratch base is used (`None` for the default hidden-sibling layout).
    /// Removed on teardown so nothing leaks on the configured filesystem.
    pub scratch_root: Option<String>,
}

/// Sanitize a string to a filesystem-safe component (alphanumerics plus `-` and
/// `_`), so an operator-supplied scratch base plus the volume id can't traverse
/// paths (e.g. `..`) outside the configured scratch directory.
fn sanitize_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out
}

/// Derive the per-volume overlay scratch directories from the CSI `target` path.
///
/// `lower` is always a hidden sibling of `target` (it is only a mount point for
/// the immutable filesystem). The writable `upper`/`work` pair must live on a
/// single filesystem:
///
///   * With `scratch_base = None` (default) they are hidden siblings of `target`
///     too — i.e. on the per-volume kubelet directory's filesystem — and are
///     cleaned up together with the volume. Behaviour is unchanged.
///   * With `scratch_base = Some(dir)` they are placed under
///     `<dir>/<sanitized volume id>/{upper,work}`, letting operators steer the
///     ephemeral write scratch onto a chosen filesystem (e.g. an SSD or tmpfs
///     mount). The per-volume subdirectory keeps concurrent volumes isolated and
///     is removed on teardown.
pub fn overlay_dirs(target: &str, scratch_base: Option<&str>, volume_id: &str) -> OverlayDirs {
    let parent = Path::new(target).parent().unwrap_or_else(|| Path::new("/"));
    let sibling = |name: &str| parent.join(name).to_string_lossy().into_owned();
    let lower = sibling(".ublk-overlay-lower");
    match scratch_base {
        Some(base) => {
            let root = Path::new(base).join(sanitize_component(volume_id));
            let upper = root.join("upper").to_string_lossy().into_owned();
            let work = root.join("work").to_string_lossy().into_owned();
            OverlayDirs {
                lower,
                upper,
                work,
                scratch_root: Some(root.to_string_lossy().into_owned()),
            }
        }
        None => OverlayDirs {
            lower,
            upper: sibling(".ublk-overlay-upper"),
            work: sibling(".ublk-overlay-work"),
            scratch_root: None,
        },
    }
}

/// Mount `dev` read-only as the overlay lower, then stack a writable overlayfs
/// with a node-local upper/work at `target`.
///
/// `dev` carries the immutable (snapshot) filesystem; it is mounted read-only at
/// `dirs.lower` (reusing the read-only mount path, including the ext `noload`
/// fixup), then an `overlay` filesystem is mounted at `target` presenting a
/// writable merged view whose writes land in `dirs.upper`.
pub fn mount_overlay(
    dev: &str,
    target: &str,
    fs_type: &str,
    mount_flags: &[String],
    dirs: &OverlayDirs,
) -> anyhow::Result<()> {
    // Start from clean scratch dirs: a stale, non-empty work dir makes the
    // overlay mount fail, and a leftover upper would resurrect prior writes.
    let _ = std::fs::remove_dir_all(&dirs.upper);
    let _ = std::fs::remove_dir_all(&dirs.work);
    for d in [&dirs.lower, &dirs.upper, &dirs.work] {
        std::fs::create_dir_all(d).with_context(|| format!("create overlay dir {d}"))?;
    }

    // Mount the immutable lower read-only (ro + ext `noload` are added by
    // `mount` when `readonly` is set).
    mount(dev, &dirs.lower, fs_type, mount_flags, true)?;

    std::fs::create_dir_all(target).with_context(|| format!("create overlay target {target}"))?;
    let opts = format!(
        "lowerdir={},upperdir={},workdir={}",
        dirs.lower, dirs.upper, dirs.work
    );
    info!(target, lower = %dirs.lower, "mounting ephemeral overlay");
    if let Err(e) = run("mount", &["-t", "overlay", "overlay", "-o", &opts, target]) {
        // Roll back the lower mount so a failed overlay doesn't leak it.
        let _ = umount(&dirs.lower);
        return Err(e);
    }
    Ok(())
}

/// Tear down an ephemeral overlay: unmount the merged view and the lower, then
/// remove the node-local scratch (discarding all pod-local writes). Idempotent.
pub fn umount_overlay(target: &str, dirs: &OverlayDirs) -> anyhow::Result<()> {
    umount(target)?;
    umount(&dirs.lower)?;
    // Best-effort scratch removal; the writes are already gone once unmounted.
    for d in [&dirs.upper, &dirs.work, &dirs.lower] {
        if let Err(e) = std::fs::remove_dir_all(d) {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!(dir = %d, error = %e, "overlay scratch removal failed");
            }
        }
    }
    // For an operator-configured scratch base, also prune the per-volume root
    // (which holds `upper`/`work`) so nothing leaks on the chosen filesystem.
    if let Some(root) = &dirs.scratch_root {
        if let Err(e) = std::fs::remove_dir_all(root) {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!(dir = %root, error = %e, "overlay scratch root removal failed");
            }
        }
    }
    Ok(())
}

/// Unmount `target` (idempotent: a "not mounted" result is treated as success).
pub fn umount(target: &str) -> anyhow::Result<()> {
    if !Path::new(target).exists() {
        return Ok(());
    }
    let output = Command::new("umount")
        .arg(target)
        .output()
        .with_context(|| format!("spawn umount {target}"))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    if stderr.contains("not mounted") || stderr.contains("not currently mounted") {
        warn!(target, "umount: target was not mounted");
        return Ok(());
    }
    bail!("umount {target} failed: {}", stderr.trim());
}

/// Send `sig` to process `pid`, logging (but not failing) if delivery fails.
pub fn signal_pid(pid: u32, sig: i32) {
    // SAFETY: `kill` is safe to call with any pid / signal number.
    let rc = unsafe { libc::kill(pid as libc::pid_t, sig) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        warn!("kill(pid={pid}, sig={sig}) failed: {err}");
    }
}

/// Run a command to completion, returning an error (with captured stderr) on
/// non-zero exit.
fn run(cmd: &str, args: &[&str]) -> anyhow::Result<()> {
    let output = Command::new(cmd)
        .args(args)
        .output()
        .with_context(|| format!("spawn `{cmd}`"))?;
    if !output.status.success() {
        bail!(
            "`{cmd} {}` failed ({}): {}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// Persisted, node-local record of a published volume, written next to the CSI
/// target on `NodePublishVolume` and reloaded on node-plugin startup.
///
/// The node service keeps its live volume table (`volume_id → device/child`)
/// only in memory, so a plugin restart loses it while the detached device child
/// keeps running (so active mounts survive the restart). Without a way to
/// re-associate the surviving device with its `volume_id`, a later
/// `NodeUnpublishVolume` cannot tear the device / overlay lower down and it
/// leaks (the kubelet volume reconciler then keeps probing the orphaned device,
/// logging `Buffer I/O error … async page read`). This record lets startup
/// recovery rebuild that association from the still-present mount.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeMeta {
    pub volume_id: String,
    /// Backing device node (`/dev/nbdN` or `/dev/ublkbN`).
    pub device: String,
    /// PID of the `ublk-azblob run` child serving the device.
    pub pid: u32,
    /// Whether the device is served over NBD (`/dev/nbdN`) vs ublk.
    pub nbd: bool,
    /// Overlay scratch dirs when the volume is published through an ephemeral
    /// overlay; `None` for a plain (directly mounted) volume.
    pub overlay: Option<OverlayDirs>,
}

/// File name of the per-volume metadata sidecar, written as a hidden sibling of
/// the CSI target (same directory as `.ublk-overlay-lower`), so it lives on the
/// per-volume kubelet directory and is naturally discarded with the volume.
const VOLUME_META_NAME: &str = ".ublk-volume-meta";

/// Path of the metadata sidecar for `target`.
pub fn meta_path(target: &str) -> std::path::PathBuf {
    Path::new(target)
        .parent()
        .unwrap_or_else(|| Path::new("/"))
        .join(VOLUME_META_NAME)
}

impl VolumeMeta {
    /// Serialize to a simple tab-separated `key\tvalue` line format (no external
    /// serialization dependency; paths never contain tabs or newlines).
    pub fn serialize(&self) -> String {
        let mut s = String::new();
        s.push_str(&format!("volume_id\t{}\n", self.volume_id));
        s.push_str(&format!("device\t{}\n", self.device));
        s.push_str(&format!("pid\t{}\n", self.pid));
        s.push_str(&format!("nbd\t{}\n", self.nbd));
        if let Some(o) = &self.overlay {
            s.push_str("overlay\ttrue\n");
            s.push_str(&format!("lower\t{}\n", o.lower));
            s.push_str(&format!("upper\t{}\n", o.upper));
            s.push_str(&format!("work\t{}\n", o.work));
            if let Some(root) = &o.scratch_root {
                s.push_str(&format!("scratch_root\t{root}\n"));
            }
        } else {
            s.push_str("overlay\tfalse\n");
        }
        s
    }

    /// Parse the tab-separated format produced by [`serialize`](Self::serialize).
    /// Returns `None` if a required field is missing or malformed.
    pub fn parse(text: &str) -> Option<Self> {
        let mut map = std::collections::HashMap::new();
        for line in text.lines() {
            if let Some((k, v)) = line.split_once('\t') {
                map.insert(k.trim(), v.to_string());
            }
        }
        let volume_id = map.get("volume_id")?.clone();
        let device = map.get("device")?.clone();
        let pid = map.get("pid")?.parse().ok()?;
        let nbd = map.get("nbd").map(|v| v == "true").unwrap_or(false);
        let overlay = if map.get("overlay").map(|v| v == "true").unwrap_or(false) {
            Some(OverlayDirs {
                lower: map.get("lower")?.clone(),
                upper: map.get("upper")?.clone(),
                work: map.get("work")?.clone(),
                scratch_root: map.get("scratch_root").cloned(),
            })
        } else {
            None
        };
        Some(VolumeMeta {
            volume_id,
            device,
            pid,
            nbd,
            overlay,
        })
    }
}

/// Write the metadata sidecar for `target` atomically (temp file + rename) so a
/// crash mid-write never leaves a torn record.
pub fn write_volume_meta(target: &str, meta: &VolumeMeta) -> anyhow::Result<()> {
    let path = meta_path(target);
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, meta.serialize())
        .with_context(|| format!("write volume meta {}", tmp.display()))?;
    std::fs::rename(&tmp, &path)
        .with_context(|| format!("rename volume meta into place {}", path.display()))?;
    Ok(())
}

/// Read and parse the metadata sidecar for `target`, if present and valid.
pub fn read_volume_meta(target: &str) -> Option<VolumeMeta> {
    let text = std::fs::read_to_string(meta_path(target)).ok()?;
    VolumeMeta::parse(&text)
}

/// Remove the metadata sidecar for `target` (idempotent).
pub fn remove_volume_meta(target: &str) {
    let path = meta_path(target);
    if let Err(e) = std::fs::remove_file(&path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            warn!(path = %path.display(), error = %e, "failed to remove volume meta");
        }
    }
}

/// One parsed line of `/proc/self/mountinfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountEntry {
    pub mountpoint: String,
    pub fstype: String,
    pub source: String,
    pub super_opts: String,
}

/// Parse `/proc/self/mountinfo` content into entries. The mountinfo format has a
/// variable number of optional tag fields terminated by a single `-`, followed
/// by `<fstype> <source> <super_opts>`.
pub fn parse_mountinfo(content: &str) -> Vec<MountEntry> {
    let mut out = Vec::new();
    for line in content.lines() {
        let toks: Vec<&str> = line.split_whitespace().collect();
        // Need at least: id parent maj:min root mountpoint opts ... - fstype src opts
        let Some(sep) = toks.iter().position(|&t| t == "-") else {
            continue;
        };
        if toks.len() < 5 || sep + 3 >= toks.len() {
            continue;
        }
        out.push(MountEntry {
            // mountinfo octal-escapes spaces etc.; our paths don't contain them.
            mountpoint: toks[4].to_string(),
            fstype: toks[sep + 1].to_string(),
            source: toks[sep + 2].to_string(),
            super_opts: toks[sep + 3].to_string(),
        });
    }
    out
}

/// From parsed mount entries, return the CSI *target* mountpoints this driver
/// owns: overlay merged mounts whose `lowerdir` is one of our `.ublk-overlay-lower`
/// mounts, plus plain volumes where a `/dev/nbdN`/`/dev/ublkbN` device is mounted
/// directly at the target. The `.ublk-overlay-lower` mounts themselves are
/// excluded (they are the lower, not a target).
pub fn our_target_mounts(entries: &[MountEntry]) -> Vec<String> {
    let mut targets = Vec::new();
    for e in entries {
        if e.mountpoint.ends_with("/.ublk-overlay-lower") {
            continue; // the lower, reached via its overlay target instead
        }
        let is_overlay_ours =
            e.fstype == "overlay" && e.super_opts.contains("/.ublk-overlay-lower");
        let is_device_ours = (e.source.starts_with("/dev/nbd")
            || e.source.starts_with("/dev/ublkb"))
            && !e.mountpoint.ends_with("/.ublk-overlay-lower");
        if is_overlay_ours || is_device_ours {
            targets.push(e.mountpoint.clone());
        }
    }
    targets
}

/// Read `/proc/self/mountinfo` and return this driver's live target mountpoints.
pub fn scan_our_targets() -> Vec<String> {
    match std::fs::read_to_string("/proc/self/mountinfo") {
        Ok(content) => our_target_mounts(&parse_mountinfo(&content)),
        Err(e) => {
            warn!(error = %e, "could not read /proc/self/mountinfo for recovery");
            Vec::new()
        }
    }
}

/// PID of the NBD server/client backing `/dev/nbdN`, read from
/// `/sys/block/nbdN/pid`; `None` if the device is not connected.
pub fn nbd_pid(device: &str) -> Option<u32> {
    let name = Path::new(device).file_name()?.to_str()?;
    let text = std::fs::read_to_string(format!("/sys/block/{name}/pid")).ok()?;
    text.trim().parse().ok()
}

/// Whether process `pid` is currently alive (`kill(pid, 0)` succeeds).
pub fn pid_alive(pid: u32) -> bool {
    // SAFETY: `kill` with signal 0 only probes for the process; no signal sent.
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

/// Poll until `pid` exits or `timeout` elapses. Returns `true` if it exited.
pub fn wait_pid_exit(pid: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !pid_alive(pid) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    !pid_alive(pid)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Filesystems whose built-in profile must be able to format a fresh blob.
    const FORMATTABLE: &[&str] = &["ext2", "ext3", "ext4", "xfs", "btrfs"];
    /// Mount-only image / pool filesystems: never freshly formatted on a live
    /// device (squashfs/zfs are read-only images; ntfs has no reliable
    /// read-write mkfs we ship), so their profile must reject `mkfs`.
    const MOUNT_ONLY: &[&str] = &["squashfs", "zfs", "ntfs"];

    #[test]
    fn fsck_mode_parses_off_values() {
        for v in ["", "  ", "false", "OFF", "No", "none", "0"] {
            assert_eq!(FsckMode::parse(v).unwrap(), FsckMode::Off, "value: {v:?}");
        }
    }

    #[test]
    fn fsck_mode_parses_preen_values() {
        for v in ["true", "TRUE", "auto", "preen", "yes", "on", "1"] {
            assert_eq!(FsckMode::parse(v).unwrap(), FsckMode::Preen, "value: {v:?}");
        }
    }

    #[test]
    fn fsck_mode_parses_force_values() {
        for v in ["force", "Full"] {
            assert_eq!(FsckMode::parse(v).unwrap(), FsckMode::Force, "value: {v:?}");
        }
    }

    #[test]
    fn fsck_mode_rejects_unknown_values() {
        assert!(FsckMode::parse("maybe").is_err());
    }

    #[test]
    fn overlay_dirs_are_hidden_siblings_of_target() {
        let dirs = overlay_dirs(
            "/var/lib/kubelet/pods/abc/volumes/x/pvc-1/mount",
            None,
            "pvc-1",
        );
        let parent = "/var/lib/kubelet/pods/abc/volumes/x/pvc-1";
        assert_eq!(dirs.lower, format!("{parent}/.ublk-overlay-lower"));
        assert_eq!(dirs.upper, format!("{parent}/.ublk-overlay-upper"));
        assert_eq!(dirs.work, format!("{parent}/.ublk-overlay-work"));
        assert!(dirs.scratch_root.is_none());
        // upper and work must share the target's parent filesystem and must not
        // sit inside the merged target (an overlayfs requirement).
        for d in [&dirs.upper, &dirs.work, &dirs.lower] {
            assert!(!d.starts_with("/var/lib/kubelet/pods/abc/volumes/x/pvc-1/mount/"));
            assert!(d.starts_with(parent));
        }
    }

    #[test]
    fn overlay_dirs_use_configured_scratch_base() {
        let dirs = overlay_dirs(
            "/var/lib/kubelet/pods/abc/volumes/x/pvc-1/mount",
            Some("/mnt/ssd/overlay"),
            "pvc-1",
        );
        let root = "/mnt/ssd/overlay/pvc-1";
        // lower stays a hidden sibling of the target (it is only a mount point).
        assert_eq!(
            dirs.lower,
            "/var/lib/kubelet/pods/abc/volumes/x/pvc-1/.ublk-overlay-lower"
        );
        // upper and work move onto the configured base, under a per-volume root
        // so they share one filesystem and can't collide across volumes.
        assert_eq!(dirs.upper, format!("{root}/upper"));
        assert_eq!(dirs.work, format!("{root}/work"));
        assert_eq!(dirs.scratch_root.as_deref(), Some(root));
    }

    #[test]
    fn overlay_dirs_sanitize_volume_id_for_scratch_base() {
        // A volume id with path separators must not escape the scratch base.
        let dirs = overlay_dirs("/tgt/mount", Some("/mnt/ssd"), "../../etc/evil");
        assert_eq!(
            dirs.scratch_root.as_deref(),
            Some("/mnt/ssd/______etc_evil")
        );
        assert!(dirs.upper.starts_with("/mnt/ssd/______etc_evil/"));
    }

    #[test]
    fn fs_profile_formattable_types_have_mkfs_options() {
        for fs in FORMATTABLE {
            assert!(
                fs_profile(fs).mkfs_options.is_some(),
                "formattable filesystem {fs:?} must have built-in mkfs options"
            );
        }
    }

    #[test]
    fn fs_profile_mount_only_and_unknown_types_have_no_mkfs_options() {
        for fs in MOUNT_ONLY.iter().chain(["not-a-fs", ""].iter()) {
            assert!(
                fs_profile(fs).mkfs_options.is_none(),
                "mount-only / unknown filesystem {fs:?} must not advertise mkfs options"
            );
        }
    }

    /// `mkfs` returns a "format not supported" error (instead of shelling out to
    /// a missing `mkfs.<fs>`) for every filesystem that cannot be created on a
    /// live device.
    #[test]
    fn mkfs_rejects_unformattable_filesystems() {
        for fs in MOUNT_ONLY.iter().chain(["not-a-fs"].iter()) {
            let err = mkfs("/dev/null", fs).expect_err(&format!(
                "mkfs({fs:?}) must fail for an unformattable filesystem"
            ));
            let msg = err.to_string();
            assert!(
                msg.contains("does not support mkfs"),
                "unexpected error for {fs:?}: {msg}"
            );
        }
    }

    fn is_root() -> bool {
        // SAFETY: `geteuid` has no preconditions and never fails.
        unsafe { libc::geteuid() == 0 }
    }

    fn have_tool(bin: &str) -> bool {
        Command::new(bin)
            .arg("-V")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    /// End-to-end check that every formattable profile actually formats and
    /// mounts: back a loop device with a sparse image, run the real
    /// [`mkfs`]/[`mount`]/[`umount`] (which source their flags from the built-in
    /// profile), and round-trip a file through the mounted filesystem.
    ///
    /// Needs root + loop devices + the per-filesystem `mkfs.<fs>` tool; it skips
    /// (rather than fails) when the environment cannot provide them. CI's
    /// `cargo test` job runs as a non-root user, so this skips there; it
    /// exercises the full format+mount path only when invoked as root locally.
    /// In CI the equivalent coverage comes from the `mount_e2e`
    /// `mount_formattable_fs_profiles` integration test, which formats+mounts the
    /// xfs/btrfs profiles on real ublk devices on the privileged e2e runner.
    #[test]
    fn formattable_profiles_format_and_mount() {
        if !is_root() || !have_tool("losetup") {
            eprintln!("skipping formattable_profiles_format_and_mount: needs root + losetup");
            return;
        }

        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let tmp =
            std::env::temp_dir().join(format!("ublk-mkfs-e2e-{}-{unique}", std::process::id()));
        std::fs::create_dir_all(&tmp).expect("create temp dir");

        for fs in FORMATTABLE {
            // ext2/ext3/ext4 all share `mkfs.ext*`; xfs needs ~300 MiB minimum,
            // so size every image generously at 512 MiB (sparse, so cheap).
            if !have_tool(&format!("mkfs.{fs}")) {
                eprintln!("skipping {fs}: mkfs.{fs} not installed");
                continue;
            }

            let img = tmp.join(format!("{fs}.img"));
            {
                let f = std::fs::File::create(&img).expect("create image file");
                f.set_len(512 * 1024 * 1024).expect("size image file");
            }
            let img_str = img.to_str().unwrap();

            // Attach a loop device for the image.
            let out = Command::new("losetup")
                .args(["--find", "--show", img_str])
                .output()
                .expect("spawn losetup");
            if !out.status.success() {
                // No usable loop device (e.g. a root container without loop
                // support): skip rather than fail, matching the documented intent.
                eprintln!(
                    "skipping {fs}: losetup --find failed (no usable loop device): {}",
                    String::from_utf8_lossy(&out.stderr)
                );
                let _ = std::fs::remove_file(&img);
                continue;
            }
            let dev = String::from_utf8(out.stdout).unwrap().trim().to_string();

            let mount_point = tmp.join(format!("{fs}.mnt"));
            let mount_point_str = mount_point.to_str().unwrap();

            // Format and mount via the real profile-driven helpers.
            mkfs(&dev, fs).unwrap_or_else(|e| panic!("mkfs.{fs} failed: {e}"));
            let mount_opts: Vec<String> = fs_profile(fs)
                .mount_options
                .iter()
                .map(|s| s.to_string())
                .collect();
            mount(&dev, mount_point_str, fs, &mount_opts, false)
                .unwrap_or_else(|e| panic!("mount {fs} failed: {e}"));

            // Round-trip a file through the mounted filesystem.
            let payload = b"precooked-fs-roundtrip";
            let file = mount_point.join("probe");
            std::fs::write(&file, payload).expect("write probe file");
            let read = std::fs::read(&file).expect("read probe file");
            assert_eq!(read, payload, "{fs} round-trip mismatch");

            umount(mount_point_str).unwrap_or_else(|e| panic!("umount {fs} failed: {e}"));
            let _ = Command::new("losetup").args(["-d", &dev]).status();
            let _ = std::fs::remove_file(&img);
        }

        let _ = std::fs::remove_dir_all(&tmp);
    }

    // ── recovery / metadata helpers ────────────────────────────────────────

    fn sample_meta(overlay: bool) -> VolumeMeta {
        VolumeMeta {
            volume_id: "vol-123#container#blob".to_string(),
            device: "/dev/nbd9".to_string(),
            pid: 4242,
            nbd: true,
            overlay: overlay.then(|| OverlayDirs {
                lower: "/kubelet/pods/u/volumes/x/.ublk-overlay-lower".to_string(),
                upper: "/kubelet/pods/u/volumes/x/.ublk-overlay-upper".to_string(),
                work: "/kubelet/pods/u/volumes/x/.ublk-overlay-work".to_string(),
                scratch_root: None,
            }),
        }
    }

    #[test]
    fn volume_meta_roundtrips_overlay_and_plain() {
        for overlay in [false, true] {
            let m = sample_meta(overlay);
            let parsed = VolumeMeta::parse(&m.serialize()).expect("parse serialized meta");
            assert_eq!(parsed, m, "roundtrip mismatch (overlay={overlay})");
        }
    }

    #[test]
    fn volume_meta_roundtrips_with_scratch_root() {
        let mut m = sample_meta(true);
        if let Some(o) = &mut m.overlay {
            o.scratch_root = Some("/mnt/ssd/scratch/vol-123".to_string());
        }
        assert_eq!(VolumeMeta::parse(&m.serialize()).unwrap(), m);
    }

    #[test]
    fn volume_meta_parse_rejects_incomplete_or_garbage() {
        assert!(VolumeMeta::parse("volume_id\tv\ndevice\t/dev/nbd0\n").is_none()); // no pid
        assert!(VolumeMeta::parse("device\t/dev/nbd0\npid\t1\n").is_none()); // no volume_id
        assert!(VolumeMeta::parse("volume_id\tv\ndevice\t/dev/nbd0\npid\tNaN\n").is_none());
        // overlay=true but missing lower/upper/work
        assert!(VolumeMeta::parse("volume_id\tv\ndevice\td\npid\t1\noverlay\ttrue\n").is_none());
    }

    #[test]
    fn meta_path_is_hidden_sibling_of_target() {
        let p = meta_path("/kubelet/pods/u/volumes/kubernetes.io~csi/pvc-x/mount");
        assert_eq!(
            p.to_string_lossy(),
            "/kubelet/pods/u/volumes/kubernetes.io~csi/pvc-x/.ublk-volume-meta"
        );
    }

    #[test]
    fn write_read_remove_volume_meta_on_disk() {
        let tmp = std::env::temp_dir().join(format!("ublk-meta-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        let target = tmp.join("mount");
        let target = target.to_string_lossy().into_owned();

        assert!(read_volume_meta(&target).is_none(), "absent → None");
        let m = sample_meta(true);
        write_volume_meta(&target, &m).expect("write meta");
        assert_eq!(read_volume_meta(&target).unwrap(), m);
        remove_volume_meta(&target);
        assert!(read_volume_meta(&target).is_none(), "removed → None");
        remove_volume_meta(&target); // idempotent
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn parse_mountinfo_extracts_mountpoint_fstype_source_opts() {
        let content = "\
790 42 43:9 / /kubelet/pods/u/volumes/x/.ublk-overlay-lower ro,relatime - ext4 /dev/nbd9 ro
791 42 0:55 / /kubelet/pods/u/volumes/x/mount rw,relatime shared:1 - overlay overlay rw,lowerdir=/kubelet/pods/u/volumes/x/.ublk-overlay-lower,upperdir=/a,workdir=/b
1 0 8:1 / / rw - ext4 /dev/sda1 rw";
        let e = parse_mountinfo(content);
        assert_eq!(e.len(), 3);
        assert_eq!(
            e[0].mountpoint,
            "/kubelet/pods/u/volumes/x/.ublk-overlay-lower"
        );
        assert_eq!(e[0].fstype, "ext4");
        assert_eq!(e[0].source, "/dev/nbd9");
        // Optional field (shared:1) before the '-' must not shift parsing.
        assert_eq!(e[1].fstype, "overlay");
        assert!(e[1]
            .super_opts
            .contains("lowerdir=/kubelet/pods/u/volumes/x/.ublk-overlay-lower"));
    }

    #[test]
    fn our_target_mounts_selects_overlay_and_device_but_not_lower() {
        let content = "\
790 42 43:9 / /kubelet/pods/u/volumes/x/.ublk-overlay-lower ro - ext4 /dev/nbd9 ro
791 42 0:55 / /kubelet/pods/u/volumes/x/mount rw - overlay overlay rw,lowerdir=/kubelet/pods/u/volumes/x/.ublk-overlay-lower,upperdir=/a,workdir=/b
792 42 43:5 / /kubelet/pods/v/volumes/y/mount rw - ext4 /dev/ublkb3 rw
793 42 43:1 / /kubelet/pods/w/volumes/z/mount rw - ext4 /dev/nbd4 rw
1 0 8:1 / / rw - ext4 /dev/sda1 rw
2 0 0:99 / /var/log rw - overlay overlay rw,lowerdir=/some/other/dir";
        let mut targets = our_target_mounts(&parse_mountinfo(content));
        targets.sort();
        assert_eq!(
            targets,
            vec![
                "/kubelet/pods/u/volumes/x/mount".to_string(), // overlay w/ our lower
                "/kubelet/pods/v/volumes/y/mount".to_string(), // /dev/ublkb3
                "/kubelet/pods/w/volumes/z/mount".to_string(), // /dev/nbd4
            ],
            "must pick our overlay + device targets, exclude the lower, /, and unrelated overlays"
        );
    }

    #[test]
    fn pid_alive_and_wait_pid_exit() {
        // This process is alive; a reaped/never-used high pid is not.
        assert!(pid_alive(std::process::id()));
        // Spawn a short-lived child, wait for it, then confirm it's gone.
        let mut child = Command::new("true").spawn().expect("spawn true");
        let pid = child.id();
        child.wait().unwrap();
        assert!(
            wait_pid_exit(pid, Duration::from_secs(2)),
            "reaped pid must read as exited"
        );
    }
}

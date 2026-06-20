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

/// Create a filesystem of type `fs_type` on `dev` (only call on a blank device).
pub fn mkfs(dev: &str, fs_type: &str) -> anyhow::Result<()> {
    let mkfs_bin = format!("mkfs.{fs_type}");
    info!(dev, fs_type, "creating filesystem");
    // `-F` forces creation without interactive confirmation; `-E nodiscard`
    // avoids a full TRIM pass (faster, and discard maps onto Clear Pages).
    // For ext4, use lazy_itable_init and lazy_journal_init to speed up mkfs
    // on large devices (avoids writing zeros to entire inode tables and journal).
    let args: Vec<&str> = if fs_type == "ext4" || fs_type == "ext3" || fs_type == "ext2" {
        vec![
            "-F",
            "-E",
            "nodiscard,lazy_itable_init=1,lazy_journal_init=1",
            dev,
        ]
    } else {
        vec![dev]
    };
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
    /// `""`/`false`/`off`/`no`/`none` ⇒ [`FsckMode::Off`];
    /// `true`/`auto`/`preen`/`yes`/`on` ⇒ [`FsckMode::Preen`];
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

#[cfg(test)]
mod tests {
    use super::FsckMode;

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
}

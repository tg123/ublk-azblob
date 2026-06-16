//! Node-side OS helpers: ublk device discovery, `mkfs`, `mount` / `umount`.
//!
//! These are blocking operations (they shell out to `mkfs`, `mount`, `blkid`,
//! `umount` and poll `/dev`); the node service runs them on a blocking thread.

use std::collections::HashSet;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{bail, Context as _};
use tracing::{info, warn};

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

/// Spawn `ublk-azblob run --size <size>` as a child process.
///
/// `env` carries the storage selectors and credentials (`AZURE_STORAGE_*`).
/// The child keeps the ublk device alive until it is signalled.
pub fn spawn_device(size: u64, env: &[(String, String)]) -> anyhow::Result<Child> {
    let exe = std::env::current_exe().context("resolve current executable")?;
    let mut cmd = Command::new(exe);
    cmd.arg("run")
        .arg("--size")
        .arg(size.to_string())
        .stdin(Stdio::null());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let child = cmd.spawn().context("spawn ublk-azblob run")?;
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
    let args: Vec<&str> = if fs_type == "ext4" || fs_type == "ext3" || fs_type == "ext2" {
        vec!["-F", "-E", "nodiscard", dev]
    } else {
        vec![dev]
    };
    run(&mkfs_bin, &args)
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

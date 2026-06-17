//! Node-side OS helpers: ublk device discovery, `mkfs`, `mount` / `umount`.
//!
//! These are blocking operations (they shell out to `mkfs`, `mount`, `blkid`,
//! `umount` and poll `/dev`); the node service runs them on a blocking thread.

use std::collections::HashSet;
use std::io::Read;
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
                    // Check if the device is not in use (no partitions exist)
                    // An unused NBD device won't have entries like /dev/nbd0p1
                    if !Path::new(&format!("{}p1", dev_path)).exists() {
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
pub fn spawn_device(size: u64, env: &[(String, String)], nbd_listen: Option<String>) -> anyhow::Result<Child> {
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
    info!(pid = child.id(), nbd_mode = nbd_listen.is_some(), "spawned device process");
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

    // Wait a bit for the NBD server to start
    let deadline = Instant::now() + timeout;
    
    // Poll for the child to either start listening or exit
    for _attempt in 0..10 {
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
                status, stderr.trim(), stdout.trim()
            );
        }
        
        // Try to connect to see if server is ready
        // Use a quick TCP connection test instead of immediately invoking nbd-client
        if let Ok(stream) = std::net::TcpStream::connect_timeout(
            &format!("{}:{}", host, port).parse().unwrap(),
            Duration::from_millis(100),
        ) {
            drop(stream);
            break;  // Server is listening
        }
    }
    
    // Final check if child is still alive
    if let Ok(Some(status)) = child.try_wait() {
        let mut stderr = String::new();
        if let Some(ref mut err) = child.stderr {
            let _ = err.read_to_string(&mut stderr);
        }
        bail!("ublk-azblob NBD server exited before connecting: {} stderr: {}", status, stderr);
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
        .arg("-L")  // Disable netlink, use legacy ioctl interface
        .output()
        .context("failed to run nbd-client")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("nbd-client failed: {stderr}");
    }

    // Verify the device is now connected
    while Instant::now() < deadline {
        if Path::new(&nbd_dev).exists() {
            // Additional check: see if it's actually connected (has size)
            if let Ok(output) = Command::new("blockdev")
                .arg("--getsize64")
                .arg(&nbd_dev)
                .output()
            {
                if output.status.success() {
                    info!(device = %nbd_dev, "NBD device connected");
                    return Ok(nbd_dev);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(250));
    }

    bail!("NBD device {} did not become ready", nbd_dev);
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
        vec!["-F", "-E", "nodiscard,lazy_itable_init=1,lazy_journal_init=1", dev]
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

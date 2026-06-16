//! Filesystem-level e2e test for `ublk-azblob`, written in Rust.
//!
//! This is the Rust replacement for the old `tests/e2e/mount_test.sh` shell
//! script.  It exercises the *full* stack: a real ublk block device
//! (`/dev/ublkbN`) backed by an Azure Page Blob (Azurite in CI), with an ext4
//! filesystem mounted on top.
//!
//! Cycle:
//!   1. create the page blob and start the ublk device
//!   2. `mkfs.ext4` + mount into a folder
//!   3. write a handful of random files, record their SHA-256 checksums
//!   4. send `SIGUSR1` to force a backend flush
//!   5. unmount and stop the device (tearing down `/dev/ublkbN`)
//!   6. start the device again over the *same* page blob, remount
//!   7. verify every file's SHA-256 matches — proving the data round-tripped
//!      through Put Page / Get Page Ranges and survived the remount
//!
//! Requirements (CI provides these): Linux ≥6.0 with `ublk_drv` loaded, root /
//! `CAP_SYS_ADMIN`, `e2fsprogs` (`mkfs.ext4`), and a running Azurite reachable
//! at `AZURE_STORAGE_ENDPOINT`.
//!
//! The whole test is gated behind the `ublk` Cargo feature; without it the test
//! crate compiles to nothing.  Run it with:
//!
//! ```text
//! sudo -E env "PATH=$PATH" \
//!   AZURE_STORAGE_ENDPOINT="http://127.0.0.1:10000/devstoreaccount1" \
//!   cargo test --release --features ublk --test mount_e2e -- --nocapture
//! ```
#![cfg(feature = "ublk")]

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::thread::sleep;
use std::time::{Duration, Instant};

/// Azurite well-known development account name.
const DEFAULT_ACCOUNT: &str = "devstoreaccount1";
/// Azurite well-known development account key.
const DEFAULT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:10000/devstoreaccount1";
const DEFAULT_CONTAINER: &str = "e2etest";
const DEFAULT_BLOB: &str = "mounttest";

const DEV_ID: &str = "0";
const BLOB_SIZE: u64 = 256 * 1024 * 1024; // 256 MiB
const NUM_FILES: usize = 8;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn log(msg: &str) {
    println!("=== {msg} ===");
}

/// True when the test can actually drive a ublk device: running as root and the
/// `ublk_drv` control node is present.
fn ublk_available() -> bool {
    // SAFETY: `geteuid` has no preconditions and never fails.
    let is_root = unsafe { libc::geteuid() } == 0;
    is_root && Path::new("/dev/ublk-control").exists()
}

/// Run a command to completion and panic if it fails.
fn run(cmd: &str, args: &[&str]) {
    log(&format!("$ {cmd} {}", args.join(" ")));
    let status = Command::new(cmd)
        .args(args)
        .status()
        .unwrap_or_else(|e| panic!("failed to spawn `{cmd}`: {e}"));
    assert!(status.success(), "`{cmd}` failed with {status}");
}

/// Common Azure environment passed to the `ublk-azblob` child process.
fn azure_env(cmd: &mut Command) {
    cmd.env(
        "AZURE_STORAGE_ACCOUNT",
        env_or("AZURE_STORAGE_ACCOUNT", DEFAULT_ACCOUNT),
    )
    .env(
        "AZURE_STORAGE_KEY",
        env_or("AZURE_STORAGE_KEY", DEFAULT_KEY),
    )
    .env(
        "AZURE_STORAGE_ENDPOINT",
        env_or("AZURE_STORAGE_ENDPOINT", DEFAULT_ENDPOINT),
    )
    .env(
        "AZURE_STORAGE_CONTAINER",
        env_or("AZURE_STORAGE_CONTAINER", DEFAULT_CONTAINER),
    )
    .env(
        "AZURE_STORAGE_BLOB",
        env_or("AZURE_STORAGE_BLOB", DEFAULT_BLOB),
    );
}

/// Start the ublk device as a child process and wait for `/dev/ublkbN` to
/// appear.  When `create` is true the page blob is provisioned first.
///
/// The returned `Child` is always `wait()`ed on by the caller (via
/// `stop_device`), so the zombie-process lint does not apply.
#[allow(clippy::zombie_processes)]
fn start_device(dev: &str, create: bool) -> Child {
    log(&format!(
        "starting ublk device {dev} ({})",
        if create {
            "--create"
        } else {
            "reuse existing blob"
        }
    ));
    let bin = env!("CARGO_BIN_EXE_ublk-azblob");
    let mut cmd = Command::new(bin);
    cmd.arg("run")
        .arg("--id")
        .arg(DEV_ID)
        .arg("--size")
        .arg(BLOB_SIZE.to_string());
    if create {
        cmd.arg("--create");
    }
    azure_env(&mut cmd);

    let mut child = cmd.spawn().expect("failed to spawn ublk-azblob");

    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if Path::new(dev).exists() {
            log(&format!("device {dev} is up (pid {})", child.id()));
            return child;
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("ublk-azblob exited before {dev} appeared: {status}");
        }
        sleep(Duration::from_secs(1));
    }
    let _ = child.kill();
    panic!("timed out waiting for {dev}");
}

/// Send `sig` to the running child process.
fn signal(child: &Child, sig: i32) {
    // SAFETY: `kill` is safe to call with a valid pid and signal number.
    let rc = unsafe { libc::kill(child.id() as libc::pid_t, sig) };
    assert_eq!(rc, 0, "kill({sig}) failed");
}

/// Stop the running device cleanly via `SIGINT` and wait for it to exit.
fn stop_device(dev: &str, mut child: Child) {
    log(&format!("stopping ublk device {dev} (pid {})", child.id()));
    signal(&child, libc::SIGINT);
    let _ = child.wait();
    // Give the kernel a moment to remove the device node.
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && Path::new(dev).exists() {
        sleep(Duration::from_secs(1));
    }
}

/// SHA-256 of a file, as a lowercase hex string.
fn sha256_file(path: &Path) -> String {
    let mut file = fs::File::open(path).unwrap_or_else(|e| panic!("open {path:?}: {e}"));
    let mut hasher = hmac_sha256::Hash::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).expect("read file");
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

/// Write `len` random bytes from `/dev/urandom` to `path`.
fn write_random_file(path: &Path, len: usize) {
    let mut urandom = fs::File::open("/dev/urandom").expect("open /dev/urandom");
    let mut data = vec![0u8; len];
    urandom.read_exact(&mut data).expect("read /dev/urandom");
    fs::write(path, &data).unwrap_or_else(|e| panic!("write {path:?}: {e}"));
}

/// Create a unique temporary directory under the system temp dir.
fn tempdir(prefix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("{prefix}-{nanos}-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[test]
fn mount_roundtrip() {
    if !ublk_available() {
        eprintln!(
            "skipping mount_roundtrip: requires root and a loaded ublk_drv \
             (no /dev/ublk-control or not running as root)"
        );
        return;
    }

    let dev = format!("/dev/ublkb{DEV_ID}");
    let mnt = tempdir("ublk-azblob-mnt");

    // ── Phase 1: provision device, make a filesystem, write random files ──────
    let child = start_device(&dev, true);

    log(&format!("mkfs.ext4 on {dev}"));
    run("mkfs.ext4", &["-q", "-F", "-E", "nodiscard", &dev]);

    log(&format!("mounting {dev} at {}", mnt.display()));
    run("mount", &[&dev, mnt.to_str().unwrap()]);

    log(&format!("writing {NUM_FILES} random files"));
    let mut checksums: Vec<(String, String)> = Vec::with_capacity(NUM_FILES);
    for i in 1..=NUM_FILES {
        let name = format!("random_{i}.bin");
        let path = mnt.join(&name);
        // Deterministic-but-varied size between 1 KiB and ~4 MiB.
        let len = 1024 * (1 + (i * 509) % 4096);
        write_random_file(&path, len);
        checksums.push((name, sha256_file(&path)));
    }
    for (name, sum) in &checksums {
        println!("{sum}  {name}");
    }

    log("sync + SIGUSR1 to force flush to the page blob");
    run("sync", &[]);
    signal(&child, libc::SIGUSR1);
    sleep(Duration::from_secs(2));

    // ── Phase 2: unmount, tear the device down ────────────────────────────────
    log(&format!("unmounting {}", mnt.display()));
    run("umount", &[mnt.to_str().unwrap()]);
    stop_device(&dev, child);

    // ── Phase 3: remount over the same blob and verify checksums ──────────────
    let child = start_device(&dev, false);

    log(&format!("remounting {dev} at {}", mnt.display()));
    run("mount", &[&dev, mnt.to_str().unwrap()]);

    log("verifying checksums after remount");
    for (name, expected) in &checksums {
        let path = mnt.join(name);
        let actual = sha256_file(&path);
        assert_eq!(
            &actual, expected,
            "checksum mismatch for {name} after remount"
        );
        println!("{name}: OK");
    }

    log(&format!("unmounting {}", mnt.display()));
    run("umount", &[mnt.to_str().unwrap()]);
    stop_device(&dev, child);

    let _ = fs::remove_dir_all(&mnt);

    log("mount e2e PASSED ✓");
}

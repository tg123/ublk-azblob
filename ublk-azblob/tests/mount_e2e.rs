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
//!
//! A second test, [`mount_read_only`](fn.mount_read_only.html), exercises
//! `run --read-only`: it asserts the kernel marks `/dev/ublkbN` read-only,
//! verifies the data is still readable, and confirms a write to the read-only
//! mount fails without mutating the backing blob.
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

// High device id (away from 0,1,…) so these tests don't collide with the ublk
// devices the k8s CSI e2e auto-assigns from 0 when the whole suite runs together.
const DEV_ID: &str = "40";
const BLOB_SIZE: u64 = 256 * 1024 * 1024; // 256 MiB
const NUM_FILES: usize = 8;

/// Parameters identifying a single device instance for a test run.
///
/// Each test uses its own `dev_id`, container and blob so independent tests
/// never collide on `/dev/ublkbN` or on the backing blob.  `cache_dir`, when
/// `Some`, enables the persistent local-disk cache layer.
struct DeviceSpec {
    dev_id: String,
    container: String,
    blob: String,
    cache_dir: Option<PathBuf>,
    /// When true the device is started with all automatic flushing disabled
    /// (`--idle-flush-secs 0 --force-flush-timeout-secs 0`), so the only thing
    /// that can persist a buffered write is an explicit flush or the
    /// flush-on-shutdown path. Used by the graceful-shutdown test.
    disable_auto_flush: bool,
}

impl DeviceSpec {
    fn dev_path(&self) -> String {
        format!("/dev/ublkb{}", self.dev_id)
    }
}

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
fn azure_env(cmd: &mut Command, container: &str, blob: &str) {
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
    .env("AZURE_STORAGE_CONTAINER", container)
    .env("AZURE_STORAGE_BLOB", blob);
}

/// Start the ublk device as a child process and wait for `/dev/ublkbN` to
/// appear.  When `create` is true the page blob is provisioned first.
///
/// The returned `Child` is always `wait()`ed on by the caller (via
/// `stop_device`), so the zombie-process lint does not apply.
#[allow(clippy::zombie_processes)]
fn start_device(spec: &DeviceSpec, create: bool) -> Child {
    start_device_opts(spec, create, false)
}

/// Like [`start_device`] but lets the caller expose the device read-only
/// (`run --read-only`).  `create` and `read_only` are mutually exclusive at the
/// CLI level, so callers pass `create=false` when `read_only=true`.
#[allow(clippy::zombie_processes)]
fn start_device_opts(spec: &DeviceSpec, create: bool, read_only: bool) -> Child {
    let dev = spec.dev_path();
    log(&format!(
        "starting ublk device {dev} ({}{}{})",
        if create {
            "--create"
        } else {
            "reuse existing blob"
        },
        if read_only { ", --read-only" } else { "" },
        match &spec.cache_dir {
            Some(d) => format!(", cache_dir={}", d.display()),
            None => String::new(),
        },
    ));
    // Prefer an externally-provided binary (the e2e runs the actual image built
    // from deploy/Dockerfile); fall back to the cargo-built binary for local runs.
    let bin = std::env::var("UBLK_AZBLOB_BIN")
        .unwrap_or_else(|_| env!("CARGO_BIN_EXE_ublk-azblob").to_string());
    let mut cmd = Command::new(&bin);
    cmd.arg("run")
        .arg("--id")
        .arg(&spec.dev_id)
        .arg("--size")
        .arg(BLOB_SIZE.to_string());
    if create {
        cmd.arg("--create");
    }
    if read_only {
        cmd.arg("--read-only");
    }
    if let Some(dir) = &spec.cache_dir {
        cmd.arg("--cache-dir").arg(dir);
    }
    if spec.disable_auto_flush {
        // Disable both the idle and the force-flush timers so a buffered write
        // is only persisted by an explicit flush or the shutdown flush path.
        cmd.arg("--idle-flush-secs")
            .arg("0")
            .arg("--force-flush-timeout-secs")
            .arg("0");
    }
    azure_env(&mut cmd, &spec.container, &spec.blob);

    let mut child = cmd.spawn().expect("failed to spawn ublk-azblob");

    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if Path::new(&dev).exists() {
            log(&format!("device {dev} is up (pid {})", child.id()));
            return child;
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("ublk-azblob exited before {dev} appeared: {status}");
        }
        sleep(Duration::from_secs(1));
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("timed out waiting for {dev}");
}

/// Send `sig` to the running child process.
fn signal(child: &Child, sig: i32) {
    // SAFETY: `kill` is safe to call with a valid pid and signal number.
    let rc = unsafe { libc::kill(child.id() as libc::pid_t, sig) };
    assert_eq!(
        rc,
        0,
        "kill({sig}) failed: {}",
        std::io::Error::last_os_error()
    );
}

/// Stop the running device cleanly via `SIGINT` and wait for it to exit.
fn stop_device(dev: &str, mut child: Child) {
    log(&format!("stopping ublk device {dev} (pid {})", child.id()));
    signal(&child, libc::SIGINT);
    let status = child.wait().expect("wait for ublk-azblob to exit");
    assert!(
        status.success(),
        "ublk-azblob exited with non-zero status while stopping: {status}"
    );
    // Give the kernel a moment to remove the device node.
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && Path::new(dev).exists() {
        sleep(Duration::from_secs(1));
    }
    // The node must actually be gone; otherwise a subsequent `start_device`
    // would see the stale node and falsely conclude the device is already up.
    assert!(
        !Path::new(dev).exists(),
        "device node {dev} still present 30s after stopping the device"
    );
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

    run_mount_roundtrip(DeviceSpec {
        dev_id: DEV_ID.to_string(),
        container: env_or("AZURE_STORAGE_CONTAINER", DEFAULT_CONTAINER),
        blob: env_or("AZURE_STORAGE_BLOB", DEFAULT_BLOB),
        cache_dir: None,
        disable_auto_flush: false,
    });
}

/// Same write → flush → remount → verify cycle, but with the persistent
/// local-disk cache (`--cache-dir`) enabled.  This proves that writes routed
/// through the file-based cache layer are flushed to the page blob and survive
/// tearing the device down and bringing it back up over the same blob.
///
/// A *fresh* cache directory is used for the second boot so the verification
/// reads must come from the page blob (via the cache's read-through), not from
/// stale local cache data left over from phase 1.
#[test]
fn mount_roundtrip_file_cache() {
    if !ublk_available() {
        eprintln!(
            "skipping mount_roundtrip_file_cache: requires root and a loaded \
             ublk_drv (no /dev/ublk-control or not running as root)"
        );
        return;
    }

    let cache_dir = tempdir("ublk-azblob-cache");
    run_mount_roundtrip(DeviceSpec {
        // Distinct device id, container and blob so this test never collides
        // with `mount_roundtrip` (or the k8s CSI e2e's low auto-assigned ids).
        dev_id: "41".to_string(),
        container: env_or("AZURE_STORAGE_CONTAINER", DEFAULT_CONTAINER),
        blob: format!("{}-fcache", env_or("AZURE_STORAGE_BLOB", DEFAULT_BLOB)),
        cache_dir: Some(cache_dir.clone()),
        disable_auto_flush: false,
    });
    let _ = fs::remove_dir_all(&cache_dir);
}

/// Drive a full mount/write/flush/remount/verify cycle for the given device.
fn run_mount_roundtrip(spec: DeviceSpec) {
    let dev = spec.dev_path();
    let mnt = tempdir("ublk-azblob-mnt");

    // ── Phase 1: provision device, make a filesystem, write random files ──────
    let child = start_device(&spec, true);

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
    // Use a *fresh* cache directory for the remount so the verification reads
    // must come from the page blob (via the cache's read-through), not from
    // stale local cache data left over from phase 1.
    if let Some(dir) = &spec.cache_dir {
        fs::remove_dir_all(dir).unwrap_or_else(|e| panic!("clear cache dir {dir:?}: {e}"));
    }
    let child = start_device(&spec, false);

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

/// e2e for read-only mode (`run --read-only`) over the kernel ublk path.
///
/// Cycle:
///   1. provision the device writable, make an ext4 filesystem, write random
///      files, record their checksums, flush and tear the device down
///   2. bring the device back up over the *same* blob with `--read-only` and
///      assert:
///      * the kernel marks `/dev/ublkbN` read-only
///        (`/sys/block/ublkbN/ro == 1`, courtesy of `UBLK_ATTR_READ_ONLY`)
///      * the filesystem can be mounted read-only and every file's checksum
///        still matches (data round-tripped and is served read-only)
///      * an attempt to write a new file fails (the mount is read-only)
///   3. reopen the device writable and confirm the blob was never modified
#[test]
fn mount_read_only() {
    if !ublk_available() {
        eprintln!(
            "skipping mount_read_only: requires root and a loaded ublk_drv \
             (no /dev/ublk-control or not running as root)"
        );
        return;
    }

    let spec = DeviceSpec {
        // Distinct, high device id and blob so this test never collides with
        // the other mount tests or the low ids the k8s CSI e2e auto-assigns.
        dev_id: "42".to_string(),
        container: env_or("AZURE_STORAGE_CONTAINER", DEFAULT_CONTAINER),
        blob: format!("{}-ro", env_or("AZURE_STORAGE_BLOB", DEFAULT_BLOB)),
        cache_dir: None,
        disable_auto_flush: false,
    };
    let dev = spec.dev_path();
    let mnt = tempdir("ublk-azblob-ro-mnt");

    // ── Phase 1: provision the device writable and seed known files ───────────
    let child = start_device(&spec, true);
    log(&format!("mkfs.ext4 on {dev}"));
    run("mkfs.ext4", &["-q", "-F", "-E", "nodiscard", &dev]);
    run("mount", &[&dev, mnt.to_str().unwrap()]);

    log(&format!("writing {NUM_FILES} random files"));
    let mut checksums: Vec<(String, String)> = Vec::with_capacity(NUM_FILES);
    for i in 1..=NUM_FILES {
        let name = format!("random_{i}.bin");
        let path = mnt.join(&name);
        let len = 1024 * (1 + (i * 509) % 4096);
        write_random_file(&path, len);
        checksums.push((name, sha256_file(&path)));
    }

    log("sync + SIGUSR1 to force flush to the page blob");
    run("sync", &[]);
    signal(&child, libc::SIGUSR1);
    sleep(Duration::from_secs(2));
    run("umount", &[mnt.to_str().unwrap()]);
    stop_device(&dev, child);

    // ── Phase 2: reopen read-only and assert the device rejects writes ────────
    let child = start_device_opts(&spec, false, true);

    // The kernel exposes the read-only attribute via /sys/block/<dev>/ro.
    let ro_attr = format!("/sys/block/ublkb{}/ro", spec.dev_id);
    let ro_val = fs::read_to_string(&ro_attr)
        .unwrap_or_else(|e| panic!("read {ro_attr}: {e}"))
        .trim()
        .to_string();
    assert_eq!(
        ro_val, "1",
        "{dev} should be read-only ({ro_attr} = {ro_val}, expected 1)"
    );

    // Mount read-only.  `noload` skips ext4 journal recovery, which would
    // otherwise try to write to the (now read-only) device.
    log(&format!("mounting {dev} read-only at {}", mnt.display()));
    run("mount", &["-o", "ro,noload", &dev, mnt.to_str().unwrap()]);

    log("verifying checksums on the read-only mount");
    for (name, expected) in &checksums {
        let actual = sha256_file(&mnt.join(name));
        assert_eq!(
            &actual, expected,
            "checksum mismatch for {name} (read-only)"
        );
    }

    log("verifying a write to the read-only mount fails");
    let new_file = mnt.join("should_not_write.bin");
    assert!(
        fs::write(&new_file, b"nope").is_err(),
        "writing {new_file:?} unexpectedly succeeded on a read-only mount"
    );
    assert!(
        !new_file.exists(),
        "{new_file:?} should not exist after a rejected write"
    );

    run("umount", &[mnt.to_str().unwrap()]);
    stop_device(&dev, child);

    // ── Phase 3: reopen writable and confirm the blob was untouched ───────────
    let child = start_device(&spec, false);
    log(&format!("remounting {dev} writable at {}", mnt.display()));
    run("mount", &[&dev, mnt.to_str().unwrap()]);
    for (name, expected) in &checksums {
        let actual = sha256_file(&mnt.join(name));
        assert_eq!(
            &actual, expected,
            "blob changed despite read-only mount for {name}"
        );
    }
    assert!(
        !mnt.join("should_not_write.bin").exists(),
        "a file rejected by the read-only mount leaked into the blob"
    );
    run("umount", &[mnt.to_str().unwrap()]);
    stop_device(&dev, child);

    let _ = fs::remove_dir_all(&mnt);
    log("mount read-only e2e PASSED ✓");
}

/// Graceful-shutdown e2e: prove a write buffered only in memory is flushed to
/// the page blob when the device receives `SIGINT` — with **no** explicit
/// `SIGUSR1`, **no** `umount` FLUSH, and **no** automatic (idle/force) flush —
/// and that the data survives tearing the device down and bringing a fresh one
/// back up over the same blob.
///
/// This validates the shutdown flush in `ublk_target::run_ublk_target`: writing
/// straight to the raw `/dev/ublkbN` with `oflag=direct` (and no `conv=fsync`)
/// leaves the data sitting in the in-memory write-back buffer, so without the
/// flush-on-shutdown the pattern would be lost after the restart.
#[test]
fn graceful_shutdown_flush() {
    if !ublk_available() {
        eprintln!(
            "skipping graceful_shutdown_flush: requires root and a loaded \
             ublk_drv (no /dev/ublk-control or not running as root)"
        );
        return;
    }

    let spec = DeviceSpec {
        // Distinct device id, container and blob so this test never collides
        // with the other mount tests (or the k8s CSI e2e's low auto-assigned ids).
        dev_id: "43".to_string(),
        container: env_or("AZURE_STORAGE_CONTAINER", DEFAULT_CONTAINER),
        blob: format!("{}-shutdown", env_or("AZURE_STORAGE_BLOB", DEFAULT_BLOB)),
        cache_dir: None,
        // The whole point: only the shutdown flush may persist the write.
        disable_auto_flush: true,
    };
    let dev = spec.dev_path();
    let work = tempdir("ublk-azblob-shutdown");

    // ── Phase 1: provision the device, write a pattern straight to the raw
    //    block device with O_DIRECT and no fsync, then SIGINT it ───────────────
    let child = start_device(&spec, true);

    // 8 MiB of random data, written 1 MiB at a time with oflag=direct so it
    // bypasses the page cache and lands in the device's in-memory buffer. No
    // `conv=fsync`, so the kernel never issues a FLUSH.
    const PATTERN_MIB: usize = 8;
    let pattern = work.join("pattern.bin");
    write_random_file(&pattern, PATTERN_MIB * 1024 * 1024);
    let expected = sha256_file(&pattern);

    log(&format!("dd pattern → raw {dev} (oflag=direct, no fsync)"));
    run(
        "dd",
        &[
            &format!("if={}", pattern.display()),
            &format!("of={dev}"),
            "bs=1M",
            &format!("count={PATTERN_MIB}"),
            "oflag=direct",
            "conv=notrunc",
        ],
    );

    // SIGINT and wait for a clean exit. `stop_device` sends SIGINT and asserts
    // the process exits successfully — the only path that can flush the buffer.
    log("SIGINT the device (relies solely on the shutdown flush)");
    stop_device(&dev, child);

    // ── Phase 2: bring up a fresh device over the same blob and read back ──────
    let child = start_device(&spec, false);

    log(&format!("dd read back from raw {dev} (iflag=direct)"));
    let readback = work.join("readback.bin");
    run(
        "dd",
        &[
            &format!("if={dev}"),
            &format!("of={}", readback.display()),
            "bs=1M",
            &format!("count={PATTERN_MIB}"),
            "iflag=direct",
        ],
    );

    let actual = sha256_file(&readback);
    assert_eq!(
        actual, expected,
        "pattern mismatch after SIGINT shutdown + remount — the buffered \
         write was not flushed on shutdown"
    );

    stop_device(&dev, child);
    let _ = fs::remove_dir_all(&work);

    log("graceful shutdown e2e PASSED ✓");
}

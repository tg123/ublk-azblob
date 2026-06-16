//! CSI **controller** e2e for `ublk-azblob`, written in Rust.
//!
//! This is the Rust replacement for the old `tests/e2e/csi/controller_test.sh`
//! shell script.  It exercises the *controller* half of the CSI driver
//! end-to-end without needing a kernel or the `ublk_drv` module: it starts the
//! `ublk-azblob csi --role controller` gRPC server against a local Azurite and
//! drives it with `grpcurl`, verifying the Identity and Controller RPCs that
//! Kubernetes' external-provisioner relies on (`GetPluginInfo`,
//! `ControllerGetCapabilities`, `CreateVolume` — including idempotency — and
//! `DeleteVolume` — including idempotency).
//!
//! Because it needs no block device it runs anywhere (CI, laptop) once Azurite
//! is reachable.  The companion `k8s_pvc_e2e.rs` covers the node/mount path on a
//! real ublk-capable host.
//!
//! The test is gated behind the `csi` Cargo feature and *skips* (rather than
//! fails) when `grpcurl` is not installed or Azurite is unreachable, mirroring
//! the skip-when-unavailable behaviour of `mount_e2e.rs`.  Run it with:
//!
//! ```text
//! AZURE_STORAGE_ENDPOINT="http://127.0.0.1:10000/devstoreaccount1" \
//!   cargo test --features csi --test csi_controller_e2e -- --nocapture
//! ```
#![cfg(feature = "csi")]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

/// Azurite well-known development account name.
const DEFAULT_ACCOUNT: &str = "devstoreaccount1";
/// Azurite well-known development account key (public, not a real secret).
const DEFAULT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:10000/devstoreaccount1";
const DEFAULT_CONTAINER: &str = "pvc";

const DRIVER_NAME: &str = "azblob.ublk.csi.tg123.github.io";

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn log(msg: &str) {
    println!("=== {msg} ===");
}

/// True when `grpcurl` is on `PATH`.
fn have_grpcurl() -> bool {
    Command::new("grpcurl")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// The vendored CSI proto directory (`<manifest>/proto/csi`).
fn proto_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("proto/csi")
}

/// A controller child process bound to a unix socket; killed on drop.
struct Controller {
    child: Child,
    sock: PathBuf,
}

impl Drop for Controller {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.sock);
    }
}

/// Start `ublk-azblob csi --role controller` on a fresh unix socket and wait for
/// the socket to appear.
fn start_controller(container: &str) -> Controller {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let sock = std::env::temp_dir().join(format!("csi-ctrl-{}-{nanos}.sock", std::process::id()));
    let _ = std::fs::remove_file(&sock);

    let bin = env!("CARGO_BIN_EXE_ublk-azblob");
    log(&format!(
        "starting CSI controller on unix://{}",
        sock.display()
    ));
    let child = Command::new(bin)
        .arg("csi")
        .arg("--role")
        .arg("controller")
        .arg("--csi-endpoint")
        .arg(format!("unix://{}", sock.display()))
        .env(
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
        .stdin(Stdio::null())
        .spawn()
        .expect("spawn ublk-azblob csi controller");

    let mut ctrl = Controller { child, sock };
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if ctrl.sock.exists() {
            log("controller socket is up");
            return ctrl;
        }
        if let Ok(Some(status)) = ctrl.child.try_wait() {
            panic!("controller exited before binding the socket: {status}");
        }
        sleep(Duration::from_secs(1));
    }
    panic!(
        "timed out waiting for controller socket {}",
        ctrl.sock.display()
    );
}

/// Invoke a gRPC method via `grpcurl`, returning stdout.  Panics on failure with
/// captured stderr.
fn grpcurl(sock: &Path, method: &str, data: Option<&str>) -> String {
    let proto = proto_dir();
    let mut cmd = Command::new("grpcurl");
    cmd.arg("-plaintext")
        .arg("-unix")
        .arg("-import-path")
        .arg(&proto)
        .arg("-proto")
        .arg("csi.proto");
    if let Some(d) = data {
        cmd.arg("-d").arg(d);
    }
    cmd.arg(sock).arg(method);

    let out = cmd.output().expect("spawn grpcurl");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "grpcurl {method} failed ({}): {}\nstdout: {stdout}",
        out.status,
        stderr.trim()
    );
    stdout
}

/// Probe whether Azurite is reachable by attempting a `CreateVolume`; returns
/// `false` (→ skip) when the controller cannot reach storage.
fn azurite_reachable(sock: &Path, container: &str, probe_vol: &str) -> bool {
    let proto = proto_dir();
    let data = format!(
        "{{\"name\":\"{probe_vol}\",\"capacity_range\":{{\"required_bytes\":1048576}},\
         \"volume_capabilities\":[{{\"mount\":{{\"fs_type\":\"ext4\"}},\"access_mode\":{{\"mode\":1}}}}]}}"
    );
    let out = Command::new("grpcurl")
        .arg("-plaintext")
        .arg("-unix")
        .arg("-import-path")
        .arg(&proto)
        .arg("-proto")
        .arg("csi.proto")
        .arg("-d")
        .arg(&data)
        .arg(sock)
        .arg("csi.v1.Controller/CreateVolume")
        .output()
        .expect("spawn grpcurl probe");
    if out.status.success() {
        // Clean the probe volume up so the real test starts from scratch.
        let _ = Command::new("grpcurl")
            .arg("-plaintext")
            .arg("-unix")
            .arg("-import-path")
            .arg(&proto)
            .arg("-proto")
            .arg("csi.proto")
            .arg("-d")
            .arg(format!("{{\"volume_id\":\"{container}/{probe_vol}\"}}"))
            .arg(sock)
            .arg("csi.v1.Controller/DeleteVolume")
            .output();
        true
    } else {
        eprintln!(
            "controller CreateVolume probe failed (Azurite unreachable?): {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
        false
    }
}

#[test]
fn controller_roundtrip() {
    if !have_grpcurl() {
        eprintln!("skipping controller_roundtrip: grpcurl not found on PATH");
        return;
    }

    let container = env_or("AZURE_STORAGE_CONTAINER", DEFAULT_CONTAINER);
    let ctrl = start_controller(&container);
    let sock = ctrl.sock.clone();

    let vol_name = format!("pvc-e2e-{}-{}", std::process::id(), rand_suffix());

    // ── Azurite preflight (skip rather than fail when storage is absent) ──────
    if !azurite_reachable(&sock, &container, &format!("{vol_name}-probe")) {
        eprintln!("skipping controller_roundtrip: Azurite is not reachable");
        return;
    }

    log("Identity/GetPluginInfo");
    let info = grpcurl(&sock, "csi.v1.Identity/GetPluginInfo", None);
    println!("{info}");
    assert!(
        info.contains(DRIVER_NAME),
        "GetPluginInfo: missing driver name {DRIVER_NAME}"
    );

    log("Controller/ControllerGetCapabilities");
    let caps = grpcurl(&sock, "csi.v1.Controller/ControllerGetCapabilities", None);
    println!("{caps}");
    assert!(
        caps.contains("CREATE_DELETE_VOLUME"),
        "missing CREATE_DELETE_VOLUME capability"
    );

    let create_req = format!(
        "{{\"name\":\"{vol_name}\",\"capacity_range\":{{\"required_bytes\":1048576}},\
         \"volume_capabilities\":[{{\"mount\":{{\"fs_type\":\"ext4\"}},\"access_mode\":{{\"mode\":1}}}}]}}"
    );

    log(&format!("Controller/CreateVolume ({vol_name})"));
    let create = grpcurl(&sock, "csi.v1.Controller/CreateVolume", Some(&create_req));
    println!("{create}");
    let expected_id = format!("{container}/{vol_name}");
    assert!(
        create.contains(&expected_id),
        "CreateVolume: expected volume id {expected_id}"
    );

    log("Controller/CreateVolume idempotency (same request)");
    grpcurl(&sock, "csi.v1.Controller/CreateVolume", Some(&create_req));

    let delete_req = format!("{{\"volume_id\":\"{expected_id}\"}}");
    log(&format!("Controller/DeleteVolume ({expected_id})"));
    grpcurl(&sock, "csi.v1.Controller/DeleteVolume", Some(&delete_req));

    log("Controller/DeleteVolume idempotency (already deleted)");
    grpcurl(&sock, "csi.v1.Controller/DeleteVolume", Some(&delete_req));

    log("controller e2e PASSED ✓");
}

/// Small random-ish suffix derived from the current time (avoids adding a `rand`
/// dependency just for a unique volume name).
fn rand_suffix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u64
}

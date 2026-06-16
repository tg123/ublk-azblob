//! Kubernetes PVC e2e for the `ublk-azblob` CSI driver, written in Rust.
//!
//! This is the Rust replacement for the old `tests/e2e/k8s/run.sh` shell script.
//! It spins up a single-node `kind` cluster, deploys the CSI driver (controller
//! + node) and an in-cluster Azurite, then exercises the full PVC lifecycle:
//!
//!   1. create a PVC backed by the `azblob-ublk` StorageClass
//!   2. run a writer Job that writes random data and records its SHA-256
//!   3. delete the writer (NodeUnpublishVolume tears the ublk device down and
//!      flushes the page blob)
//!   4. run a reader Job that mounts the *same* PVC on a fresh ublk device over
//!      the existing page blob and verifies the SHA-256 still matches
//!
//! This is the Kubernetes counterpart of `mount_e2e.rs` and proves the data
//! survives provision → write → unmount → remount through the page blob.
//!
//! Requirements (provided by the CI workflow): a Linux host with `ublk_drv`
//! loaded, root, Docker, `kind`, and `kubectl`.  When any of these is missing
//! the test *skips* (returns) rather than failing, mirroring `mount_e2e.rs`.
//!
//! Gated behind the `csi` Cargo feature.  Run it with:
//!
//! ```text
//! sudo -E env "PATH=$PATH" cargo test --features csi --test k8s_pvc_e2e -- --nocapture
//! ```
#![cfg(feature = "csi")]

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// Azurite well-known development account + key (public, not a real secret).
/// NEVER use these credentials against real Azure Storage / in production.
const ACCOUNT: &str = "devstoreaccount1";
const KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const NS: &str = "kube-system";
const CONTAINER: &str = "pvc";

fn cluster_name() -> String {
    std::env::var("KIND_CLUSTER").unwrap_or_else(|_| "azblob-e2e".to_string())
}

fn image() -> String {
    std::env::var("E2E_IMAGE").unwrap_or_else(|_| "ublk-azblob:e2e".to_string())
}

fn log(msg: &str) {
    println!("=== {msg} ===");
}

/// Repository root (two levels up from this crate's manifest dir).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate parent")
        .to_path_buf()
}

/// This test's manifest directory (`tests/e2e/k8s`).
fn k8s_dir() -> PathBuf {
    repo_root().join("tests/e2e/k8s")
}

/// True when `bin` is runnable (used for preflight skip checks).
fn have(bin: &str) -> bool {
    Command::new(bin)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn is_root() -> bool {
    // SAFETY: `geteuid` has no preconditions and never fails.
    let uid = unsafe { libc::geteuid() };
    uid == 0
}

/// Run a command, streaming output; panic on non-zero exit.
fn run(cmd: &str, args: &[&str]) {
    log(&format!("$ {cmd} {}", args.join(" ")));
    let status = Command::new(cmd)
        .args(args)
        .status()
        .unwrap_or_else(|e| panic!("failed to spawn `{cmd}`: {e}"));
    assert!(
        status.success(),
        "`{cmd} {}` failed with {status}",
        args.join(" ")
    );
}

/// Run a command, returning success/failure (does not panic).
fn try_run(cmd: &str, args: &[&str]) -> bool {
    Command::new(cmd)
        .args(args)
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Apply a manifest via `kubectl apply -f <path>`.
fn kubectl_apply(path: &Path) {
    run("kubectl", &["apply", "-f", path.to_str().unwrap()]);
}

/// Pipe `yaml` into `kubectl apply -f -`.
fn kubectl_apply_stdin(yaml: &str) {
    use std::io::Write;
    log("$ kubectl apply -f - (stdin)");
    let mut child = Command::new("kubectl")
        .args(["apply", "-f", "-"])
        .stdin(Stdio::piped())
        .spawn()
        .expect("spawn kubectl apply -f -");
    child
        .stdin
        .as_mut()
        .expect("kubectl stdin")
        .write_all(yaml.as_bytes())
        .expect("write manifest to kubectl");
    let status = child.wait().expect("wait kubectl apply -f -");
    assert!(status.success(), "kubectl apply -f - failed with {status}");
}

/// RAII guard that deletes the kind cluster on drop (mirrors the bash `trap`).
struct ClusterGuard {
    name: String,
}

impl Drop for ClusterGuard {
    fn drop(&mut self) {
        log(&format!("tearing down kind cluster {}", self.name));
        let _ = try_run("kind", &["delete", "cluster", "--name", &self.name]);
    }
}

#[test]
fn pvc_write_remount_verify() {
    // ── Preflight: skip gracefully when the environment can't drive ublk ──────
    if !is_root() {
        eprintln!("SKIP: must run as root");
        return;
    }
    if !Path::new("/dev/ublk-control").exists() {
        eprintln!("SKIP: ublk_drv not loaded (no /dev/ublk-control)");
        return;
    }
    for tool in ["docker", "kind", "kubectl"] {
        if !have(tool) {
            eprintln!("SKIP: {tool} not found");
            return;
        }
    }

    let repo = repo_root();
    let here = k8s_dir();
    let cluster = cluster_name();
    let img = image();

    // ── Build + load the driver image ─────────────────────────────────────────
    log(&format!("building driver image {img}"));
    run(
        "docker",
        &[
            "build",
            "-f",
            repo.join("deploy/Dockerfile").to_str().unwrap(),
            "-t",
            &img,
            repo.to_str().unwrap(),
        ],
    );

    log(&format!("creating kind cluster {cluster}"));
    run(
        "kind",
        &[
            "create",
            "cluster",
            "--name",
            &cluster,
            "--config",
            here.join("kind-config.yaml").to_str().unwrap(),
            "--wait",
            "120s",
        ],
    );
    let _guard = ClusterGuard {
        name: cluster.clone(),
    };

    log(&format!("loading {img} into the cluster"));
    run("kind", &["load", "docker-image", &img, "--name", &cluster]);

    // ── Deploy Azurite + driver config ────────────────────────────────────────
    log("deploying Azurite");
    kubectl_apply(&here.join("azurite.yaml"));
    run(
        "kubectl",
        &[
            "-n",
            NS,
            "rollout",
            "status",
            "deployment/azurite",
            "--timeout=120s",
        ],
    );

    log("creating driver secret + config");
    let endpoint = format!("http://azurite.{NS}.svc.cluster.local:10000/devstoreaccount1");
    kubectl_apply_stdin(&render_secret());
    kubectl_apply_stdin(&render_config(&endpoint));

    // ── Deploy the CSI driver, pinned to the locally-built image ──────────────
    let manifests = stage_manifests(&repo, &img);
    log("deploying CSI driver");
    for m in [
        "csi-driver.yaml",
        "rbac.yaml",
        "storageclass.yaml",
        "controller.yaml",
        "node.yaml",
    ] {
        kubectl_apply(&manifests.join(m));
    }

    log("waiting for the driver to become ready");
    run(
        "kubectl",
        &[
            "-n",
            NS,
            "rollout",
            "status",
            "deployment/csi-azblob-controller",
            "--timeout=180s",
        ],
    );
    run(
        "kubectl",
        &[
            "-n",
            NS,
            "rollout",
            "status",
            "daemonset/csi-azblob-node",
            "--timeout=180s",
        ],
    );

    // ── Run the PVC write/remount/verify cycle ────────────────────────────────
    log("creating PVC");
    kubectl_apply(&repo.join("deploy/example/pvc.yaml"));

    log("running writer Job");
    kubectl_apply(&here.join("writer.yaml"));
    if !try_run(
        "kubectl",
        &[
            "wait",
            "--for=condition=complete",
            "job/azblob-writer",
            "--timeout=240s",
        ],
    ) {
        dump_diagnostics("azblob-writer");
        panic!("writer Job did not complete");
    }
    let _ = try_run("kubectl", &["logs", "-l", "app=azblob-writer", "--tail=50"]);

    log("deleting writer (triggers NodeUnpublishVolume / device teardown)");
    run(
        "kubectl",
        &[
            "delete",
            "-f",
            here.join("writer.yaml").to_str().unwrap(),
            "--wait=true",
        ],
    );

    log("running reader Job (remounts the same PVC, verifies checksum)");
    kubectl_apply(&here.join("reader.yaml"));
    if !try_run(
        "kubectl",
        &[
            "wait",
            "--for=condition=complete",
            "job/azblob-reader",
            "--timeout=240s",
        ],
    ) {
        dump_diagnostics("azblob-reader");
        panic!("reader Job did not complete — data did not survive the remount");
    }
    let _ = try_run("kubectl", &["logs", "-l", "app=azblob-reader", "--tail=50"]);

    log("k8s PVC e2e PASSED ✓");
}

/// Render the driver secret manifest (account + accountKey).
fn render_secret() -> String {
    use std::fmt::Write as _;
    let account_b64 = b64(ACCOUNT.as_bytes());
    let key_b64 = b64(KEY.as_bytes());
    let mut s = String::new();
    writeln!(s, "apiVersion: v1").unwrap();
    writeln!(s, "kind: Secret").unwrap();
    writeln!(s, "metadata:").unwrap();
    writeln!(s, "  name: csi-azblob-secret").unwrap();
    writeln!(s, "  namespace: {NS}").unwrap();
    writeln!(s, "type: Opaque").unwrap();
    writeln!(s, "data:").unwrap();
    writeln!(s, "  account: {account_b64}").unwrap();
    writeln!(s, "  accountKey: {key_b64}").unwrap();
    s
}

/// Render the driver config map (endpoint + container).
fn render_config(endpoint: &str) -> String {
    use std::fmt::Write as _;
    let mut s = String::new();
    writeln!(s, "apiVersion: v1").unwrap();
    writeln!(s, "kind: ConfigMap").unwrap();
    writeln!(s, "metadata:").unwrap();
    writeln!(s, "  name: csi-azblob-config").unwrap();
    writeln!(s, "  namespace: {NS}").unwrap();
    writeln!(s, "data:").unwrap();
    writeln!(s, "  endpoint: {endpoint}").unwrap();
    writeln!(s, "  container: {CONTAINER}").unwrap();
    s
}

/// Copy the deploy manifests to a temp dir and pin the image to the local build
/// (image + imagePullPolicy: Never), mirroring the `sed` rewrites in run.sh.
fn stage_manifests(repo: &Path, img: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("ublk-azblob-manifests-{nanos}"));
    std::fs::create_dir_all(&dir).expect("create manifest staging dir");
    let src = repo.join("deploy/kubernetes");
    for entry in std::fs::read_dir(&src).expect("read deploy/kubernetes") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        let content = std::fs::read_to_string(&path).expect("read manifest");
        let content = content
            .replace(
                "image: ghcr.io/tg123/ublk-azblob:latest",
                &format!("image: {img}"),
            )
            .replace("imagePullPolicy: IfNotPresent", "imagePullPolicy: Never");
        std::fs::write(dir.join(entry.file_name()), content).expect("write staged manifest");
    }
    dir
}

/// Best-effort cluster diagnostics on failure (mirrors the bash failure dumps).
fn dump_diagnostics(app: &str) {
    let _ = try_run("kubectl", &["describe", &format!("job/{app}")]);
    let _ = try_run(
        "kubectl",
        &["logs", "-l", &format!("app={app}"), "--tail=200"],
    );
    let _ = try_run(
        "kubectl",
        &[
            "-n",
            NS,
            "logs",
            "-l",
            "app=csi-azblob-node",
            "-c",
            "azblob",
            "--tail=200",
        ],
    );
}

/// Minimal standard base64 encoder (avoids adding a dependency just to build two
/// Secret values).
fn b64(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[((n >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(n & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

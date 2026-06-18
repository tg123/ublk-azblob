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
//! the test *skips* (returns) rather than failing, mirroring `mount_e2e.rs` —
//! except in the dedicated CI runner (which sets `K8S_E2E_REQUIRE=1`), where an
//! unmet precondition is a hard failure so a misconfigured environment can't
//! report a misleading green pass.
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
const NS: &str = "kube-system";

fn cluster_name() -> String {
    std::env::var("K8S_E2E_CLUSTER_NAME")
        .or_else(|_| std::env::var("KIND_CLUSTER"))
        .unwrap_or_else(|_| "azblob-e2e".to_string())
}

fn use_existing_cluster() -> bool {
    std::env::var("K8S_E2E_USE_EXISTING_CLUSTER").is_ok()
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
    // `kubectl` has no global `--version` flag (it errors with
    // "unknown flag: --version"); its version subcommand is `version --client`.
    // `helm` also errors with "unknown flag: --version", use `version` subcommand.
    // `docker`/`kind` accept `--version`.
    let args: &[&str] = match bin {
        "kubectl" => &["version", "--client"],
        "helm" => &["version"],
        _ => &["--version"],
    };
    Command::new(bin)
        .args(args)
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

/// Apply a manifest via `kubectl apply -f <path>`, with retry for API server readiness.
fn kubectl_apply(path: &Path) {
    let args = ["apply", "-f", path.to_str().unwrap(), "--validate=false"];

    // Retry up to 10 times with exponential backoff (1s, 2s, 4s, 8s, 16s, 32s...)
    for attempt in 1..=10 {
        log(&format!("$ kubectl {} (attempt {attempt})", args.join(" ")));

        if try_run("kubectl", &args) {
            return;
        }

        if attempt < 10 {
            let delay = 2u64.pow(attempt - 1).min(32); // Cap at 32s
            log(&format!(
                "kubectl apply failed (attempt {attempt}), retrying in {delay}s..."
            ));
            std::thread::sleep(std::time::Duration::from_secs(delay));
        }
    }

    // All attempts failed, use run() for proper panic message
    run("kubectl", &args);
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

/// Handle an unmet precondition.
///
/// For local/manual runs the test skips gracefully (mirroring `mount_e2e.rs`).
/// In the dedicated CI runner (the `tests/e2e/k8s` docker-compose stack, which
/// exports `K8S_E2E_REQUIRE=1`) every precondition is guaranteed to be
/// satisfied, so an unmet one means the environment is broken — fail loudly
/// instead of skipping, which would otherwise report a misleading green pass.
fn skip_or_fail(reason: &str) {
    if std::env::var_os("K8S_E2E_REQUIRE").is_some() {
        panic!("{reason} (K8S_E2E_REQUIRE is set, refusing to skip)");
    }
    eprintln!("SKIP: {reason}");
}

#[test]
fn pvc_write_remount_verify() {
    test_basic_mount_and_recovery();
}

/// Test 1: Simple mount, read, write
fn test_basic_mount_and_recovery() {
    // ── Preflight: skip gracefully when the environment can't drive ublk or NBD ──────
    if !is_root() {
        skip_or_fail("must run as root");
        return;
    }
    // Check if either ublk or NBD is available
    let has_ublk = Path::new("/dev/ublk-control").exists();
    let has_nbd = Path::new("/dev/nbd0").exists();
    if !has_ublk && !has_nbd {
        skip_or_fail("neither ublk_drv nor nbd module loaded (no /dev/ublk-control or /dev/nbd0)");
        return;
    }
    if !has_ublk {
        eprintln!("INFO: ublk_drv not available, test will use NBD mode (e2e.values.yaml has node.useNbd: true)");
    }
    let required_tools = if use_existing_cluster() {
        vec!["docker", "kubectl", "helm"]
    } else {
        vec!["docker", "kind", "kubectl", "helm"]
    };
    for tool in required_tools {
        if !have(tool) {
            skip_or_fail(&format!("{tool} not found"));
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

    // ── Create cluster or use existing one ────────────────────────────────────
    let _guard = setup_cluster(&cluster, &img, &here);

    // ── Deploy Azurite ─────────────────────────────────────────────────────────
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

    // ── Deploy CSI driver using Helm ──────────────────────────────────────────
    let endpoint = format!("http://azurite.{NS}.svc.cluster.local:10000/devstoreaccount1");
    deploy_csi_driver_helm(&repo, &here, &endpoint);

    // ── Create secret in default namespace for PVC provisioning ───────────────
    log("creating azblob-csi-secret in default namespace");
    let secret_yaml = format!(
        r#"apiVersion: v1
kind: Secret
metadata:
  name: azblob-csi-secret
  namespace: default
type: Opaque
stringData:
  AZURE_STORAGE_ACCOUNT: devstoreaccount1
  accountKey: Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
"#
    );
    kubectl_apply_stdin(&secret_yaml);

    // ── Run the PVC write/remount/verify cycle ────────────────────────────────
    log("creating PVC");
    kubectl_apply(&repo.join("deploy/example/pvc.yaml"));

    // Debug: Check CSI controller logs before creating writer
    log("checking CSI controller logs");
    let _ = try_run(
        "kubectl",
        &[
            "-n",
            NS,
            "logs",
            "-l",
            "app=csi-ublk-azblob-controller",
            "-c",
            "azblob",
            "--tail=50",
        ],
    );
    let _ = try_run(
        "kubectl",
        &[
            "-n",
            NS,
            "logs",
            "-l",
            "app=csi-ublk-azblob-controller",
            "-c",
            "csi-provisioner",
            "--tail=50",
        ],
    );

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

    // ── Test 2: Pod migration between nodes ────────────────────────────────────
    test_pod_migration(&here);

    log("k8s PVC e2e PASSED ✓");
}

/// Setup cluster (create or use existing) and load image
fn setup_cluster(cluster: &str, img: &str, here: &Path) -> Option<ClusterGuard> {
    if use_existing_cluster() {
        log(&format!(
            "using existing cluster {cluster} (K8S_E2E_USE_EXISTING_CLUSTER set)"
        ));
        // Import image to containerd in k3s (bypass kind load)
        log(&format!("importing {img} to k3s containerd"));

        // Import to all k3s nodes (server + agent)
        for node in &["k8s-k3s-server-1", "k8s-k3s-agent-1"] {
            log(&format!("importing to {node}"));
            let docker_save = Command::new("docker")
                .args(["save", img])
                .stdout(Stdio::piped())
                .spawn()
                .expect("docker save");
            let status = Command::new("docker")
                .args([
                    "exec",
                    "-i",
                    node,
                    "ctr",
                    "--namespace=k8s.io",
                    "images",
                    "import",
                    "-",
                ])
                .stdin(docker_save.stdout.unwrap())
                .status()
                .expect("ctr import");
            assert!(status.success(), "failed to import image to {node}");
        }
        None // no cleanup guard for external cluster
    } else {
        log(&format!("creating kind cluster {cluster}"));
        run(
            "kind",
            &[
                "create",
                "cluster",
                "--name",
                cluster,
                "--config",
                here.join("kind-config.yaml").to_str().unwrap(),
                "--wait",
                "120s",
            ],
        );
        log(&format!("loading {img} into the cluster"));
        run("kind", &["load", "docker-image", img, "--name", cluster]);
        Some(ClusterGuard {
            name: cluster.to_string(),
        })
    }
}

/// Deploy CSI driver using Helm
fn deploy_csi_driver_helm(repo: &Path, here: &Path, endpoint: &str) {
    log("deploying CSI driver via Helm");

    // Copy e2e.values.yaml and patch endpoint
    let values_src = here.join("e2e.values.yaml");
    let values_content = std::fs::read_to_string(&values_src).expect("read e2e.values.yaml");

    // Add endpoint to env section
    let patched = values_content.replace(
        "# Azurite endpoint - will be set by test",
        &format!(
            "# Azurite endpoint\n    - name: AZURE_STORAGE_ENDPOINT\n      value: \"{}\"",
            endpoint
        ),
    );

    let values_tmp = std::env::temp_dir().join("e2e-patched.values.yaml");
    std::fs::write(&values_tmp, patched).expect("write patched values");

    // helm install
    run(
        "helm",
        &[
            "install",
            "csi-ublk-azblob",
            repo.join("deploy/chart").to_str().unwrap(),
            "-n",
            NS,
            "-f",
            values_tmp.to_str().unwrap(),
            "--wait",
            "--timeout=180s",
        ],
    );

    log("waiting for the driver to become ready");
    run(
        "kubectl",
        &[
            "-n",
            NS,
            "rollout",
            "status",
            "deployment/csi-ublk-azblob-controller",
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
            "daemonset/csi-ublk-azblob-node",
            "--timeout=180s",
        ],
    );
}

/// Test pod migration between nodes
fn test_pod_migration(_here: &Path) {
    log("TEST 2: Pod migration between nodes");

    // Get list of nodes
    let nodes_out = Command::new("kubectl")
        .args(["get", "nodes", "-o", "jsonpath={.items[*].metadata.name}"])
        .output()
        .expect("get nodes");
    let nodes_str = String::from_utf8_lossy(&nodes_out.stdout);
    let nodes: Vec<&str> = nodes_str.split_whitespace().collect();

    if nodes.len() < 2 {
        log("SKIP pod migration test: cluster has <2 nodes");
        return;
    }

    log(&format!(
        "found {} nodes: {}",
        nodes.len(),
        nodes.join(", ")
    ));

    // Create a deployment with PVC (instead of job)
    let deployment_yaml = format!(
        r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: azblob-migration-test
spec:
  replicas: 1
  selector:
    matchLabels:
      app: azblob-migration-test
  template:
    metadata:
      labels:
        app: azblob-migration-test
    spec:
      nodeSelector:
        kubernetes.io/hostname: {}
      containers:
      - name: writer
        image: busybox
        command:
          - /bin/sh
          - -c
          - |
            echo "Writing test data on node $(hostname)"
            dd if=/dev/urandom of=/data/migration-test.dat bs=1M count=10
            sha256sum /data/migration-test.dat > /data/checksum.txt
            echo "Data written, sleeping..."
            sleep 3600
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: azblob-pvc
"#,
        nodes[0]
    );

    log(&format!("creating deployment on node {}", nodes[0]));
    kubectl_apply_stdin(&deployment_yaml);
    // Wait for pod to be ready
    if !try_run(
        "kubectl",
        &[
            "wait",
            "--for=condition=Ready",
            "pod",
            "-l",
            "app=azblob-migration-test",
            "--timeout=180s",
        ],
    ) {
        panic!("migration test pod did not become ready");
    }

    // Read checksum
    let checksum1_out = Command::new("kubectl")
        .args([
            "exec",
            "deployment/azblob-migration-test",
            "--",
            "cat",
            "/data/checksum.txt",
        ])
        .output()
        .expect("read checksum");
    let checksum1 = String::from_utf8_lossy(&checksum1_out.stdout);
    log(&format!(
        "checksum on node {}: {}",
        nodes[0],
        checksum1.trim()
    ));

    // Delete pod to trigger NodeUnpublishVolume
    log("deleting pod (will trigger graceful shutdown and flush)");
    run(
        "kubectl",
        &[
            "delete",
            "deployment",
            "azblob-migration-test",
            "--wait=true",
        ],
    );

    // Recreate deployment on different node
    let deployment_yaml2 = format!(
        r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: azblob-migration-test
spec:
  replicas: 1
  selector:
    matchLabels:
      app: azblob-migration-test
  template:
    metadata:
      labels:
        app: azblob-migration-test
    spec:
      nodeSelector:
        kubernetes.io/hostname: {}
      containers:
      - name: reader
        image: busybox
        command:
          - /bin/sh
          - -c
          - |
            echo "Reading test data on node $(hostname)"
            if [ -f /data/migration-test.dat ]; then
              sha256sum -c /data/checksum.txt
              if [ $? -eq 0 ]; then
                echo "MIGRATION SUCCESS: Data verified on new node!"
              else
                echo "MIGRATION FAILED: Checksum mismatch!"
                exit 1
              fi
            else
              echo "MIGRATION FAILED: Data file not found!"
              exit 1
            fi
            sleep 3600
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: azblob-pvc
"#,
        nodes[1]
    );

    log(&format!("recreating deployment on node {}", nodes[1]));
    kubectl_apply_stdin(&deployment_yaml2);

    // Wait for pod to be ready
    if !try_run(
        "kubectl",
        &[
            "wait",
            "--for=condition=Ready",
            "pod",
            "-l",
            "app=azblob-migration-test",
            "--timeout=180s",
        ],
    ) {
        dump_diagnostics("azblob-migration-test");
        panic!("migration test pod did not become ready on new node");
    }

    // Check logs for success message
    let logs_out = Command::new("kubectl")
        .args(["logs", "deployment/azblob-migration-test"])
        .output()
        .expect("get logs");
    let logs = String::from_utf8_lossy(&logs_out.stdout);
    log(&format!("migration test logs:\n{}", logs));

    if !logs.contains("MIGRATION SUCCESS") {
        panic!("pod migration failed: data did not survive node migration");
    }

    log("✓ Pod migration test PASSED: data survived node migration");

    // Cleanup
    run(
        "kubectl",
        &[
            "delete",
            "deployment",
            "azblob-migration-test",
            "--wait=true",
        ],
    );
}

/// Best-effort cluster diagnostics on failure (mirrors the bash failure dumps).
fn dump_diagnostics(app: &str) {
    log(&format!("=== diagnostics for {app} ==="));
    let _ = try_run("kubectl", &["describe", &format!("job/{app}")]);
    let _ = try_run(
        "kubectl",
        &["logs", "-l", &format!("app={app}"), "--tail=200"],
    );

    // Dump CSI controller logs (both containers)
    log("=== CSI controller logs (azblob) ===");
    let _ = try_run(
        "kubectl",
        &[
            "-n",
            NS,
            "logs",
            "-l",
            "app=csi-ublk-azblob-controller",
            "-c",
            "azblob",
            "--tail=200",
        ],
    );
    log("=== CSI controller logs (csi-provisioner) ===");
    let _ = try_run(
        "kubectl",
        &[
            "-n",
            NS,
            "logs",
            "-l",
            "app=csi-ublk-azblob-controller",
            "-c",
            "csi-provisioner",
            "--tail=200",
        ],
    );

    // Dump CSI node logs
    log("=== CSI node logs ===");
    let _ = try_run(
        "kubectl",
        &[
            "-n",
            NS,
            "logs",
            "-l",
            "app=csi-ublk-azblob-node",
            "-c",
            "azblob",
            "--tail=200",
        ],
    );

    // Dump PVC/PV status
    log("=== PVC/PV status ===");
    let _ = try_run("kubectl", &["get", "pvc,pv", "-o", "wide"]);
    let _ = try_run("kubectl", &["describe", "pvc"]);
}

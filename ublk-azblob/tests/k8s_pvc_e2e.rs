//! Kubernetes PVC e2e for the `ublk-azblob` CSI driver, written in Rust.
//!
//! It runs against the multi-node k3s cluster provided by the docker-compose
//! harness (`tests/e2e/docker-compose.yml`), deploys the CSI driver
//! (controller + node) and an in-cluster Azurite, then exercises the full PVC
//! lifecycle:
//!
//!   1. create a PVC backed by the `azblob-ublk` StorageClass
//!   2. run a writer Job that writes random data and records its SHA-256
//!   3. delete the writer (NodeUnpublishVolume tears the device down and
//!      flushes the page blob)
//!   4. run a reader Job that mounts the *same* PVC on a fresh device over
//!      the existing page blob and verifies the SHA-256 still matches
//!
//! This is the Kubernetes counterpart of `mount_e2e.rs` and proves the data
//! survives provision → write → unmount → remount through the page blob.
//!
//! Requirements (provided by the CI workflow): a Linux host with `ublk_drv`
//! (or `nbd`) loaded, root, Docker, and `kubectl`/`helm`.  When any of these is
//! missing the test *skips* (returns) rather than failing, mirroring
//! `mount_e2e.rs` — except in the dedicated CI runner (which sets
//! `K8S_E2E_REQUIRE=1`), where an
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

/// Kubernetes namespace the e2e deploys Azurite and the CSI driver into.
const NS: &str = "kube-system";

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
    // `docker` accepts `--version`.
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

/// Whether the e2e should drive the volume over NBD (vs. ublk).
///
/// Honours the `UBLK_E2E_USE_NBD` env override (`1`/`true` → NBD); otherwise
/// auto-detects: prefer ublk when `/dev/ublk-control` is present (CI loads
/// `ublk_drv`), and fall back to NBD where it isn't (e.g. WSL2, which ships
/// `/dev/nbd*` but no ublk).
fn use_nbd() -> bool {
    match std::env::var("UBLK_E2E_USE_NBD") {
        Ok(v) => v == "1" || v.eq_ignore_ascii_case("true"),
        Err(_) => !Path::new("/dev/ublk-control").exists(),
    }
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
    // Check that the selected backend's device interface is available.
    let nbd = use_nbd();
    let has_ublk = Path::new("/dev/ublk-control").exists();
    let has_nbd = Path::new("/dev/nbd0").exists();
    if nbd && !has_nbd {
        skip_or_fail("NBD mode selected but nbd module not loaded (no /dev/nbd0)");
        return;
    }
    if !nbd && !has_ublk {
        skip_or_fail("ublk mode selected but ublk_drv not loaded (no /dev/ublk-control)");
        return;
    }
    eprintln!("INFO: e2e using {} mode", if nbd { "NBD" } else { "ublk" });
    for tool in ["docker", "kubectl", "helm"] {
        if !have(tool) {
            skip_or_fail(&format!("{tool} not found"));
            return;
        }
    }

    let repo = repo_root();
    let here = k8s_dir();
    let img = image();

    // ── Build + load the driver image ─────────────────────────────────────────
    // When the harness has already built the image (the unified e2e runner
    // packages the once-compiled binary into a thin image and sets
    // `K8S_E2E_SKIP_IMAGE_BUILD=1`), skip the in-test build so the crate is
    // compiled exactly once across the whole suite.
    if std::env::var("K8S_E2E_SKIP_IMAGE_BUILD").is_ok() {
        log(&format!("using prebuilt driver image {img}"));
    } else {
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
    }

    // ── Load the freshly-built image into the k3s cluster ─────────────────────
    load_image_into_k3s(&img);

    // ── Deploy CSI driver using Helm ──────────────────────────────────────────
    // Azurite runs as the docker-compose `azurite` service (shared with the
    // mount/NBD e2e); the CSI pods reach it subdomain-style via hostAliases (see
    // e2e.values.yaml) pointing at its fixed compose IP. Subdomain form keeps the
    // SharedKey canonicalization single-account (no /account/account double path).
    let endpoint = format!("http://%s.azurite.{NS}.svc.cluster.local:10000/");
    deploy_csi_driver_helm(&repo, &here, &endpoint);

    // ── Create secret in default namespace for PVC provisioning ───────────────
    log("creating azblob-csi-secret in default namespace");
    let secret_yaml = r#"apiVersion: v1
kind: Secret
metadata:
  name: azblob-csi-secret
  namespace: default
type: Opaque
stringData:
  AZURE_STORAGE_ACCOUNT: devstoreaccount1
  accountKey: Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
"#;
    kubectl_apply_stdin(secret_yaml);

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

    // ── Test 3: Two disks mounted on the same node, no conflict ────────────────
    test_multi_disk_same_node(&here);

    // ── Test 4: Online volume expansion (resize a PVC, grow the filesystem) ─────
    test_volume_expansion(&here);

    log("k8s PVC e2e PASSED ✓");
}

/// Import the freshly-built driver image into the k3s cluster's containerd on
/// every node (the cluster is provided by the docker-compose harness).
fn load_image_into_k3s(img: &str) {
    log(&format!("importing {img} to k3s containerd"));
    for node in &["ublk-e2e-k3s-server", "ublk-e2e-k3s-agent"] {
        log(&format!("importing to {node}"));
        let mut docker_save = Command::new("docker")
            .args(["save", img])
            .stdout(Stdio::piped())
            .spawn()
            .expect("docker save");
        let save_stdout = docker_save.stdout.take().expect("docker save stdout");
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
            .stdin(save_stdout)
            .status()
            .expect("ctr import");
        let save_status = docker_save.wait().expect("wait docker save");
        assert!(save_status.success(), "docker save failed for {node}");
        assert!(status.success(), "failed to import image to {node}");
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

    // helm install. Override node.useNbd for the selected backend so CI (which
    // loads ublk_drv) exercises ublk while NBD-only hosts (e.g. WSL2) use NBD.
    let use_nbd_set = format!("node.useNbd={}", use_nbd());
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
            "--set",
            &use_nbd_set,
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
    // Wait for the deployment to roll out. Use `rollout status` (not
    // `kubectl wait` on a label) so the wait tracks only this deployment's
    // ReplicaSet and is not tripped up by old/terminating pods sharing the label.
    if !try_run(
        "kubectl",
        &[
            "rollout",
            "status",
            "deployment/azblob-migration-test",
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

    // Wait for the migrated deployment to roll out on the new node. Use
    // `rollout status` so the wait follows only the new ReplicaSet and isn't
    // tripped up by the old pod (which may still be terminating and shares the
    // `app=azblob-migration-test` label, causing `kubectl wait` to error when it
    // disappears mid-wait).
    if !try_run(
        "kubectl",
        &[
            "rollout",
            "status",
            "deployment/azblob-migration-test",
            "--timeout=240s",
        ],
    ) {
        dump_diagnostics("azblob-migration-test");
        panic!("migration test pod did not become ready on new node");
    }

    // Check logs for the success message. The in-pod check (sha256sum over the
    // remounted volume) runs after the container becomes Ready, so poll the logs
    // for a bit rather than reading once — otherwise we can race the check.
    let mut logs = String::new();
    let mut migrated = false;
    for _ in 0..30 {
        let logs_out = Command::new("kubectl")
            .args(["logs", "deployment/azblob-migration-test"])
            .output()
            .expect("get logs");
        logs = String::from_utf8_lossy(&logs_out.stdout).to_string();
        if logs.contains("MIGRATION SUCCESS") {
            migrated = true;
            break;
        }
        if logs.contains("MIGRATION FAILED") {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
    log(&format!("migration test logs:\n{}", logs));

    if !migrated {
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

/// Test 3: two volumes mounted simultaneously on the *same* node must not
/// conflict.
///
/// Provisions two independent PVCs and mounts both in a single pod pinned to one
/// node, so two ublk/NBD devices are live on the same node at the same time. Each
/// volume gets its own distinct random payload + SHA-256; the test then verifies
/// each checksum independently. A device-id collision, NBD-port collision, or any
/// cross-wiring between the two volumes would surface as a mount failure or a
/// checksum mismatch (data from one disk bleeding into the other).
fn test_multi_disk_same_node(_here: &Path) {
    log("TEST 3: two disks on the same node (no conflict)");

    // Pin both volumes to a single node so the two devices are guaranteed to be
    // co-located. Use the first node reported by the cluster.
    let nodes_out = Command::new("kubectl")
        .args(["get", "nodes", "-o", "jsonpath={.items[*].metadata.name}"])
        .output()
        .expect("get nodes");
    let nodes_str = String::from_utf8_lossy(&nodes_out.stdout);
    let node = match nodes_str.split_whitespace().next() {
        Some(n) => n.to_string(),
        None => {
            panic!("multi-disk test: no nodes found");
        }
    };
    log(&format!("pinning both volumes to node {node}"));

    // Two independent PVCs → two distinct page blobs → two distinct devices.
    let pvcs = r#"apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: azblob-pvc-multi-a
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: azblob-ublk
  resources:
    requests:
      storage: 256Mi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: azblob-pvc-multi-b
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: azblob-ublk
  resources:
    requests:
      storage: 256Mi
"#;
    log("creating two PVCs (azblob-pvc-multi-a, azblob-pvc-multi-b)");
    kubectl_apply_stdin(pvcs);

    // A single pod that mounts BOTH PVCs, writes distinct data to each, and
    // records a SHA-256 next to each payload. Co-locating both mounts in one pod
    // on one node forces the two devices to be live simultaneously on that node.
    let pod_yaml = format!(
        r#"apiVersion: batch/v1
kind: Job
metadata:
  name: azblob-multi-writer
spec:
  backoffLimit: 2
  template:
    metadata:
      labels:
        app: azblob-multi-writer
    spec:
      restartPolicy: Never
      nodeSelector:
        kubernetes.io/hostname: {node}
      containers:
        - name: writer
          image: busybox:1.36
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -e
              echo "writing distinct payloads to /data-a and /data-b on $(hostname)"
              dd if=/dev/urandom of=/data-a/payload bs=1M count=8
              dd if=/dev/urandom of=/data-b/payload bs=1M count=8
              sha256sum /data-a/payload | sed 's# .*# /data-a/payload#' > /data-a/payload.sha256
              sha256sum /data-b/payload | sed 's# .*# /data-b/payload#' > /data-b/payload.sha256
              # The two devices must hold different data — a conflict would make
              # these checksums match (one device shadowing the other).
              a=$(sha256sum /data-a/payload | cut -d' ' -f1)
              b=$(sha256sum /data-b/payload | cut -d' ' -f1)
              if [ "$a" = "$b" ]; then
                echo "CONFLICT: both volumes hold identical data"
                exit 1
              fi
              sync
              echo "multi-writer done"
          volumeMounts:
            - name: data-a
              mountPath: /data-a
            - name: data-b
              mountPath: /data-b
      volumes:
        - name: data-a
          persistentVolumeClaim:
            claimName: azblob-pvc-multi-a
        - name: data-b
          persistentVolumeClaim:
            claimName: azblob-pvc-multi-b
"#
    );

    log("running multi-disk writer Job (mounts both PVCs on one node)");
    kubectl_apply_stdin(&pod_yaml);
    if !try_run(
        "kubectl",
        &[
            "wait",
            "--for=condition=complete",
            "job/azblob-multi-writer",
            "--timeout=240s",
        ],
    ) {
        dump_diagnostics("azblob-multi-writer");
        panic!("multi-disk writer Job did not complete — two disks conflicted on the same node");
    }
    let _ = try_run(
        "kubectl",
        &["logs", "-l", "app=azblob-multi-writer", "--tail=50"],
    );

    log("deleting multi-disk writer (tears down both devices, flushes both blobs)");
    run(
        "kubectl",
        &["delete", "job", "azblob-multi-writer", "--wait=true"],
    );

    // Remount both PVCs again on the same node and verify each payload survived
    // independently — proving the two co-located volumes never crossed wires.
    let reader_yaml = format!(
        r#"apiVersion: batch/v1
kind: Job
metadata:
  name: azblob-multi-reader
spec:
  backoffLimit: 2
  template:
    metadata:
      labels:
        app: azblob-multi-reader
    spec:
      restartPolicy: Never
      nodeSelector:
        kubernetes.io/hostname: {node}
      containers:
        - name: reader
          image: busybox:1.36
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -e
              echo "verifying both payloads on $(hostname)"
              sha256sum -c /data-a/payload.sha256
              sha256sum -c /data-b/payload.sha256
              echo "multi-reader verified OK"
          volumeMounts:
            - name: data-a
              mountPath: /data-a
            - name: data-b
              mountPath: /data-b
      volumes:
        - name: data-a
          persistentVolumeClaim:
            claimName: azblob-pvc-multi-a
        - name: data-b
          persistentVolumeClaim:
            claimName: azblob-pvc-multi-b
"#
    );

    log("running multi-disk reader Job (remounts both PVCs, verifies both checksums)");
    kubectl_apply_stdin(&reader_yaml);
    if !try_run(
        "kubectl",
        &[
            "wait",
            "--for=condition=complete",
            "job/azblob-multi-reader",
            "--timeout=240s",
        ],
    ) {
        dump_diagnostics("azblob-multi-reader");
        panic!("multi-disk reader Job did not complete — data did not survive two-disk remount");
    }
    let _ = try_run(
        "kubectl",
        &["logs", "-l", "app=azblob-multi-reader", "--tail=50"],
    );

    log("✓ Multi-disk same-node test PASSED: two co-located volumes, no conflict");

    // Cleanup
    run(
        "kubectl",
        &["delete", "job", "azblob-multi-reader", "--wait=true"],
    );
    run(
        "kubectl",
        &[
            "delete",
            "pvc",
            "azblob-pvc-multi-a",
            "azblob-pvc-multi-b",
            "--wait=true",
        ],
    );
}

/// Test 4: volume expansion.  Create a PVC, write data + record the filesystem
/// size, then grow the PVC (`kubectl patch`).  The csi-resizer drives
/// ControllerExpandVolume (which resizes the backing page blob); a fresh reader
/// pod remounts the PVC at the new size, kubelet calls NodeExpandVolume which
/// grows the filesystem with `resize2fs`.  We assert both that the original data
/// survived and that the filesystem capacity actually grew.
fn test_volume_expansion(_here: &Path) {
    log("TEST 4: volume expansion (resize PVC, grow filesystem)");

    let old_size = "256Mi";
    let new_size = "512Mi";

    let pvc = format!(
        r#"apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: azblob-pvc-expand
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: azblob-ublk
  resources:
    requests:
      storage: {old_size}
"#
    );
    log(&format!("creating PVC azblob-pvc-expand ({old_size})"));
    kubectl_apply_stdin(&pvc);

    // Writer: record the filesystem's total 1K-block count *before* the resize,
    // alongside a checksummed payload.
    let writer_yaml = r#"apiVersion: batch/v1
kind: Job
metadata:
  name: azblob-expand-writer
spec:
  backoffLimit: 2
  template:
    metadata:
      labels:
        app: azblob-expand-writer
    spec:
      restartPolicy: Never
      containers:
        - name: writer
          image: busybox:1.36
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -e
              echo "writing payload + recording fs size on $(hostname)"
              dd if=/dev/urandom of=/data/payload bs=1M count=8
              sha256sum /data/payload | sed 's# .*# /data/payload#' > /data/payload.sha256
              df -k /data | tail -1 | awk '{print $2}' > /data/df_before
              echo "fs 1K-blocks before resize: $(cat /data/df_before)"
              sync
              echo "expand-writer done"
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: azblob-pvc-expand
"#;
    log("running expand writer Job");
    kubectl_apply_stdin(writer_yaml);
    if !try_run(
        "kubectl",
        &[
            "wait",
            "--for=condition=complete",
            "job/azblob-expand-writer",
            "--timeout=240s",
        ],
    ) {
        dump_diagnostics("azblob-expand-writer");
        panic!("expand writer Job did not complete");
    }
    let _ = try_run("kubectl", &["logs", "-l", "app=azblob-expand-writer", "--tail=50"]);

    log("deleting expand writer (tears down device, flushes blob)");
    run(
        "kubectl",
        &["delete", "job", "azblob-expand-writer", "--wait=true"],
    );

    // Patch the PVC to request the larger size — this is what `kubectl edit pvc`
    // does under the hood and is what the csi-resizer watches for.
    log(&format!("patching PVC azblob-pvc-expand to {new_size}"));
    let patch = format!(
        r#"{{"spec":{{"resources":{{"requests":{{"storage":"{new_size}"}}}}}}}}"#
    );
    run(
        "kubectl",
        &["patch", "pvc", "azblob-pvc-expand", "--type=merge", "-p", &patch],
    );

    // Wait for the csi-resizer to drive ControllerExpandVolume: the bound PV's
    // capacity reflects the resized backing blob once the controller side
    // completes.  The filesystem grow happens later, on the node, when a pod
    // remounts the volume.
    let pv = {
        let out = Command::new("kubectl")
            .args([
                "get",
                "pvc",
                "azblob-pvc-expand",
                "-o",
                "jsonpath={.spec.volumeName}",
            ])
            .output()
            .expect("get pvc volumeName");
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    };
    assert!(!pv.is_empty(), "expand: PVC has no bound PV");
    log(&format!("waiting for PV {pv} capacity to reach {new_size}"));
    let mut grown = false;
    for attempt in 1..=60 {
        let out = Command::new("kubectl")
            .args([
                "get",
                "pv",
                &pv,
                "-o",
                "jsonpath={.spec.capacity.storage}",
            ])
            .output()
            .expect("get pv capacity");
        let cap = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if cap == new_size {
            grown = true;
            break;
        }
        if attempt % 10 == 0 {
            log(&format!("  PV capacity still {cap} (attempt {attempt})"));
        }
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
    if !grown {
        dump_diagnostics("csi-ublk-azblob-controller");
        panic!("expand: PV {pv} capacity never reached {new_size} (ControllerExpandVolume)");
    }
    log(&format!("PV {pv} capacity is now {new_size}"));

    // Reader: remount the PVC.  kubelet sees the pending filesystem resize and
    // calls NodeExpandVolume (resize2fs).  Assert the payload survived and that
    // the filesystem total grew beyond what the writer recorded.
    let reader_yaml = r#"apiVersion: batch/v1
kind: Job
metadata:
  name: azblob-expand-reader
spec:
  backoffLimit: 2
  template:
    metadata:
      labels:
        app: azblob-expand-reader
    spec:
      restartPolicy: Never
      containers:
        - name: reader
          image: busybox:1.36
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -e
              echo "verifying payload + fs growth on $(hostname)"
              sha256sum -c /data/payload.sha256
              before=$(cat /data/df_before)
              after=$(df -k /data | tail -1 | awk '{print $2}')
              echo "fs 1K-blocks before=$before after=$after"
              if [ "$after" -le "$before" ]; then
                echo "FILESYSTEM DID NOT GROW after expansion"
                exit 1
              fi
              echo "expand-reader verified OK"
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: azblob-pvc-expand
"#;
    log("running expand reader Job (remounts PVC, verifies data + fs growth)");
    kubectl_apply_stdin(reader_yaml);
    if !try_run(
        "kubectl",
        &[
            "wait",
            "--for=condition=complete",
            "job/azblob-expand-reader",
            "--timeout=240s",
        ],
    ) {
        dump_diagnostics("azblob-expand-reader");
        panic!("expand reader Job did not complete — filesystem did not grow or data was lost");
    }
    let _ = try_run("kubectl", &["logs", "-l", "app=azblob-expand-reader", "--tail=50"]);

    log("✓ Volume expansion test PASSED: blob + filesystem grew, data intact");

    // Cleanup
    run(
        "kubectl",
        &["delete", "job", "azblob-expand-reader", "--wait=true"],
    );
    run(
        "kubectl",
        &["delete", "pvc", "azblob-pvc-expand", "--wait=true"],
    );
}

/// Best-effort cluster diagnostics on failure (mirrors the bash failure dumps).
fn dump_diagnostics(app: &str) {
    log(&format!("diagnostics for {app}"));
    let _ = try_run("kubectl", &["describe", &format!("job/{app}")]);
    let _ = try_run(
        "kubectl",
        &["logs", "-l", &format!("app={app}"), "--tail=200"],
    );

    // Dump CSI controller logs (both containers)
    log("CSI controller logs (azblob)");
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
    log("CSI controller logs (csi-provisioner)");
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
    log("CSI node logs");
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
    log("PVC/PV status");
    let _ = try_run("kubectl", &["get", "pvc,pv", "-o", "wide"]);
    let _ = try_run("kubectl", &["describe", "pvc"]);
}

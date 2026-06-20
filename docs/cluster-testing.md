# Real-cluster testing guide

This walks through deploying the `ublk-azblob` CSI driver on a real Kubernetes
cluster (e.g. AKS) and exercising every provisioning mode end to end:

1. **empty disk** — a fresh, dynamically-provisioned writable volume,
2. **read-only template** — many PVCs sharing one golden-image blob (no copy,
   no lock/lease),
3. **read-write template** — a per-PVC writable copy of a golden image
   (server-side `Put Page From URL`),
4. **node-to-node migration** — a pod moves nodes and its data survives.

> The Azurite-backed `tests/e2e` suite covers the same paths in CI; this guide is
> for validating against **real Azure Storage** on a real cluster.

## Prerequisites

- A Kubernetes cluster you have **cluster-admin** on (the chart creates a
  `CSIDriver`, `ClusterRole`, a controller `Deployment` and a node `DaemonSet`).
- Nodes that can run the device backend:
  - **NBD mode** (`node.useNbd: true`, recommended for portability) needs the
    `nbd` kernel module; or
  - **ublk mode** needs Linux ≥ 6.0 with `ublk_drv` loaded.
- `kubectl`, `helm`, and access to push/pull the driver image.
- Azure auth the driver can use — one of: a **service principal**
  (`AZURE_CLIENT_ID` / `AZURE_TENANT_ID` / `AZURE_CLIENT_SECRET`), **Workload
  Identity**, **Managed Identity**, or a storage **account key**.
- A **destination** storage account the identity can write to, and (for the
  template modes) a **source** golden-image blob the identity can read.

Set some shared variables:

```bash
export CTX=<your-kube-context>
export NS=<test-namespace>                 # where PVCs/pods live
export DEST_ACCOUNT=<dest-storage-account> # where new volumes are written
export TEMPLATE_URL="https://<acct>.blob.core.windows.net/<container>/<golden-image>"
export IMAGE=docker.io/<you>/ublk-azblob:latest
```

## 1. Build & push the driver image

```bash
DOCKER_BUILDKIT=1 docker build -f deploy/Dockerfile -t "$IMAGE" .
docker push "$IMAGE"
```

## 2. Create the credentials secret

The chart looks up a per-namespace secret (default name `azblob-csi-secret`) in
the PVC's namespace. For service-principal auth:

```bash
kubectl --context "$CTX" -n "$NS" create secret generic azblob-csi-secret \
  --from-literal=AZURE_CLIENT_ID="$AZURE_CLIENT_ID" \
  --from-literal=AZURE_TENANT_ID="$AZURE_TENANT_ID" \
  --from-literal=AZURE_CLIENT_SECRET="$AZURE_CLIENT_SECRET"
```

> For an account-key deployment, create the secret with `accountKey: <key>`
> instead.

## 3. Deploy the driver with Helm

Create a `values.yaml` (NBD mode shown; adjust `nodeSelector`/resources to your
cluster). The three StorageClasses below cover all test modes.

```yaml
image:
  repository: docker.io/<you>/ublk-azblob
  tag: latest
  pullPolicy: Always

node:
  useNbd: true                       # or false for ublk mode
  # nodeSelector: { <label>: <value> }   # pin to capable nodes if needed

sidecars:
  provisioner:
    timeout: 15m                     # large read-write template copies need headroom

perNamespaceSecret:
  name: azblob-csi-secret

# Empty-disk dynamic volumes.
storageClass:
  name: azblob-rw
  parameters:
    storageAccount: <dest-storage-account>
    container: ${pvc.namespace}
    blobPathTemplate: ${pvc.namespace}/volumes/${pv.name}
    fsType: ext4

additionalStorageClasses:
  # Read-only golden-image mount (no copy, no lock/lease; shared across PVCs).
  - name: azblob-template-ro
    reclaimPolicy: Retain
    volumeBindingMode: Immediate
    parameters:
      readOnly: "true"
      templateBlobUrl: "https://<acct>.blob.core.windows.net/<container>/<golden-image>"

  # Read-write golden-image copy (server-side s2s into a fresh per-PVC blob).
  - name: azblob-template-rw
    reclaimPolicy: Delete
    volumeBindingMode: Immediate
    parameters:
      storageAccount: <dest-storage-account>
      container: ${pvc.namespace}
      blobPathTemplate: ${pvc.namespace}/tmpl/${pv.name}
      fsType: ext4
      templateBlobUrl: "https://<acct>.blob.core.windows.net/<container>/<golden-image>"
```

```bash
helm --kube-context "$CTX" upgrade --install ublk-azblob-csi deploy/chart \
  -n kube-system -f values.yaml
kubectl --context "$CTX" -n kube-system rollout status deploy/ublk-azblob-csi-controller
kubectl --context "$CTX" -n kube-system rollout status ds/ublk-azblob-csi-node
```

## 4. Empty disk

```bash
kubectl --context "$CTX" apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: empty-pvc, namespace: $NS }
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: azblob-rw
  resources: { requests: { storage: 8Gi } }
---
apiVersion: v1
kind: Pod
metadata: { name: empty-test, namespace: $NS }
spec:
  restartPolicy: Never
  containers:
    - name: app
      image: busybox:1.36
      command: ["sh","-c","mount|grep /data; echo hello > /data/file; sync; cat /data/file; sleep 3600"]
      volumeMounts: [{ name: vol, mountPath: /data }]
  volumes: [{ name: vol, persistentVolumeClaim: { claimName: empty-pvc } }]
EOF
kubectl --context "$CTX" -n "$NS" logs -f empty-test
```

Expect a fresh `ext4` mount and `hello` read back.

## 5. Read-only template mount

```bash
kubectl --context "$CTX" apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: ro-pvc, namespace: $NS }
spec:
  accessModes: ["ReadOnlyMany"]
  storageClassName: azblob-template-ro
  resources: { requests: { storage: 1Gi } }   # node sizes the device from the blob
---
apiVersion: v1
kind: Pod
metadata: { name: ro-test, namespace: $NS }
spec:
  restartPolicy: Never
  containers:
    - name: app
      image: busybox:1.36
      command: ["sh","-c","mount|grep /data; ls /data; (touch /data/x 2>&1 && echo WRITABLE) || echo 'OK read-only'; sleep 3600"]
      volumeMounts: [{ name: vol, mountPath: /data, readOnly: true }]
  volumes: [{ name: vol, persistentVolumeClaim: { claimName: ro-pvc, readOnly: true } }]
EOF
```

Expect the volume to bind at the **template's size**, the mount to be `ro`, the
golden-image contents listed, and writes rejected (`OK read-only`). The PV's
`volumeHandle` starts with `ro:`; deleting the PVC never deletes the template.

## 6. Read-write template copy

```bash
kubectl --context "$CTX" apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: rw-pvc, namespace: $NS }
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: azblob-template-rw
  resources: { requests: { storage: 64Gi } }   # >= template size
EOF
# Immediate binding runs the server-side copy inside CreateVolume; watch it bind:
kubectl --context "$CTX" -n "$NS" get pvc rw-pvc -w
# Controller log shows: "server-side copy (Put Page From URL)".
```

Then mount it (a `Deployment` makes the migration test in step 7 easy):

```bash
kubectl --context "$CTX" apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: { name: rw-app, namespace: $NS }
spec:
  replicas: 1
  selector: { matchLabels: { app: rw-app } }
  template:
    metadata: { labels: { app: rw-app } }
    spec:
      containers:
        - name: app
          image: busybox:1.36
          command: ["sh","-c","sleep 36000"]
          volumeMounts: [{ name: vol, mountPath: /data }]
      volumes: [{ name: vol, persistentVolumeClaim: { claimName: rw-pvc } }]
EOF
POD=$(kubectl --context "$CTX" -n "$NS" get pod -l app=rw-app -o jsonpath='{.items[0].metadata.name}')
kubectl --context "$CTX" -n "$NS" exec "$POD" -- sh -c \
  'ls /data; echo "marker $(date)" > /data/MARKER.txt; cat /data/MARKER.txt; sync'
```

Expect the golden-image contents present **and** writable (`MARKER.txt` written).

## 7. Node-to-node migration

Force the `rw-app` pod onto a different node and confirm the data survives:

```bash
POD=$(kubectl --context "$CTX" -n "$NS" get pod -l app=rw-app -o jsonpath='{.items[0].metadata.name}')
NODE=$(kubectl --context "$CTX" -n "$NS" get pod -l app=rw-app -o jsonpath='{.items[0].spec.nodeName}')

kubectl --context "$CTX" cordon "$NODE"
kubectl --context "$CTX" -n "$NS" delete pod "$POD" --wait=true
kubectl --context "$CTX" -n "$NS" rollout status deploy/rw-app
kubectl --context "$CTX" uncordon "$NODE"

NEWPOD=$(kubectl --context "$CTX" -n "$NS" get pod -l app=rw-app -o jsonpath='{.items[0].metadata.name}')
kubectl --context "$CTX" -n "$NS" exec "$NEWPOD" -- sh -c 'hostname; cat /data/MARKER.txt; ls /data'
```

The marker written on the old node must be readable on the new node — the
device is torn down on `NodeUnpublishVolume` (flushing to the blob) and
re-mounted on the new node.

## 8. Cleanup

```bash
kubectl --context "$CTX" -n "$NS" delete deploy rw-app
kubectl --context "$CTX" -n "$NS" delete pod empty-test ro-test --ignore-not-found
kubectl --context "$CTX" -n "$NS" delete pvc empty-pvc ro-pvc rw-pvc
# Retained read-only PVs are left behind by design; inspect/remove with:
kubectl --context "$CTX" get pv | grep ro-pvc
# helm --kube-context "$CTX" uninstall ublk-azblob-csi -n kube-system   # full teardown
```

> `azblob-template-rw` uses `reclaimPolicy: Delete`, so deleting `rw-pvc` removes
> the copied blob. `azblob-template-ro` uses `Retain` and `DeleteVolume` is a
> no-op for `ro:` volumes, so the shared golden image is never touched.

## Tunables

| Env (on the driver) | Default | Purpose |
| --- | --- | --- |
| `UBLK_COPY_CONCURRENCY` | `32` | In-flight `Put Page From URL` requests during a read-write copy. |
| `UBLK_COPY_CHUNK_BYTES` | `4194304` | Per-request copy chunk (512-aligned, ≤ 4 MiB). |
| `sidecars.provisioner.timeout` (chart) | `60s` | `CreateVolume` RPC timeout; raise for large read-write template copies. |

# ublk-azblob CSI Driver Helm Chart

A Helm chart for deploying the ublk-azblob CSI driver on Kubernetes. This driver enables mounting Azure Page Blob Storage as block devices using the Linux ublk (userspace block) kernel subsystem.

## Prerequisites

- Kubernetes 1.20+
- Helm 3.0+
- **For ublk mode (default):**
  - Linux kernel >= 6.0 with `CONFIG_BLK_DEV_UBLK=y` or `=m`
  - Ubuntu 24.04 (kernel 6.8+) recommended
  - `ublk_drv` kernel module loaded on nodes: `sudo modprobe ublk_drv`
- **For NBD mode (compatibility):**
  - Any modern Linux kernel (no special version requirements)
  - `nbd` kernel module loaded: `sudo modprobe nbd`
  - Standard `nbd-client` tool (usually pre-installed)

## Installation

### From the Helm repository (Artifact Hub)

The chart is published to a GitHub Pages based Helm repository and indexed on
[Artifact Hub](https://artifacthub.io/packages/search?repo=ublk-azblob-csi).

```bash
helm repo add ublk-azblob https://tg123.github.io/ublk-azblob
helm repo update
helm install ublk-azblob-csi ublk-azblob/ublk-azblob-csi
```

### From source

```bash
git clone https://github.com/tg123/ublk-azblob.git
helm install ublk-azblob-csi ./ublk-azblob/deploy/chart
```

### Quick Start (Per-Namespace Mode)

```bash
# Install the chart
helm install ublk-azblob-csi ./chart

# Create secret in your namespace
kubectl create secret generic azblob-csi-secret \
  --namespace=default \
  --from-literal=AZURE_CLIENT_ID=<your-client-id> \
  --from-literal=AZURE_TENANT_ID=<your-tenant-id> \
  --from-literal=AZURE_CLIENT_SECRET=<your-secret>

# Create a PVC
kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-data
  namespace: default
spec:
  storageClassName: azblob-ublk
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 10Gi
EOF
```

### Global Secret Mode

```bash
# Install with global secret mode
helm install ublk-azblob-csi ./chart \
  --set secretSearchMode=global \
  --set globalSecret.create=true \
  --set globalSecret.authMethod=service-principal \
  --set globalSecret.servicePrincipal.clientId=<client-id> \
  --set globalSecret.servicePrincipal.tenantId=<tenant-id> \
  --set globalSecret.servicePrincipal.clientSecret=<secret> \
  --set globalSecret.storageAccount=<account> \
  --set globalSecret.storageContainer=<container>
```

## Configuration

The following table lists the configurable parameters of the chart and their default values.

### Deployment Mode

| Parameter | Description | Default |
|-----------|-------------|---------|
| `secretSearchMode` | Where credentials are sourced: `per-namespace` or `global` | `per-namespace` |

### Driver Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `driver.name` | CSI driver name | `azblob.ublk.csi.tg123.github.io` |

### Image Configuration

A single image is shared by the controller and node plugins.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `image.repository` | Driver image repository | `docker.io/farmer1992/ublk-azblob` |
| `image.tag` | Driver image tag | `latest` |
| `image.pullPolicy` | Driver image pull policy | `IfNotPresent` |

### Azure I/O Gateway

Every Azure download (read) and upload (write / clear / copy) — on both the
controller (bulk template copy) and the node plugin (foreground I/O + flush) —
funnels through one gateway enforcing a shared concurrency budget, optional
per-direction bandwidth ceilings, and priority scheduling (foreground read >
flush > copy > warm-up). All values default to `0` (auto/unlimited); only
non-zero values are passed to the binary, which otherwise auto-sizes the budget
to the logical CPU count.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `io.concurrency` | Total concurrent Azure requests shared across both directions (`0` = auto/CPU count) | `0` |
| `io.downloadConcurrency` | Per-direction ceiling on concurrent downloads (`0` = full shared budget) | `0` |
| `io.uploadConcurrency` | Per-direction ceiling on concurrent uploads (`0` = full shared budget) | `0` |
| `io.downloadBandwidth` | Download bandwidth ceiling in bytes/sec (`0` = unlimited) | `0` |
| `io.uploadBandwidth` | Upload bandwidth ceiling in bytes/sec (`0` = unlimited) | `0` |

### Controller Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `controller.replicas` | Number of controller replicas | `1` |
| `controller.resources.limits.cpu` | Controller CPU limit | `200m` |
| `controller.resources.limits.memory` | Controller memory limit | `256Mi` |
| `controller.resources.requests.cpu` | Controller CPU request | `100m` |
| `controller.resources.requests.memory` | Controller memory request | `128Mi` |
| `controller.nodeSelector` | Controller node selector | `{}` |
| `controller.tolerations` | Controller tolerations | `[]` |
| `controller.affinity` | Controller affinity | `{}` |

### Node Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `node.useNbd` | Use NBD instead of ublk (for older kernels) | `false` |
| `node.nbd.portStart` | NBD server port range start | `10809` |
| `node.cache.enabled` | Enable the shared local-disk cache on each node | `false` |
| `node.cache.hostPath` | Host directory used as the shared cache | `/var/lib/ublk-azblob/cache` |
| `node.cache.maxBytes` | Max total cache bytes shared across volumes (0 = unlimited) | `0` |
| `node.cache.pageSize` | Cache page size in bytes | `1048576` |
| `node.cache.sharePages` | Share clean pages across volumes caching the same blob (cross-process page sharing). **Currently disabled / no-op** | `false` |
| `node.cache.warmup` | Background cache warm-up: each volume prefetches its blob into the cache on start | `false` |
| `node.cache.warmupBytes` | Cap in bytes for warm-up (0 = auto: the cache byte budget when set, else the whole device) | `0` |
| `node.resources.limits.cpu` | Node CPU limit | `500m` |
| `node.resources.limits.memory` | Node memory limit | `512Mi` |
| `node.resources.requests.cpu` | Node CPU request | `100m` |
| `node.resources.requests.memory` | Node memory request | `128Mi` |
| `node.nodeSelector` | Node selector | `{}` |
| `node.tolerations` | Node tolerations | `[{operator: Exists}]` |
| `node.affinity` | Node affinity | `{}` |

### StorageClass Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `storageClass.create` | Create StorageClass | `true` |
| `storageClass.name` | StorageClass name | `azblob-ublk` |
| `storageClass.isDefault` | Set as default StorageClass | `false` |
| `storageClass.reclaimPolicy` | Reclaim policy | `Delete` |
| `storageClass.volumeBindingMode` | Volume binding mode | `WaitForFirstConsumer` |
| `storageClass.allowVolumeExpansion` | Allow volume expansion | `false` |
| `storageClass.parameters.storageAccount` | Storage account (supports variables) | `""` |
| `storageClass.parameters.container` | Container name (supports variables) | `ublk-azblob-volumes` |
| `storageClass.parameters.blobPathTemplate` | Blob path template | `${pvc.namespace}/volumes/${pv.name}` |
| `storageClass.parameters.newBlobFsType` | Filesystem to format a freshly-provisioned blob (formattable profiles: ext2/3/4, xfs, btrfs) | `ext4` |
| `storageClass.parameters.templateBlobFsType` | Filesystem the node mounts a `templateBlobUrl` image as (template only; never reformatted; image-only types squashfs/ntfs work out of the box, `zfs` needs a custom image with the ZFS kernel module + zfsutils-linux) | `""` |
| `storageClass.parameters.templateBlobMountArgsOverwrite` | Advanced: override the built-in mount options of the `templateBlobFsType` profile (template only; comma/space-separated) | `""` |
| `storageClass.parameters.fsck` | Run `fsck` before mounting a writable, formatted volume: `"false"`/`"off"` (default, skip), `"true"`/`"preen"` (`fsck -a`), or `"force"` (`fsck -f -y`). Skipped for freshly-formatted and read-only volumes | `""` |
| `storageClass.parameters.templateBlobUrl` | Golden-image template blob URL (optional SAS; `?snapshot=` ⇒ mount the immutable snapshot directly read-only, no copy/lock/lease; non-snapshot ⇒ copy into the per-PVC blob read-write and skip format) | `""` |

### Global Secret Configuration (secretSearchMode: global)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `globalSecret.create` | Create global secret | `false` |
| `globalSecret.name` | Secret name | `csi-ublk-azblob-secret` |
| `globalSecret.authMethod` | Auth method: `service-principal`, `workload-identity`, `shared-key` | `service-principal` |
| `globalSecret.servicePrincipal.clientId` | Service Principal client ID | `""` |
| `globalSecret.servicePrincipal.tenantId` | Service Principal tenant ID | `""` |
| `globalSecret.servicePrincipal.clientSecret` | Service Principal client secret | `""` |
| `globalSecret.workloadIdentity.clientId` | Workload Identity client ID | `""` |
| `globalSecret.workloadIdentity.tenantId` | Workload Identity tenant ID | `""` |
| `globalSecret.sharedKey.accountKey` | SharedKey account key | `""` |
| `globalSecret.storageAccount` | Storage account name | `""` |
| `globalSecret.storageContainer` | Storage container name | `""` |
| `globalSecret.endpoint` | Custom endpoint with `%s` account template (Azurite/sovereign clouds), e.g. `http://%s.blob.localhost:10000/` | `""` |

### Per-Namespace Secret Configuration (secretSearchMode: per-namespace)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `perNamespaceSecret.name` | Secret name in each namespace | `azblob-csi-secret` |

### RBAC Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `rbac.create` | Create RBAC resources | `true` |
| `serviceAccount.controller.create` | Create controller ServiceAccount | `true` |
| `serviceAccount.controller.name` | Controller ServiceAccount name | `csi-ublk-azblob-controller` |
| `serviceAccount.controller.annotations` | Controller ServiceAccount annotations | `{}` |
| `serviceAccount.node.create` | Create node ServiceAccount | `true` |
| `serviceAccount.node.name` | Node ServiceAccount name | `csi-ublk-azblob-node` |
| `serviceAccount.node.annotations` | Node ServiceAccount annotations | `{}` |

### Other Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `namespace` | Namespace for driver components | `kube-system` |
| `additionalStorageClasses` | Additional StorageClasses | `[]` |

## Variable Expansion

All StorageClass parameters support variable expansion:

- `${pvc.namespace}` - PVC namespace
- `${pvc.name}` - PVC name
- `${pv.name}` - PV name (auto-generated: `pvc-<uuid>`)

### Examples

```yaml
# Namespace-specific storage account
storageClass:
  parameters:
    storageAccount: "${pvc.namespace}storage"
    container: "volumes"
    blobPathTemplate: "${pv.name}"
# Result: productionstorage/volumes/pvc-xxxxx

# Use namespace as container
storageClass:
  parameters:
    container: "${pvc.namespace}"
    blobPathTemplate: "volumes/${pv.name}"
# Result: <account>/production/volumes/pvc-xxxxx

# Include PVC name for readability
storageClass:
  parameters:
    blobPathTemplate: "${pvc.namespace}/${pvc.name}/${pv.name}"
# Result: <account>/<container>/production/myapp/pvc-xxxxx
```

## Examples

### Per-Namespace with Custom Path Pattern

```yaml
# values-per-namespace.yaml
secretSearchMode: per-namespace

storageClass:
  name: azblob-ublk
  parameters:
    container: "volumes"
    blobPathTemplate: "${pvc.namespace}/${pvc.name}/${pv.name}"
    newBlobFsType: ext4
```

```bash
helm install ublk-azblob-csi ./chart -f values-per-namespace.yaml
```

### Global Secret with Multiple StorageClasses

```yaml
# values-global.yaml
secretSearchMode: global

globalSecret:
  create: true
  authMethod: service-principal
  servicePrincipal:
    clientId: "12345678-1234-1234-1234-123456789012"
    tenantId: "87654321-4321-4321-4321-210987654321"
    clientSecret: "your-secret"
  storageAccount: "prodstorageaccount"
  storageContainer: "volumes"

storageClass:
  name: azblob-ublk-standard
  parameters:
    blobPathTemplate: "${pvc.namespace}/standard/${pv.name}"
    newBlobFsType: ext4

additionalStorageClasses:
  - name: azblob-ublk-xfs
    isDefault: false
    reclaimPolicy: Delete
    volumeBindingMode: WaitForFirstConsumer
    parameters:
      container: "database-volumes"
      blobPathTemplate: "${pvc.namespace}/db/${pv.name}"
      newBlobFsType: xfs
  
  - name: azblob-ublk-scratch
    isDefault: false
    reclaimPolicy: Retain
    volumeBindingMode: WaitForFirstConsumer
    parameters:
      container: "scratch"
      blobPathTemplate: "${pvc.namespace}/${pv.name}"
      newBlobFsType: ext4
```

```bash
helm install ublk-azblob-csi ./chart -f values-global.yaml
```

### Workload Identity

```yaml
# values-workload-identity.yaml
secretSearchMode: global

globalSecret:
  create: true
  authMethod: workload-identity
  workloadIdentity:
    clientId: "12345678-1234-1234-1234-123456789012"
    tenantId: "87654321-4321-4321-4321-210987654321"
  storageAccount: "prodstorageaccount"
  storageContainer: "volumes"

serviceAccount:
  controller:
    annotations:
      azure.workload.identity/client-id: "12345678-1234-1234-1234-123456789012"
  node:
    annotations:
      azure.workload.identity/client-id: "12345678-1234-1234-1234-123456789012"
```

```bash
helm install ublk-azblob-csi ./chart -f values-workload-identity.yaml
```

### NBD Mode (for older kernels without ublk_drv)

```yaml
# values-nbd.yaml
secretSearchMode: per-namespace

# Use NBD instead of ublk
node:
  useNbd: true
  nbd:
    portStart: 10809

storageClass:
  name: azblob-nbd
  parameters:
    container: "volumes"
    blobPathTemplate: '${pvc.namespace}/${pv.name}'
    newBlobFsType: ext4
```

```bash
# Ensure nbd module is loaded on nodes
# (This is usually available by default on most distributions)
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: nbd-loader
  namespace: kube-system
spec:
  selector:
    matchLabels:
      app: nbd-loader
  template:
    metadata:
      labels:
        app: nbd-loader
    spec:
      hostPID: true
      hostNetwork: true
      containers:
        - name: loader
          image: alpine:latest
          command:
            - sh
            - -c
            - |
              nsenter -t 1 -m modprobe nbd max_part=8
              echo "NBD module loaded"
              sleep infinity
          securityContext:
            privileged: true
EOF

# Install with NBD mode
helm install ublk-azblob-csi ./chart -f values-nbd.yaml
```

**Note:** NBD mode works on older kernels (no ublk_drv required) but has slightly lower performance than ublk mode.

## Upgrading

```bash
helm upgrade ublk-azblob-csi ./chart -f values.yaml
```

## Uninstalling

```bash
helm uninstall ublk-azblob-csi
```

**Note:** PVCs and PVs are not automatically deleted. Delete them manually if needed.

## Troubleshooting

### Kernel module not loaded

**Symptom (ublk mode):** Pods fail to mount volumes with error about /dev/ublk-control

**Solution:**
```bash
# On each node
sudo modprobe ublk_drv

# Make it persistent
echo "ublk_drv" | sudo tee /etc/modules-load.d/ublk.conf
```

**Symptom (NBD mode):** Pods fail to mount volumes with error about NBD

**Solution:**
```bash
# On each node
sudo modprobe nbd max_part=8

# Make it persistent
echo "nbd" | sudo tee /etc/modules-load.d/nbd.conf
echo "options nbd max_part=8" | sudo tee /etc/modprobe.d/nbd.conf
```

### Older kernel without ublk_drv

**Symptom:** Kernel < 6.0 or CONFIG_BLK_DEV_UBLK not enabled

**Solution:** Use NBD mode instead:
```bash
helm upgrade ublk-azblob-csi ./chart --set node.useNbd=true
```

### Secret not found

**Symptom:** PVC stuck in Pending with event "secret not found"

**Solution (per-namespace mode):**
```bash
kubectl create secret generic azblob-csi-secret \
  --namespace=<your-namespace> \
  --from-literal=AZURE_CLIENT_ID=<id> \
  --from-literal=AZURE_TENANT_ID=<tenant> \
  --from-literal=AZURE_CLIENT_SECRET=<secret>
```

### Authentication failures

**Symptom:** Controller logs show authentication errors

**Solution:** Verify credentials in secret:
```bash
kubectl get secret azblob-csi-secret -n <namespace> -o jsonpath='{.data}' | jq 'map_values(@base64d)'
```

## License

See [LICENSE](../../LICENSE) in the repository root.

## Contributing

See [CONTRIBUTING](../../CONTRIBUTING.md) in the repository root.

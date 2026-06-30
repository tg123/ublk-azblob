# Quick-start: mount an Azure Page Blob as a local disk (Docker Compose)

This example turns an **Azure Page Blob** into a regular Linux filesystem you can
read and write, using `ublk-azblob`. The page blob is the durable storage; the
local block device is just a window onto it.

For a zero-credential demo it runs **Azurite** (Microsoft's local Azure Storage
emulator) as the "remote" endpoint — the exact same Blob REST protocol as real
Azure. Swap one environment variable to point at a real Azure page blob.

```
┌────────────┐   Azure Blob REST    ┌──────────────┐   ublk / NBD   ┌───────────┐
│  Azurite   │◄────────────────────►│  ublk-azblob │◄──────────────►│ /dev/ublkb│
│ (page blob)│   (HTTP, like Azure) │   container  │  block device  │  + ext4   │
└────────────┘                      └──────────────┘                └───────────┘
```

## Requirements

* Docker + Docker Compose, on a **Linux host**.
* A block-device kernel module loaded on the **host** — **ublk preferred**:
  ```bash
  sudo modprobe ublk_drv     # preferred (Linux ≥ 6.0)
  # or, if ublk is unavailable:
  sudo modprobe nbd
  ```
  The demo auto-detects which is present and uses ublk when it can.

> The image isn't published yet, so Compose builds it from `deploy/Dockerfile`
> on first run (`--build`). This takes a few minutes once.

## Run it

```bash
cd examples/quickstart
docker compose up --build -d                 # start in the background
docker compose logs -f ublk-azblob           # watch until "✓ … mounted", then Ctrl-C
```

The driver provisions the page blob, brings the device up, formats it (first
run) and mounts it; the log ends with a directory listing of the mounted blob.

Browse and write to the mounted disk from inside the container:

```bash
docker compose exec ublk-azblob ls -la /mnt/azblob
docker compose exec ublk-azblob sh -c 'echo "written via the demo" > /mnt/azblob/note.txt'
```

Tear down (flushes the write-back buffer and detaches cleanly):

```bash
docker compose down
```

Because Azurite persists to `./azurite-data/`, the page blob — and everything you
wrote to it — survives `down`/`up`. `--create` in `mount-demo.sh` is idempotent
(it never zeroes an existing blob of the same size), so your data is preserved
across runs; the script also skips `mkfs` when a filesystem is already present.

## Use a real Azure page blob

Skip Azurite and point at your own blob. Provide a SAS (read/write) or an account
key, then bring up just the driver service:

```bash
export UBLK_BLOB_URL="https://<account>.blob.core.windows.net/<container>/<blob>.img"
export AZURE_STORAGE_SAS="sv=...&sig=..."     # or AZURE_STORAGE_KEY="<base64 key>"
docker compose up --build ublk-azblob
```

The driver also supports Managed Identity, Workload Identity and a service
principal — see the top-level README and `--help`.

## Browse the disk from the host (optional)

The disk is mounted **inside the container** at `/mnt/azblob`, so browse it with
`docker compose exec` (above). Surfacing the mount on the host needs shared mount
propagation, which is what the Kubernetes CSI driver handles for you — see the
[Kubernetes section](../../README.md#kubernetes-csi-driver--install--example).

## Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Azurite + the ublk-azblob mounter service |
| `mount-demo.sh` | Entrypoint: provision blob → auto-detect ublk/NBD → mkfs → mount |

## Troubleshooting

* **`neither ublk_drv nor NBD`** — load a module on the host: `sudo modprobe ublk_drv`.
* **`operation not permitted` / device errors** — the service must be
  `privileged: true` with `/dev` shared (already set in the compose file).
* **Want Kubernetes instead?** This same driver ships as a CSI driver — see the
  [Kubernetes section](../../README.md#kubernetes-csi-driver) in the main README.

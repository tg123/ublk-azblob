# scripts

Helper bash scripts for common `ublk-azblob` workflows.

## `import-folder.sh`

Loads a local folder into an Azure Page Blob through the **block-device** path
and prints the resulting blob URL. It performs the full cycle:

1. **start** the ublk device over the page blob (`ublk-azblob run --create`)
2. **mkfs** — create a filesystem on the new `/dev/ublkbN` (default `ext4`)
3. **mount** — mount it on a temporary mount point
4. **copy** — copy the contents of the source folder into the mount
5. **flush** — `sync` + `SIGUSR1` to drain pending writes to the page blob
6. **unmount + stop** the device cleanly
7. **output** — print the blob URL on stdout

The data lands in the blob as a real filesystem image, so it can later be served
read/write with `ublk-azblob run` (or NBD) and mounted directly.

### Requirements

- Linux ≥ 6.0 with `ublk_drv` loaded (`sudo modprobe ublk_drv`)
- root / `CAP_SYS_ADMIN`
- `mkfs.<fs>` for the chosen filesystem (e.g. `e2fsprogs` for `ext4`)
- a built `ublk-azblob` binary
  (`cargo build --release --features ublk -p ublk-azblob`)

### Example (Azurite / local dev)

```bash
sudo modprobe ublk_drv
cargo build --release --features ublk -p ublk-azblob

sudo AZURE_STORAGE_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" \
  scripts/import-folder.sh \
    --endpoint http://127.0.0.1:10000/devstoreaccount1 \
    --account devstoreaccount1 \
    --container mycontainer \
    --blob folder.img \
    --src ./mydir \
    --size 268435456
# ... → prints: http://127.0.0.1:10000/devstoreaccount1/mycontainer/folder.img
```

All storage selectors also accept their `AZURE_STORAGE_*` environment-variable
equivalents. Run `scripts/import-folder.sh --help` for the full option list.

For production, select Managed Identity by passing the matching flags after
`--`, e.g. `-- --msi`.

# Follow-ups

This PR (Kubernetes CSI driver) grew large, so the items below are intentionally
deferred to follow-up PRs to keep the diff reviewable.

## Tests

- [ ] **Graceful-shutdown e2e for both targets.** Add a test that proves a write
      buffered in memory is flushed to the page blob on `SIGINT`/`SIGTERM`
      (no explicit `SIGUSR1`/`NBD_CMD_FLUSH` and no umount FLUSH), then survives
      a remount:
  - **ublk** (`tests/mount_e2e.rs`): start `run` with auto-flush disabled
    (`--idle-flush-secs 0 --force-flush-timeout-secs 0`), `dd` a known pattern to
    the raw `/dev/ublkbN` with `oflag=direct` (no fsync), `SIGINT` the process and
    wait for a clean exit, then start a fresh device over the same blob and verify
    the pattern read back matches. Validates the shutdown flush added to
    `ublk_target::run_ublk_target`.
  - **nbd** (`tests/nbd_e2e.rs`): same idea over the in-process NBD client —
    `write_at` a pattern, `SIGINT` the server (instead of `kill`), restart over
    the same blob, `read_at` and verify. (The `nbd_e2e.rs` header comment "the NBD
    path installs no signal handler" is stale — `run_nbd_target` flushes on
    SIGINT/SIGTERM.)

## Bugs found in review (not yet fixed)

- [ ] **Coordination opt-in never reaches the node** (`csi/controller.rs` ~195).
      `CreateVolume` builds `volume_context` with container/blob/account/endpoint/
      size/fsType but not the `coordination` / `leaseNamespace` /
      `recoveryTimeoutSecs` keys, so the node's `child_env` never enables the
      cluster/blob lease even when the StorageClass opts in. Propagate those
      StorageClass parameters into `volume_context`.
- [ ] **Blob lease is not threaded into the device data path**
      (`coordination/mod.rs` ~226). The lease id is only used by the renewal loop;
      the device serves I/O through a separate `AzurePageBlobBackend` whose writes
      don't carry the lease condition, leaving a split-brain write window if the
      lease is broken. Condition writes/flush on the held lease (or document the
      guarantee precisely).
- [ ] **Long-lived child's stdout/stderr are piped but never drained**
      (`csi/mount.rs` ~99 / `spawn_device`). On the success path neither pipe is
      read, so a chatty `ublk-azblob run` child can fill the 64 KiB pipe buffer
      and block on write, hanging the device. Either inherit the streams (so they
      flow to the node plugin's logs) or drain them on a background thread.

## CI

(done) Merged `e2e` and `k8s-e2e` into a single workflow + docker-compose that
compiles the crate once (`--features "ublk csi"`) and runs mount/NBD and the
k8s PVC e2e from that one build (the binary is packaged into a thin image via
`deploy/Dockerfile --target runtime-prebuilt`). See `tests/e2e/docker-compose.yml`
and `tests/e2e/run.sh`.

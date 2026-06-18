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

## CI

- [ ] **Merge `e2e` and `k8s-e2e` workflows so the image/binary is built once.**
      Today `e2e.yml` builds the `tests/e2e/Dockerfile` runner (compiles
      `--features ublk`, runs `mount_e2e`/`nbd_e2e`) and `k8s-e2e.yml` builds the
      `tests/e2e/k8s/Dockerfile` runner *and* the `deploy/Dockerfile` driver
      image. Consolidate into a single workflow that builds the driver image once
      and shares it (artifact or local registry) across both suites.

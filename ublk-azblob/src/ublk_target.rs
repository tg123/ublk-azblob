//! ublk target — userspace block device I/O loop.
//!
//! This module bridges the Linux ublk kernel framework (via `libublk`) and the
//! `BlobBackend` trait.  Each I/O request received from the kernel is
//! dispatched to the appropriate `BlobBackend` method.
//!
//! ## Feature flag
//! The real ublk loop is gated behind the `ublk` Cargo feature because it
//! requires:
//! - Linux kernel ≥ 6.0 with `ublk_drv` loaded
//! - Root / `CAP_SYS_ADMIN`
//! - The `libublk` crate
//!
//! Without the feature flag the module exposes a stub that prints a clear
//! error and exits.  All `BlobBackend` logic (read/write/clear) can still be
//! exercised through unit tests and the `test` subcommand without the kernel
//! driver.
//!
//! ## Signals (feature = "ublk")
//! Once the device is up the process installs handlers for:
//! - `SIGINT` / `SIGTERM` → tear the device down cleanly (queues drain, then
//!   `/dev/ublkbN` is removed).
//! - `SIGUSR1` → force a `BlobBackend::flush`, draining any pending writes to
//!   durable storage without unmounting.

use crate::backend::BlobBackend;
use std::sync::Arc;

/// Configuration for the ublk device.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct UblkConfig {
    /// Logical block size in bytes (must be a multiple of 512).
    pub block_size: u32,
    /// Total device size in bytes (must be a multiple of `block_size`).
    pub dev_size: u64,
    /// Number of io_uring queues (1 is fine for Phase 1).
    pub nr_queues: u16,
    /// Queue depth (number of concurrent in-flight I/O operations per queue).
    pub queue_depth: u16,
    /// Device id to request (`-1` lets the kernel auto-allocate).
    pub id: i32,
    /// Expose the device read-only (kernel rejects writes with `EROFS`).
    pub read_only: bool,
}

impl Default for UblkConfig {
    fn default() -> Self {
        Self {
            block_size: 512,
            dev_size: 0,
            nr_queues: 1,
            queue_depth: 64,
            id: -1,
            read_only: false,
        }
    }
}

/// Run the ublk device, blocking until the device is stopped.
///
/// On success the function returns when the device is cleanly shut down.
/// On platforms without ublk support (or without the `ublk` feature) it
/// returns an error immediately.
pub async fn run_ublk_target(backend: Arc<dyn BlobBackend>, cfg: UblkConfig) -> anyhow::Result<()> {
    #[cfg(feature = "ublk")]
    {
        // `libublk` drives blocking, per-thread io_uring queues and joins them,
        // so run the whole target on a dedicated blocking thread and keep the
        // current Tokio runtime free to service the backend's HTTP futures.
        let rt = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || run_ublk_target_blocking(backend, cfg, rt))
            .await
            .map_err(|e| anyhow::anyhow!("ublk target task panicked: {e}"))?
    }
    #[cfg(not(feature = "ublk"))]
    {
        let _ = (backend, cfg);
        anyhow::bail!(
            "ublk kernel target is not compiled in.\n\
             Rebuild with `--features ublk` on a Linux host with ublk_drv loaded.\n\
             To exercise the BlobBackend without a kernel, use the `test` \
             subcommand (write → read → clear → verify smoke test)."
        );
    }
}

// ── Real ublk implementation (feature = "ublk") ───────────────────────────────

#[cfg(feature = "ublk")]
mod signals {
    use std::sync::atomic::{AtomicBool, Ordering};
    use tracing::warn;

    pub static STOP: AtomicBool = AtomicBool::new(false);
    pub static FLUSH: AtomicBool = AtomicBool::new(false);

    extern "C" fn on_stop(_sig: libc::c_int) {
        STOP.store(true, Ordering::SeqCst);
    }

    extern "C" fn on_flush(_sig: libc::c_int) {
        FLUSH.store(true, Ordering::SeqCst);
    }

    /// Install async-signal-safe handlers that only flip atomics.
    ///
    /// Uses `sigaction` rather than the deprecated `signal()` so behaviour is
    /// well-defined in the presence of the threads spawned by Tokio and
    /// `libublk`. Any installation failure is logged as a warning so the device
    /// can continue running even if one signal hook is unavailable.
    fn set_handler(sig: libc::c_int, handler: extern "C" fn(libc::c_int)) -> std::io::Result<()> {
        // SAFETY: `sa` is fully initialised before use and the handler is
        // async-signal-safe (it only stores into an atomic).
        unsafe {
            let mut sa: libc::sigaction = std::mem::zeroed();
            sa.sa_sigaction = handler as libc::sighandler_t;
            if libc::sigemptyset(&mut sa.sa_mask) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            sa.sa_flags = libc::SA_RESTART;
            if libc::sigaction(sig, &sa, std::ptr::null_mut()) != 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        Ok(())
    }

    pub fn install() {
        // Reset state so a stop/flush from a previous run in the same process
        // does not leak into this one.
        STOP.store(false, Ordering::SeqCst);
        FLUSH.store(false, Ordering::SeqCst);
        for (sig, name, handler) in [
            (
                libc::SIGINT,
                "SIGINT",
                on_stop as extern "C" fn(libc::c_int),
            ),
            (libc::SIGTERM, "SIGTERM", on_stop),
            (libc::SIGUSR1, "SIGUSR1", on_flush),
        ] {
            if let Err(e) = set_handler(sig, handler) {
                warn!(signal = name, err = %e, "failed to install signal handler");
            }
        }
    }
}

#[cfg(feature = "ublk")]
fn run_ublk_target_blocking(
    backend: Arc<dyn BlobBackend>,
    cfg: UblkConfig,
    rt: tokio::runtime::Handle,
) -> anyhow::Result<()> {
    use anyhow::Context as _;
    use libublk::ctrl::UblkCtrl;
    use libublk::ctrl::UblkCtrlBuilder;
    use libublk::io::{BufDescList, UblkDev, UblkIOCtx, UblkQueue};
    use libublk::{sys, BufDesc, UblkFlags, UblkIORes};
    use std::rc::Rc;
    use std::time::Duration;
    use tracing::{error, info};

    info!(
        dev_size = cfg.dev_size,
        nr_queues = cfg.nr_queues,
        queue_depth = cfg.queue_depth,
        id = cfg.id,
        read_only = cfg.read_only,
        "starting ublk target"
    );

    let ctrl = UblkCtrlBuilder::default()
        .name("azblob")
        .id(cfg.id)
        .nr_queues(cfg.nr_queues)
        .depth(cfg.queue_depth)
        .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
        .build()
        .context("build UblkCtrl (is ublk_drv loaded? do you have root?)")?;

    let dev_size = cfg.dev_size;
    let block_size = cfg.block_size;
    let read_only = cfg.read_only;
    if block_size < 512 || !block_size.is_multiple_of(512) || !block_size.is_power_of_two() {
        anyhow::bail!(
            "block_size ({block_size}) must be a power of two that is >= 512 and a multiple of 512"
        );
    }
    let block_size_u64 = u64::from(block_size);
    let block_size_usize = usize::try_from(block_size).expect("block size fits in usize");
    let tgt_init = move |dev: &mut UblkDev| {
        // Advertise a volatile write cache so the kernel issues FLUSH on
        // sync/umount, then honour the configured logical block size.  The
        // power-of-two check above guarantees `trailing_zeros` is the correct
        // shift for `block_size`.
        dev.set_default_params(dev_size);
        dev.tgt.params.basic.logical_bs_shift = block_size.trailing_zeros() as u8;
        // In read-only mode advertise UBLK_ATTR_READ_ONLY so the kernel marks
        // /dev/ublkbN read-only and rejects writes with EROFS.
        if read_only {
            dev.tgt.params.basic.attrs |= sys::UBLK_ATTR_READ_ONLY;
        }
        Ok(())
    };

    // Per-queue handler.  `run_target` requires the closure to be `Clone`;
    // `Arc` and `Handle` are both cheap to clone.
    let q_handler = {
        let backend = backend.clone();
        let rt = rt.clone();
        move |qid: u16, dev: &UblkDev| {
            let bufs = Rc::new(dev.alloc_queue_io_bufs());
            let bufs_io = bufs.clone();
            let backend = backend.clone();
            let rt = rt.clone();

            let io_handler = move |q: &UblkQueue, tag: u16, _io: &UblkIOCtx| {
                let iod = q.get_iod(tag);
                let op = iod.op_flags & 0xff;
                let off = iod.start_sector * block_size_u64;
                let len = usize::try_from(iod.nr_sectors).expect("nr_sectors fits in usize")
                    * block_size_usize;
                let buf = &bufs_io[tag as usize];

                let res: i32 = match op {
                    sys::UBLK_IO_OP_READ => match rt.block_on(backend.read(off, len as u64)) {
                        Ok(data) => {
                            if data.len() != len {
                                // A short/over-long backend response would mask
                                // out-of-bounds reads or backend corruption;
                                // surface it as an I/O error instead.
                                error!(
                                    tag,
                                    off,
                                    len,
                                    got = data.len(),
                                    "read returned wrong length"
                                );
                                -libc::EIO
                            } else {
                                let dst = unsafe {
                                    std::slice::from_raw_parts_mut(buf.as_mut_ptr(), len)
                                };
                                dst.copy_from_slice(&data);
                                len as i32
                            }
                        }
                        Err(e) => {
                            error!(tag, off, len, err = %e, "read failed");
                            -libc::EIO
                        }
                    },
                    sys::UBLK_IO_OP_WRITE => {
                        let src = unsafe { std::slice::from_raw_parts(buf.as_ptr(), len) };
                        let data = bytes::Bytes::copy_from_slice(src);
                        match rt.block_on(backend.write(off, data)) {
                            Ok(()) => len as i32,
                            Err(e) => {
                                error!(tag, off, len, err = %e, "write failed");
                                -libc::EIO
                            }
                        }
                    }
                    sys::UBLK_IO_OP_FLUSH => match rt.block_on(backend.flush()) {
                        Ok(()) => 0,
                        Err(e) => {
                            error!(tag, err = %e, "flush failed");
                            -libc::EIO
                        }
                    },
                    sys::UBLK_IO_OP_DISCARD | sys::UBLK_IO_OP_WRITE_ZEROES => {
                        match rt.block_on(backend.clear(off, len as u64)) {
                            Ok(()) => 0,
                            Err(e) => {
                                error!(tag, off, len, err = %e, "discard/clear failed");
                                -libc::EIO
                            }
                        }
                    }
                    unknown => {
                        error!(tag, op = unknown, "unknown I/O op — returning EINVAL");
                        -libc::EINVAL
                    }
                };

                let io_slice = bufs_io[tag as usize].as_slice();
                q.complete_io_cmd_unified(
                    tag,
                    BufDesc::Slice(io_slice),
                    Ok(UblkIORes::Result(res)),
                )
                .unwrap();
            };

            match UblkQueue::new(qid, dev)
                .and_then(|q| q.submit_fetch_commands_unified(BufDescList::Slices(Some(&bufs))))
            {
                Ok(queue) => queue.wait_and_handle_io(io_handler),
                Err(e) => error!(qid, err = %e, "failed to set up ublk queue"),
            }
        }
    };

    // Post-start hook: runs on this thread once `/dev/ublkbN` exists.  Wait for
    // a stop signal, servicing SIGUSR1 flush requests in the meantime, then tear
    // the device down so the queue threads drain and `run_target` returns.
    let device_fn = {
        let backend = backend.clone();
        let rt = rt.clone();
        move |ctrl: &UblkCtrl| {
            signals::install();
            info!(dev_id = ctrl.dev_info().dev_id, "ublk device ready");
            while !signals::STOP.load(std::sync::atomic::Ordering::SeqCst) {
                if signals::FLUSH.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    info!("SIGUSR1 received — forcing backend flush");
                    if let Err(e) = rt.block_on(backend.flush()) {
                        error!(err = %e, "forced flush failed");
                    }
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            info!("stop signal received — shutting down ublk device");
            if let Err(e) = ctrl.kill_dev() {
                error!(err = ?e, "kill_dev failed");
            }
        }
    };

    ctrl.run_target(tgt_init, q_handler, device_fn)
        .map_err(|e| anyhow::anyhow!("run ublk target: {e:?}"))?;

    info!("ublk target stopped");
    Ok(())
}

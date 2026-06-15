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
//! exercised through the integration tests without the kernel driver.

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
}

impl Default for UblkConfig {
    fn default() -> Self {
        Self {
            block_size: 512,
            dev_size: 0,
            nr_queues: 1,
            queue_depth: 64,
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
        run_ublk_target_inner(backend, cfg).await
    }
    #[cfg(not(feature = "ublk"))]
    {
        let _ = (backend, cfg);
        anyhow::bail!(
            "ublk kernel target is not compiled in.\n\
             Rebuild with `--features ublk` on a Linux host with ublk_drv loaded.\n\
             For testing the BlobBackend without a kernel, use the e2e integration tests."
        );
    }
}

// ── Real ublk implementation (feature = "ublk") ───────────────────────────────

#[cfg(feature = "ublk")]
async fn run_ublk_target_inner(
    backend: Arc<dyn BlobBackend>,
    cfg: UblkConfig,
) -> anyhow::Result<()> {
    use anyhow::Context as _;
    use libublk::{
        ctrl::UblkCtrlBuilder,
        io::{UblkDev, UblkIOCtx, UblkQueue},
        sys, UblkError, UblkFlags, UblkIORes,
    };
    use std::rc::Rc;
    use tracing::{error, info};

    info!(
        dev_size = cfg.dev_size,
        nr_queues = cfg.nr_queues,
        queue_depth = cfg.queue_depth,
        "starting ublk target"
    );

    // Build the ublk control device.
    let ctrl = UblkCtrlBuilder::default()
        .name("azblob")
        .nr_queues(cfg.nr_queues)
        .depth(cfg.queue_depth)
        .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
        .build()
        .context("build UblkCtrl (is ublk_drv loaded? do you have root?)")?;

    // We use tokio for async I/O but libublk queues are per-thread.
    let backend_clone = backend.clone();
    let rt = tokio::runtime::Handle::current();

    let dev = UblkDev::new(
        "azblob".to_string(),
        |dev: &mut UblkDev| {
            dev.set_default_params(cfg.dev_size);
            Ok(serde_json::json!({}))
        },
        &ctrl,
    )
    .context("create UblkDev")?;

    // Run one queue (Phase 1: single queue, single thread).
    // TODO(Phase 2): spawn one task per queue for parallelism.
    let backend_q = backend_clone.clone();
    ctrl.run_target(&dev, |qid: u16, dev: &UblkDev| {
        let bufs = Rc::new(dev.alloc_queue_io_bufs());
        let bufs2 = bufs.clone();
        let backend_io = backend_q.clone();

        let handler = move |q: &UblkQueue, tag: u16, _io: &UblkIOCtx| {
            let iod = q.get_iod(tag);
            let op = iod.op_flags & 0xFF;
            let off = (iod.start_sector as u64) * 512;
            let len = (iod.nr_sectors as u64) * 512;

            match op as u32 {
                sys::UBLK_IO_OP_READ => {
                    let buf = &mut bufs2[tag as usize];
                    let buf_slice =
                        unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr(), len as usize) };

                    // Drive the async read synchronously from this sync context.
                    match rt.block_on(backend_io.read(off, len)) {
                        Ok(data) => {
                            let copy_len = data.len().min(len as usize);
                            buf_slice[..copy_len].copy_from_slice(&data[..copy_len]);
                            q.complete_io_cmd(tag, Ok(UblkIORes::Result(copy_len as i32)))
                                .unwrap();
                        }
                        Err(e) => {
                            error!(tag, off, len, err = %e, "read failed");
                            q.complete_io_cmd(tag, Err(UblkError::OtherError(-libc::EIO)))
                                .unwrap();
                        }
                    }
                }
                sys::UBLK_IO_OP_WRITE => {
                    let buf = &bufs2[tag as usize];
                    let data = bytes::Bytes::copy_from_slice(unsafe {
                        std::slice::from_raw_parts(buf.as_ptr(), len as usize)
                    });

                    match rt.block_on(backend_io.write(off, data)) {
                        Ok(()) => {
                            q.complete_io_cmd(tag, Ok(UblkIORes::Result(len as i32)))
                                .unwrap();
                        }
                        Err(e) => {
                            error!(tag, off, len, err = %e, "write failed");
                            q.complete_io_cmd(tag, Err(UblkError::OtherError(-libc::EIO)))
                                .unwrap();
                        }
                    }
                }
                sys::UBLK_IO_OP_DISCARD | sys::UBLK_IO_OP_WRITE_ZEROES => {
                    match rt.block_on(backend_io.clear(off, len)) {
                        Ok(()) => {
                            q.complete_io_cmd(tag, Ok(UblkIORes::Result(0))).unwrap();
                        }
                        Err(e) => {
                            error!(tag, off, len, err = %e, "discard/clear failed");
                            q.complete_io_cmd(tag, Err(UblkError::OtherError(-libc::EIO)))
                                .unwrap();
                        }
                    }
                }
                sys::UBLK_IO_OP_FLUSH => match rt.block_on(backend_io.flush()) {
                    Ok(()) => {
                        q.complete_io_cmd(tag, Ok(UblkIORes::Result(0))).unwrap();
                    }
                    Err(e) => {
                        error!(tag, err = %e, "flush failed");
                        q.complete_io_cmd(tag, Err(UblkError::OtherError(-libc::EIO)))
                            .unwrap();
                    }
                },
                unknown => {
                    error!(tag, op = unknown, "unknown I/O op — returning EIO");
                    q.complete_io_cmd(tag, Err(UblkError::OtherError(-libc::EIO)))
                        .unwrap();
                }
            }
        };

        let queue = UblkQueue::new(qid, dev).unwrap().submit_fetch_commands()?;
        queue.wait_and_handle_io(handler);
        Ok(())
    })
    .context("run ublk target")?;

    info!("ublk target stopped");
    Ok(())
}

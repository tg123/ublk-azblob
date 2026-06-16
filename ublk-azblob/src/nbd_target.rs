//! NBD target — userspace block device I/O loop over the Network Block Device
//! protocol.
//!
//! This module is the portable, *compatibility* counterpart to
//! [`crate::ublk_target`].  Where the ublk target needs Linux ≥ 6.0 with
//! `ublk_drv` loaded and `CAP_SYS_ADMIN`, the NBD target only needs a TCP
//! socket and the standard `nbd` client (`nbd-client` / `/dev/nbdX`, available
//! since Linux 2.x), so it runs on older kernels, inside containers without the
//! ublk device, and on any host that can speak the NBD protocol (including
//! `qemu-nbd` based tooling).
//!
//! It implements the server side of the NBD *fixed newstyle* handshake and the
//! transmission phase, dispatching each request to the same [`BlobBackend`]
//! trait used by the ublk target:
//!
//! - `NBD_CMD_READ`         → [`BlobBackend::read`]
//! - `NBD_CMD_WRITE`        → [`BlobBackend::write`]
//! - `NBD_CMD_FLUSH`        → [`BlobBackend::flush`]
//! - `NBD_CMD_TRIM`         → [`BlobBackend::clear`]
//! - `NBD_CMD_WRITE_ZEROES` → [`BlobBackend::clear`]
//! - `NBD_CMD_DISC`         → flush and close the connection
//!
//! Only *simple* replies are emitted (no structured replies), which every NBD
//! client understands.  The server advertises a 512-byte minimum / 4 KiB
//! preferred block size so clients align their I/O to the Azure Page Blob
//! granularity.

use crate::backend::BlobBackend;
use anyhow::Context as _;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

// ── Protocol constants ────────────────────────────────────────────────────────

// Handshake magics.
const NBDMAGIC: u64 = 0x4e42444d_41474943; // "NBDMAGIC"
const IHAVEOPT: u64 = 0x49484156_454f5054; // "IHAVEOPT"
const NBD_REP_MAGIC: u64 = 0x0003_e889_0455_65a9;

// Handshake flags (server → client).
const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1 << 0;
const NBD_FLAG_NO_ZEROES: u16 = 1 << 1;

// Client handshake flags (client → server).
const NBD_FLAG_C_NO_ZEROES: u32 = 1 << 1;

// Transmission flags (advertised in the export info).
const NBD_FLAG_HAS_FLAGS: u16 = 1 << 0;
const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;
const NBD_FLAG_SEND_TRIM: u16 = 1 << 5;
const NBD_FLAG_SEND_WRITE_ZEROES: u16 = 1 << 6;

// Option types (client → server).
const NBD_OPT_EXPORT_NAME: u32 = 1;
const NBD_OPT_ABORT: u32 = 2;
const NBD_OPT_LIST: u32 = 3;
const NBD_OPT_INFO: u32 = 6;
const NBD_OPT_GO: u32 = 7;

// Option reply types (server → client).
const NBD_REP_ACK: u32 = 1;
const NBD_REP_SERVER: u32 = 2;
const NBD_REP_INFO: u32 = 3;
const NBD_REP_ERR_UNSUP: u32 = 0x8000_0001;

// Info types (inside NBD_REP_INFO).
const NBD_INFO_EXPORT: u16 = 0;
const NBD_INFO_BLOCK_SIZE: u16 = 3;

// Transmission phase magics.
const NBD_REQUEST_MAGIC: u32 = 0x25609513;
const NBD_SIMPLE_REPLY_MAGIC: u32 = 0x67446698;

// Transmission commands.
const NBD_CMD_READ: u16 = 0;
const NBD_CMD_WRITE: u16 = 1;
const NBD_CMD_DISC: u16 = 2;
const NBD_CMD_FLUSH: u16 = 3;
const NBD_CMD_TRIM: u16 = 4;
const NBD_CMD_WRITE_ZEROES: u16 = 6;

// NBD error codes (a subset of errno used on the wire).
const NBD_EIO: u32 = 5;
const NBD_EINVAL: u32 = 22;
const NBD_ENOSPC: u32 = 28;

/// Logical block size advertised to NBD clients.  Matches the Azure Page Blob
/// 512-byte alignment requirement enforced by [`BlobBackend`].
const BLOCK_SIZE: u32 = 512;

/// The single export name this server serves.  NBD clients that omit a name
/// (the common case for `nbd-client host port /dev/nbd0`) get this export.
const EXPORT_NAME: &str = "azblob";

// ── Public entry point ─────────────────────────────────────────────────────────

/// Run the NBD server, listening on `addr` and serving `dev_size` bytes of the
/// given [`BlobBackend`].
///
/// Each accepted connection is handled concurrently.  The function runs until
/// the listener fails or the future is dropped (e.g. on Ctrl-C from the caller).
pub async fn run_nbd_target(
    backend: Arc<dyn BlobBackend>,
    addr: &str,
    dev_size: u64,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind NBD listener on {addr}"))?;
    let local = listener.local_addr().context("listener local_addr")?;
    info!(
        addr = %local,
        dev_size,
        export = EXPORT_NAME,
        "NBD server listening — connect with e.g. `nbd-client {host} {port} /dev/nbd0`",
        host = local.ip(),
        port = local.port(),
    );

    loop {
        let (stream, peer) = listener.accept().await.context("accept NBD connection")?;
        let backend = backend.clone();
        info!(peer = %peer, "NBD client connected");
        tokio::spawn(async move {
            if let Err(e) = serve_connection(stream, backend, dev_size).await {
                // A client disconnecting mid-stream is normal; log at warn so it
                // is visible without being alarming.
                warn!(peer = %peer, err = %e, "NBD connection ended");
            } else {
                info!(peer = %peer, "NBD client disconnected");
            }
        });
    }
}

// ── Connection handling ────────────────────────────────────────────────────────

/// Transmission flags advertised for the export.
fn transmission_flags() -> u16 {
    NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM | NBD_FLAG_SEND_WRITE_ZEROES
}

/// Drive one client connection: handshake → option haggling → transmission.
async fn serve_connection(
    mut stream: TcpStream,
    backend: Arc<dyn BlobBackend>,
    dev_size: u64,
) -> anyhow::Result<()> {
    stream.set_nodelay(true).ok();

    // ── Fixed newstyle handshake ──
    stream.write_u64(NBDMAGIC).await?;
    stream.write_u64(IHAVEOPT).await?;
    stream
        .write_u16(NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES)
        .await?;
    stream.flush().await?;

    let client_flags = stream.read_u32().await.context("read client flags")?;
    let no_zeroes = client_flags & NBD_FLAG_C_NO_ZEROES != 0;

    // ── Option haggling ──
    // Returns once the client selects an export (GO/EXPORT_NAME) or aborts.
    if !negotiate(&mut stream, dev_size, no_zeroes).await? {
        // Client aborted or asked only for information; nothing more to do.
        return Ok(());
    }

    // ── Transmission phase ──
    transmission(&mut stream, backend, dev_size).await
}

/// Option haggling loop.  Returns `Ok(true)` when the client has selected an
/// export and the connection should proceed to the transmission phase, or
/// `Ok(false)` when the client aborted / only queried information.
async fn negotiate(stream: &mut TcpStream, dev_size: u64, no_zeroes: bool) -> anyhow::Result<bool> {
    loop {
        let magic = stream.read_u64().await.context("read option magic")?;
        if magic != IHAVEOPT {
            anyhow::bail!("bad option magic: {magic:#x}");
        }
        let opt = stream.read_u32().await.context("read option type")?;
        let len = stream.read_u32().await.context("read option length")?;
        let mut data = vec![0u8; len as usize];
        stream
            .read_exact(&mut data)
            .await
            .context("read option data")?;

        match opt {
            NBD_OPT_EXPORT_NAME => {
                // Legacy selection: reply with the export tuple and switch to
                // transmission immediately (no reply header, no trailing ACK).
                stream.write_u64(dev_size).await?;
                stream.write_u16(transmission_flags()).await?;
                if !no_zeroes {
                    stream.write_all(&[0u8; 124]).await?;
                }
                stream.flush().await?;
                return Ok(true);
            }
            NBD_OPT_INFO | NBD_OPT_GO => {
                send_export_info(stream, opt, dev_size).await?;
                send_opt_reply(stream, opt, NBD_REP_ACK, &[]).await?;
                stream.flush().await?;
                if opt == NBD_OPT_GO {
                    return Ok(true);
                }
                // NBD_OPT_INFO is informational only; keep haggling.
            }
            NBD_OPT_LIST => {
                // Advertise our single export name.
                let name = EXPORT_NAME.as_bytes();
                let mut buf = Vec::with_capacity(4 + name.len());
                buf.extend_from_slice(&(name.len() as u32).to_be_bytes());
                buf.extend_from_slice(name);
                send_opt_reply(stream, opt, NBD_REP_SERVER, &buf).await?;
                send_opt_reply(stream, opt, NBD_REP_ACK, &[]).await?;
                stream.flush().await?;
            }
            NBD_OPT_ABORT => {
                send_opt_reply(stream, opt, NBD_REP_ACK, &[]).await?;
                stream.flush().await?;
                return Ok(false);
            }
            other => {
                warn!(option = other, "unsupported NBD option — replying UNSUP");
                send_opt_reply(stream, other, NBD_REP_ERR_UNSUP, &[]).await?;
                stream.flush().await?;
            }
        }
    }
}

/// Send an `NBD_REP_INFO` (`NBD_INFO_EXPORT` + `NBD_INFO_BLOCK_SIZE`) in
/// response to `NBD_OPT_GO` / `NBD_OPT_INFO`.
async fn send_export_info(stream: &mut TcpStream, opt: u32, dev_size: u64) -> anyhow::Result<()> {
    // NBD_INFO_EXPORT: u16 type, u64 size, u16 transmission flags.
    let mut export = Vec::with_capacity(12);
    export.extend_from_slice(&NBD_INFO_EXPORT.to_be_bytes());
    export.extend_from_slice(&dev_size.to_be_bytes());
    export.extend_from_slice(&transmission_flags().to_be_bytes());
    send_opt_reply(stream, opt, NBD_REP_INFO, &export).await?;

    // NBD_INFO_BLOCK_SIZE: u16 type, u32 min, u32 preferred, u32 max.
    let mut bs = Vec::with_capacity(14);
    bs.extend_from_slice(&NBD_INFO_BLOCK_SIZE.to_be_bytes());
    bs.extend_from_slice(&BLOCK_SIZE.to_be_bytes()); // minimum
    bs.extend_from_slice(&(BLOCK_SIZE * 8).to_be_bytes()); // preferred (4 KiB)
    bs.extend_from_slice(&(32 * 1024 * 1024u32).to_be_bytes()); // maximum (32 MiB)
    send_opt_reply(stream, opt, NBD_REP_INFO, &bs).await?;
    Ok(())
}

/// Write one option reply header + payload.
async fn send_opt_reply(
    stream: &mut TcpStream,
    opt: u32,
    rep_type: u32,
    data: &[u8],
) -> anyhow::Result<()> {
    stream.write_u64(NBD_REP_MAGIC).await?;
    stream.write_u32(opt).await?;
    stream.write_u32(rep_type).await?;
    stream.write_u32(data.len() as u32).await?;
    if !data.is_empty() {
        stream.write_all(data).await?;
    }
    Ok(())
}

/// Transmission phase: read requests until disconnect.
async fn transmission(
    stream: &mut TcpStream,
    backend: Arc<dyn BlobBackend>,
    dev_size: u64,
) -> anyhow::Result<()> {
    loop {
        // Request header: magic, flags, type, handle, offset, length.
        let magic = match stream.read_u32().await {
            Ok(m) => m,
            // Clean EOF: client closed the socket.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e).context("read request magic"),
        };
        if magic != NBD_REQUEST_MAGIC {
            anyhow::bail!("bad request magic: {magic:#x}");
        }
        let _flags = stream.read_u16().await.context("read request flags")?;
        let cmd = stream.read_u16().await.context("read request type")?;
        let handle = stream.read_u64().await.context("read request handle")?;
        let offset = stream.read_u64().await.context("read request offset")?;
        let length = stream.read_u32().await.context("read request length")? as u64;

        match cmd {
            NBD_CMD_DISC => {
                // Disconnect: best-effort flush, then close.
                if let Err(e) = backend.flush().await {
                    error!(err = %e, "flush on disconnect failed");
                }
                return Ok(());
            }
            NBD_CMD_READ => {
                if let Some(err) = bounds_error(offset, length, dev_size) {
                    simple_reply(stream, err, handle, &[]).await?;
                    stream.flush().await?;
                    continue;
                }
                match backend.read(offset, length).await {
                    Ok(data) if data.len() as u64 == length => {
                        simple_reply(stream, 0, handle, &data).await?;
                    }
                    Ok(other) => {
                        error!(offset, length, got = other.len(), "short read from backend");
                        simple_reply(stream, NBD_EIO, handle, &[]).await?;
                    }
                    Err(e) => {
                        error!(offset, length, err = %e, "read failed");
                        simple_reply(stream, NBD_EIO, handle, &[]).await?;
                    }
                }
            }
            NBD_CMD_WRITE => {
                let mut buf = vec![0u8; length as usize];
                stream
                    .read_exact(&mut buf)
                    .await
                    .context("read write payload")?;
                if let Some(err) = bounds_error(offset, length, dev_size) {
                    simple_reply(stream, err, handle, &[]).await?;
                    stream.flush().await?;
                    continue;
                }
                match backend.write(offset, bytes::Bytes::from(buf)).await {
                    Ok(()) => simple_reply(stream, 0, handle, &[]).await?,
                    Err(e) => {
                        error!(offset, length, err = %e, "write failed");
                        simple_reply(stream, NBD_EIO, handle, &[]).await?;
                    }
                }
            }
            NBD_CMD_FLUSH => match backend.flush().await {
                Ok(()) => simple_reply(stream, 0, handle, &[]).await?,
                Err(e) => {
                    error!(err = %e, "flush failed");
                    simple_reply(stream, NBD_EIO, handle, &[]).await?;
                }
            },
            NBD_CMD_TRIM | NBD_CMD_WRITE_ZEROES => {
                if let Some(err) = bounds_error(offset, length, dev_size) {
                    simple_reply(stream, err, handle, &[]).await?;
                    stream.flush().await?;
                    continue;
                }
                match backend.clear(offset, length).await {
                    Ok(()) => simple_reply(stream, 0, handle, &[]).await?,
                    Err(e) => {
                        error!(offset, length, err = %e, "trim/write-zeroes failed");
                        simple_reply(stream, NBD_EIO, handle, &[]).await?;
                    }
                }
            }
            other => {
                warn!(cmd = other, "unsupported NBD command — replying EINVAL");
                simple_reply(stream, NBD_EINVAL, handle, &[]).await?;
            }
        }
        stream.flush().await?;
    }
}

/// Validate a request against alignment and device bounds, returning the NBD
/// error code to send if it is invalid.
fn bounds_error(offset: u64, length: u64, dev_size: u64) -> Option<u32> {
    if !offset.is_multiple_of(u64::from(BLOCK_SIZE))
        || !length.is_multiple_of(u64::from(BLOCK_SIZE))
    {
        return Some(NBD_EINVAL);
    }
    match offset.checked_add(length) {
        Some(end) if end <= dev_size => None,
        _ => Some(NBD_ENOSPC),
    }
}

/// Emit a simple reply header, followed by `data` (for reads).
async fn simple_reply(
    stream: &mut TcpStream,
    error: u32,
    handle: u64,
    data: &[u8],
) -> anyhow::Result<()> {
    stream.write_u32(NBD_SIMPLE_REPLY_MAGIC).await?;
    stream.write_u32(error).await?;
    stream.write_u64(handle).await?;
    if !data.is_empty() {
        stream.write_all(data).await?;
    }
    Ok(())
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::mem::MemBackend;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    /// Spawn an NBD server on an ephemeral port backed by a `MemBackend` and
    /// return its address plus the backend (for assertions).
    async fn spawn_server(size: u64) -> (String, Arc<dyn BlobBackend>) {
        let backend: Arc<dyn BlobBackend> = Arc::new(MemBackend::new(size).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let b = backend.clone();
        tokio::spawn(async move {
            // Accept a single connection and serve it.
            let (stream, _) = listener.accept().await.unwrap();
            let _ = serve_connection(stream, b, size).await;
        });
        (addr, backend)
    }

    /// Perform the fixed-newstyle handshake using NBD_OPT_GO and return the
    /// connected stream positioned at the transmission phase.
    async fn handshake(addr: &str) -> TcpStream {
        let mut s = TcpStream::connect(addr).await.unwrap();
        assert_eq!(s.read_u64().await.unwrap(), NBDMAGIC);
        assert_eq!(s.read_u64().await.unwrap(), IHAVEOPT);
        let server_flags = s.read_u16().await.unwrap();
        assert!(server_flags & NBD_FLAG_FIXED_NEWSTYLE != 0);

        // Client flags: select NO_ZEROES.
        s.write_u32(NBD_FLAG_C_NO_ZEROES).await.unwrap();

        // NBD_OPT_GO with export name "azblob" and zero info requests.
        let name = EXPORT_NAME.as_bytes();
        let mut data = Vec::new();
        data.extend_from_slice(&(name.len() as u32).to_be_bytes());
        data.extend_from_slice(name);
        data.extend_from_slice(&0u16.to_be_bytes()); // number of info requests
        s.write_u64(IHAVEOPT).await.unwrap();
        s.write_u32(NBD_OPT_GO).await.unwrap();
        s.write_u32(data.len() as u32).await.unwrap();
        s.write_all(&data).await.unwrap();
        s.flush().await.unwrap();

        // Drain option replies until ACK for NBD_OPT_GO.
        loop {
            assert_eq!(s.read_u64().await.unwrap(), NBD_REP_MAGIC);
            let opt = s.read_u32().await.unwrap();
            let rep = s.read_u32().await.unwrap();
            let len = s.read_u32().await.unwrap();
            let mut buf = vec![0u8; len as usize];
            s.read_exact(&mut buf).await.unwrap();
            assert_eq!(opt, NBD_OPT_GO);
            if rep == NBD_REP_ACK {
                break;
            }
        }
        s
    }

    async fn send_request(
        s: &mut TcpStream,
        cmd: u16,
        handle: u64,
        offset: u64,
        length: u32,
        payload: &[u8],
    ) {
        s.write_u32(NBD_REQUEST_MAGIC).await.unwrap();
        s.write_u16(0).await.unwrap();
        s.write_u16(cmd).await.unwrap();
        s.write_u64(handle).await.unwrap();
        s.write_u64(offset).await.unwrap();
        s.write_u32(length).await.unwrap();
        if !payload.is_empty() {
            s.write_all(payload).await.unwrap();
        }
        s.flush().await.unwrap();
    }

    /// Read a simple reply header; returns (error, handle).
    async fn read_reply(s: &mut TcpStream) -> (u32, u64) {
        assert_eq!(s.read_u32().await.unwrap(), NBD_SIMPLE_REPLY_MAGIC);
        let error = s.read_u32().await.unwrap();
        let handle = s.read_u64().await.unwrap();
        (error, handle)
    }

    #[tokio::test]
    async fn write_then_read_roundtrip() {
        let (addr, _backend) = spawn_server(4096).await;
        let mut s = handshake(&addr).await;

        let payload = vec![0xABu8; 512];
        send_request(&mut s, NBD_CMD_WRITE, 1, 512, 512, &payload).await;
        let (err, handle) = read_reply(&mut s).await;
        assert_eq!(err, 0);
        assert_eq!(handle, 1);

        send_request(&mut s, NBD_CMD_READ, 2, 512, 512, &[]).await;
        let (err, handle) = read_reply(&mut s).await;
        assert_eq!(err, 0);
        assert_eq!(handle, 2);
        let mut got = vec![0u8; 512];
        s.read_exact(&mut got).await.unwrap();
        assert_eq!(got, payload);
    }

    #[tokio::test]
    async fn trim_zeroes_region() {
        let (addr, _backend) = spawn_server(4096).await;
        let mut s = handshake(&addr).await;

        send_request(&mut s, NBD_CMD_WRITE, 1, 0, 1024, &vec![0xFFu8; 1024]).await;
        assert_eq!(read_reply(&mut s).await.0, 0);

        send_request(&mut s, NBD_CMD_TRIM, 2, 0, 1024, &[]).await;
        assert_eq!(read_reply(&mut s).await.0, 0);

        send_request(&mut s, NBD_CMD_READ, 3, 0, 1024, &[]).await;
        assert_eq!(read_reply(&mut s).await.0, 0);
        let mut got = vec![0u8; 1024];
        s.read_exact(&mut got).await.unwrap();
        assert!(got.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn flush_succeeds() {
        let (addr, _backend) = spawn_server(4096).await;
        let mut s = handshake(&addr).await;
        send_request(&mut s, NBD_CMD_FLUSH, 7, 0, 0, &[]).await;
        let (err, handle) = read_reply(&mut s).await;
        assert_eq!(err, 0);
        assert_eq!(handle, 7);
    }

    #[tokio::test]
    async fn unaligned_request_rejected() {
        let (addr, _backend) = spawn_server(4096).await;
        let mut s = handshake(&addr).await;
        // Offset 1 is not 512-aligned → EINVAL.
        send_request(&mut s, NBD_CMD_READ, 9, 1, 512, &[]).await;
        let (err, handle) = read_reply(&mut s).await;
        assert_eq!(err, NBD_EINVAL);
        assert_eq!(handle, 9);
    }

    #[tokio::test]
    async fn out_of_bounds_rejected() {
        let (addr, _backend) = spawn_server(512).await;
        let mut s = handshake(&addr).await;
        send_request(&mut s, NBD_CMD_READ, 11, 0, 1024, &[]).await;
        let (err, handle) = read_reply(&mut s).await;
        assert_eq!(err, NBD_ENOSPC);
        assert_eq!(handle, 11);
    }
}

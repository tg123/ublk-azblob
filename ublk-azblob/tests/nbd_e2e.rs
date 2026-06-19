//! Block-device-level e2e test for `ublk-azblob`'s NBD compatibility target,
//! written in Rust.
//!
//! This is the NBD counterpart of [`mount_e2e`](./mount_e2e.rs).  Where the
//! mount test exercises the kernel `ublk` path (and therefore needs root, the
//! `ublk_drv` module and the `ublk` Cargo feature), the NBD target speaks the
//! Network Block Device *fixed-newstyle* protocol over a plain TCP socket, so
//! the whole stack can be driven by an in-process NBD client with **no kernel
//! module, no root and no extra Cargo feature**.  Only a reachable Azurite (the
//! real Azure Page Blob backend in CI) is required.
//!
//! Cycle:
//!   1. start `ublk-azblob run --create --nbd 127.0.0.1:<port>` against Azurite
//!   2. perform the NBD handshake, then `NBD_CMD_WRITE` a handful of random
//!      512-aligned regions, recording each region's SHA-256
//!   3. read every region back in the *same* session and verify it matches
//!   4. `NBD_CMD_FLUSH` then `NBD_CMD_DISC` to force a backend flush to the page
//!      blob, then stop the server
//!   5. start the server again over the *same* page blob (no `--create`)
//!   6. read every region back and verify its SHA-256 still matches — proving
//!      the data round-tripped through Put Page / Get Page Ranges and survived
//!      tearing the NBD server down and bringing it back up
//!
//! The test is **not** gated behind the `ublk` feature, so it compiles into the
//! default test binary.  When Azurite is not reachable (the common case for a
//! plain `cargo test` run) it skips cleanly instead of failing.
//!
//! A second test, [`nbd_read_only`](fn.nbd_read_only.html), exercises
//! `run --read-only`: it asserts the export advertises `NBD_FLAG_READ_ONLY`,
//! reads succeed, and writes / trims / write-zeroes are rejected with `EPERM`
//! without mutating the backing blob.
//!
//! Run it (with Azurite up) via:
//!
//! ```text
//! AZURE_STORAGE_ENDPOINT="http://127.0.0.1:10000/devstoreaccount1" \
//!   cargo test --test nbd_e2e -- --nocapture
//! ```

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::thread::sleep;
use std::time::{Duration, Instant};

/// Azurite well-known development account name.
const DEFAULT_ACCOUNT: &str = "devstoreaccount1";
/// Azurite well-known development account key.
const DEFAULT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:10000/devstoreaccount1";
const DEFAULT_CONTAINER: &str = "e2etest";
const DEFAULT_BLOB: &str = "nbdtest";

/// Host:port the `nbd_roundtrip` test's NBD server binds to.  Kept distinct from
/// the default NBD port (10809) to avoid clashing with anything a developer may
/// already be running locally.
const NBD_ADDR_ROUNDTRIP: &str = "127.0.0.1:11809";
/// Host:port the `nbd_read_only` test's NBD server binds to.  A separate port
/// from [`NBD_ADDR_ROUNDTRIP`] so the two tests can run in parallel (the default
/// for `cargo test`) without racing for a single socket.
const NBD_ADDR_READ_ONLY: &str = "127.0.0.1:11810";
const BLOB_SIZE: u64 = 64 * 1024 * 1024; // 64 MiB
const NUM_REGIONS: usize = 8;
/// Logical block size advertised by the NBD target (matches `BLOCK_SIZE` in
/// `src/nbd_target.rs`).
const BLOCK_SIZE: u64 = 512;

// ── NBD protocol constants (mirror src/nbd_target.rs) ────────────────────────

const NBDMAGIC: u64 = 0x4e42444d_41474943;
const IHAVEOPT: u64 = 0x49484156_454f5054;
const NBD_REP_MAGIC: u64 = 0x0003_e889_0455_65a9;

const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1 << 0;
const NBD_FLAG_C_NO_ZEROES: u32 = 1 << 1;

const NBD_OPT_GO: u32 = 7;
const NBD_REP_ACK: u32 = 1;
const NBD_REP_INFO: u32 = 3;

/// `NBD_INFO_EXPORT` information-reply type (carries size + transmission flags).
const NBD_INFO_EXPORT: u16 = 0;
/// Transmission flag set when the export is read-only (mirrors
/// `NBD_FLAG_READ_ONLY` in `src/nbd_target.rs`).
const NBD_FLAG_READ_ONLY: u16 = 1 << 1;

const NBD_REQUEST_MAGIC: u32 = 0x25609513;
const NBD_SIMPLE_REPLY_MAGIC: u32 = 0x67446698;

const NBD_CMD_READ: u16 = 0;
const NBD_CMD_WRITE: u16 = 1;
const NBD_CMD_DISC: u16 = 2;
const NBD_CMD_FLUSH: u16 = 3;
const NBD_CMD_TRIM: u16 = 4;
const NBD_CMD_WRITE_ZEROES: u16 = 6;

/// NBD error code returned for an operation rejected on a read-only export
/// (`EPERM`; mirrors `NBD_EPERM` in `src/nbd_target.rs`).
const NBD_EPERM: u32 = 1;

const EXPORT_NAME: &str = "azblob";

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn log(msg: &str) {
    println!("=== {msg} ===");
}

/// Derive the `host:port` authority of the Azurite endpoint so the test can
/// probe whether Azurite is reachable before running.
fn endpoint_authority() -> String {
    let ep = env_or("AZURE_STORAGE_ENDPOINT", DEFAULT_ENDPOINT);
    // Strip the scheme, then keep everything up to the first '/'.
    let without_scheme = ep.split("://").nth(1).unwrap_or(&ep);
    without_scheme
        .split('/')
        .next()
        .unwrap_or(without_scheme)
        .to_string()
}

/// True when Azurite (the Azure Page Blob backend) is reachable — without it
/// the NBD server cannot create/serve a blob, so the test skips.
fn azurite_available() -> bool {
    let authority = endpoint_authority();
    let Ok(mut addrs) = authority.to_socket_addrs() else {
        return false;
    };
    addrs.any(|addr| TcpStream::connect_timeout(&addr, Duration::from_secs(2)).is_ok())
}

/// Common Azure environment passed to the `ublk-azblob` child process.
fn azure_env(cmd: &mut Command, container: &str, blob: &str) {
    cmd.env(
        "AZURE_STORAGE_ACCOUNT",
        env_or("AZURE_STORAGE_ACCOUNT", DEFAULT_ACCOUNT),
    )
    .env(
        "AZURE_STORAGE_KEY",
        env_or("AZURE_STORAGE_KEY", DEFAULT_KEY),
    )
    .env(
        "AZURE_STORAGE_ENDPOINT",
        env_or("AZURE_STORAGE_ENDPOINT", DEFAULT_ENDPOINT),
    )
    .env("AZURE_STORAGE_CONTAINER", container)
    .env("AZURE_STORAGE_BLOB", blob);
}

/// Start the NBD server as a child process and wait until it accepts a TCP
/// connection on `addr`.  When `create` is true the page blob is provisioned
/// first.
///
/// The returned `Child` is always `wait()`ed on by the caller (via
/// `stop_server`), so the zombie-process lint does not apply.
#[allow(clippy::zombie_processes)]
fn start_server(addr: &str, container: &str, blob: &str, create: bool) -> Child {
    start_server_opts(addr, container, blob, create, false)
}

/// Like [`start_server`] but lets the caller expose the export read-only
/// (`run --read-only`).  `create` and `read_only` are mutually exclusive at the
/// CLI level, so callers pass `create=false` when `read_only=true`.
#[allow(clippy::zombie_processes)]
fn start_server_opts(
    addr: &str,
    container: &str,
    blob: &str,
    create: bool,
    read_only: bool,
) -> Child {
    log(&format!(
        "starting NBD server on {addr} ({}{})",
        if create { "--create" } else { "reuse blob" },
        if read_only { ", --read-only" } else { "" }
    ));
    // Prefer an externally-provided binary (the e2e runs the actual image built
    // from deploy/Dockerfile); fall back to the cargo-built binary for local runs.
    let bin = std::env::var("UBLK_AZBLOB_BIN")
        .unwrap_or_else(|_| env!("CARGO_BIN_EXE_ublk-azblob").to_string());
    let mut cmd = Command::new(&bin);
    cmd.arg("run")
        .arg("--size")
        .arg(BLOB_SIZE.to_string())
        .arg("--nbd")
        .arg(addr);
    if create {
        cmd.arg("--create");
    }
    if read_only {
        cmd.arg("--read-only");
    }
    azure_env(&mut cmd, container, blob);

    let mut child = cmd.spawn().expect("failed to spawn ublk-azblob");

    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok(stream) = TcpStream::connect(addr) {
            drop(stream);
            log(&format!("NBD server is up (pid {})", child.id()));
            return child;
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("ublk-azblob exited before the NBD port opened: {status}");
        }
        sleep(Duration::from_millis(500));
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("timed out waiting for the NBD server to listen on {addr}");
}

/// Stop the running NBD server: the data was already flushed (`NBD_CMD_FLUSH` +
/// `NBD_CMD_DISC`) before this is called, so the process is simply killed.  The
/// NBD path installs no signal handler, so a clean exit status is not expected.
fn stop_server(mut child: Child) {
    log(&format!("stopping NBD server (pid {})", child.id()));
    let _ = child.kill();
    let _ = child.wait();
}

/// SHA-256 of a byte slice, as a lowercase hex string.
fn sha256_hex(data: &[u8]) -> String {
    let digest = hmac_sha256::Hash::hash(data);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

/// `len` random bytes from `/dev/urandom`.
fn random_bytes(len: usize) -> Vec<u8> {
    let mut urandom = std::fs::File::open("/dev/urandom").expect("open /dev/urandom");
    let mut data = vec![0u8; len];
    urandom.read_exact(&mut data).expect("read /dev/urandom");
    data
}

// ── Minimal synchronous NBD client ───────────────────────────────────────────

/// An NBD client connection sitting in the transmission phase.
struct NbdClient {
    stream: TcpStream,
    handle: u64,
    /// Transmission flags advertised by the server for the selected export
    /// (captured from `NBD_INFO_EXPORT` during the handshake).
    transmission_flags: u16,
}

impl NbdClient {
    /// Connect to `addr` and complete the fixed-newstyle handshake using
    /// `NBD_OPT_GO`, leaving the connection in the transmission phase.
    fn connect(addr: &str) -> NbdClient {
        let mut stream = TcpStream::connect(addr).expect("connect NBD server");
        stream.set_nodelay(true).ok();
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(30)))
            .unwrap();

        // Server greeting.
        assert_eq!(read_u64(&mut stream), NBDMAGIC, "bad NBD greeting magic");
        assert_eq!(read_u64(&mut stream), IHAVEOPT, "bad NBD option magic");
        let server_flags = read_u16(&mut stream);
        assert!(
            server_flags & NBD_FLAG_FIXED_NEWSTYLE != 0,
            "server is not fixed-newstyle"
        );

        // Client flags.
        write_u32(&mut stream, NBD_FLAG_C_NO_ZEROES);

        // NBD_OPT_GO { name, 0 info requests }.
        let name = EXPORT_NAME.as_bytes();
        let mut opt = Vec::new();
        opt.extend_from_slice(&(name.len() as u32).to_be_bytes());
        opt.extend_from_slice(name);
        opt.extend_from_slice(&0u16.to_be_bytes());
        write_u64(&mut stream, IHAVEOPT);
        write_u32(&mut stream, NBD_OPT_GO);
        write_u32(&mut stream, opt.len() as u32);
        stream.write_all(&opt).unwrap();
        stream.flush().unwrap();

        // Drain option replies until the GO is ACKed, capturing the export's
        // transmission flags from the NBD_INFO_EXPORT reply along the way.
        let mut transmission_flags = 0u16;
        loop {
            assert_eq!(
                read_u64(&mut stream),
                NBD_REP_MAGIC,
                "bad option reply magic"
            );
            let opt_echo = read_u32(&mut stream);
            let rep = read_u32(&mut stream);
            let len = read_u32(&mut stream) as usize;
            let mut buf = vec![0u8; len];
            stream.read_exact(&mut buf).unwrap();
            assert_eq!(opt_echo, NBD_OPT_GO, "unexpected option echoed");
            // NBD_INFO_EXPORT: u16 info type, u64 size, u16 transmission flags.
            if rep == NBD_REP_INFO
                && buf.len() >= 12
                && u16::from_be_bytes([buf[0], buf[1]]) == NBD_INFO_EXPORT
            {
                transmission_flags = u16::from_be_bytes([buf[10], buf[11]]);
            }
            if rep == NBD_REP_ACK {
                break;
            }
        }

        NbdClient {
            stream,
            handle: 0,
            transmission_flags,
        }
    }

    fn next_handle(&mut self) -> u64 {
        self.handle += 1;
        self.handle
    }

    /// Send a request header (and optional payload) and read the simple-reply
    /// header back, asserting the handle matches and returning the error code.
    fn request(&mut self, cmd: u16, offset: u64, length: u32, payload: &[u8]) -> u32 {
        let handle = self.next_handle();
        write_u32(&mut self.stream, NBD_REQUEST_MAGIC);
        write_u16(&mut self.stream, 0); // flags
        write_u16(&mut self.stream, cmd);
        write_u64(&mut self.stream, handle);
        write_u64(&mut self.stream, offset);
        write_u32(&mut self.stream, length);
        if !payload.is_empty() {
            self.stream.write_all(payload).unwrap();
        }
        self.stream.flush().unwrap();

        assert_eq!(
            read_u32(&mut self.stream),
            NBD_SIMPLE_REPLY_MAGIC,
            "bad simple-reply magic"
        );
        let error = read_u32(&mut self.stream);
        let got_handle = read_u64(&mut self.stream);
        assert_eq!(got_handle, handle, "reply handle mismatch");
        error
    }

    fn write_at(&mut self, offset: u64, data: &[u8]) {
        let err = self.request(NBD_CMD_WRITE, offset, data.len() as u32, data);
        assert_eq!(err, 0, "NBD write at {offset} failed (err {err})");
    }

    fn read_at(&mut self, offset: u64, length: u32) -> Vec<u8> {
        let err = self.request(NBD_CMD_READ, offset, length, &[]);
        assert_eq!(err, 0, "NBD read at {offset} failed (err {err})");
        let mut buf = vec![0u8; length as usize];
        self.stream.read_exact(&mut buf).unwrap();
        buf
    }

    fn flush(&mut self) {
        let err = self.request(NBD_CMD_FLUSH, 0, 0, &[]);
        assert_eq!(err, 0, "NBD flush failed (err {err})");
    }

    /// Send `NBD_CMD_DISC` (no reply is sent for a disconnect) and drop the
    /// connection.
    fn disconnect(mut self) {
        let handle = self.next_handle();
        write_u32(&mut self.stream, NBD_REQUEST_MAGIC);
        write_u16(&mut self.stream, 0);
        write_u16(&mut self.stream, NBD_CMD_DISC);
        write_u64(&mut self.stream, handle);
        write_u64(&mut self.stream, 0);
        write_u32(&mut self.stream, 0);
        let _ = self.stream.flush();
    }
}

// ── Blocking big-endian wire helpers ─────────────────────────────────────────

fn read_u16(s: &mut TcpStream) -> u16 {
    let mut b = [0u8; 2];
    s.read_exact(&mut b).unwrap();
    u16::from_be_bytes(b)
}

fn read_u32(s: &mut TcpStream) -> u32 {
    let mut b = [0u8; 4];
    s.read_exact(&mut b).unwrap();
    u32::from_be_bytes(b)
}

fn read_u64(s: &mut TcpStream) -> u64 {
    let mut b = [0u8; 8];
    s.read_exact(&mut b).unwrap();
    u64::from_be_bytes(b)
}

fn write_u16(s: &mut TcpStream, v: u16) {
    s.write_all(&v.to_be_bytes()).unwrap();
}

fn write_u32(s: &mut TcpStream, v: u32) {
    s.write_all(&v.to_be_bytes()).unwrap();
}

fn write_u64(s: &mut TcpStream, v: u64) {
    s.write_all(&v.to_be_bytes()).unwrap();
}

// ── The test ─────────────────────────────────────────────────────────────────

#[test]
fn nbd_roundtrip() {
    if !azurite_available() {
        eprintln!(
            "skipping nbd_roundtrip: Azurite is not reachable at {} \
             (set AZURE_STORAGE_ENDPOINT and start Azurite to run this test)",
            endpoint_authority()
        );
        return;
    }
    // The NBD server child process must exist (e.g. an earlier `/dev/ublkb`
    // device node must not collide); a previous run must have released the port.
    assert!(
        TcpStream::connect(NBD_ADDR_ROUNDTRIP).is_err(),
        "{NBD_ADDR_ROUNDTRIP} is already in use; another NBD server is running"
    );

    let container = env_or("AZURE_STORAGE_CONTAINER", DEFAULT_CONTAINER);
    let blob = env_or("AZURE_STORAGE_BLOB", DEFAULT_BLOB);

    // ── Phase 1: provision the blob, write random regions, verify in-session ──
    let child = start_server(NBD_ADDR_ROUNDTRIP, &container, &blob, true);
    let mut client = NbdClient::connect(NBD_ADDR_ROUNDTRIP);

    log(&format!("writing {NUM_REGIONS} random regions"));
    let mut checksums: Vec<(u64, usize, String)> = Vec::with_capacity(NUM_REGIONS);
    for i in 0..NUM_REGIONS {
        // Place each region on a distinct 1 MiB boundary (512-aligned), with a
        // deterministic-but-varied 512-aligned length between 512 B and ~32 KiB.
        let offset = (i as u64) * 1024 * 1024;
        let blocks = 1 + (i * 7) % 64;
        let len = blocks * BLOCK_SIZE as usize;
        let data = random_bytes(len);
        client.write_at(offset, &data);
        let sum = sha256_hex(&data);
        println!("{sum}  offset={offset} len={len}");
        checksums.push((offset, len, sum));
    }

    log("verifying regions in the same session (before any restart)");
    for (offset, len, expected) in &checksums {
        let got = client.read_at(*offset, *len as u32);
        assert_eq!(
            &sha256_hex(&got),
            expected,
            "in-session checksum mismatch at offset {offset}"
        );
    }

    log("flush + disconnect to force the data out to the page blob");
    client.flush();
    client.disconnect();
    // Give the server a moment to process the disconnect/flush before teardown.
    sleep(Duration::from_secs(1));
    stop_server(child);

    // Wait for the port to be released so the second server can bind it.
    wait_for_port_release(NBD_ADDR_ROUNDTRIP);

    // ── Phase 2: restart over the same blob and re-verify ─────────────────────
    let child = start_server(NBD_ADDR_ROUNDTRIP, &container, &blob, false);
    let mut client = NbdClient::connect(NBD_ADDR_ROUNDTRIP);

    log("verifying regions after restart (data round-tripped through Azure)");
    for (offset, len, expected) in &checksums {
        let got = client.read_at(*offset, *len as u32);
        assert_eq!(
            &sha256_hex(&got),
            expected,
            "post-restart checksum mismatch at offset {offset}"
        );
        println!("offset={offset}: OK");
    }

    client.disconnect();
    sleep(Duration::from_secs(1));
    stop_server(child);

    log("nbd e2e PASSED ✓");
}

/// e2e for read-only mode (`run --read-only`) over the NBD path.
///
/// Cycle:
///   1. provision the blob writable, write a few random regions, flush, stop
///   2. restart the server with `--read-only` over the *same* blob and assert:
///      * the export advertises `NBD_FLAG_READ_ONLY`
///      * reads return the data written in phase 1 (it round-tripped through
///        Azure and is served read-only)
///      * `NBD_CMD_WRITE`, `NBD_CMD_TRIM` and `NBD_CMD_WRITE_ZEROES` are all
///        rejected with `EPERM`
///   3. restart writable once more and confirm the blob was never modified by
///      the rejected writes (checksums still match phase 1)
#[test]
fn nbd_read_only() {
    if !azurite_available() {
        eprintln!(
            "skipping nbd_read_only: Azurite is not reachable at {} \
             (set AZURE_STORAGE_ENDPOINT and start Azurite to run this test)",
            endpoint_authority()
        );
        return;
    }
    assert!(
        TcpStream::connect(NBD_ADDR_READ_ONLY).is_err(),
        "{NBD_ADDR_READ_ONLY} is already in use; another NBD server is running"
    );

    let container = env_or("AZURE_STORAGE_CONTAINER", DEFAULT_CONTAINER);
    // Distinct blob so this test never collides with `nbd_roundtrip`.
    let blob = format!("{}-ro", env_or("AZURE_STORAGE_BLOB", DEFAULT_BLOB));

    // ── Phase 1: provision the blob writable and seed known regions ───────────
    let child = start_server(NBD_ADDR_READ_ONLY, &container, &blob, true);
    let mut client = NbdClient::connect(NBD_ADDR_READ_ONLY);
    assert_eq!(
        client.transmission_flags & NBD_FLAG_READ_ONLY,
        0,
        "writable export must not advertise the read-only flag"
    );

    log(&format!("writing {NUM_REGIONS} random regions"));
    let mut checksums: Vec<(u64, usize, String)> = Vec::with_capacity(NUM_REGIONS);
    for i in 0..NUM_REGIONS {
        let offset = (i as u64) * 1024 * 1024;
        let blocks = 1 + (i * 7) % 64;
        let len = blocks * BLOCK_SIZE as usize;
        let data = random_bytes(len);
        client.write_at(offset, &data);
        checksums.push((offset, len, sha256_hex(&data)));
    }
    client.flush();
    client.disconnect();
    sleep(Duration::from_secs(1));
    stop_server(child);

    wait_for_port_release(NBD_ADDR_READ_ONLY);

    // ── Phase 2: reopen read-only and assert the export rejects mutations ─────
    let child = start_server_opts(NBD_ADDR_READ_ONLY, &container, &blob, false, true);
    let mut client = NbdClient::connect(NBD_ADDR_READ_ONLY);
    assert_eq!(
        client.transmission_flags & NBD_FLAG_READ_ONLY,
        NBD_FLAG_READ_ONLY,
        "read-only export must advertise NBD_FLAG_READ_ONLY"
    );

    log("verifying reads succeed on the read-only export");
    for (offset, len, expected) in &checksums {
        let got = client.read_at(*offset, *len as u32);
        assert_eq!(
            &sha256_hex(&got),
            expected,
            "read-only checksum mismatch at offset {offset}"
        );
    }

    log("verifying writes / trim / write-zeroes are rejected with EPERM");
    // Target the first seeded region so a *successful* write would be detectable.
    let (offset, len, _) = checksums[0];
    let payload = random_bytes(len);
    let werr = client.request(NBD_CMD_WRITE, offset, len as u32, &payload);
    assert_eq!(werr, NBD_EPERM, "write on read-only export should be EPERM");
    let terr = client.request(NBD_CMD_TRIM, offset, len as u32, &[]);
    assert_eq!(terr, NBD_EPERM, "trim on read-only export should be EPERM");
    let zerr = client.request(NBD_CMD_WRITE_ZEROES, offset, len as u32, &[]);
    assert_eq!(
        zerr, NBD_EPERM,
        "write-zeroes on read-only export should be EPERM"
    );

    // Reads still work after the rejected mutations (the stream stayed in sync).
    let got = client.read_at(offset, len as u32);
    assert_eq!(
        &sha256_hex(&got),
        &checksums[0].2,
        "data changed under a read-only export"
    );
    client.disconnect();
    sleep(Duration::from_secs(1));
    stop_server(child);

    wait_for_port_release(NBD_ADDR_READ_ONLY);

    // ── Phase 3: reopen writable and confirm nothing was mutated ──────────────
    let child = start_server(NBD_ADDR_READ_ONLY, &container, &blob, false);
    let mut client = NbdClient::connect(NBD_ADDR_READ_ONLY);
    log("verifying the blob was never modified by the rejected writes");
    for (offset, len, expected) in &checksums {
        let got = client.read_at(*offset, *len as u32);
        assert_eq!(
            &sha256_hex(&got),
            expected,
            "blob changed despite read-only mount at offset {offset}"
        );
    }
    client.disconnect();
    sleep(Duration::from_secs(1));
    stop_server(child);

    log("nbd read-only e2e PASSED ✓");
}

/// Wait for `addr` to be released so the next server can bind it.
fn wait_for_port_release(addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && TcpStream::connect(addr).is_ok() {
        sleep(Duration::from_millis(500));
    }
}

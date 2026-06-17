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

/// Host:port the in-test NBD server binds to.  Kept distinct from the default
/// NBD port (10809) to avoid clashing with anything a developer may already be
/// running locally.
const NBD_ADDR: &str = "127.0.0.1:11809";
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

const NBD_REQUEST_MAGIC: u32 = 0x25609513;
const NBD_SIMPLE_REPLY_MAGIC: u32 = 0x67446698;

const NBD_CMD_READ: u16 = 0;
const NBD_CMD_WRITE: u16 = 1;
const NBD_CMD_DISC: u16 = 2;
const NBD_CMD_FLUSH: u16 = 3;

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
/// connection on [`NBD_ADDR`].  When `create` is true the page blob is
/// provisioned first.
///
/// The returned `Child` is always `wait()`ed on by the caller (via
/// `stop_server`), so the zombie-process lint does not apply.
#[allow(clippy::zombie_processes)]
fn start_server(container: &str, blob: &str, create: bool) -> Child {
    log(&format!(
        "starting NBD server on {NBD_ADDR} ({})",
        if create { "--create" } else { "reuse blob" }
    ));
    let bin = env!("CARGO_BIN_EXE_ublk-azblob");
    let mut cmd = Command::new(bin);
    cmd.arg("run")
        .arg("--size")
        .arg(BLOB_SIZE.to_string())
        .arg("--nbd")
        .arg(NBD_ADDR);
    if create {
        cmd.arg("--create");
    }
    azure_env(&mut cmd, container, blob);

    let mut child = cmd.spawn().expect("failed to spawn ublk-azblob");

    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok(stream) = TcpStream::connect(NBD_ADDR) {
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
    panic!("timed out waiting for the NBD server to listen on {NBD_ADDR}");
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
}

impl NbdClient {
    /// Connect to [`NBD_ADDR`] and complete the fixed-newstyle handshake using
    /// `NBD_OPT_GO`, leaving the connection in the transmission phase.
    fn connect() -> NbdClient {
        let mut stream = TcpStream::connect(NBD_ADDR).expect("connect NBD server");
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

        // Drain option replies until the GO is ACKed.
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
            if rep == NBD_REP_ACK {
                break;
            }
        }

        NbdClient { stream, handle: 0 }
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
        TcpStream::connect(NBD_ADDR).is_err(),
        "{NBD_ADDR} is already in use; another NBD server is running"
    );

    let container = env_or("AZURE_STORAGE_CONTAINER", DEFAULT_CONTAINER);
    let blob = env_or("AZURE_STORAGE_BLOB", DEFAULT_BLOB);

    // ── Phase 1: provision the blob, write random regions, verify in-session ──
    let child = start_server(&container, &blob, true);
    let mut client = NbdClient::connect();

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
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && TcpStream::connect(NBD_ADDR).is_ok() {
        sleep(Duration::from_millis(500));
    }

    // ── Phase 2: restart over the same blob and re-verify ─────────────────────
    let child = start_server(&container, &blob, false);
    let mut client = NbdClient::connect();

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

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
//! read-only via `?snapshot=`: it snapshots the blob, asserts the export advertises `NBD_FLAG_READ_ONLY`,
//! reads succeed, and writes / trims / write-zeroes are rejected with `EPERM`
//! without mutating the backing blob.
//!
//! A third test, [`nbd_template_copy`](fn.nbd_template_copy.html), exercises the
//! `copy` subcommand: it seeds a source blob, copies it into a fresh target via
//! a `templateBlobUrl`, and verifies the target round-trips the source data.
//!
//! A fourth test, [`nbd_graceful_shutdown_flush`](fn.nbd_graceful_shutdown_flush.html),
//! proves a write buffered only in memory is flushed to the page blob when the
//! server receives `SIGINT` (no explicit `NBD_CMD_FLUSH`, disconnect FLUSH, or
//! automatic idle/force flush) and survives a restart over the same blob.
//!
//! Run it (with Azurite up) via:
//!
//! ```text
//! AZURE_STORAGE_ENDPOINT="http://127.0.0.1:10000/devstoreaccount1" \
//!   cargo test --test nbd_e2e -- --nocapture
//! ```

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

/// Azurite well-known development account key.
const DEFAULT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:10000/devstoreaccount1";
const DEFAULT_ACCOUNT: &str = "devstoreaccount1";
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
/// Host:port for the `nbd_template_copy` test (distinct so it can run in
/// parallel with the others).
const NBD_ADDR_TEMPLATE: &str = "127.0.0.1:11811";
/// Host:port for the `nbd_graceful_shutdown_flush` test (distinct so it can run
/// in parallel with the others).
const NBD_ADDR_SHUTDOWN: &str = "127.0.0.1:11812";
/// Host:port the `nbd_blob_lock_conflict` test's *lock-holding* server binds to.
const NBD_ADDR_LOCK_HOLDER: &str = "127.0.0.1:11813";
/// Host:port the `nbd_blob_lock_conflict` test's *would-be take-over* server is
/// told to bind to.  It never actually binds — the blob lock is held, so the
/// process exits during `acquire_lock`, before reaching the NBD listener — but a
/// distinct port guarantees the failure is the held lock and not a port clash.
const NBD_ADDR_LOCK_TAKER: &str = "127.0.0.1:11814";
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
///
/// The account, container and blob are collapsed into a single
/// `AZURE_STORAGE_BLOB_URL` (Azurite path-style, so the URL host already
/// carries the account); only the SharedKey is passed separately.
fn azure_env(cmd: &mut Command, container: &str, blob: &str, snapshot: Option<&str>) {
    let endpoint = env_or("AZURE_STORAGE_ENDPOINT", DEFAULT_ENDPOINT);
    let mut blob_url = format!("{}/{}/{}", endpoint.trim_end_matches('/'), container, blob);
    if let Some(s) = snapshot {
        // A snapshot is selected via the URL's `?snapshot=` query (the only way
        // a device is exposed read-only); there is no separate snapshot flag/env.
        blob_url.push_str("?snapshot=");
        blob_url.push_str(s);
    }
    cmd.env(
        "AZURE_STORAGE_KEY",
        env_or("AZURE_STORAGE_KEY", DEFAULT_KEY),
    )
    .env("AZURE_STORAGE_BLOB_URL", blob_url);
}

/// Start the NBD server as a child process and wait until it accepts a TCP
/// connection on `addr`.  When `create` is true the page blob is provisioned
/// first.
///
/// The returned `Child` is always `wait()`ed on by the caller (via
/// `stop_server`), so the zombie-process lint does not apply.
#[allow(clippy::zombie_processes)]
fn start_server(addr: &str, container: &str, blob: &str, create: bool) -> Child {
    start_server_opts(
        addr, container, blob, create, /* snapshot */ None,
        /* disable_auto_flush */ false,
    )
}

/// Create a snapshot of the test blob with the Azure CLI (`az storage blob
/// snapshot`) and return its `x-ms-snapshot` id (the only way to expose the
/// export read-only).
fn create_snapshot(container: &str, blob: &str) -> String {
    let account = env_or("AZURE_STORAGE_ACCOUNT", DEFAULT_ACCOUNT);
    let key = env_or("AZURE_STORAGE_KEY", DEFAULT_KEY);
    let endpoint = env_or("AZURE_STORAGE_ENDPOINT", DEFAULT_ENDPOINT);
    log(&format!("creating snapshot of blob {blob} via az"));
    let out = Command::new("az")
        .args([
            "storage",
            "blob",
            "snapshot",
            "--account-name",
            &account,
            "--account-key",
            &key,
            "--blob-endpoint",
            &endpoint,
            "--container-name",
            container,
            "--name",
            blob,
            "--query",
            "snapshot",
            "--output",
            "tsv",
        ])
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn `az`: {e}"));
    assert!(
        out.status.success(),
        "az storage blob snapshot failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert!(!id.is_empty(), "az returned an empty snapshot id");
    log(&format!("created snapshot {id}"));
    id
}

/// Like [`start_server`] but lets the caller bring the export up against a blob
/// `snapshot` (via `?snapshot=` in the blob URL), which exposes it read-only, and/or
/// disable automatic flushing.  `create` and a snapshot are mutually exclusive
/// (a snapshot is immutable), so callers pass `create=false` when supplying a
/// snapshot.  When `disable_auto_flush` is set the server runs with
/// `--idle-flush-secs 0 --force-flush-timeout-secs 0` so the only thing that can
/// persist a buffered write is an explicit `NBD_CMD_FLUSH` or the
/// flush-on-shutdown path.
#[allow(clippy::zombie_processes)]
fn start_server_opts(
    addr: &str,
    container: &str,
    blob: &str,
    create: bool,
    snapshot: Option<&str>,
    disable_auto_flush: bool,
) -> Child {
    log(&format!(
        "starting NBD server on {addr} ({}{})",
        if create { "--create" } else { "reuse blob" },
        match snapshot {
            Some(s) => format!(", snapshot={s}"),
            None => String::new(),
        }
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
    if disable_auto_flush {
        cmd.arg("--idle-flush-secs")
            .arg("0")
            .arg("--force-flush-timeout-secs")
            .arg("0");
    }
    azure_env(&mut cmd, container, blob, snapshot);

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
/// `NBD_CMD_DISC`) before this is called, so the process is simply killed.
/// `child.kill()` sends `SIGKILL`, which is uncatchable, so the server's
/// SIGINT/SIGTERM graceful-flush handler never runs and a clean exit status is
/// not expected. (Use [`stop_server_graceful`] when a clean exit is required.)
fn stop_server(mut child: Child) {
    log(&format!("stopping NBD server (pid {})", child.id()));
    let _ = child.kill();
    let _ = child.wait();
}

/// Send `sig` to the running child process.
fn signal(child: &Child, sig: i32) {
    // SAFETY: `kill` is safe to call with a valid pid and signal number.
    let rc = unsafe { libc::kill(child.id() as libc::pid_t, sig) };
    assert_eq!(
        rc,
        0,
        "kill({sig}) failed: {}",
        std::io::Error::last_os_error()
    );
}

/// Stop the NBD server cleanly via `SIGINT` and wait for it to exit.
///
/// `run_nbd_target` installs SIGINT/SIGTERM handlers that flush all dirty data
/// to the page blob before exiting, so a clean (zero) exit status is expected.
fn stop_server_graceful(mut child: Child) {
    log(&format!(
        "SIGINT NBD server (pid {}) — relies on the shutdown flush",
        child.id()
    ));
    signal(&child, libc::SIGINT);
    let status = child.wait().expect("wait for NBD server to exit");
    assert!(
        status.success(),
        "NBD server exited with non-zero status on SIGINT: {status}"
    );
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
    // Shut down gracefully (SIGINT) rather than SIGKILL so the holder releases
    // its blob lease; otherwise the stale lease would block Phase 2 reopening
    // the same blob writable (the blob lock is on by default).
    stop_server_graceful(child);

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

/// e2e for read-only mode (read-only via `?snapshot=`) over the NBD path.
///
/// Cycle:
///   1. provision the blob writable, write a few random regions, flush, stop
///   2. snapshot the blob and restart the server against that snapshot, then assert:
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
    // Shut down gracefully (SIGINT) rather than SIGKILL so the writable holder
    // releases its blob lease; otherwise the stale lease would block Phase 3
    // reopening the same blob writable (the blob lock is on by default).
    stop_server_graceful(child);

    wait_for_port_release(NBD_ADDR_READ_ONLY);

    // ── Phase 2: snapshot the blob, then reopen that snapshot (read-only) ─────
    let snapshot = create_snapshot(&container, &blob);
    let child = start_server_opts(
        NBD_ADDR_READ_ONLY,
        &container,
        &blob,
        /* create */ false,
        /* snapshot */ Some(&snapshot),
        /* disable_auto_flush */ false,
    );
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

/// e2e for the `templateBlobUrl` read-write copy path (`ublk-azblob copy`).
///
/// Cycle:
///   1. provision a *template* blob, write random regions, flush, stop — this is
///      the golden image
///   2. run `ublk-azblob copy` to clone the template into a fresh target blob
///   3. open the target read-write over NBD and assert every region matches the
///      template (the copy round-tripped), and that the target is writable
///      (write a new region and read it back)
#[test]
fn nbd_template_copy() {
    if !azurite_available() {
        eprintln!(
            "skipping nbd_template_copy: Azurite is not reachable at {} \
             (set AZURE_STORAGE_ENDPOINT and start Azurite to run this test)",
            endpoint_authority()
        );
        return;
    }
    assert!(
        TcpStream::connect(NBD_ADDR_TEMPLATE).is_err(),
        "{NBD_ADDR_TEMPLATE} is already in use; another NBD server is running"
    );

    let container = env_or("AZURE_STORAGE_CONTAINER", DEFAULT_CONTAINER);
    let template_blob = format!("{}-tmpl-src", env_or("AZURE_STORAGE_BLOB", DEFAULT_BLOB));
    let target_blob = format!("{}-tmpl-dst", env_or("AZURE_STORAGE_BLOB", DEFAULT_BLOB));

    // ── Phase 1: build the golden-image template blob ─────────────────────────
    let child = start_server(NBD_ADDR_TEMPLATE, &container, &template_blob, true);
    let mut client = NbdClient::connect(NBD_ADDR_TEMPLATE);
    log(&format!(
        "writing {NUM_REGIONS} random regions to the template"
    ));
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
    wait_for_port_release(NBD_ADDR_TEMPLATE);

    // ── Phase 2: copy the template into a fresh target blob ───────────────────
    let endpoint = env_or("AZURE_STORAGE_ENDPOINT", DEFAULT_ENDPOINT);
    let template_url = format!(
        "{}/{}/{}",
        endpoint.trim_end_matches('/'),
        container,
        template_blob
    );
    run_copy(&template_url, &container, &target_blob);

    // ── Phase 3: open the target read-write and verify the copy ───────────────
    let child = start_server(NBD_ADDR_TEMPLATE, &container, &target_blob, false);
    let mut client = NbdClient::connect(NBD_ADDR_TEMPLATE);
    log("verifying the copied target matches the template");
    for (offset, len, expected) in &checksums {
        let got = client.read_at(*offset, *len as u32);
        assert_eq!(
            &sha256_hex(&got),
            expected,
            "copied target differs from template at offset {offset}"
        );
    }
    // The target is a writable per-volume blob (unlike a read-only mount): write
    // a fresh region and read it back to prove it accepts writes.
    let extra_offset = 32 * 1024 * 1024;
    let extra = random_bytes(BLOCK_SIZE as usize);
    client.write_at(extra_offset, &extra);
    let got = client.read_at(extra_offset, BLOCK_SIZE as u32);
    assert_eq!(
        sha256_hex(&got),
        sha256_hex(&extra),
        "target is not writable after copy"
    );
    client.flush();
    client.disconnect();
    sleep(Duration::from_secs(1));
    stop_server(child);

    log("nbd template copy e2e PASSED ✓");
}

/// Wait for `addr` to be released so the next server can bind it.
fn wait_for_port_release(addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && TcpStream::connect(addr).is_ok() {
        sleep(Duration::from_millis(500));
    }
}

/// Run the one-shot `ublk-azblob copy --template-url <url>` subcommand, copying
/// the template into the target `container`/`blob`. Panics on failure.
fn run_copy(template_url: &str, container: &str, target_blob: &str) {
    let bin = std::env::var("UBLK_AZBLOB_BIN")
        .unwrap_or_else(|_| env!("CARGO_BIN_EXE_ublk-azblob").to_string());
    let mut cmd = Command::new(&bin);
    cmd.arg("copy").arg("--template-url").arg(template_url);
    azure_env(&mut cmd, container, target_blob, None);
    log(&format!("$ ublk-azblob copy --template-url {template_url}"));
    let status = cmd.status().expect("spawn ublk-azblob copy");
    assert!(status.success(), "`ublk-azblob copy` failed with {status}");
}

/// Graceful-shutdown e2e for the NBD target: prove a write buffered only in
/// memory is flushed to the page blob when the server receives `SIGINT` — with
/// **no** explicit `NBD_CMD_FLUSH`, **no** disconnect FLUSH, and **no**
/// automatic (idle/force) flush — and that the data survives restarting the
/// server over the same blob.
///
/// This validates the SIGINT/SIGTERM shutdown flush in
/// `nbd_target::run_nbd_target`: the client writes a pattern and then the
/// process is SIGINT'd directly (instead of `kill`), so without the
/// flush-on-shutdown the pattern would be lost after the restart.
#[test]
fn nbd_graceful_shutdown_flush() {
    if !azurite_available() {
        eprintln!(
            "skipping nbd_graceful_shutdown_flush: Azurite is not reachable at {} \
             (set AZURE_STORAGE_ENDPOINT and start Azurite to run this test)",
            endpoint_authority()
        );
        return;
    }
    assert!(
        TcpStream::connect(NBD_ADDR_SHUTDOWN).is_err(),
        "{NBD_ADDR_SHUTDOWN} is already in use; another NBD server is running"
    );

    let container = env_or("AZURE_STORAGE_CONTAINER", DEFAULT_CONTAINER);
    // Distinct blob so this test never collides with the other NBD tests.
    let blob = format!("{}-shutdown", env_or("AZURE_STORAGE_BLOB", DEFAULT_BLOB));

    // ── Phase 1: provision the blob, write regions, then SIGINT (no FLUSH) ─────
    // Auto-flushing is disabled so only the shutdown flush can persist the data.
    let child = start_server_opts(
        NBD_ADDR_SHUTDOWN,
        &container,
        &blob,
        /* create */ true,
        /* snapshot */ None,
        /* disable_auto_flush */ true,
    );
    let mut client = NbdClient::connect(NBD_ADDR_SHUTDOWN);

    log(&format!(
        "writing {NUM_REGIONS} random regions (no NBD_CMD_FLUSH)"
    ));
    let mut checksums: Vec<(u64, usize, String)> = Vec::with_capacity(NUM_REGIONS);
    for i in 0..NUM_REGIONS {
        let offset = (i as u64) * 1024 * 1024;
        let blocks = 1 + (i * 7) % 64;
        let len = blocks * BLOCK_SIZE as usize;
        let data = random_bytes(len);
        client.write_at(offset, &data);
        checksums.push((offset, len, sha256_hex(&data)));
    }

    // Drop the connection *without* NBD_CMD_FLUSH or NBD_CMD_DISC, so the only
    // path that can persist the buffered writes is the SIGINT shutdown flush.
    drop(client);
    log("SIGINT the server (relies solely on the shutdown flush)");
    stop_server_graceful(child);

    wait_for_port_release(NBD_ADDR_SHUTDOWN);

    // ── Phase 2: restart over the same blob and verify the pattern survived ────
    let child = start_server(NBD_ADDR_SHUTDOWN, &container, &blob, false);
    let mut client = NbdClient::connect(NBD_ADDR_SHUTDOWN);

    log("verifying regions after SIGINT shutdown + restart");
    for (offset, len, expected) in &checksums {
        let got = client.read_at(*offset, *len as u32);
        assert_eq!(
            &sha256_hex(&got),
            expected,
            "post-shutdown checksum mismatch at offset {offset} — the buffered \
             write was not flushed on SIGINT"
        );
    }

    client.disconnect();
    sleep(Duration::from_secs(1));
    stop_server(child);

    log("nbd graceful shutdown e2e PASSED ✓");
}

/// Spawn a second `ublk-azblob run --nbd` against a blob whose lock is already
/// held by a live server and assert it **refuses to start**: the blob lock is
/// on by default, so `acquire_lock` gets `LockError::Held`, and the process
/// exits non-zero before ever binding its NBD port.  Returns the captured
/// stderr so the caller can assert on the failure message.
///
/// stderr is drained on a helper thread so a chatty child can never fill the
/// pipe buffer and deadlock before it exits.
///
/// The child is always reaped: the success path observes its exit via
/// `try_wait()`, and every early-return panic path `kill()`s then `wait()`s it,
/// so the `zombie_processes` lint does not apply.
#[allow(clippy::zombie_processes)]
fn expect_blob_lock_conflict(addr: &str, container: &str, blob: &str) -> String {
    log(&format!(
        "starting a second NBD server on {addr} for the *same* blob (expecting a lock conflict)"
    ));
    let bin = std::env::var("UBLK_AZBLOB_BIN")
        .unwrap_or_else(|_| env!("CARGO_BIN_EXE_ublk-azblob").to_string());
    let mut cmd = Command::new(&bin);
    // Reuse the existing blob (no --create): the lock is what must reject us.
    cmd.arg("run")
        .arg("--size")
        .arg(BLOB_SIZE.to_string())
        .arg("--nbd")
        .arg(addr)
        .stderr(Stdio::piped());
    azure_env(&mut cmd, container, blob, None);

    let mut child = cmd.spawn().expect("failed to spawn ublk-azblob");
    let mut stderr_pipe = child.stderr.take().expect("piped stderr");
    let reader = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = stderr_pipe.read_to_string(&mut s);
        s
    });

    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait on conflicting server") {
            break status;
        }
        // It must never reach the NBD listener: the lock is acquired first.
        if TcpStream::connect(addr).is_ok() {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the second server bound {addr} despite the blob lock being held");
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "the conflicting NBD server did not exit within the timeout; \
                 it should have failed to acquire the already-held blob lock"
            );
        }
        sleep(Duration::from_millis(200));
    };

    assert!(
        !status.success(),
        "the second server exited successfully ({status}); \
         it should have failed to acquire the already-held blob lock"
    );

    reader.join().unwrap_or_default()
}

/// Blob-lock conflict e2e for the NBD target: prove the default blob lease
/// ("blob lock") makes a blob single-writer.  While one server holds the lock,
/// a second `run` against the same blob must refuse to start; once the holder
/// shuts down cleanly (releasing the lease), a fresh server can take the lock.
///
/// This validates the blob-lock-only (no `--coordination`) startup path in
/// `coordination::Coordinator::acquire`: a held lease with no liveness arbiter
/// is refused rather than broken.
#[test]
fn nbd_blob_lock_conflict() {
    if !azurite_available() {
        eprintln!(
            "skipping nbd_blob_lock_conflict: Azurite is not reachable at {} \
             (set AZURE_STORAGE_ENDPOINT and start Azurite to run this test)",
            endpoint_authority()
        );
        return;
    }
    assert!(
        TcpStream::connect(NBD_ADDR_LOCK_HOLDER).is_err(),
        "{NBD_ADDR_LOCK_HOLDER} is already in use; another NBD server is running"
    );
    assert!(
        TcpStream::connect(NBD_ADDR_LOCK_TAKER).is_err(),
        "{NBD_ADDR_LOCK_TAKER} is already in use; another NBD server is running"
    );

    let container = env_or("AZURE_STORAGE_CONTAINER", DEFAULT_CONTAINER);
    // Distinct blob so this test never collides with the other NBD tests.
    let blob = format!("{}-lock", env_or("AZURE_STORAGE_BLOB", DEFAULT_BLOB));

    // ── Phase 1: a first server provisions the blob and holds the blob lock ────
    let holder = start_server(NBD_ADDR_LOCK_HOLDER, &container, &blob, true);
    // Sanity: the holder really is serving (and thus owns the lease).
    NbdClient::connect(NBD_ADDR_LOCK_HOLDER).disconnect();

    // ── Phase 2: a second server for the same blob must refuse to start ────────
    let stderr = expect_blob_lock_conflict(NBD_ADDR_LOCK_TAKER, &container, &blob);
    assert!(
        stderr.contains("blob lease is already held"),
        "the second server failed, but not with the expected blob-lock error; stderr was:\n{stderr}"
    );
    log("second server correctly refused to start while the blob lock was held ✓");

    // ── Phase 3: release the lock (clean shutdown), then a fresh server can take it ─
    log("SIGINT the holder so it releases the blob lease");
    stop_server_graceful(holder);
    wait_for_port_release(NBD_ADDR_LOCK_HOLDER);

    let taker = start_server(NBD_ADDR_LOCK_HOLDER, &container, &blob, false);
    NbdClient::connect(NBD_ADDR_LOCK_HOLDER).disconnect();
    log("a fresh server acquired the blob lock after it was released ✓");
    sleep(Duration::from_secs(1));
    stop_server(taker);

    log("nbd blob lock conflict e2e PASSED ✓");
}

//! Centralized Azure I/O gateway.
//!
//! Every Azure download (read) and upload (write / clear / server-side copy)
//! funnels through a single process-wide [`AzureIoGateway`]. Because
//! [`AzurePageBlobBackend`](super::azure::AzurePageBlobBackend) is the only
//! place that issues Azure SDK requests, routing its primitives through the
//! gateway makes it the one place that enforces, for each direction
//! independently:
//!
//! 1. **Bandwidth** — a byte-rate ceiling (leaky bucket), `0` = unlimited.
//! 2. **Threads / concurrency** — a single shared worker budget across both
//!    directions; at most that many Azure requests are in flight at once
//!    *combined* (see below for how the two directions share it).
//! 3. **Fairness** — a *provider/consumer* model: producers (on-demand reads,
//!    flush write-back, server-side copy, cache warm-up) enqueue work onto a
//!    priority queue that the workers drain highest-priority-first. This
//!    prevents background work from starving foreground I/O. The priority order
//!    is **foreground read > flush > copy > warm-up**.
//!
//! Downloads and uploads keep *separate* priority queues and bandwidth limiters
//! (the priority order above is enforced *within* each direction: downloads
//! foreground read > copy > warm-up; uploads flush > copy), but they share a
//! single concurrency budget. Rather than statically splitting the worker
//! threads in half, both directions draw from one shared pool of permits sized
//! to the total budget, so a busy direction can use the *entire* budget while
//! the other is idle (e.g. downloads alone can reach the full CPU count when no
//! uploads are in flight), and the two only contend for threads when both are
//! active.

use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;

use async_priority_channel as pc;
use futures::future::BoxFuture;
use leaky_bucket::RateLimiter;
use std::sync::Arc;
use tokio::sync::{oneshot, Semaphore};
use tokio::task::JoinHandle;
use tracing::trace;

use super::{cpu_count, MAX_PAGE_REQUEST_BYTES};

/// What kind of work a request is, used purely to pick a scheduling priority
/// *within* its direction. The direction (download vs upload) is decided by the
/// operation itself (read vs write/clear/copy), not by the class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoClass {
    /// On-demand read serving a block-device request — highest priority.
    ForegroundRead,
    /// Write-back of dirty cache/buffer pages to the blob.
    Flush,
    /// Bulk template copy (download or upload side of a streamed copy, or the
    /// server-side `Put Page From URL`).
    Copy,
    /// Background cache warm-up prefetch — lowest priority.
    Warmup,
}

impl IoClass {
    /// Scheduling priority; higher is served first. Mirrors the requested global
    /// order foreground read > flush > copy > warm-up.
    fn priority(self) -> u8 {
        match self {
            IoClass::ForegroundRead => 3,
            IoClass::Flush => 2,
            IoClass::Copy => 1,
            IoClass::Warmup => 0,
        }
    }
}

tokio::task_local! {
    /// The I/O class of the currently executing producer, if it set one. Read by
    /// the backend primitives to label their submissions (e.g. a read issued by
    /// the warm-up runs under `IoClass::Warmup`).
    static CURRENT_CLASS: IoClass;
}

/// The I/O class set for the current task by an enclosing [`with_class`] scope,
/// or `None` when the task did not set one (e.g. an on-demand block read).
pub fn current_class() -> Option<IoClass> {
    CURRENT_CLASS.try_with(|c| *c).ok()
}

/// Run `fut` with `class` recorded as the current I/O class, so any Azure
/// download/upload it triggers (directly or via an inner backend) is scheduled
/// under that class. The label is inherited by child futures polled on the same
/// task (e.g. `buffer_unordered` / `for_each_concurrent` combinators) but **not**
/// across `tokio::spawn`, so set it inside the spawned task when needed.
pub async fn with_class<F: Future>(class: IoClass, fut: F) -> F::Output {
    CURRENT_CLASS.scope(class, fut).await
}

/// A unit of work handed to a worker: the boxed future already owns its result
/// channel; `bytes` is the payload size charged against the bandwidth limiter.
struct Job {
    fut: BoxFuture<'static, ()>,
    bytes: u64,
}

/// One direction's pipeline: a priority queue plus the worker pool draining it.
struct Pipeline {
    tx: pc::Sender<Job, u8>,
}

impl Pipeline {
    /// Build a pipeline with up to `workers` consumer tasks draining its queue
    /// and an optional byte-rate limiter (`bandwidth_bps == 0` ⇒ unlimited).
    /// Every job additionally acquires one permit from the gateway-wide
    /// `concurrency` semaphore before running, so the two directions share a
    /// single thread budget dynamically instead of each owning a fixed slice.
    fn new(
        workers: usize,
        concurrency: Arc<Semaphore>,
        bandwidth_bps: u64,
        handles: &mut Vec<JoinHandle<()>>,
    ) -> Self {
        let workers = workers.max(1);
        let (tx, rx) = pc::unbounded::<Job, u8>();
        let limiter = build_limiter(bandwidth_bps);
        for _ in 0..workers {
            let rx = rx.clone();
            let limiter = limiter.clone();
            let concurrency = concurrency.clone();
            handles.push(tokio::spawn(worker_loop(rx, concurrency, limiter)));
        }
        Self { tx }
    }
}

/// Build a leaky-bucket limiter for `bandwidth_bps` bytes/sec, or `None` when
/// unlimited. Tokens are bytes. `max` is sized to at least one max page write so
/// a single ≤`MAX_PAGE_REQUEST_BYTES` request never deadlocks; larger requests
/// (e.g. big downloads) are charged in `max`-sized rounds by the worker.
fn build_limiter(bandwidth_bps: u64) -> Option<Arc<RateLimiter>> {
    if bandwidth_bps == 0 {
        return None;
    }
    // Refill every 100ms so the rate is smooth (10 refills/sec).
    const REFILLS_PER_SEC: u64 = 10;
    let refill = (bandwidth_bps / REFILLS_PER_SEC).max(1) as usize;
    let max = (bandwidth_bps.max(MAX_PAGE_REQUEST_BYTES)) as usize;
    let limiter = RateLimiter::builder()
        .interval(Duration::from_millis(1000 / REFILLS_PER_SEC))
        .refill(refill)
        .max(max)
        // Start full so a burst up to `max` is admitted immediately.
        .initial(max)
        .build();
    Some(Arc::new(limiter))
}

/// Consumer loop: pull the highest-priority job, pay its bandwidth cost, claim a
/// shared concurrency permit, then run it. Bandwidth tokens are acquired
/// *before* the concurrency permit so a request throttled by a per-direction
/// bandwidth ceiling waits for tokens *without* occupying a shared slot — this
/// prevents token-starved workers in one direction from pinning the whole
/// shared budget and starving the other direction. The permit (held for the
/// whole request) is what bounds *combined* download+upload parallelism to the
/// gateway's total budget while letting either direction borrow the other's
/// idle capacity. Tradeoff: tokens are deducted slightly before the transfer
/// actually runs (while the permit is being acquired).
async fn worker_loop(
    rx: pc::Receiver<Job, u8>,
    concurrency: Arc<Semaphore>,
    limiter: Option<Arc<RateLimiter>>,
) {
    while let Ok((job, _priority)) = rx.recv().await {
        if let Some(rl) = &limiter {
            // Charge the *full* byte cost against the limiter, but never request
            // more than `rl.max()` tokens in a single `acquire` (which would
            // wait forever). A request larger than `max` — e.g. a download of a
            // cache page sized above `MAX_PAGE_REQUEST_BYTES` — is paid in
            // successive `max`-sized rounds so the configured bandwidth ceiling
            // is honoured for large reads rather than silently exceeded.
            let mut remaining = job.bytes as usize;
            while remaining > 0 {
                let chunk = remaining.min(rl.max());
                rl.acquire(chunk).await;
                remaining -= chunk;
            }
        }
        // Held until the end of the loop body, so the request occupies one slot
        // of the shared budget for its entire lifetime. The semaphore is never
        // closed for a live gateway, so this acquire cannot fail.
        let _permit = concurrency
            .acquire()
            .await
            .expect("Azure I/O gateway concurrency semaphore closed");
        job.fut.await;
    }
}

/// Configuration for the gateway's two pipelines.
#[derive(Clone, Copy, Debug)]
pub struct IoGatewayConfig {
    /// Total number of concurrent Azure requests across *both* directions. This
    /// is the shared thread budget; downloads and uploads draw from it
    /// dynamically rather than each owning a fixed half.
    pub concurrency: usize,
    /// Per-direction ceiling on concurrent download (read) requests. Capped by
    /// `concurrency`; defaults to it so a download burst can use the whole
    /// budget when uploads are idle.
    pub download_concurrency: usize,
    /// Per-direction ceiling on concurrent upload (write/clear/copy) requests.
    /// Capped by `concurrency`; defaults to it for the same reason.
    pub upload_concurrency: usize,
    /// Download bandwidth ceiling in bytes/sec (`0` = unlimited).
    pub download_bandwidth_bps: u64,
    /// Upload bandwidth ceiling in bytes/sec (`0` = unlimited).
    pub upload_bandwidth_bps: u64,
}

impl IoGatewayConfig {
    /// Auto-size the shared concurrency budget to the logical CPU count, with
    /// each direction allowed to use *all* of it when the other is idle
    /// (dynamic split, not a fixed half each); bandwidth unlimited. Environment
    /// variables, when set to a non-zero value, are used as the defaults for
    /// each field (`UBLK_IO_CONCURRENCY` for the shared budget,
    /// `UBLK_DOWNLOAD_CONCURRENCY` / `UBLK_UPLOAD_CONCURRENCY` for the
    /// per-direction ceilings, `UBLK_DOWNLOAD_BANDWIDTH` /
    /// `UBLK_UPLOAD_BANDWIDTH` in bytes/sec for bandwidth). An env concurrency
    /// of `0` is treated as unset (the shared budget falls back to the CPU
    /// count, and each per-direction ceiling falls back to the shared budget),
    /// matching the CLI flags — which take precedence over these defaults when
    /// explicitly provided (see `main.rs`).
    pub fn auto() -> Self {
        let cpu = cpu_count().max(1);
        let concurrency = env_usize("UBLK_IO_CONCURRENCY").unwrap_or(cpu);
        Self {
            concurrency,
            download_concurrency: env_usize("UBLK_DOWNLOAD_CONCURRENCY").unwrap_or(concurrency),
            upload_concurrency: env_usize("UBLK_UPLOAD_CONCURRENCY").unwrap_or(concurrency),
            download_bandwidth_bps: env_u64("UBLK_DOWNLOAD_BANDWIDTH").unwrap_or(0),
            upload_bandwidth_bps: env_u64("UBLK_UPLOAD_BANDWIDTH").unwrap_or(0),
        }
    }
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()?
        .parse::<usize>()
        .ok()
        .filter(|&n| n > 0)
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse::<u64>().ok()
}

/// Process-wide Azure I/O gateway. Cheap to clone (`Arc` internally via the
/// channel senders); obtain the shared instance with [`AzureIoGateway::global`].
pub struct AzureIoGateway {
    download: Pipeline,
    upload: Pipeline,
    // Worker handles are retained so the spawned tasks are owned by the gateway;
    // they run for the gateway's lifetime (the process, for the global one).
    _workers: Vec<JoinHandle<()>>,
}

static GLOBAL: OnceLock<Arc<AzureIoGateway>> = OnceLock::new();

impl AzureIoGateway {
    /// Build a gateway and spawn its worker pools. Must be called from within a
    /// Tokio runtime (a multi-threaded runtime, so workers make progress while
    /// the I/O loop blocks on a result).
    pub fn new(cfg: IoGatewayConfig) -> Self {
        let total = cfg.concurrency.max(1);
        // Shared budget: combined download+upload in-flight never exceeds it,
        // but a single direction can claim all of it when the other is idle.
        let concurrency = Arc::new(Semaphore::new(total));
        let mut workers = Vec::new();
        // A direction never needs more worker tasks than the shared budget, so
        // clamp its pool to `total` (extra workers would only ever block on the
        // semaphore). Each pool can still grow to the full budget.
        let download_workers = cfg.download_concurrency.clamp(1, total);
        let upload_workers = cfg.upload_concurrency.clamp(1, total);
        let download = Pipeline::new(
            download_workers,
            concurrency.clone(),
            cfg.download_bandwidth_bps,
            &mut workers,
        );
        let upload = Pipeline::new(
            upload_workers,
            concurrency.clone(),
            cfg.upload_bandwidth_bps,
            &mut workers,
        );
        trace!(
            concurrency = total,
            download_concurrency = download_workers,
            upload_concurrency = upload_workers,
            download_bandwidth_bps = cfg.download_bandwidth_bps,
            upload_bandwidth_bps = cfg.upload_bandwidth_bps,
            "Azure I/O gateway started"
        );
        Self {
            download,
            upload,
            _workers: workers,
        }
    }

    /// Initialize the process-wide gateway with `cfg`. The first call wins;
    /// later calls (and any prior [`global`](Self::global) that lazily
    /// initialized it) are ignored.
    pub fn init_global(cfg: IoGatewayConfig) {
        let _ = GLOBAL.set(Arc::new(Self::new(cfg)));
    }

    /// The process-wide gateway, lazily initialized from [`IoGatewayConfig::auto`]
    /// (which honours the `UBLK_*` environment overrides) if not already set by
    /// [`init_global`](Self::init_global).
    pub fn global() -> Arc<AzureIoGateway> {
        GLOBAL
            .get_or_init(|| Arc::new(Self::new(IoGatewayConfig::auto())))
            .clone()
    }

    /// Submit a download (read) `work` future of `bytes` payload under `class`,
    /// awaiting its result. Runs on a download worker subject to the download
    /// bandwidth and concurrency limits and the priority queue.
    pub async fn download<T, F>(&self, class: IoClass, bytes: u64, work: F) -> T
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.submit(&self.download, class, bytes, work).await
    }

    /// Submit an upload (write/clear/copy) `work` future of `bytes` payload under
    /// `class`, awaiting its result. Runs on an upload worker subject to the
    /// upload bandwidth and concurrency limits and the priority queue.
    pub async fn upload<T, F>(&self, class: IoClass, bytes: u64, work: F) -> T
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.submit(&self.upload, class, bytes, work).await
    }

    async fn submit<T, F>(&self, pipeline: &Pipeline, class: IoClass, bytes: u64, work: F) -> T
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (mut res_tx, res_rx) = oneshot::channel::<T>();
        let fut: BoxFuture<'static, ()> = Box::pin(async move {
            let work = std::pin::pin!(work);
            // Race the work against the submitter dropping the result receiver.
            // If the submitter is cancelled (e.g. a flush hits
            // `flush_io_timeout_secs` and drops this future), `res_tx.closed()`
            // fires and we cancel `work` instead of running it to completion —
            // dropping the in-flight Azure request and releasing the shared
            // concurrency permit promptly rather than after the SDK call
            // eventually returns.
            let out = tokio::select! {
                out = work => Some(out),
                _ = res_tx.closed() => None,
            };
            if let Some(out) = out {
                let _ = res_tx.send(out);
            }
        });
        let job = Job { fut, bytes };
        // The unbounded queue only rejects when closed, which never happens for a
        // live gateway (it owns the receivers via the worker tasks).
        pipeline
            .tx
            .send(job, class.priority())
            .await
            .expect("Azure I/O gateway queue closed");
        res_rx.await.expect("Azure I/O gateway worker dropped job")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    fn cfg(dl: usize, ul: usize, dl_bps: u64, ul_bps: u64) -> IoGatewayConfig {
        // Total budget = sum of the per-direction ceilings, so each direction's
        // worker count is the binding limit (matching these tests' intent).
        cfg_total(dl + ul, dl, ul, dl_bps, ul_bps)
    }

    fn cfg_total(total: usize, dl: usize, ul: usize, dl_bps: u64, ul_bps: u64) -> IoGatewayConfig {
        IoGatewayConfig {
            concurrency: total,
            download_concurrency: dl,
            upload_concurrency: ul,
            download_bandwidth_bps: dl_bps,
            upload_bandwidth_bps: ul_bps,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn passes_through_results() {
        let gw = AzureIoGateway::new(cfg(2, 2, 0, 0));
        let out: u32 = gw
            .download(IoClass::ForegroundRead, 0, async { 7u32 })
            .await;
        assert_eq!(out, 7);
        let out: u32 = gw.upload(IoClass::Flush, 0, async { 9u32 }).await;
        assert_eq!(out, 9);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrency_is_capped_by_worker_count() {
        // With a single upload worker, no two upload jobs overlap.
        let gw = Arc::new(AzureIoGateway::new(cfg(1, 1, 0, 0)));
        let inflight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let gw = gw.clone();
            let inflight = inflight.clone();
            let max_seen = max_seen.clone();
            tasks.push(tokio::spawn(async move {
                gw.upload(IoClass::Flush, 0, async move {
                    let n = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                    max_seen.fetch_max(n, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    inflight.fetch_sub(1, Ordering::SeqCst);
                })
                .await;
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        assert_eq!(
            max_seen.load(Ordering::SeqCst),
            1,
            "single worker must serialize"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn higher_priority_is_served_first() {
        // One worker; flood the queue with low-priority (Copy) jobs while it is
        // busy, then enqueue a high-priority (Flush) job. The Flush job must run
        // before the still-queued Copy jobs.
        let gw = Arc::new(AzureIoGateway::new(cfg(1, 1, 0, 0)));
        let order = Arc::new(tokio::sync::Mutex::new(Vec::<&'static str>::new()));

        // Occupy the single worker with a blocker so the rest queue up.
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let blocker = {
            let gw = gw.clone();
            tokio::spawn(async move {
                gw.upload(IoClass::Flush, 0, async move {
                    let _ = release_rx.await;
                })
                .await;
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Queue several low-priority copies.
        let mut lows = Vec::new();
        for _ in 0..4 {
            let gw = gw.clone();
            let order = order.clone();
            lows.push(tokio::spawn(async move {
                gw.upload(IoClass::Copy, 0, async move {
                    order.lock().await.push("copy");
                })
                .await;
            }));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Now a high-priority flush enters the (non-empty) queue.
        let high = {
            let gw = gw.clone();
            let order = order.clone();
            tokio::spawn(async move {
                gw.upload(IoClass::Flush, 0, async move {
                    order.lock().await.push("flush");
                })
                .await;
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Release the blocker; the worker now drains the queue by priority.
        release_tx.send(()).unwrap();
        blocker.await.unwrap();
        high.await.unwrap();
        for l in lows {
            l.await.unwrap();
        }

        let order = order.lock().await;
        assert_eq!(
            order[0], "flush",
            "high-priority flush must precede queued copies: {order:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bandwidth_limit_paces_throughput() {
        // 10 KiB/s upload limit; pushing 4 KiB across jobs must take a
        // measurable amount of time rather than completing instantly.
        let bps = 10 * 1024;
        let gw = Arc::new(AzureIoGateway::new(cfg(4, 4, 0, bps)));
        // Drain the initial burst budget first.
        gw.upload(IoClass::Flush, MAX_PAGE_REQUEST_BYTES, async {})
            .await;
        let start = Instant::now();
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let gw = gw.clone();
            tasks.push(tokio::spawn(async move {
                // 1 KiB each ⇒ 4 KiB total, ≈0.4s at 10 KiB/s.
                gw.upload(IoClass::Flush, 1024, async {}).await;
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        assert!(
            start.elapsed() >= Duration::from_millis(250),
            "bandwidth limiter should have paced the uploads, took {:?}",
            start.elapsed()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn large_request_is_charged_full_byte_cost() {
        // A request larger than the limiter's `max` must be charged its *full*
        // byte cost (in `max`-sized rounds), not silently clamped to a single
        // `max` charge. Here the rate is `MAX_PAGE_REQUEST_BYTES`/s, so the
        // limiter `max` is `MAX_PAGE_REQUEST_BYTES`; a download of twice that
        // drains the initial burst on the first round and must wait a full
        // refill (~1s) for the second. With the old clamping bug the read was
        // charged a single round and completed within the burst (instantly).
        let bps = MAX_PAGE_REQUEST_BYTES; // limiter max == MAX_PAGE_REQUEST_BYTES
        let gw = AzureIoGateway::new(cfg(1, 1, bps, 0));
        let start = Instant::now();
        gw.download(IoClass::ForegroundRead, 2 * MAX_PAGE_REQUEST_BYTES, async {
        })
        .await;
        assert!(
            start.elapsed() >= Duration::from_millis(500),
            "an over-max request must be paced for its full size, took {:?}",
            start.elapsed()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_direction_can_use_the_whole_budget() {
        // Total budget 4, each direction allowed up to the full budget. With no
        // uploads in flight, downloads alone must be able to reach 4 concurrent
        // requests (the old fixed half-split would have capped them at 2).
        let gw = Arc::new(AzureIoGateway::new(cfg_total(4, 4, 4, 0, 0)));
        let inflight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let gw = gw.clone();
            let inflight = inflight.clone();
            let max_seen = max_seen.clone();
            tasks.push(tokio::spawn(async move {
                gw.download(IoClass::ForegroundRead, 0, async move {
                    let n = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                    max_seen.fetch_max(n, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    inflight.fetch_sub(1, Ordering::SeqCst);
                })
                .await;
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        assert_eq!(
            max_seen.load(Ordering::SeqCst),
            4,
            "downloads alone should be able to use the whole shared budget"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn combined_concurrency_capped_by_shared_budget() {
        // Total budget 2, but each direction may individually use up to 2.
        // Flooding both directions at once must never exceed 2 in flight
        // *combined* — the shared semaphore, not the per-direction pools, is the
        // binding limit.
        let gw = Arc::new(AzureIoGateway::new(cfg_total(2, 2, 2, 0, 0)));
        let inflight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for i in 0..8 {
            let gw = gw.clone();
            let inflight = inflight.clone();
            let max_seen = max_seen.clone();
            tasks.push(tokio::spawn(async move {
                let body = {
                    let inflight = inflight.clone();
                    let max_seen = max_seen.clone();
                    async move {
                        let n = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                        max_seen.fetch_max(n, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        inflight.fetch_sub(1, Ordering::SeqCst);
                    }
                };
                if i % 2 == 0 {
                    gw.download(IoClass::ForegroundRead, 0, body).await;
                } else {
                    gw.upload(IoClass::Flush, 0, body).await;
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        assert!(
            max_seen.load(Ordering::SeqCst) <= 2,
            "combined in-flight must not exceed the shared budget, saw {}",
            max_seen.load(Ordering::SeqCst)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dropping_submission_cancels_work_and_frees_permit() {
        // Restores the documented `flush_io_timeout_secs` abort semantics: when a
        // submitter drops its `submit` future (e.g. a flush times out), the
        // in-flight work must be cancelled and its shared concurrency permit
        // released promptly, rather than pinning the budget until the underlying
        // call eventually returns.
        //
        // Single shared permit: job A occupies it and never completes on its own.
        // After A's work is observably running, drop A's submission; a follow-up
        // job B must then acquire the freed permit and complete.
        let gw = Arc::new(AzureIoGateway::new(cfg_total(1, 1, 1, 0, 0)));
        let started = Arc::new(tokio::sync::Notify::new());

        let gw_a = gw.clone();
        let started_a = started.clone();
        let task_a = tokio::spawn(async move {
            gw_a.upload(IoClass::Flush, 0, async move {
                started_a.notify_one();
                // Never completes; only cancellation (res_tx.closed()) ends it.
                std::future::pending::<()>().await;
            })
            .await;
        });

        // Wait until A holds the permit and is actually running.
        started.notified().await;

        // Drop A's submission. The worker should observe `res_tx.closed()`,
        // cancel A's work, and release the shared permit.
        task_a.abort();

        // B can only run if the permit was freed. With the bug (work runs to
        // completion regardless), the single permit stays pinned by A forever and
        // this times out.
        let out = tokio::time::timeout(
            Duration::from_secs(5),
            gw.upload(IoClass::Flush, 0, async { 42u32 }),
        )
        .await
        .expect("permit was not freed after the dropped submission was cancelled; B timed out");
        assert_eq!(out, 42);
    }

    // ── Pure-function / helper coverage ────────────────────────────────────

    #[test]
    fn priority_order_is_foreground_flush_copy_warmup() {
        assert!(IoClass::ForegroundRead.priority() > IoClass::Flush.priority());
        assert!(IoClass::Flush.priority() > IoClass::Copy.priority());
        assert!(IoClass::Copy.priority() > IoClass::Warmup.priority());
        // Lowest is strictly the warm-up class.
        for c in [IoClass::ForegroundRead, IoClass::Flush, IoClass::Copy] {
            assert!(c.priority() > IoClass::Warmup.priority());
        }
    }

    #[test]
    fn build_limiter_is_none_when_unlimited() {
        assert!(build_limiter(0).is_none(), "0 bps must mean unlimited");
    }

    #[test]
    fn build_limiter_max_is_at_least_one_max_page() {
        // A sub-page-rate limit still admits a full max-page request in one
        // round (max is floored at MAX_PAGE_REQUEST_BYTES) so it never deadlocks.
        let small = build_limiter(10 * 1024).expect("some");
        assert_eq!(small.max() as u64, MAX_PAGE_REQUEST_BYTES);
        // A rate above the page size raises max to the rate itself.
        let big = build_limiter(8 * MAX_PAGE_REQUEST_BYTES).expect("some");
        assert_eq!(big.max() as u64, 8 * MAX_PAGE_REQUEST_BYTES);
    }

    #[test]
    fn env_helpers_parse_and_filter() {
        // Unique per-test var names so this never races other tests' env.
        let uvar = "UBLK_TEST_ENV_USIZE_8131";
        let u64var = "UBLK_TEST_ENV_U64_8131";

        std::env::remove_var(uvar);
        assert_eq!(env_usize(uvar), None, "unset → None");
        std::env::set_var(uvar, "0");
        assert_eq!(env_usize(uvar), None, "0 is treated as unset");
        std::env::set_var(uvar, "not-a-number");
        assert_eq!(env_usize(uvar), None, "garbage → None");
        std::env::set_var(uvar, "128");
        assert_eq!(env_usize(uvar), Some(128));
        std::env::remove_var(uvar);

        std::env::remove_var(u64var);
        assert_eq!(env_u64(u64var), None);
        // 0 is a *valid* u64 here (unlimited bandwidth), unlike env_usize.
        std::env::set_var(u64var, "0");
        assert_eq!(env_u64(u64var), Some(0));
        std::env::set_var(u64var, "1048576");
        assert_eq!(env_u64(u64var), Some(1048576));
        std::env::remove_var(u64var);
    }

    // ── task-local class propagation ───────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn with_class_sets_inherits_but_does_not_cross_spawn() {
        // No scope ⇒ no class (an on-demand block read).
        assert_eq!(current_class(), None);

        with_class(IoClass::Flush, async {
            assert_eq!(current_class(), Some(IoClass::Flush));
            // Inherited by a child future polled on the *same* task.
            let inner = async { current_class() }.await;
            assert_eq!(inner, Some(IoClass::Flush));

            // A nested scope overrides, then restores on exit.
            with_class(IoClass::Warmup, async {
                assert_eq!(current_class(), Some(IoClass::Warmup));
            })
            .await;
            assert_eq!(current_class(), Some(IoClass::Flush));

            // NOT inherited across tokio::spawn (a fresh task has no scope).
            let spawned = tokio::spawn(async { current_class() }).await.unwrap();
            assert_eq!(spawned, None, "class must not leak across spawn");
        })
        .await;

        assert_eq!(current_class(), None, "class is cleared after the scope");
    }

    // ── per-direction concurrency ceiling ──────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn per_direction_ceiling_caps_one_direction_below_shared_budget() {
        // Shared budget is 8, but downloads are limited to 2: even flooding 8
        // download jobs, at most 2 run at once (the per-direction worker pool,
        // not the shared semaphore, is the binding limit here).
        let gw = Arc::new(AzureIoGateway::new(cfg_total(8, 2, 8, 0, 0)));
        let inflight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let gw = gw.clone();
            let inflight = inflight.clone();
            let max_seen = max_seen.clone();
            tasks.push(tokio::spawn(async move {
                gw.download(IoClass::ForegroundRead, 0, async move {
                    let n = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                    max_seen.fetch_max(n, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    inflight.fetch_sub(1, Ordering::SeqCst);
                })
                .await;
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        assert_eq!(
            max_seen.load(Ordering::SeqCst),
            2,
            "download_concurrency must cap downloads below the shared budget"
        );
    }

    // ── download-side priority (complement to the upload-side test) ─────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn download_foreground_read_preempts_queued_warmup() {
        let gw = Arc::new(AzureIoGateway::new(cfg(1, 1, 0, 0)));
        let order = Arc::new(tokio::sync::Mutex::new(Vec::<&'static str>::new()));

        // Occupy the single download worker.
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let blocker = {
            let gw = gw.clone();
            tokio::spawn(async move {
                gw.download(IoClass::ForegroundRead, 0, async move {
                    let _ = release_rx.await;
                })
                .await;
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Queue several low-priority warm-up reads behind the blocker.
        let mut lows = Vec::new();
        for _ in 0..4 {
            let gw = gw.clone();
            let order = order.clone();
            lows.push(tokio::spawn(async move {
                gw.download(IoClass::Warmup, 0, async move {
                    order.lock().await.push("warmup");
                })
                .await;
            }));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;

        // A foreground read enters the non-empty queue last but must run first.
        let high = {
            let gw = gw.clone();
            let order = order.clone();
            tokio::spawn(async move {
                gw.download(IoClass::ForegroundRead, 0, async move {
                    order.lock().await.push("foreground");
                })
                .await;
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;

        release_tx.send(()).unwrap();
        blocker.await.unwrap();
        high.await.unwrap();
        for l in lows {
            l.await.unwrap();
        }

        let order = order.lock().await;
        assert_eq!(
            order[0], "foreground",
            "foreground read must precede queued warm-ups: {order:?}"
        );
    }

    // ── bandwidth: unlimited & cross-direction independence ─────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn unlimited_bandwidth_does_not_pace() {
        // bps == 0 ⇒ no limiter; a large transfer completes promptly.
        let gw = AzureIoGateway::new(cfg(4, 4, 0, 0));
        let start = Instant::now();
        gw.upload(IoClass::Flush, 64 * MAX_PAGE_REQUEST_BYTES, async {})
            .await;
        assert!(
            start.elapsed() < Duration::from_millis(200),
            "unlimited bandwidth must not pace, took {:?}",
            start.elapsed()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn download_bandwidth_limit_does_not_throttle_uploads() {
        // Only the download direction is rate-limited; an upload of the same
        // (large) size must not be paced by the download limiter.
        let bps = 8 * 1024; // tiny download ceiling
        let gw = AzureIoGateway::new(cfg(2, 2, bps, 0));
        let start = Instant::now();
        gw.upload(IoClass::Flush, 16 * MAX_PAGE_REQUEST_BYTES, async {})
            .await;
        assert!(
            start.elapsed() < Duration::from_millis(200),
            "upload must be unaffected by the download bandwidth limit, took {:?}",
            start.elapsed()
        );
    }

    // ── config auto-sizing from the environment ────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auto_uses_cpu_count_and_honours_env_overrides() {
        // Defaults (vars unset): shared budget == cpu_count, per-direction
        // ceilings default to it, bandwidth unlimited.
        for v in [
            "UBLK_IO_CONCURRENCY",
            "UBLK_DOWNLOAD_CONCURRENCY",
            "UBLK_UPLOAD_CONCURRENCY",
            "UBLK_DOWNLOAD_BANDWIDTH",
            "UBLK_UPLOAD_BANDWIDTH",
        ] {
            std::env::remove_var(v);
        }
        let d = IoGatewayConfig::auto();
        assert_eq!(d.concurrency, cpu_count().max(1));
        assert_eq!(d.download_concurrency, d.concurrency);
        assert_eq!(d.upload_concurrency, d.concurrency);
        assert_eq!(d.download_bandwidth_bps, 0);
        assert_eq!(d.upload_bandwidth_bps, 0);

        // Explicit overrides are honoured; a 0 concurrency is treated as unset.
        std::env::set_var("UBLK_IO_CONCURRENCY", "64");
        std::env::set_var("UBLK_DOWNLOAD_CONCURRENCY", "0"); // unset → falls back
        std::env::set_var("UBLK_UPLOAD_CONCURRENCY", "16");
        std::env::set_var("UBLK_DOWNLOAD_BANDWIDTH", "1048576");
        let o = IoGatewayConfig::auto();
        assert_eq!(o.concurrency, 64);
        assert_eq!(o.download_concurrency, 64, "0 falls back to shared budget");
        assert_eq!(o.upload_concurrency, 16);
        assert_eq!(o.download_bandwidth_bps, 1048576);
        assert_eq!(o.upload_bandwidth_bps, 0);
        for v in [
            "UBLK_IO_CONCURRENCY",
            "UBLK_DOWNLOAD_CONCURRENCY",
            "UBLK_UPLOAD_CONCURRENCY",
            "UBLK_DOWNLOAD_BANDWIDTH",
        ] {
            std::env::remove_var(v);
        }
    }

    // ── end-to-end mixed workload ──────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn e2e_mixed_workload_respects_shared_budget_and_completes() {
        // Flood both directions with all four I/O classes at once against a
        // shared budget of 3, with the upload direction additionally rate
        // limited. Invariants checked end-to-end:
        //   * combined download+upload in-flight never exceeds the shared budget,
        //   * every submitted job runs exactly once and returns its own value.
        let gw = Arc::new(AzureIoGateway::new(cfg_total(3, 3, 3, 0, 64 * 1024)));
        let inflight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));

        let classes = [
            IoClass::ForegroundRead,
            IoClass::Flush,
            IoClass::Copy,
            IoClass::Warmup,
        ];
        let mut tasks = Vec::new();
        for i in 0..40u32 {
            let gw = gw.clone();
            let inflight = inflight.clone();
            let max_seen = max_seen.clone();
            let completed = completed.clone();
            let class = classes[(i as usize) % classes.len()];
            tasks.push(tokio::spawn(async move {
                let body = {
                    let inflight = inflight.clone();
                    let max_seen = max_seen.clone();
                    let completed = completed.clone();
                    async move {
                        let n = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                        max_seen.fetch_max(n, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        inflight.fetch_sub(1, Ordering::SeqCst);
                        completed.fetch_add(1, Ordering::SeqCst);
                        i
                    }
                };
                // Alternate directions; uploads carry a byte cost (rate limited).
                if i % 2 == 0 {
                    gw.download(class, 256, body).await
                } else {
                    gw.upload(class, 256, body).await
                }
            }));
        }
        let mut seen = std::collections::HashSet::new();
        for t in tasks {
            seen.insert(t.await.unwrap());
        }
        assert_eq!(seen.len(), 40, "every job must run exactly once");
        assert_eq!(completed.load(Ordering::SeqCst), 40);
        assert!(
            max_seen.load(Ordering::SeqCst) <= 3,
            "combined in-flight must never exceed the shared budget, saw {}",
            max_seen.load(Ordering::SeqCst)
        );
    }
}

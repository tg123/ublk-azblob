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
//! 2. **Threads / concurrency** — a fixed worker pool; at most that many Azure
//!    requests are in flight at once.
//! 3. **Fairness** — a *provider/consumer* model: producers (on-demand reads,
//!    flush write-back, server-side copy, cache warm-up) enqueue work onto a
//!    priority queue that the workers drain highest-priority-first. This
//!    prevents background work from starving foreground I/O. The priority order
//!    is **foreground read > flush > copy > warm-up**.
//!
//! Downloads and uploads have *separate* pools, limiters and queues, so the two
//! directions never contend with each other; the priority order above is
//! enforced *within* each direction (downloads: foreground read > copy >
//! warm-up; uploads: flush > copy).

use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;

use async_priority_channel as pc;
use futures::future::BoxFuture;
use leaky_bucket::RateLimiter;
use std::sync::Arc;
use tokio::sync::oneshot;
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
    /// Build a pipeline with `workers` consumer tasks and an optional byte-rate
    /// limiter (`bandwidth_bps == 0` ⇒ unlimited).
    fn new(workers: usize, bandwidth_bps: u64, handles: &mut Vec<JoinHandle<()>>) -> Self {
        let workers = workers.max(1);
        let (tx, rx) = pc::unbounded::<Job, u8>();
        let limiter = build_limiter(bandwidth_bps);
        for _ in 0..workers {
            let rx = rx.clone();
            let limiter = limiter.clone();
            handles.push(tokio::spawn(worker_loop(rx, limiter)));
        }
        Self { tx }
    }
}

/// Build a leaky-bucket limiter for `bandwidth_bps` bytes/sec, or `None` when
/// unlimited. Tokens are bytes. `max` is sized to admit the largest single
/// request (one max page write) so acquiring a full chunk can never deadlock.
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

/// Consumer loop: pull the highest-priority job, pay its bandwidth cost, run it.
async fn worker_loop(rx: pc::Receiver<Job, u8>, limiter: Option<Arc<RateLimiter>>) {
    while let Ok((job, _priority)) = rx.recv().await {
        if let Some(rl) = &limiter {
            if job.bytes > 0 {
                // Clamp to the limiter's capacity so an unexpectedly large
                // request can never wait forever for more tokens than `max`.
                let permits = (job.bytes as usize).min(rl.max());
                rl.acquire(permits).await;
            }
        }
        job.fut.await;
    }
}

/// Configuration for the gateway's two pipelines.
#[derive(Clone, Copy, Debug)]
pub struct IoGatewayConfig {
    /// Max concurrent download (read) requests.
    pub download_concurrency: usize,
    /// Max concurrent upload (write/clear/copy) requests.
    pub upload_concurrency: usize,
    /// Download bandwidth ceiling in bytes/sec (`0` = unlimited).
    pub download_bandwidth_bps: u64,
    /// Upload bandwidth ceiling in bytes/sec (`0` = unlimited).
    pub upload_bandwidth_bps: u64,
}

impl IoGatewayConfig {
    /// Auto-size concurrency so that *download + upload = logical CPU count*
    /// (split as evenly as possible, at least one worker each); bandwidth
    /// unlimited. Environment variables, when set to a non-zero value, are used
    /// as the defaults for each field (`UBLK_DOWNLOAD_CONCURRENCY`,
    /// `UBLK_UPLOAD_CONCURRENCY`, `UBLK_DOWNLOAD_BANDWIDTH`,
    /// `UBLK_UPLOAD_BANDWIDTH`, bytes/sec for bandwidth). An env concurrency of
    /// `0` is treated as unset (falls back to the CPU-count split), matching the
    /// CLI flags — which take precedence over these defaults when explicitly
    /// provided (see `main.rs`).
    pub fn auto() -> Self {
        let cpu = cpu_count().max(1);
        let download = (cpu / 2).max(1);
        let upload = cpu.saturating_sub(download).max(1);
        Self {
            download_concurrency: env_usize("UBLK_DOWNLOAD_CONCURRENCY").unwrap_or(download),
            upload_concurrency: env_usize("UBLK_UPLOAD_CONCURRENCY").unwrap_or(upload),
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
        let mut workers = Vec::new();
        let download = Pipeline::new(
            cfg.download_concurrency,
            cfg.download_bandwidth_bps,
            &mut workers,
        );
        let upload = Pipeline::new(
            cfg.upload_concurrency,
            cfg.upload_bandwidth_bps,
            &mut workers,
        );
        trace!(
            download_concurrency = cfg.download_concurrency,
            upload_concurrency = cfg.upload_concurrency,
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
        let (res_tx, res_rx) = oneshot::channel::<T>();
        let fut: BoxFuture<'static, ()> = Box::pin(async move {
            let out = work.await;
            // Receiver only gone if the submitter was cancelled; drop the result.
            let _ = res_tx.send(out);
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
        IoGatewayConfig {
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
}

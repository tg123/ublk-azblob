//! Throughput / IOPS / latency benchmark for any [`BlobBackend`].
//!
//! The benchmark issues a fixed number of fixed-size I/O operations against the
//! backend using a configurable number of concurrent workers (mirroring a ublk
//! queue depth) and reports throughput, IOPS, and latency percentiles for each
//! workload phase.
//!
//! It is backend-agnostic: it runs against `AzurePageBlobBackend` (Azurite or
//! real Azure) as well as the in-memory `MemBackend` used in unit tests.

use crate::backend::BlobBackend;
use anyhow::{bail, Context as _};
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

/// Which I/O workloads to run, in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Workload {
    /// Sequential writes, then sequential reads.
    Seq,
    /// Random writes, then random reads.
    Rand,
    /// All four phases: sequential write/read then random write/read.
    All,
}

impl Workload {
    fn phases(self) -> &'static [Phase] {
        match self {
            Workload::Seq => &[
                Phase {
                    op: Op::Write,
                    access: Access::Sequential,
                },
                Phase {
                    op: Op::Read,
                    access: Access::Sequential,
                },
            ],
            Workload::Rand => &[
                Phase {
                    op: Op::Write,
                    access: Access::Random,
                },
                Phase {
                    op: Op::Read,
                    access: Access::Random,
                },
            ],
            Workload::All => &[
                Phase {
                    op: Op::Write,
                    access: Access::Sequential,
                },
                Phase {
                    op: Op::Read,
                    access: Access::Sequential,
                },
                Phase {
                    op: Op::Write,
                    access: Access::Random,
                },
                Phase {
                    op: Op::Read,
                    access: Access::Random,
                },
            ],
        }
    }
}

#[derive(Clone, Copy)]
struct Phase {
    op: Op,
    access: Access,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    Read,
    Write,
}

impl Op {
    fn label(self) -> &'static str {
        match self {
            Op::Read => "read",
            Op::Write => "write",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Access {
    Sequential,
    Random,
}

impl Access {
    fn label(self) -> &'static str {
        match self {
            Access::Sequential => "seq",
            Access::Random => "rand",
        }
    }
}

/// Benchmark parameters.
#[derive(Clone, Debug)]
pub struct BenchConfig {
    /// Size of the backing blob in bytes (multiple of `block_size`).
    pub size: u64,
    /// I/O size per operation in bytes (multiple of 512).
    pub block_size: u64,
    /// Number of operations to issue per phase.
    pub count: u64,
    /// Number of concurrent in-flight operations (queue depth).
    pub concurrency: u64,
    /// Which workloads to run.
    pub workload: Workload,
    /// Provision (create/overwrite) the blob before benchmarking.
    pub create: bool,
}

/// Summary statistics for a single benchmark phase.
#[derive(Clone, Debug)]
pub struct PhaseResult {
    pub name: String,
    pub ops: u64,
    pub bytes: u64,
    pub elapsed: Duration,
    pub latencies: LatencyStats,
}

impl PhaseResult {
    /// Throughput in mebibytes per second.
    pub fn throughput_mib_s(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs <= 0.0 {
            return 0.0;
        }
        (self.bytes as f64) / (1024.0 * 1024.0) / secs
    }

    /// Operations per second (IOPS).
    pub fn iops(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs <= 0.0 {
            return 0.0;
        }
        (self.ops as f64) / secs
    }
}

/// Latency distribution for a phase.
#[derive(Clone, Debug)]
pub struct LatencyStats {
    pub min: Duration,
    pub avg: Duration,
    pub p50: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub max: Duration,
}

impl LatencyStats {
    fn from_sorted(mut samples: Vec<Duration>) -> Self {
        if samples.is_empty() {
            return Self {
                min: Duration::ZERO,
                avg: Duration::ZERO,
                p50: Duration::ZERO,
                p95: Duration::ZERO,
                p99: Duration::ZERO,
                max: Duration::ZERO,
            };
        }
        samples.sort_unstable();
        let sum: Duration = samples.iter().sum();
        let avg = sum / (samples.len() as u32);
        Self {
            min: samples[0],
            avg,
            p50: percentile(&samples, 50.0),
            p95: percentile(&samples, 95.0),
            p99: percentile(&samples, 99.0),
            max: samples[samples.len() - 1],
        }
    }
}

/// Nearest-rank percentile of a pre-sorted slice.
fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = (pct / 100.0 * sorted.len() as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[idx]
}

/// Run the benchmark described by `cfg` against `backend`, returning per-phase
/// results.  Results are also logged via `tracing` for CLI visibility.
pub async fn run_bench(
    backend: Arc<dyn BlobBackend>,
    cfg: BenchConfig,
) -> anyhow::Result<Vec<PhaseResult>> {
    validate(&cfg)?;

    if cfg.create {
        info!(size = cfg.size, "provisioning benchmark blob");
        backend.create(cfg.size).await.context("create blob")?;
    }

    // `validate` guarantees size >= block_size (both non-zero), so nblocks >= 1.
    let nblocks = cfg.size / cfg.block_size;

    info!(
        size = cfg.size,
        block_size = cfg.block_size,
        count = cfg.count,
        concurrency = cfg.concurrency,
        nblocks,
        "starting benchmark"
    );

    let mut results = Vec::new();
    for phase in cfg.workload.phases() {
        let result = run_phase(&backend, &cfg, *phase, nblocks).await?;
        log_result(&result);
        results.push(result);
    }

    Ok(results)
}

async fn run_phase(
    backend: &Arc<dyn BlobBackend>,
    cfg: &BenchConfig,
    phase: Phase,
    nblocks: u64,
) -> anyhow::Result<PhaseResult> {
    // Shared write payload reused across operations (writes only). 0xA5 is an
    // arbitrary non-zero fill pattern (alternating bits) so writes don't look
    // like a zero/clear request.
    let payload = Bytes::from(vec![0xA5u8; cfg.block_size as usize]);
    let next = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut workers = Vec::with_capacity(cfg.concurrency as usize);
    for worker_id in 0..cfg.concurrency {
        let backend = Arc::clone(backend);
        let next = Arc::clone(&next);
        let payload = payload.clone();
        let block_size = cfg.block_size;
        let count = cfg.count;
        workers.push(tokio::spawn(async move {
            let mut latencies: Vec<Duration> = Vec::new();
            loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= count {
                    break;
                }
                let block = match phase.access {
                    Access::Sequential => idx % nblocks,
                    // Modulo introduces a negligible bias when nblocks is not a
                    // power of two; acceptable for spreading benchmark offsets.
                    Access::Random => next_rand(idx.wrapping_add(worker_id)) % nblocks,
                };
                let offset = block * block_size;

                let op_start = Instant::now();
                match phase.op {
                    Op::Read => {
                        backend.read(offset, block_size).await.context("read")?;
                    }
                    Op::Write => {
                        backend
                            .write(offset, payload.clone())
                            .await
                            .context("write")?;
                    }
                }
                latencies.push(op_start.elapsed());
            }
            Ok::<Vec<Duration>, anyhow::Error>(latencies)
        }));
    }

    let mut latencies: Vec<Duration> = Vec::with_capacity(cfg.count as usize);
    for worker in workers {
        let mut part = worker.await.context("benchmark worker panicked")??;
        latencies.append(&mut part);
    }

    // Writes are flushed so buffered backends count the real durable cost.
    if phase.op == Op::Write {
        backend.flush().await.context("flush")?;
    }
    let elapsed = start.elapsed();

    let ops = latencies.len() as u64;
    Ok(PhaseResult {
        name: format!("{}-{}", phase.access.label(), phase.op.label()),
        ops,
        bytes: ops * cfg.block_size,
        elapsed,
        latencies: LatencyStats::from_sorted(latencies),
    })
}

fn validate(cfg: &BenchConfig) -> anyhow::Result<()> {
    if cfg.block_size == 0 || !cfg.block_size.is_multiple_of(512) {
        bail!(
            "block_size ({}) must be a non-zero multiple of 512",
            cfg.block_size
        );
    }
    if cfg.size == 0 || !cfg.size.is_multiple_of(512) {
        bail!("size ({}) must be a non-zero multiple of 512", cfg.size);
    }
    if cfg.size < cfg.block_size {
        bail!(
            "size ({}) must be >= block_size ({})",
            cfg.size,
            cfg.block_size
        );
    }
    if cfg.count == 0 {
        bail!("count must be > 0");
    }
    if cfg.concurrency == 0 {
        bail!("concurrency must be > 0");
    }
    Ok(())
}

/// Deterministic stateless hash of `seed` using the SplitMix64 finalizer —
/// avoids pulling in a `rand` dependency just to spread out block offsets.
/// This is the SplitMix64 output (mix) function applied to `state = seed + GAMMA`;
/// constants are from the reference implementation
/// (https://prng.di.unimi.it/splitmix64.c).  Distinct seeds yield well-distributed
/// distinct outputs, which is all the benchmark needs to scatter block offsets.
fn next_rand(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn log_result(r: &PhaseResult) {
    info!(
        phase = %r.name,
        ops = r.ops,
        "{:>9} | {:>10.2} MiB/s | {:>10.0} IOPS | lat min {:?} avg {:?} p50 {:?} p95 {:?} p99 {:?} max {:?}",
        r.name,
        r.throughput_mib_s(),
        r.iops(),
        r.latencies.min,
        r.latencies.avg,
        r.latencies.p50,
        r.latencies.p95,
        r.latencies.p99,
        r.latencies.max,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::mem::MemBackend;

    fn cfg(workload: Workload) -> BenchConfig {
        BenchConfig {
            size: 64 * 1024,
            block_size: 4096,
            count: 64,
            concurrency: 4,
            workload,
            create: true,
        }
    }

    #[tokio::test]
    async fn runs_all_phases() {
        let backend: Arc<dyn BlobBackend> = Arc::new(MemBackend::new(64 * 1024).unwrap());
        let results = run_bench(backend, cfg(Workload::All)).await.unwrap();
        assert_eq!(results.len(), 4);
        for r in &results {
            assert_eq!(r.ops, 64);
            assert_eq!(r.bytes, 64 * 4096);
            assert!(r.throughput_mib_s() >= 0.0);
            assert!(r.iops() >= 0.0);
        }
    }

    #[tokio::test]
    async fn seq_and_rand_phase_names() {
        let backend: Arc<dyn BlobBackend> = Arc::new(MemBackend::new(64 * 1024).unwrap());
        let seq = run_bench(Arc::clone(&backend), cfg(Workload::Seq))
            .await
            .unwrap();
        assert_eq!(seq[0].name, "seq-write");
        assert_eq!(seq[1].name, "seq-read");

        let rand = run_bench(backend, cfg(Workload::Rand)).await.unwrap();
        assert_eq!(rand[0].name, "rand-write");
        assert_eq!(rand[1].name, "rand-read");
    }

    #[tokio::test]
    async fn rejects_invalid_config() {
        let backend: Arc<dyn BlobBackend> = Arc::new(MemBackend::new(4096).unwrap());

        let mut bad = cfg(Workload::Seq);
        bad.block_size = 500; // not a multiple of 512
        assert!(run_bench(Arc::clone(&backend), bad).await.is_err());

        let mut bad = cfg(Workload::Seq);
        bad.count = 0;
        assert!(run_bench(Arc::clone(&backend), bad).await.is_err());

        let mut bad = cfg(Workload::Seq);
        bad.concurrency = 0;
        assert!(run_bench(backend, bad).await.is_err());
    }

    #[test]
    fn percentile_nearest_rank() {
        let samples: Vec<Duration> = (1..=100).map(|i| Duration::from_millis(i)).collect();
        assert_eq!(percentile(&samples, 50.0), Duration::from_millis(50));
        assert_eq!(percentile(&samples, 99.0), Duration::from_millis(99));
        assert_eq!(percentile(&samples, 100.0), Duration::from_millis(100));
    }

    #[test]
    fn random_blocks_stay_in_range() {
        let nblocks = 16u64;
        for i in 0..1000 {
            assert!(next_rand(i) % nblocks < nblocks);
        }
    }
}

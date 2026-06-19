//! In-memory `BlobBackend` implementation for unit tests.
//!
//! `MemBackend` requires no network connection and no kernel support, making
//! it ideal for exercising the alignment and read/write/clear logic in CI.

use super::BlobBackend;
use anyhow::{anyhow, bail};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Mutex;

/// In-memory blob backend.  Thread-safe via a `Mutex`.
#[allow(dead_code)]
pub struct MemBackend {
    data: Mutex<Vec<u8>>,
}

impl MemBackend {
    /// Create a new, initially-zeroed `MemBackend` of the given size.
    ///
    /// `size` must be a non-zero multiple of 512.
    #[allow(dead_code)]
    pub fn new(size: u64) -> anyhow::Result<Self> {
        check_alignment(size, 512, "size")?;
        if size == 0 {
            bail!("size must be > 0");
        }
        Ok(Self {
            data: Mutex::new(vec![0u8; size as usize]),
        })
    }
}

#[async_trait]
impl BlobBackend for MemBackend {
    async fn create(&self, size: u64) -> anyhow::Result<()> {
        check_alignment(size, 512, "size")?;
        if size == 0 {
            bail!("size must be > 0");
        }
        let mut data = self.data.lock().map_err(|e| anyhow!("lock: {e}"))?;
        *data = vec![0u8; size as usize];
        Ok(())
    }

    async fn read(&self, offset: u64, len: u64) -> anyhow::Result<Bytes> {
        check_alignment(offset, 512, "offset")?;
        check_alignment(len, 512, "len")?;
        let data = self.data.lock().map_err(|e| anyhow!("lock: {e}"))?;
        let end = (offset + len) as usize;
        if end > data.len() {
            bail!(
                "read out of bounds: offset={offset} len={len} size={}",
                data.len()
            );
        }
        Ok(Bytes::copy_from_slice(&data[offset as usize..end]))
    }

    async fn write(&self, offset: u64, payload: Bytes) -> anyhow::Result<()> {
        check_alignment(offset, 512, "offset")?;
        check_alignment(payload.len() as u64, 512, "data.len()")?;
        let mut data = self.data.lock().map_err(|e| anyhow!("lock: {e}"))?;
        let end = offset as usize + payload.len();
        if end > data.len() {
            bail!(
                "write out of bounds: offset={offset} len={} size={}",
                payload.len(),
                data.len()
            );
        }
        data[offset as usize..end].copy_from_slice(&payload);
        Ok(())
    }

    async fn clear(&self, offset: u64, len: u64) -> anyhow::Result<()> {
        check_alignment(offset, 512, "offset")?;
        check_alignment(len, 512, "len")?;
        let mut data = self.data.lock().map_err(|e| anyhow!("lock: {e}"))?;
        let end = (offset + len) as usize;
        if end > data.len() {
            bail!(
                "clear out of bounds: offset={offset} len={len} size={}",
                data.len()
            );
        }
        data[offset as usize..end].fill(0);
        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn delete(&self) -> anyhow::Result<()> {
        let mut data = self.data.lock().map_err(|e| anyhow!("lock: {e}"))?;
        *data = Vec::new();
        Ok(())
    }

    async fn size(&self) -> anyhow::Result<u64> {
        let data = self.data.lock().map_err(|e| anyhow!("lock: {e}"))?;
        Ok(data.len() as u64)
    }
}

#[allow(dead_code)]
fn check_alignment(value: u64, align: u64, name: &str) -> anyhow::Result<()> {
    if !value.is_multiple_of(align) {
        bail!("{name} ({value}) is not aligned to {align} bytes");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_and_size() {
        let b = MemBackend::new(4096).unwrap();
        assert_eq!(b.size().await.unwrap(), 4096);
        b.create(8192).await.unwrap();
        assert_eq!(b.size().await.unwrap(), 8192);
    }

    #[tokio::test]
    async fn test_write_read() {
        let b = MemBackend::new(4096).unwrap();
        let payload = Bytes::from(vec![0xABu8; 512]);
        b.write(512, payload.clone()).await.unwrap();
        let read_back = b.read(512, 512).await.unwrap();
        assert_eq!(read_back, payload);
        // Unwritten area should be zeroed
        let zero = b.read(0, 512).await.unwrap();
        assert!(zero.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn test_clear() {
        let b = MemBackend::new(4096).unwrap();
        let payload = Bytes::from(vec![0xFFu8; 1024]);
        b.write(0, payload).await.unwrap();
        b.clear(0, 1024).await.unwrap();
        let after = b.read(0, 1024).await.unwrap();
        assert!(after.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn test_alignment_errors() {
        let b = MemBackend::new(4096).unwrap();
        assert!(b.read(1, 512).await.is_err(), "unaligned offset");
        assert!(b.read(0, 100).await.is_err(), "unaligned len");
        assert!(
            b.write(0, Bytes::from(vec![0u8; 100])).await.is_err(),
            "unaligned data"
        );
        assert!(b.clear(0, 100).await.is_err(), "unaligned clear len");
    }

    #[tokio::test]
    async fn test_out_of_bounds() {
        let b = MemBackend::new(512).unwrap();
        assert!(b.read(0, 1024).await.is_err(), "read past end");
        assert!(
            b.write(0, Bytes::from(vec![0u8; 1024])).await.is_err(),
            "write past end"
        );
        assert!(b.clear(0, 1024).await.is_err(), "clear past end");
    }

    #[tokio::test]
    async fn test_full_write_read_clear_cycle() {
        const SIZE: u64 = 4096;
        let b = MemBackend::new(SIZE).unwrap();

        // Write pattern to every page
        for page in 0..(SIZE / 512) {
            let pattern = vec![(page & 0xFF) as u8; 512];
            b.write(page * 512, Bytes::from(pattern)).await.unwrap();
        }

        // Read back and verify
        for page in 0..(SIZE / 512) {
            let data = b.read(page * 512, 512).await.unwrap();
            assert!(data.iter().all(|&x| x == (page & 0xFF) as u8));
        }

        // Clear the middle two pages and verify they're zeroed
        b.clear(512, 1024).await.unwrap();
        let zeroed = b.read(512, 1024).await.unwrap();
        assert!(zeroed.iter().all(|&x| x == 0));

        // First and last pages should still hold the pattern
        let first = b.read(0, 512).await.unwrap();
        assert!(first.iter().all(|&x| x == 0));
        let last = b.read(SIZE - 512, 512).await.unwrap();
        assert!(last.iter().all(|&x| x == ((SIZE / 512 - 1) & 0xFF) as u8));
    }
}

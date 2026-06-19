//! Import a local folder into the backing blob.
//!
//! The folder is serialized into a single in-memory **tar** archive and written
//! to the page blob as a raw image.  Combined with `--snapshot`, this provides a
//! simple way to seed a blob with the contents of a directory and capture an
//! immutable point-in-time copy of it.
//!
//! The blob is treated as a flat byte image: the tar stream is padded up to a
//! 512-byte boundary (the Page Blob alignment requirement) and uploaded in
//! aligned chunks via the [`BlobBackend`] trait.

use crate::backend::BlobBackend;
use anyhow::{bail, Context as _};
use bytes::Bytes;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

/// Block alignment required by Azure Page Blobs (and the `BlobBackend` trait).
const BLOCK: u64 = 512;

/// Size of each aligned upload chunk (4 MiB; a multiple of `BLOCK`).
const CHUNK: usize = 4 * 1024 * 1024;

/// Round `n` up to the next multiple of `align`.
fn round_up(n: u64, align: u64) -> u64 {
    n.div_ceil(align) * align
}

/// Serialize the contents of `dir` into an in-memory tar archive.
///
/// The directory's entries are stored at the root of the archive (i.e. relative
/// to `dir`), so extracting the archive reproduces the folder's contents.
pub fn build_tar(dir: &Path) -> anyhow::Result<Vec<u8>> {
    if !dir.is_dir() {
        bail!("import path '{}' is not a directory", dir.display());
    }
    let mut builder = tar::Builder::new(Vec::new());
    builder
        .append_dir_all(".", dir)
        .with_context(|| format!("archive directory '{}'", dir.display()))?;
    let data = builder.into_inner().context("finalize tar archive")?;
    Ok(data)
}

/// Import the folder at `dir` into `backend`.
///
/// * Builds a tar archive of `dir`.
/// * Provisions the blob: `size` (when given) must be a multiple of 512 and at
///   least large enough to hold the archive; otherwise the blob is sized to the
///   archive rounded up to the next 512-byte boundary.
/// * Writes the archive (zero-padded to alignment) to the blob and flushes.
/// * When `snapshot` is set, creates a snapshot and returns its identifier.
pub async fn import_folder(
    backend: Arc<dyn BlobBackend>,
    dir: &Path,
    size: Option<u64>,
    snapshot: bool,
) -> anyhow::Result<Option<String>> {
    let tar = build_tar(dir)?;
    let tar_len = tar.len() as u64;
    info!(
        path = %dir.display(),
        tar_bytes = tar_len,
        "built tar archive of folder"
    );

    let needed = round_up(tar_len, BLOCK).max(BLOCK);
    let dev_size = match size {
        Some(s) => {
            if !s.is_multiple_of(BLOCK) {
                bail!("--size {s} must be a multiple of {BLOCK} bytes");
            }
            if s < needed {
                bail!(
                    "--size {s} is too small for the {tar_len}-byte archive \
                     (need at least {needed} bytes)"
                );
            }
            s
        }
        None => needed,
    };

    info!(size = dev_size, "provisioning blob for import");
    backend.create(dev_size).await.context("create blob")?;

    // Pad the archive to a 512-byte boundary so every write is aligned.
    let mut buf = tar;
    let padded_len = round_up(buf.len() as u64, BLOCK) as usize;
    buf.resize(padded_len, 0);

    let mut offset: u64 = 0;
    for chunk in buf.chunks(CHUNK) {
        backend
            .write(offset, Bytes::copy_from_slice(chunk))
            .await
            .with_context(|| format!("write archive chunk at offset {offset}"))?;
        offset += chunk.len() as u64;
    }
    backend.flush().await.context("flush after import")?;
    info!(written = padded_len, "imported folder into blob");

    if snapshot {
        let id = backend.snapshot().await.context("create snapshot")?;
        info!(snapshot = %id, "created snapshot");
        Ok(Some(id))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::mem::MemBackend;
    use std::io::Read as _;

    fn unique_tmp_dir(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("ublk-azblob-import-{tag}-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn round_up_works() {
        assert_eq!(round_up(0, 512), 0);
        assert_eq!(round_up(1, 512), 512);
        assert_eq!(round_up(512, 512), 512);
        assert_eq!(round_up(513, 512), 1024);
    }

    #[test]
    fn build_tar_roundtrips_files() {
        let dir = unique_tmp_dir("build");
        std::fs::write(dir.join("a.txt"), b"hello").unwrap();
        std::fs::create_dir(dir.join("sub")).unwrap();
        std::fs::write(dir.join("sub/b.txt"), b"world").unwrap();

        let archive = build_tar(&dir).unwrap();
        let mut found = std::collections::HashMap::new();
        let mut ar = tar::Archive::new(std::io::Cursor::new(archive));
        for entry in ar.entries().unwrap() {
            let mut entry = entry.unwrap();
            let path = entry.path().unwrap().to_string_lossy().into_owned();
            if entry.header().entry_type().is_file() {
                let mut contents = String::new();
                entry.read_to_string(&mut contents).unwrap();
                found.insert(path, contents);
            }
        }
        std::fs::remove_dir_all(&dir).ok();

        assert_eq!(found.get("a.txt").map(String::as_str), Some("hello"));
        assert_eq!(found.get("sub/b.txt").map(String::as_str), Some("world"));
    }

    #[test]
    fn build_tar_rejects_non_directory() {
        let dir = unique_tmp_dir("nondir");
        let file = dir.join("f.txt");
        std::fs::write(&file, b"x").unwrap();
        assert!(build_tar(&file).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn import_writes_archive_to_backend() {
        let dir = unique_tmp_dir("import");
        std::fs::write(dir.join("hello.txt"), b"hello world").unwrap();

        let backend: Arc<dyn BlobBackend> = Arc::new(MemBackend::new(512).unwrap());
        let snap = import_folder(backend.clone(), &dir, None, false)
            .await
            .unwrap();
        assert!(snap.is_none());

        // Read the whole blob back and confirm the tar extracts the file.
        let size = backend.size().await.unwrap();
        assert!(size.is_multiple_of(BLOCK));
        let data = backend.read(0, size).await.unwrap();

        let mut ar = tar::Archive::new(std::io::Cursor::new(data.as_ref()));
        let mut got = None;
        for entry in ar.entries().unwrap() {
            let mut entry = entry.unwrap();
            if entry.path().unwrap().to_string_lossy() == "hello.txt" {
                let mut s = String::new();
                entry.read_to_string(&mut s).unwrap();
                got = Some(s);
            }
        }
        std::fs::remove_dir_all(&dir).ok();
        assert_eq!(got.as_deref(), Some("hello world"));
    }

    #[tokio::test]
    async fn import_rejects_too_small_size() {
        let dir = unique_tmp_dir("toosmall");
        std::fs::write(dir.join("big.bin"), vec![0u8; 64 * 1024]).unwrap();

        let backend: Arc<dyn BlobBackend> = Arc::new(MemBackend::new(512).unwrap());
        let err = import_folder(backend, &dir, Some(512), false).await;
        std::fs::remove_dir_all(&dir).ok();
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn import_rejects_unaligned_size() {
        let dir = unique_tmp_dir("unaligned");
        std::fs::write(dir.join("a.txt"), b"x").unwrap();

        let backend: Arc<dyn BlobBackend> = Arc::new(MemBackend::new(512).unwrap());
        let err = import_folder(backend, &dir, Some(1000), false).await;
        std::fs::remove_dir_all(&dir).ok();
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn snapshot_default_unsupported_on_membackend() {
        let backend: Arc<dyn BlobBackend> = Arc::new(MemBackend::new(512).unwrap());
        assert!(backend.snapshot().await.is_err());
    }
}

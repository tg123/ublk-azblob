//! `ublk-azblob-import` — standalone tool: import a local folder into an Azure
//! Page Blob.
//!
//! The folder is serialized into a single tar archive and written to the page
//! blob as a raw image.  With `--snapshot`, an immutable point-in-time snapshot
//! of the blob is created right after the import completes.
//!
//! ```text
//! ublk-azblob-import [STORAGE/AUTH OPTIONS] --path <DIR> [--size <BYTES>] [--snapshot]
//! ```

use clap::Parser;
use std::path::PathBuf;
use ublk_azblob::cli::{init_tracing, StorageArgs};
use ublk_azblob::import::import_folder;

#[derive(Parser, Debug)]
#[command(
    name = "ublk-azblob-import",
    about = "Import a local folder into an Azure Page Blob (as a tar image)",
    version
)]
struct Cli {
    #[command(flatten)]
    storage: StorageArgs,

    /// Local folder to import.
    #[arg(long)]
    path: PathBuf,

    /// Blob size in bytes (must be a multiple of 512).
    ///
    /// Must be large enough to hold the archive.  When omitted, the blob is
    /// sized to the archive rounded up to the next 512-byte boundary.
    #[arg(long, env = "UBLK_DEV_SIZE")]
    size: Option<u64>,

    /// Create a snapshot of the blob after the import completes.
    #[arg(long)]
    snapshot: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = Cli::parse();
    let backend = cli.storage.build_backend()?;

    if let Some(id) = import_folder(backend, &cli.path, cli.size, cli.snapshot).await? {
        println!("{id}");
    }

    Ok(())
}

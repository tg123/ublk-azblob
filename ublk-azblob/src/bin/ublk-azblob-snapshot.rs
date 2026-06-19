//! `ublk-azblob-snapshot` — standalone tool: create a read-only snapshot of an
//! Azure Page Blob.
//!
//! Prints the snapshot identifier (the snapshot timestamp) on success.
//!
//! ```text
//! ublk-azblob-snapshot [STORAGE/AUTH OPTIONS]
//! ```

use clap::Parser;
use ublk_azblob::cli::{init_tracing, StorageArgs};

#[derive(Parser, Debug)]
#[command(
    name = "ublk-azblob-snapshot",
    about = "Create a read-only snapshot of an Azure Page Blob",
    version
)]
struct Cli {
    #[command(flatten)]
    storage: StorageArgs,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = Cli::parse();
    let backend = cli.storage.build_backend()?;

    let id = backend.snapshot().await?;
    println!("{id}");

    Ok(())
}

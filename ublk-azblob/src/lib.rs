//! `ublk-azblob` library crate.
//!
//! Shared building blocks used by the `ublk-azblob` block-device binary and the
//! standalone tools (`ublk-azblob-import`, `ublk-azblob-snapshot`):
//!
//! * [`backend`] — the `BlobBackend` trait and its implementations (the only
//!   boundary that touches the Azure SDK).
//! * [`auth`] — Azure credential factory (Managed Identity / Shared Key).
//! * [`import`] — import a local folder into the backing blob.
//! * [`ublk_target`] / [`nbd_target`] — the device serving loops.
//! * [`cli`] — shared CLI options and backend/auth construction helpers reused
//!   by every binary so the tools accept the same storage/auth flags.

pub mod auth;
pub mod backend;
pub mod cli;
pub mod import;
pub mod nbd_target;
pub mod ublk_target;

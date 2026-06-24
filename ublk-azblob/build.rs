//! Build script.
//!
//! Generates Rust bindings for the Container Storage Interface (CSI) gRPC
//! service from the vendored `proto/csi/csi.proto`, but **only** when the `csi`
//! Cargo feature is enabled.  `csi` is on by default, so `protoc` is required
//! for a default build; a `--no-default-features` build skips this step and
//! needs no extra tooling.

fn main() {
    // Cargo sets `CARGO_FEATURE_<NAME>` for every enabled feature.
    if std::env::var_os("CARGO_FEATURE_CSI").is_none() {
        return;
    }

    println!("cargo:rerun-if-changed=proto/csi/csi.proto");

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["proto/csi/csi.proto"], &["proto/csi"])
        .expect("compile CSI proto (is `protoc` installed?)");
}

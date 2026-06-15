#!/usr/bin/env bash
# run_e2e.sh — entrypoint for the e2e test-runner container.
#
# Runs the built-in BlobBackend smoke test against Azurite:
#   create → write → read-verify → clear → read-zero-verify
#
# NOTE: The full kernel ublk device path (/dev/ublkbN) requires ublk_drv and
# CAP_SYS_ADMIN, which GitHub-hosted runners do not provide.  This test
# exercises the AzurePageBlobBackend ↔ Azurite path only (no kernel involvement).
# The kernel path is an opt-in documented in DESIGN.md and README.md.

set -euo pipefail

: "${AZURE_STORAGE_ACCOUNT:?AZURE_STORAGE_ACCOUNT must be set}"
: "${AZURE_STORAGE_KEY:?AZURE_STORAGE_KEY must be set}"
: "${AZURE_STORAGE_ENDPOINT:?AZURE_STORAGE_ENDPOINT must be set}"
: "${AZURE_STORAGE_CONTAINER:?AZURE_STORAGE_CONTAINER must be set}"
: "${AZURE_STORAGE_BLOB:?AZURE_STORAGE_BLOB must be set}"
: "${BLOB_SIZE:=4096}"

echo "=== ublk-azblob e2e test ==="
echo "Endpoint : $AZURE_STORAGE_ENDPOINT"
echo "Account  : $AZURE_STORAGE_ACCOUNT"
echo "Container: $AZURE_STORAGE_CONTAINER"
echo "Blob     : $AZURE_STORAGE_BLOB"
echo "Size     : $BLOB_SIZE bytes"
echo ""

echo "--- Running BlobBackend smoke test against Azurite ---"
ublk-azblob test \
  --storage-account   "$AZURE_STORAGE_ACCOUNT" \
  --storage-key       "$AZURE_STORAGE_KEY" \
  --storage-endpoint  "$AZURE_STORAGE_ENDPOINT" \
  --container         "$AZURE_STORAGE_CONTAINER" \
  --blob              "$AZURE_STORAGE_BLOB" \
  --size              "$BLOB_SIZE"

echo ""
echo "=== All e2e assertions passed ==="

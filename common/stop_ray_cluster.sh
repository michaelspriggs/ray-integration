#!/bin/bash

set -euo pipefail

echo "=== Stopping Ray cluster ==="

# --------------------------------------------
# Stop Ray on local node
# --------------------------------------------
echo "Stopping Ray on local node..."
ray stop --force >/dev/null 2>&1 || true

# --------------------------------------------
# Stop Ray on all allocated nodes
# --------------------------------------------
if [[ -f "${LSB_DJOB_HOSTFILE:-}" ]]; then
  echo "Stopping Ray on all cluster nodes..."

  mapfile -t hosts < <(sort "$LSB_DJOB_HOSTFILE" | uniq)

  for host in "${hosts[@]}"; do
    echo "Stopping Ray on $host..."
    blaunch -z "$host" ray stop --force >/dev/null 2>&1 || true
  done
else
  echo "WARNING: LSB_DJOB_HOSTFILE not found. Only local Ray stopped."
fi

# --------------------------------------------
# Cleanup temp dir (optional)
# --------------------------------------------
if [[ -n "${RAY_TMPDIR:-}" && -d "${RAY_TMPDIR}" ]]; then
  echo "Cleaning up temporary directory: $RAY_TMPDIR"
  rm -rf "$RAY_TMPDIR" || true
fi

echo "=== Ray cluster stopped ==="
``

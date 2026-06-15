#!/bin/bash

set -euo pipefail

echo "Stopping Ray runtime on current node..."
ray stop || true

if [[ -n "${RAY_TMPDIR:-}" && -d "${RAY_TMPDIR}" ]]; then
  echo "Ray temporary directory preserved at: ${RAY_TMPDIR}"
fi

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_PREFIX="${ENV_PREFIX:-$HOME/.conda/envs/l2b313}"
CONDA_HOME="${CONDA_HOME:-$HOME/miniconda3}"

source "$CONDA_HOME/etc/profile.d/conda.sh"

export QHOME="${QHOME:-$ROOT_DIR/q}"
export QLIC="${QLIC:-$ROOT_DIR/q}"
export PYKX_Q_EXECUTABLE="${PYKX_Q_EXECUTABLE:-$ROOT_DIR/q/l64/q}"
export L2B_DB_DIR="${L2B_DB_DIR:-/home/rut/l2b_kdb_db}"
export L2B_PERSIST_MS="${L2B_PERSIST_MS:-5000}"
export FLUSH_MS="${FLUSH_MS:-100}"
export L2_LEVELS="${L2_LEVELS:-100}"
export Q_PORT="${Q_PORT:-5010}"
export LD_LIBRARY_PATH="$ENV_PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export L2B_COMPRESS="${L2B_COMPRESS:-1}"
export L2B_COMPRESS_BLOCKLOG2="${L2B_COMPRESS_BLOCKLOG2:-17}"
export L2B_COMPRESS_ALG="${L2B_COMPRESS_ALG:-3}"
export L2B_COMPRESS_LEVEL="${L2B_COMPRESS_LEVEL:-0}"

cd "$ROOT_DIR"
conda run --no-capture-output -p "$ENV_PREFIX" python "$ROOT_DIR/collector.py"

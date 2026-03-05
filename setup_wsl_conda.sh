#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_PREFIX="${ENV_PREFIX:-$HOME/.conda/envs/l2b313}"
CONDA_HOME="${CONDA_HOME:-$HOME/miniconda3}"

if [[ ! -d "$CONDA_HOME" ]]; then
  curl -fsSL https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
  bash /tmp/miniconda.sh -b -p "$CONDA_HOME"
fi

source "$CONDA_HOME/etc/profile.d/conda.sh"
conda config --set always_yes yes --set changeps1 no >/dev/null

if [[ ! -d "$ENV_PREFIX" ]]; then
  conda create -p "$ENV_PREFIX" python=3.13 -y
fi

conda run -p "$ENV_PREFIX" python -m pip install --upgrade pip
conda run -p "$ENV_PREFIX" python -m pip install -r "$ROOT_DIR/requirements.txt"

if [[ ! -x "$ROOT_DIR/q/l64/q" ]]; then
  cat >/tmp/l2b_install_q.py <<'PY'
import pykx as kx
kx.util.install_q(location='__ROOT__/q', date='2024.07.08')
print('installed q')
PY
  sed -i "s|__ROOT__|$ROOT_DIR|g" /tmp/l2b_install_q.py
  conda run -p "$ENV_PREFIX" python /tmp/l2b_install_q.py
fi

cp -f "$ROOT_DIR/kc.lic" "$ROOT_DIR/q/kc.lic"
cp -f "$ROOT_DIR/kc.lic" "$ROOT_DIR/q/k4.lic"

echo "Setup complete"
echo "Run with: ./run.sh"

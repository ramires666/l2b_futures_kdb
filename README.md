# L2B kdb+ Realtime Collector

English is the primary documentation language in this repository.  
Russian version: [`README.ru.md`](README.ru.md)

## Overview
This service collects Binance **USD-M Futures** streams:
- `<symbol>@trade`
- `<symbol>@depth@100ms`

It aggregates data every `100ms` and stores it in kdb+ tables:
- `trades` (all trades)
- `l2` (top-N bid/ask snapshot per window)

## Environment
- Workspace: `/home/rut/l2b_futures_kdb`
- Conda env: `/home/rut/.conda/envs/l2b313`
- q binary: `/home/rut/l2b_futures_kdb/q/l64/q`
- Default DB dir: `/home/rut/l2b_kdb_db`

## Quick Start
```bash
cd /home/rut/l2b_futures_kdb
cp .env.example .env   # put your KDB license string into KDB=
./setup_wsl_conda.sh
./run.sh
```

## Project Layout
- Collector code: `collector.py`
- kdb ingest script: `kdb/ingest.q`
- Symbol list: `coinz.csv`
- Runtime launcher: `run.sh`

## Configuration (env)
- `SYMBOLS_FILE` default: `coinz.csv`
- `SYMBOLS` optional extra symbols (comma-separated)
- `FLUSH_MS` default: `100`
- `L2_LEVELS` default: `100`
- `L2B_DB_DIR` default: `/home/rut/l2b_kdb_db`
- `L2B_PERSIST_MS` default: `5000`
- `Q_PORT` default: `5010`
- `SNAPSHOT_LIMIT` default: `1000`
- `REQUEST_WEIGHT_LIMIT_PER_MIN` default: `2400`
- `RESYNC_LOG_COOLDOWN_SEC` default: `30`
- `L2B_COMPRESS` default: `1` (enabled)
- `L2B_COMPRESS_BLOCKLOG2` default: `17`
- `L2B_COMPRESS_ALG` default: `3` (snappy)
- `L2B_COMPRESS_LEVEL` default: `0`

Example:
```bash
export SYMBOLS="SOLUSDT,BNBUSDT"
export L2_LEVELS=100
export L2B_DB_DIR=/home/rut/l2b_kdb_db
export L2B_COMPRESS_ALG=2   # gzip
./run.sh
```

## Data Files
Data is written into kdb binary chunk files:
- `/home/rut/l2b_kdb_db/trades/YYYY.MM.DD/*.qbin`
- `/home/rut/l2b_kdb_db/l2/YYYY.MM.DD/*.qbin`

## Accessing Data From Other Applications
### 1) Live data over q IPC (recommended)
The running collector starts a q process on `Q_PORT` (default `5010`).
If you use `pykx` only as an IPC client (without embedded q), set:
`PYKX_UNLICENSED=true`.

q client example:
```q
h:hopen `:localhost:5010;
h"count .ingest.trades";
h"5#select from .ingest.trades where sym=`BTCUSDT";
h"10#select from .ingest.l2 where sym=`BTCUSDT,lvl<5";
hclose h;
```

Python (PyKX) example:
```python
import pykx as kx

q = kx.QConnection("localhost", 5010)
trade_count = q("count .ingest.trades").py()
last_trades = q("5#select from .ingest.trades where sym=`BTCUSDT").py()
top5_l2 = q("10#select from .ingest.l2 where sym=`BTCUSDT,lvl<5").py()
q.close()
```

Shell example:
```bash
export PYKX_UNLICENSED=true
python your_client.py
```

### 2) Historical chunks from disk (`.qbin`)
Read chunk files through the same q IPC connection:

```python
import glob
import pykx as kx

q = kx.QConnection("localhost", 5010)
paths = sorted(glob.glob("/home/rut/l2b_kdb_db/trades/2026.03.05/*.qbin"))

for p in paths[:3]:
    rows = q(f'count get `$":{p}"').py()
    print(p, rows)

sample = q(f'2#select sym,px,qty from get `$":{paths[-1]}"').py()
q.close()
```

Tables schema:
- `trades`: `ts, window_ts, sym, trade_id, px, qty, is_bm, evt_ts`
- `l2`: `ts, window_ts, sym, side, lvl, px, qty, evt_ts`

## Health Check
1. Put symbols into `coinz.csv`.
2. Start with `./run.sh`.
3. Verify periodic logs like `flush trades=... l2=...`.
4. Verify new files appear under `/home/rut/l2b_kdb_db`.

## Resume After WSL Restart
```bash
cd /home/rut/l2b_futures_kdb
./run.sh
```

If dependencies changed:
```bash
cd /home/rut/l2b_futures_kdb
./setup_wsl_conda.sh
```

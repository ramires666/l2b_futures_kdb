# L2B kdb+ Realtime Collector

Сервис собирает `trade` и `depth@100ms` из Binance **USD-M Futures**, агрегирует каждые `100ms` и пишет в `kdb+`:
- `trades` (все сделки, без потерь)
- `l2` (snapshot top-N уровней bid/ask на окно)

## Что уже настроено
- отдельная Linux `conda` среда в WSL (`Python 3.13`)
- нативный `q` бинарник (`q/l64/q`)
- лицензия из `kc.lic`
- база по умолчанию внутри WSL: `/home/rut/l2b_kdb_db` (быстрее, чем `/mnt/*`)

## Быстрый старт
```bash
cd /home/rut/l2b_futures_kdb
cp .env.example .env   # и вставь свой ключ в переменную KDB
./setup_wsl_conda.sh
./run.sh
```

## Где что лежит
- Код: `/home/rut/l2b_futures_kdb`
- Conda env: `/home/rut/.conda/envs/l2b313`
- `q` бинарник и лицензия: `/home/rut/l2b_futures_kdb/q`
- Список монет: `/home/rut/l2b_futures_kdb/coinz.csv`
- Данные (kdb chunks): `/home/rut/l2b_kdb_db`

## Конфиг (env)
- `SYMBOLS_FILE` по умолчанию `coinz.csv`
- `SYMBOLS` доп. список через запятую
- `FLUSH_MS` по умолчанию `100`
- `L2_LEVELS` по умолчанию `100`
- `L2B_DB_DIR` по умолчанию `/home/rut/l2b_kdb_db`
- `L2B_PERSIST_MS` по умолчанию `5000`
- `Q_PORT` по умолчанию `5010`
- `L2B_COMPRESS` по умолчанию `1` (включено)
- `L2B_COMPRESS_BLOCKLOG2` по умолчанию `17`
- `L2B_COMPRESS_ALG` по умолчанию `3` (snappy)
- `L2B_COMPRESS_LEVEL` по умолчанию `0`
- `RESYNC_LOG_COOLDOWN_SEC` по умолчанию `30` (не чаще одного warning на символ)

Пример:
```bash
export SYMBOLS="SOLUSDT,BNBUSDT"
export L2_LEVELS=100
export L2B_DB_DIR=/home/rut/l2b_kdb_db
export L2B_COMPRESS_ALG=2   # gzip (если нужен максимум сжатия)
./run.sh
```

## Где файлы
Пишется в бинарных kdb чанках:
- `/home/rut/l2b_kdb_db/trades/YYYY.MM.DD/*.qbin`
- `/home/rut/l2b_kdb_db/l2/YYYY.MM.DD/*.qbin`

## Проверка перед сессией
1. Заполни `coinz.csv` твоим списком монет.
2. Запусти `./run.sh`.
3. Убедись, что в логах идут `flush trades=... l2=...`.
4. Проверь, что в `/home/rut/l2b_kdb_db` появляются новые файлы.

## После рестарта WSL
```bash
cd /home/rut/l2b_futures_kdb
./run.sh
```

Если нужно пересобрать окружение:
```bash
cd /home/rut/l2b_futures_kdb
./setup_wsl_conda.sh
```

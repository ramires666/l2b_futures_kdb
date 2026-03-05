# L2B kdb+ Realtime Collector

Основной язык документации в репозитории: английский.  
English version: [`README.md`](README.md)

## Обзор
Сервис собирает потоки Binance **USD-M Futures**:
- `<symbol>@trade`
- `<symbol>@depth@100ms`

Данные агрегируются каждые `100ms` и пишутся в таблицы kdb+:
- `trades` (все сделки)
- `l2` (top-N bid/ask snapshot на окно)

## Окружение
- Рабочая папка: `/home/rut/l2b_futures_kdb`
- Conda env: `/home/rut/.conda/envs/l2b313`
- Бинарник q: `/home/rut/l2b_futures_kdb/q/l64/q`
- База по умолчанию: `/home/rut/l2b_kdb_db`

## Быстрый старт
```bash
cd /home/rut/l2b_futures_kdb
cp .env.example .env   # вставьте строку лицензии в KDB=
./setup_wsl_conda.sh
./run.sh
```

## Структура проекта
- Коллектор: `collector.py`
- Скрипт ingest для kdb: `kdb/ingest.q`
- Список символов: `coinz.csv`
- Скрипт запуска: `run.sh`

## Конфигурация (env)
- `SYMBOLS_FILE` по умолчанию: `coinz.csv`
- `SYMBOLS` доп. символы через запятую
- `FLUSH_MS` по умолчанию: `100`
- `L2_LEVELS` по умолчанию: `100`
- `L2B_DB_DIR` по умолчанию: `/home/rut/l2b_kdb_db`
- `L2B_PERSIST_MS` по умолчанию: `5000`
- `Q_PORT` по умолчанию: `5010`
- `SNAPSHOT_LIMIT` по умолчанию: `1000`
- `REQUEST_WEIGHT_LIMIT_PER_MIN` по умолчанию: `2400`
- `RESYNC_LOG_COOLDOWN_SEC` по умолчанию: `30`
- `L2B_COMPRESS` по умолчанию: `1` (включено)
- `L2B_COMPRESS_BLOCKLOG2` по умолчанию: `17`
- `L2B_COMPRESS_ALG` по умолчанию: `3` (snappy)
- `L2B_COMPRESS_LEVEL` по умолчанию: `0`

Пример:
```bash
export SYMBOLS="SOLUSDT,BNBUSDT"
export L2_LEVELS=100
export L2B_DB_DIR=/home/rut/l2b_kdb_db
export L2B_COMPRESS_ALG=2   # gzip
./run.sh
```

## Файлы данных
Данные пишутся в бинарные kdb чанки:
- `/home/rut/l2b_kdb_db/trades/YYYY.MM.DD/*.qbin`
- `/home/rut/l2b_kdb_db/l2/YYYY.MM.DD/*.qbin`

## Проверка работы
1. Заполните `coinz.csv`.
2. Запустите `./run.sh`.
3. Убедитесь, что в логах есть `flush trades=... l2=...`.
4. Убедитесь, что появляются новые файлы в `/home/rut/l2b_kdb_db`.

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

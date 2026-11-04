# FinDB

A small, local [QuestDB](https://questdb.com) time-series database for financial data
(Vietnam and crypto market data, Vietnam financial statements), plus the Python
scripts that fill it. Everything is kept **simple and short**.

## Setup

### 1. QuestDB

QuestDB and a Java runtime go in `.runtime/` (not committed), data and config in `data/` (not committed).

    .runtime/questdb/   # QuestDB 10.1.0 "no-jre" binary (questdb.sh, questdb.jar)
    .runtime/jdk/       # Java 25 (macOS layout: jdk/Contents/Home)

Create `data/conf/server.conf`:

```properties
# local only
http.net.bind.to=127.0.0.1:9000
pg.net.bind.to=127.0.0.1:8812
http.min.net.bind.to=127.0.0.1:9003
line.tcp.enabled=false

# login (pick your own)
http.user=<user>
http.password=<password>
pg.user=<user>
pg.password=<password>

# tables/columns are created with DDL, never implicitly by ingest
line.auto.create.new.tables=false
line.auto.create.new.columns=false

query.timeout=30s
ram.usage.limit.percent=60
telemetry.enabled=false
```

Start / stop:

    ./db.sh start | stop | status

- Web console: http://127.0.0.1:9000
- Postgres wire: `127.0.0.1:8812`, database `qdb`
- Does not auto-start after reboot.

### 2. Python env (conda)

    conda env create -f environment.yml                    # once
    conda env update -n findb -f environment.yml --prune   # after editing deps

Add new deps to `environment.yml` (conda-forge first, `pip:` section if not on conda).

### 3. Secrets (`.env`, not committed)

```sh
QDB_USER=<user>          # same as server.conf
QDB_PASSWORD=<password>
DNSE_API_KEY=...         # only for streams/dnse.py
DNSE_API_SECRET=...
```

## Layout

    db.sh            start/stop/status QuestDB
    environment.yml  conda env `findb`
    qdb.py           shared helpers: sql(query), write(table, df, symbols, at)
    tables/          batch ingest, ONE FILE = ONE TABLE (exception: vn_fs.py)
    run_all.sh       runs every tables/*.py (for cron)
    streams/         long-running WebSocket recorders (not run by run_all.sh)

## Querying

From Python, through `qdb.py`:

```python
from qdb import sql
df = sql("SELECT * FROM vn_eod WHERE timestamp > dateadd('d', -30, now())")
```

Or over HTTP, e.g. Parquet export:

```python
import io, requests, pandas as pd
r = requests.get("http://127.0.0.1:9000/exp", auth=(USER, PASSWORD),
                 params={"query": "SELECT * FROM vn_eod", "fmt": "parquet"})
df = pd.read_parquet(io.BytesIO(r.content))
```

## Tables

### Batch (`tables/`)

Run one table from the repo root, as a module so `qdb` imports work:

    conda activate findb
    python -m tables.vn_eod

Run all: `./run_all.sh`

| File | Table(s) |
|---|---|
| `vn_symbols.py` | `vn_symbols` – Vietnam listed symbols |
| `vn_eod.py` | `vn_eod` – Vietnam daily bars (VND, shares) |
| `vn_fs.py` | `vn_<template>_<report>` + `vn_fs_columns` – financial statements |

**Financial statements (`vn_fs.py`).** Vietcap statements go into 12 tables
`vn_<template>_<report>` (template: corporate, bank, securities, insurance;
report: income, balance, cashflow), one row per (symbol, period); `quarter` 1-4,
`0` = annual; `timestamp` = period end. One file fills all 12 because a symbol's
template is only known after fetching it.

Column names come from `tables/vn_fs_columns.csv` (Vietcap field code → readable
`column_name`, with EN/VI titles; also loaded into `vn_fs_columns`). That CSV is the
schema: to add a column, add a row; the next run `ALTER TABLE ... ADD COLUMN`s it.
Fields with data but missing from the CSV are listed at the end of each run.

### Streams (`streams/`)

Run in a terminal and leave it open (needs `./db.sh start` first).
`timestamp` is always exchange time. Each logs connects, gaps, errors and one
status line per minute.

**`streams/binance.py`** – Binance spot (`SYMBOLS` in the file, e.g. BTCUSDT, ETHUSDT)
into `crypto_trades` (every trade, `side` = taker side) and `crypto_orderbook`
(top 5 bids/asks, ~10/s). Missed trades (disconnects, restarts) are refilled via REST
from the last `trade_id` in the DB. Order book snapshots missed while stopped are lost.

**`streams/dnse.py`** – Vietnam via DNSE Lightspeed (`SYMBOLS` in the file, e.g.
VN30F1M, VIC, HPG; futures aliases like VN30F1M resolve to the current contract at
each connect) into `vn_trades` and `vn_orderbook` (HOSE: 3 levels, futures/HNX/UPCOM: 10).
Prices/quantities are in VND and shares like `vn_eod` (futures: index points and
contracts). Trades have no id: `total_volume` (cumulative for the day) is the key and
is used to detect and refill gaps via REST. Needs `DNSE_API_KEY` / `DNSE_API_SECRET`.

    conda activate findb
    caffeinate -i python -m streams.binance          # Ctrl+C to stop; caffeinate keeps a Mac awake
    caffeinate -i python -m streams.dnse             # start before 9:00, stop after 15:00 (VN time)
    python -m streams.binance --dry-run 60 OUT_DIR   # test: no DB, writes parquet to look at

## Adding a table

Create `tables/<table_name>.py`, file name = table name:

```python
import pandas as pd
from qdb import sql, write

TABLE = "stock_prices"
SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  timestamp TIMESTAMP, ticker SYMBOL, close DOUBLE, volume LONG
) TIMESTAMP(timestamp) PARTITION BY MONTH WAL DEDUP UPSERT KEYS(timestamp, ticker);
"""

def fetch() -> pd.DataFrame:
    ...  # must return a DataFrame with a tz-aware `timestamp` column

if __name__ == "__main__":
    sql(SCHEMA)
    write(TABLE, fetch(), symbols=["ticker"], at="timestamp")
```

## Rules

- Every table has a designated `timestamp` and `DEDUP UPSERT KEYS(timestamp, <natural key>)`.
  Re-ingesting overlapping data is then safe (last write wins), so each run just
  re-fetches a recent window; no state tracking needed.
- Repeated categorical strings (ticker, exchange) → `SYMBOL`.
- Partition: `DAY` for intraday/tick data, `MONTH` for daily bars, `YEAR` for slow data.
- Schema changes: run `ALTER TABLE` in the web console, then update `SCHEMA` in the
  file to match. No migration framework.
- Connect only through `qdb.py`; credentials live only in `.env`.

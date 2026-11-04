# FinDB

Local QuestDB for financial data. Keep everything **simple and short**.

## Server

QuestDB 10.1.0 + Java 25 live in `.runtime/` (not committed). Data in `data/`.

    ./db.sh start | stop | status

- Web console: http://127.0.0.1:9000 (login `51labs` / `m1m2`)
- Postgres wire: 127.0.0.1:8812, db `qdb`, same login
- Config: `data/conf/server.conf` (bound to 127.0.0.1 only; ILP/TCP 9009 disabled;
  ILP auto-create of tables/columns is off, so create them with DDL first)
- Does not auto-start after reboot.

## Sharing with students (Cloudflare quick tunnel)

`cloudflared` lives in `.runtime/cloudflared`. Only port 9000 (web console + REST) is exposed;
ingest keeps writing to 127.0.0.1 and needs no change.

    ./tunnel.sh      # leave the tab open; prints https://<random>.trycloudflare.com, Ctrl+C to stop

The URL changes on every restart, so send the new one. Students log in with the same user.
Python (from anywhere):

```python
import io, requests, pandas as pd
r = requests.get(URL + "/exp", auth=("51labs", "m1m2"),
                 params={"query": "SELECT * FROM vn_eod WHERE timestamp > dateadd('d', -30, now())", "fmt": "parquet"})
df = pd.read_parquet(io.BytesIO(r.content))
# or: questdb.connect(f"wss::addr={host}:443;username=51labs;password=m1m2;").query(q).to_pandas()
```

Limits in `server.conf` (Cloudflare cuts requests at 100s): `query.timeout=30s`,
`/exp` 90s, CSV/JSON capped at 1M rows (Parquet and `wss` are not capped), RAM 60%.

## Python env (conda, not uv)

    conda env create -f environment.yml              # once
    conda env update -n findb -f environment.yml --prune   # after editing deps

Add new deps to `environment.yml` (conda-forge first, `pip:` section if not on conda).

## Layout

    db.sh            start/stop/status QuestDB
    tunnel.sh        expose port 9000 via a Cloudflare quick tunnel
    environment.yml  conda env `findb`
    qdb.py           shared helpers: sql(query), write(table, df, symbols, at)
    tables/          ONE FILE = ONE TABLE (exception: vn_fs.py, see below)
    run_all.sh       runs every tables/*.py (for cron)
    streams/         long-running WebSocket recorders (NOT run by run_all.sh)

## Streams

`streams/binance.py` records Binance spot (`SYMBOLS` in the file, now BTCUSDT, ETHUSDT)
into `crypto_trades` (every trade, `side` = taker side) and `crypto_orderbook`
(top 5 bids/asks, ~10/s). `timestamp` is always exchange time.

Run it in a terminal tab and leave it open (needs `./db.sh start` first):

    conda activate findb
    caffeinate -i python -m streams.binance          # Ctrl+C to stop; caffeinate keeps the Mac awake
    python -m streams.binance --dry-run 60 OUT_DIR   # test: no DB, writes parquet to look at

It logs connects, gaps, errors and one status line per minute.
Missed trades (disconnects, restarts) are refilled via REST from the last `trade_id`
in the DB. Order book snapshots missed while it is stopped are lost.

`streams/dnse.py` does the same for Vietnam via DNSE Lightspeed (`SYMBOLS` in the file,
now VN30F1M, VIC, HPG; futures aliases like VN30F1M resolve to the current contract,
e.g. `41I1GB000`, at each connect) into `vn_trades` and `vn_orderbook` (HOSE: 3 levels,
futures/HNX/UPCOM: 10). Prices/quantities are converted to VND and shares like `vn_eod`
(futures: index points and contracts). Trades have no id: `total_volume` (cumulative for
the day) is the key and is used to detect and refill gaps via REST. Needs
`DNSE_API_KEY` / `DNSE_API_SECRET` in `.env` (gitignored).

    caffeinate -i python -m streams.dnse             # start before 9:00, Ctrl+C after 15:00
    python -m streams.dnse --dry-run 60 OUT_DIR

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

## Financial statements (`tables/vn_fs.py`)

Vietcap statements go into 12 tables `vn_<template>_<report>`
(template: corporate, bank, securities, insurance; report: income, balance, cashflow),
one row per (symbol, period); `quarter` 1-4, `0` = annual; `timestamp` = period end.
One file fills all 12 because a symbol's template is only known after fetching it.

Column names come from `tables/vn_fs_columns.csv` (Vietcap field code → readable
`column_name`, with EN/VI titles; also loaded into table `vn_fs_columns`). That CSV
is the schema: to add a column, add a row; the next run `ALTER TABLE ... ADD COLUMN`s it.
Fields with data but missing from the CSV are listed at the end of each run.

Run one table (from repo root, as a module so `qdb` imports work):

    conda activate findb
    python -m tables.stock_prices

Run all: `./run_all.sh`

## Rules

- Every table has a designated `timestamp` and `DEDUP UPSERT KEYS(timestamp, <natural key>)`.
  Re-ingesting overlapping data is then safe (last write wins), so each run
  just re-fetches a recent window, no state tracking needed.
- Repeated categorical strings (ticker, exchange) → `SYMBOL`.
- Partition: `DAY` for intraday/tick data, `MONTH` for daily bars, `YEAR` for slow data.
- Schema changes: run `ALTER TABLE` in the web console, then update `SCHEMA`
  in the file to match. No migration framework.
- Connect only through `qdb.py`; don't duplicate credentials.

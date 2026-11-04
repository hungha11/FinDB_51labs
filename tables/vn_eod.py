import time

import pandas as pd
import requests

from qdb import sql, write

TABLE = "vn_eod"
SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  timestamp TIMESTAMP, symbol SYMBOL CAPACITY 4096 INDEX,
  open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume LONG
) TIMESTAMP(timestamp) PARTITION BY MONTH WAL DEDUP UPSERT KEYS(timestamp, symbol);
"""

URL = "https://api.vietstock.vn/tvnew/history"
HEADERS = {"Origin": "https://stockchart.vietstock.vn",
           "Referer": "https://stockchart.vietstock.vn/", "User-Agent": "Mozilla/5.0"}
START = pd.Timestamp("2000-01-01", tz="UTC")  # backfill from here
CHUNK = pd.DateOffset(years=10)  # Vietstock returns max 5000 bars per call
RECENT = pd.Timedelta(days=30)  # re-fetch window for symbols already in DB
BATCH = 100  # symbols per write


def get(params):
    """GET with one retry on network errors."""
    for attempt in range(2):
        try:
            r = requests.get(URL, params=params, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            if attempt:
                raise
            time.sleep(2)


def load(symbol, start, end):
    """Daily adjusted OHLCV for [start, end] dates (UTC); None if no data."""
    p = get({"symbol": symbol, "resolution": "1D", "from": int(start.timestamp()),
             "to": int((end + pd.Timedelta(days=1)).timestamp()) - 1})
    if p.get("s") != "ok" or not p.get("t"):
        return None
    return pd.DataFrame({
        "timestamp": pd.to_datetime(p["t"], unit="s", utc=True).normalize(),
        "symbol": symbol,
        "open": pd.Series(p["o"], dtype="float64"),
        "high": pd.Series(p["h"], dtype="float64"),
        "low": pd.Series(p["l"], dtype="float64"),
        "close": pd.Series(p["c"], dtype="float64"),
        "volume": pd.Series(p["v"], dtype="int64"),
    })


def fetch(symbol, start, end):
    """Like load(), split into CHUNK-sized calls."""
    parts = []
    while start <= end:
        stop = min(start + CHUNK - pd.Timedelta(days=1), end)
        parts.append(load(symbol, start, stop))
        start = stop + pd.Timedelta(days=1)
    parts = [x for x in parts if x is not None]
    return pd.concat(parts, ignore_index=True) if parts else None


if __name__ == "__main__":
    sql(SCHEMA)
    symbols = sql("SELECT symbol FROM vn_symbols LATEST ON timestamp PARTITION BY symbol")["symbol"].sort_values()
    last = sql(f"SELECT symbol, max(timestamp) last_ts FROM {TABLE}").set_index("symbol")["last_ts"]
    last = pd.to_datetime(last).dt.tz_localize("UTC")
    today = pd.Timestamp(pd.Timestamp.now(tz="Asia/Ho_Chi_Minh").date(), tz="UTC")

    buf, rows, failed = [], 0, []
    for i, s in enumerate(symbols, 1):
        start = last[s] - RECENT if s in last.index else START
        try:
            df = fetch(s, start, today)
        except Exception as e:
            print(f"!! {s}: {e}")
            df = None
        if df is None:
            failed.append(s)
        else:
            buf.append(df)
        if buf and (len(buf) >= BATCH or i == len(symbols)):
            out = pd.concat(buf, ignore_index=True)
            out["symbol"] = out["symbol"].astype("category")  # client needs one contiguous buffer
            write(TABLE, out, symbols=["symbol"], at="timestamp")
            rows += len(out)
            buf = []
            print(f"{i}/{len(symbols)} symbols, {rows} rows")

    print(f"done: {rows} rows, {len(symbols) - len(failed)} ok, {len(failed)} no data/failed: {failed}")

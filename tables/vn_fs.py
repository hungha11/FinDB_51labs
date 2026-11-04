"""Vietcap financial statements -> 12 tables vn_<template>_<report> + vn_fs_columns.

Exception to one-file-one-table: a symbol's template (corporate/bank/securities/
insurance) is only known after fetching its income statement, so one run fills all.
Column names come from vn_fs_columns.csv (the schema source of truth).
"""
import time
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd
import requests

from qdb import sql, write

COLUMNS = pd.read_csv(Path(__file__).with_name("vn_fs_columns.csv"))
REPORTS = {"income": "INCOME_STATEMENT", "balance": "BALANCE_SHEET", "cashflow": "CASH_FLOW"}
TEMPLATES = {"isb": "bank", "iss": "securities", "isi": "insurance"}  # income prefix -> template; else corporate
META = {"organCode", "ticker", "createDate", "updateDate", "yearReport", "lengthReport", "publicDate"}

URL = "https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/{}/financial-statement"
HEADERS = {"Accept": "application/json", "Origin": "https://trading.vietcap.com.vn",
           "Referer": "https://trading.vietcap.com.vn/", "User-Agent": "Mozilla/5.0"}
BATCH = 100  # symbols per write

DICT_TABLE = "vn_fs_columns"
DICT_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {DICT_TABLE} (
  timestamp TIMESTAMP, template SYMBOL, report SYMBOL, field SYMBOL,
  column_name VARCHAR, title_en VARCHAR, title_vi VARCHAR
) TIMESTAMP(timestamp) PARTITION BY YEAR WAL DEDUP UPSERT KEYS(timestamp, template, report, field);
"""


def table(template, report):
    return f"vn_{template}_{report}"


def schema(template, report, cols):
    metrics = ", ".join(f'"{c}" DOUBLE' for c in cols)
    return f"""
CREATE TABLE IF NOT EXISTS {table(template, report)} (
  timestamp TIMESTAMP, symbol SYMBOL CAPACITY 4096, year INT, quarter INT,
  public_date TIMESTAMP, update_date TIMESTAMP, {metrics}
) TIMESTAMP(timestamp) PARTITION BY YEAR WAL DEDUP UPSERT KEYS(timestamp, symbol, quarter);
"""


def ensure_schema():
    """Create missing tables; add (never drop/rename) columns new in the CSV."""
    for (t, r), g in COLUMNS.groupby(["template", "report"]):
        name = table(t, r)
        sql(schema(t, r, g["column_name"]))
        have = set(sql(f"SELECT \"column\" FROM table_columns('{name}')")["column"])
        for c in g["column_name"]:
            if c not in have:
                print(f"+ {name}.{c}")
                sql(f'ALTER TABLE {name} ADD COLUMN "{c}" DOUBLE')
    sql(DICT_SCHEMA)
    d = COLUMNS.assign(timestamp=pd.Timestamp(0, tz="UTC"))  # static dictionary: fixed ts keeps DEDUP idempotent
    for c in ["template", "report", "field"]:
        d[c] = d[c].astype("category")
    write(DICT_TABLE, d, symbols=["template", "report", "field"], at="timestamp")


def get(symbol, section):
    """Vietcap often hangs instead of failing: short timeout, several retries."""
    for attempt in range(5):
        try:
            r = requests.get(URL.format(symbol), params={"section": section}, headers=HEADERS, timeout=10)
            r.raise_for_status()
            p = r.json()
            if p.get("successful") is False or p.get("code", 0) != 0:
                raise RuntimeError(p.get("msg", "Vietcap API error"))
            return p["data"]
        except (requests.RequestException, ValueError):
            if attempt == 4:
                raise
            time.sleep(1 + attempt)


def records(data):
    """[(quarter, record)]; quarter 0 = annual."""
    return [(0, r) for r in data.get("years") or []] + [(r["lengthReport"], r) for r in data.get("quarters") or []]


def template_of(data):
    keys = {k for _, r in records(data) for k, v in r.items() if k not in META and v}
    return next((t for p, t in TEMPLATES.items() if any(k.startswith(p) for k in keys)), "corporate")


def frame(symbol, data, cols, unknown):
    """One row per period with the table's columns; counts non-null fields missing from the CSV."""
    rows = []
    for q, r in records(data):
        y = r["yearReport"]
        end = pd.Timestamp(y, 12, 31) if q == 0 else pd.Timestamp(y, 3 * q, 1) + pd.offsets.MonthEnd(0)
        rows.append({"timestamp": end, "symbol": symbol, "year": y, "quarter": q,
                     "public_date": r.get("publicDate"), "update_date": r.get("updateDate"),
                     **{c: r.get(f) for f, c in cols.items()}})
        unknown.update(k for k, v in r.items() if k not in META and k not in cols and v)
    df = pd.DataFrame(rows, columns=["timestamp", "symbol", "year", "quarter", "public_date", "update_date", *cols.values()])
    df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
    df["public_date"] = pd.to_datetime(df["public_date"], format="ISO8601").dt.normalize().dt.tz_localize("UTC")  # a date, like vn_eod
    df["update_date"] = pd.to_datetime(df["update_date"], format="ISO8601").dt.tz_localize("Asia/Ho_Chi_Minh").dt.tz_convert("UTC")
    df[list(cols.values())] = df[list(cols.values())].astype("float64")
    return df


def fetch(symbol, unknown):
    """{table_name: DataFrame} for one symbol; {} if Vietcap has no statements."""
    income = get(symbol, REPORTS["income"])
    if not records(income):
        return {}
    t = template_of(income)
    out = {}
    for r, section in REPORTS.items():
        data = income if r == "income" else get(symbol, section)
        g = COLUMNS[(COLUMNS.template == t) & (COLUMNS.report == r)]
        if records(data):
            out[table(t, r)] = frame(symbol, data, dict(zip(g["field"], g["column_name"])), unknown[table(t, r)])
    return out


def flush(buf):
    for name, parts in buf.items():
        df = pd.concat(parts, ignore_index=True)
        df["symbol"] = df["symbol"].astype("category")  # client needs one contiguous buffer
        write(name, df, symbols=["symbol"], at="timestamp")
    buf.clear()


if __name__ == "__main__":
    ensure_schema()
    symbols = sql("SELECT symbol FROM vn_symbols LATEST ON timestamp PARTITION BY symbol")["symbol"].sort_values()
    buf, unknown, failed, templates = {}, defaultdict(Counter), [], Counter()
    for i, s in enumerate(symbols, 1):
        try:
            frames = fetch(s, unknown)
        except Exception as e:
            print(f"!! {s}: {e}")
            frames = {}
        if frames:
            templates[next(iter(frames)).split("_")[1]] += 1
        else:
            failed.append(s)
        for name, df in frames.items():
            buf.setdefault(name, []).append(df)
        if i % BATCH == 0 or i == len(symbols):
            flush(buf)
            print(f"{i}/{len(symbols)} symbols")
    print(f"done: {len(symbols) - len(failed)} ok {dict(templates)}, {len(failed)} no data/failed: {failed}")
    for name, c in unknown.items():
        if c:
            print(f"unknown fields in {name} (add to vn_fs_columns.csv): {dict(c.most_common(10))}")

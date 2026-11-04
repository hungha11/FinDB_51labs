"""Binance spot WebSocket recorder -> crypto_trades + crypto_orderbook (top 5). Runs until Ctrl+C.

    python -m streams.binance                       # record into QuestDB
    python -m streams.binance --dry-run 60 OUT_DIR  # no DB: save parquet files to look at
"""
import asyncio
import contextlib
import json
import sys
import time

import pandas as pd
import requests
import websockets

from qdb import sql, write

SYMBOLS = ["btcusdt", "ethusdt"]
MARKET = "spot"
LEVELS = 5
FLUSH = 1.0  # seconds between DB writes
ROTATE = 23 * 3600  # Binance cuts connections at 24h: open a new one before that...
OVERLAP = 10  # ...and keep both for a few seconds (duplicates are dropped)

SCHEMAS = [
    """
CREATE TABLE IF NOT EXISTS crypto_trades (
  timestamp TIMESTAMP, exchange SYMBOL, market SYMBOL, symbol SYMBOL,
  trade_id LONG, price DOUBLE, qty DOUBLE, side SYMBOL
) TIMESTAMP(timestamp) PARTITION BY DAY WAL
  DEDUP UPSERT KEYS(timestamp, exchange, market, symbol, trade_id);
""",
    f"""
CREATE TABLE IF NOT EXISTS crypto_orderbook (
  timestamp TIMESTAMP, exchange SYMBOL, market SYMBOL, symbol SYMBOL,
  {", ".join(f"{s}{i}_{f} DOUBLE" for s in ("bid", "ask") for i in range(1, LEVELS + 1) for f in ("price", "qty"))}
) TIMESTAMP(timestamp) PARTITION BY DAY WAL
  DEDUP UPSERT KEYS(timestamp, exchange, market, symbol);
""",
]
TRADE_COLS = ["timestamp", "symbol", "trade_id", "price", "qty", "side"]
BOOK_COLS = ["timestamp", "symbol"] + [
    f"{s}{i}_{f}" for s in ("bid", "ask") for i in range(1, LEVELS + 1) for f in ("price", "qty")]

# times in microseconds; depth5 has no timestamp, so it takes E from the diff event with the same update id
URL = "wss://stream.binance.com:9443/stream?timeUnit=MICROSECOND&streams=" + "/".join(
    f"{s}@trade/{s}@depth{LEVELS}@100ms/{s}@depth@100ms" for s in SYMBOLS)

trades, books = [], []  # rows waiting for the next flush
last_trade, last_book = {}, {}  # symbol -> last id seen; drops overlap duplicates
diff_time, pending = {}, {}  # (symbol, update id) -> event time / depth5 waiting for it


def log(*a):
    print(pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S"), *a, flush=True)


def trim(d, n=1000):
    while len(d) > n:
        del d[next(iter(d))]


def trade_row(symbol, ts, tid, price, qty, buyer_maker):
    # buyer is maker -> the seller hit the bid -> aggressor side is sell
    return ts, symbol, tid, float(price), float(qty), "sell" if buyer_maker else "buy"


def fill_gap(symbol, first, last):
    """Refill missed trades first..last via REST (runs in a thread; paced to stay far under rate limits)."""
    log(f"gap {symbol} trades {first}..{last} ({last - first + 1}), refilling")
    try:
        while first <= last:
            time.sleep(0.5)
            r = requests.get("https://api.binance.com/api/v3/historicalTrades",
                             params={"symbol": symbol, "fromId": first, "limit": 1000},
                             headers={"X-MBX-TIME-UNIT": "MICROSECOND"}, timeout=10)
            r.raise_for_status()
            got = [x for x in r.json() if x["id"] <= last]
            if not got:
                break
            trades.extend(trade_row(symbol, x["time"], x["id"], x["price"], x["qty"], x["isBuyerMaker"])
                          for x in got)
            first = got[-1]["id"] + 1
        log(f"gap {symbol} refilled")
    except requests.RequestException as e:
        log(f"!! gap fill {symbol} stopped at {first}: {e!r}")


def add_trade(d):
    symbol, t = d["s"], d["t"]
    last = last_trade.get(symbol)
    if last is not None and t <= last:
        return  # already seen (overlapping connections)
    if last is not None and t > last + 1:
        asyncio.get_running_loop().run_in_executor(None, fill_gap, symbol, last + 1, t - 1)
    last_trade[symbol] = t
    trades.append(trade_row(symbol, d["T"], t, d["p"], d["q"], d["m"]))


def add_book(symbol, uid, ts, d):
    if uid <= last_book.get(symbol, -1):
        return
    last_book[symbol] = uid
    row = [ts, symbol]
    for side in (d["bids"][:LEVELS], d["asks"][:LEVELS]):
        row += [float(x) for level in side for x in level] + [None] * 2 * (LEVELS - len(side))
    books.append(row)


def handle(stream, d):
    symbol, kind = stream.split("@", 1)
    symbol = symbol.upper()
    if kind == "trade":
        add_trade(d)
    elif kind == "depth@100ms":  # diff: only its time is used
        diff_time[(symbol, d["u"])] = d["E"]
        trim(diff_time)
        p = pending.pop((symbol, d["u"]), None)
        if p:
            add_book(symbol, d["u"], d["E"], p)
    else:  # depth5
        u = d["lastUpdateId"]
        if (symbol, u) in diff_time:
            add_book(symbol, u, diff_time[(symbol, u)], d)
        else:
            pending[(symbol, u)] = d
            trim(pending)


async def consume():
    async with websockets.connect(URL, max_size=None) as ws:
        log("connected")
        async for raw in ws:
            m = json.loads(raw)
            if m["data"].get("e") == "serverShutdown":
                return
            handle(m["stream"], m["data"])


async def keep():
    """Always one live connection; rotated before 24h with a short overlap."""
    old = cur = None
    try:
        while True:
            cur = asyncio.create_task(consume())
            if old:
                await asyncio.sleep(OVERLAP)
                old.cancel()
            done, _ = await asyncio.wait({cur}, timeout=ROTATE)
            old = None if done else cur
            if done:
                log(f"disconnected ({cur.exception()!r}), reconnecting")
                await asyncio.sleep(1)
    finally:
        for t in (old, cur):
            if t:
                t.cancel()


def frame(rows, cols):
    df = pd.DataFrame(rows, columns=cols)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="us", utc=True)
    df.insert(1, "exchange", "binance")
    df.insert(2, "market", MARKET)
    for c in ("exchange", "market", "symbol", "side"):
        if c in df:
            df[c] = df[c].astype("category")  # -> SYMBOL
    return df


written = {"crypto_trades": 0, "crypto_orderbook": 0}  # rows since the last status line


async def flush(sink):
    for table, rows, cols in (("crypto_trades", trades, TRADE_COLS), ("crypto_orderbook", books, BOOK_COLS)):
        batch = rows[:]
        if not batch:
            continue
        try:
            await asyncio.to_thread(sink, table, frame(batch, cols))
            del rows[:len(batch)]
            written[table] += len(batch)
        except Exception as e:
            log(f"!! write {table} ({len(batch)} rows kept for retry): {e!r}")


async def flusher(sink):
    n = 0
    while True:
        await asyncio.sleep(FLUSH)
        await flush(sink)
        n += 1
        if n % 60 == 0:  # status line about once a minute
            log(f"last minute: {written['crypto_trades']} trades, {written['crypto_orderbook']} books written")
            written.update(dict.fromkeys(written, 0))


async def run(sink, seconds=None):
    tasks = [asyncio.create_task(keep()), asyncio.create_task(flusher(sink))]
    try:
        await asyncio.wait(tasks, timeout=seconds)
    finally:  # also on Ctrl+C: write what is buffered
        for t in tasks:
            t.cancel()
        await flush(sink)
        log("stopped")


if __name__ == "__main__":
    if sys.argv[1:2] == ["--dry-run"]:
        seconds, out, got = float(sys.argv[2]), sys.argv[3], {}
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(run(lambda table, df: got.setdefault(table, []).append(df), seconds))
        for table, dfs in got.items():
            df = pd.concat(dfs, ignore_index=True)
            df.to_parquet(f"{out}/{table}.parquet")
            log(f"{table}: {len(df)} rows -> {out}/{table}.parquet")
    else:
        for s in SCHEMAS:
            sql(s)
        # resume: trades missed while we were down get refilled on the first live trade
        seen = sql(f"SELECT symbol, max(trade_id) id FROM crypto_trades "
                   f"WHERE exchange = 'binance' AND market = '{MARKET}'")
        last_trade.update((s, int(i)) for s, i in zip(seen["symbol"], seen["id"]))
        log(f"recording {', '.join(SYMBOLS)}, resume from {last_trade}")
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(run(lambda table, df: write(table, df, at="timestamp")))

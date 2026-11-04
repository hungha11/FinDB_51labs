"""DNSE Lightspeed WebSocket recorder -> vn_trades + vn_orderbook. Runs until Ctrl+C.

    python -m streams.dnse                       # record into QuestDB
    python -m streams.dnse --dry-run 60 OUT_DIR  # no DB: save parquet files to look at

Needs DNSE_API_KEY and DNSE_API_SECRET in .env at the repo root.
"""
import asyncio
import base64
import contextlib
import hashlib
import hmac
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests
import websockets

from qdb import sql, write

SYMBOLS = ["VN30F1M", "VIC", "HPG"]  # futures aliases (VN30F1M, VN30F2M...) -> current contract at connect
BOARD = "G1"  # round lots
LEVELS = 10  # HOSE sends 3 levels, HNX/UPCOM/futures 10; missing levels stay null
FLUSH = 1.0  # seconds between DB writes
ROTATE = 7.5 * 3600  # DNSE cuts connections at 8h: open a new one before that...
OVERLAP = 10  # ...and keep both for a few seconds (duplicates are dropped)
HEARTBEAT = 25  # send a ping when nothing arrived for this long

WS_URL = "wss://ws-openapi.dnse.com.vn/v1/stream?encoding=json"
REST_URL = "https://openapi.dnse.com.vn"
EXCHANGES = {"STO": "HOSE", "STX": "HNX", "UPX": "UPCOM", "DVX": "DERIVATIVES"}
SESSIONS = {"10": "ATO", "40": "continuous", "30": "ATC", "80": "PCA", "99": "closed"}
SIDES = {"BUY": "buy", "SELL": "sell"}  # aggressor side, computed by DNSE; UNSPECIFIED -> null
# stocks come in thousand VND and 10-share units -> VND and shares (as in vn_eod); futures: points, contracts
PRICE_MULT = {"DVX": 1}
QTY_MULT = {"DVX": 1}

SCHEMAS = [
    """
CREATE TABLE IF NOT EXISTS vn_trades (
  timestamp TIMESTAMP, exchange SYMBOL, symbol SYMBOL,
  price DOUBLE, qty LONG, side SYMBOL, total_volume LONG, session SYMBOL
) TIMESTAMP(timestamp) PARTITION BY DAY WAL
  DEDUP UPSERT KEYS(timestamp, symbol, total_volume);
""",
    f"""
CREATE TABLE IF NOT EXISTS vn_orderbook (
  timestamp TIMESTAMP, exchange SYMBOL, symbol SYMBOL,
  {", ".join(f"{s}{i}_{f} DOUBLE" for s in ("bid", "ask") for i in range(1, LEVELS + 1) for f in ("price", "qty"))}
) TIMESTAMP(timestamp) PARTITION BY DAY WAL
  DEDUP UPSERT KEYS(timestamp, symbol);
""",
]
TRADE_COLS = ["timestamp", "exchange", "symbol", "price", "qty", "side", "total_volume", "session"]
BOOK_COLS = ["timestamp", "exchange", "symbol"] + [
    f"{s}{i}_{f}" for s in ("bid", "ask") for i in range(1, LEVELS + 1) for f in ("price", "qty")]

ENV = {k.strip(): v.strip().strip("'\"") for k, v in (
    line.split("=", 1) for line in open(Path(__file__).parent.parent / ".env") if "=" in line and line[0] != "#")}
KEY, SECRET = ENV["DNSE_API_KEY"], ENV["DNSE_API_SECRET"]

trades, books = [], []  # rows waiting for the next flush
last_trade = {}  # symbol -> (day, total_volume, timestamp) of the last trade seen
last_book = {}  # symbol -> last book row (without timestamp); drops repeats and overlap duplicates


def log(*a):
    print(pd.Timestamp.now(tz="Asia/Ho_Chi_Minh").strftime("%Y-%m-%d %H:%M:%S"), *a, flush=True)


def rest(path, **params):
    """Signed GET (HMAC over method, path, date, nonce)."""
    date = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")
    nonce = uuid.uuid4().hex
    msg = f"(request-target): get {path}\ndate: {date}\nnonce: {nonce}"
    sig = quote(base64.b64encode(hmac.new(SECRET.encode(), msg.encode(), hashlib.sha256).digest()).decode(), safe="")
    r = requests.get(REST_URL + path, params=params, timeout=15, headers={
        "x-api-key": KEY, "Date": date, "version": "2026-07-23",
        "X-Signature": f'Signature keyId="{KEY}",algorithm="hmac-sha256",'
                       f'headers="(request-target) date",signature="{sig}",nonce="{nonce}"'})
    r.raise_for_status()
    return r.json()


def resolve(symbols):
    """Futures aliases -> current contract code, e.g. VN30F1M -> 41I1GB000."""
    futures = rest("/market/instruments", marketId="DVX", securityGroupId="FU", limit=100)["data"]
    codes = {x["symbolType"]: x["symbol"] for x in futures}
    return [codes.get(s, s) for s in symbols]


def auth_message():
    ts, nonce = int(time.time()), str(int(time.time() * 1e6))
    sig = hmac.new(SECRET.encode(), f"{KEY}:{ts}:{nonce}".encode(), hashlib.sha256).hexdigest()
    return {"action": "auth", "api_key": KEY, "signature": sig, "timestamp": ts, "nonce": nonce}


def to_us(t):
    """Exchange time -> epoch microseconds. WS: {Seconds, Nanos}; REST: 'YYYY-MM-DD HH:MM:SS.mmm' in GMT+7."""
    if isinstance(t, dict):
        return t["Seconds"] * 1_000_000 + t["Nanos"] // 1000
    return pd.Timestamp(t, tz="Asia/Ho_Chi_Minh").value // 1000


def day(us):
    return pd.Timestamp(us, unit="us", tz="Asia/Ho_Chi_Minh").date()


def trade_row(d):
    m = d["marketId"]
    pm, qm = PRICE_MULT.get(m, 1000), QTY_MULT.get(m, 10)
    return (to_us(d["time"]), EXCHANGES.get(m, m), d["symbol"], d["matchPrice"] * pm, int(d["matchQtty"] * qm),
            SIDES.get(d.get("side")), int(d["totalVolumeTraded"] * qm), SESSIONS.get(d.get("tradingSessionId")))


def fill_gap(symbol, after, upto, since):
    """Refill trades with after < total_volume <= upto via REST (runs in a thread)."""
    log(f"gap {symbol} total_volume {after}..{upto}, refilling")
    params = {"boardId": BOARD, "from": since // 1_000_000, "to": int(time.time()), "limit": 1000, "order": "ASC"}
    try:
        while True:
            body = rest(f"/price/{symbol}/trades", **params)
            trades.extend(r for r in map(trade_row, body.get("trades") or []) if after < r[6] <= upto)
            if not body.get("nextPageToken"):
                break
            params["nextPageToken"] = body["nextPageToken"]
            time.sleep(0.5)
        log(f"gap {symbol} refilled")
    except requests.RequestException as e:
        log(f"!! gap fill {symbol}: {e!r}")


def add_trade(d):
    row = trade_row(d)
    ts, symbol, qty, total = row[0], row[2], row[4], row[6]
    last = last_trade.get(symbol)
    if last and last[0] == day(ts):  # total_volume restarts every day
        if total <= last[1]:
            return  # already seen (overlapping connections)
        if total > last[1] + qty:
            asyncio.get_running_loop().run_in_executor(None, fill_gap, symbol, last[1], total - qty, last[2])
    last_trade[symbol] = (day(ts), total, ts)
    trades.append(row)


def add_book(d):
    m = d["marketId"]
    pm, qm = PRICE_MULT.get(m, 1000), QTY_MULT.get(m, 10)
    row = [EXCHANGES.get(m, m), d["symbol"]]
    for side in ((d.get("bid") or [])[:LEVELS], (d.get("offer") or [])[:LEVELS]):
        row += [x for lv in side for x in (lv["price"] * pm, lv["qtty"] * qm)] + [None] * 2 * (LEVELS - len(side))
    if row == last_book.get(d["symbol"]):
        return  # unchanged top levels
    last_book[d["symbol"]] = row
    books.append([to_us(d["time"])] + row)


async def consume():
    symbols = await asyncio.to_thread(resolve, SYMBOLS)
    async with websockets.connect(WS_URL, max_size=None) as ws:
        await ws.recv()  # welcome
        await ws.send(json.dumps(auth_message()))
        reply = json.loads(await ws.recv())
        if reply.get("action") != "auth_success":
            raise ConnectionError(reply)
        await ws.send(json.dumps({"action": "subscribe", "channels": [
            {"name": f"tick_extra.{BOARD}.json", "symbols": symbols},
            {"name": f"top_price.{BOARD}.json", "symbols": symbols}]}))
        log(f"connected: {', '.join(symbols)}")
        while True:
            try:
                m = json.loads(await asyncio.wait_for(ws.recv(), HEARTBEAT))
            except asyncio.TimeoutError:
                await ws.send(json.dumps({"action": "ping"}))
                continue
            action, kind = m.get("action"), m.get("T")
            if action == "ping":
                await ws.send(json.dumps({"action": "pong", "timestamp": m.get("timestamp")}))
            elif action == "connection_expired":
                return
            elif action == "error":
                raise ConnectionError(m)
            elif kind == "te":
                add_trade(m)
            elif kind == "q":
                add_book(m)


async def keep():
    """Always one live connection; rotated before 8h with a short overlap."""
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
                await asyncio.sleep(5)
    finally:
        for t in (old, cur):
            if t:
                t.cancel()


def frame(rows, cols):
    df = pd.DataFrame(rows, columns=cols)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="us", utc=True)
    for c in ("exchange", "symbol", "side", "session"):
        if c in df:
            df[c] = df[c].astype("category")  # -> SYMBOL
    return df


written = {"vn_trades": 0, "vn_orderbook": 0}  # rows since the last status line


async def flush(sink):
    for table, rows, cols, key in (("vn_trades", trades, TRADE_COLS, ["timestamp", "symbol", "total_volume"]),
                                   ("vn_orderbook", books, BOOK_COLS, ["timestamp", "symbol"])):
        batch = rows[:]
        if not batch:
            continue
        try:
            # same as the table's DEDUP: several book updates in one millisecond -> keep the last
            df = frame(batch, cols).drop_duplicates(key, keep="last")
            await asyncio.to_thread(sink, table, df)
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
            log(f"last minute: {written['vn_trades']} trades, {written['vn_orderbook']} books written")
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
        # resume today: trades missed while we were down get refilled on the first live trade
        today = pd.Timestamp.now(tz="Asia/Ho_Chi_Minh").normalize().tz_convert("UTC")
        seen = sql(f"SELECT symbol, max(total_volume) total, max(timestamp) ts FROM vn_trades "
                   f"WHERE timestamp >= '{today:%Y-%m-%dT%H:%M:%S}Z'")
        for s, total, ts in zip(seen["symbol"], seen["total"], pd.to_datetime(seen["ts"], utc=True)):
            last_trade[s] = (day(ts.value // 1000), int(total), ts.value // 1000)
        log(f"recording {', '.join(SYMBOLS)}, resume from {last_trade}")
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(run(lambda table, df: write(table, df, at="timestamp")))

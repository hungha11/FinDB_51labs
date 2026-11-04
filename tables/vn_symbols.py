import re
import unicodedata

import pandas as pd

from qdb import sql, write

TABLE = "vn_symbols"
SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  timestamp TIMESTAMP, symbol SYMBOL INDEX, name VARCHAR, exchange SYMBOL, industry SYMBOL
) TIMESTAMP(timestamp) PARTITION BY YEAR WAL DEDUP UPSERT KEYS(timestamp, symbol);
"""

URL = "https://cafef.vn/du-lieu/ajax/pagenew/databusiness/congtyniemyet.ashx?centerid=0&skip=0&take=5000&major=0"
EXCHANGES = {1: "HOSE", 2: "HNX", 9: "UPCOM"}
# CafeF strays -> main sectors; non-industries -> None
INDUSTRY_FIX = {
    "Xây dựng": "Bất động sản và Xây dựng",
    "Chứng khoán và Đầu tư": "Tài chính",
    "Doanh nghiệp Upcom": None,
}
NAME_FIX = [  # (regex, replacement), case-insensitive
    (r"công ty cổ phần", "CTCP"),
    (r"thương mại cổ phần", "TMCP"),
    (r"tổng công ty", "Tổng Công ty"),
]


def clean(s):
    """NFC, trim, collapse spaces; empty -> None."""
    if not isinstance(s, str):
        return None
    s = " ".join(unicodedata.normalize("NFC", s).split())
    return s or None


def clean_name(s):
    s = clean(s)
    for pat, rep in NAME_FIX:
        s = re.sub(pat, rep, s, flags=re.I) if s else s
    return s


def fetch() -> pd.DataFrame:
    raw = pd.DataFrame(pd.read_json(URL, storage_options={"User-Agent": "Mozilla/5.0"})["Data"].tolist())
    raw = raw[raw["TradeCenterId"].isin(EXCHANGES)]
    industry = raw["CategoryName"].map(clean)
    df = pd.DataFrame({
        "timestamp": pd.Timestamp.now(tz="UTC"),  # ingest time
        "symbol": raw["Symbol"].str.strip().str.upper(),
        "name": raw["CompanyName"].map(clean_name),
        "exchange": raw["TradeCenterId"].map(EXCHANGES),
        "industry": industry.map(lambda x: INDUSTRY_FIX.get(x, x)),
    })
    return df.sort_values("symbol").reset_index(drop=True)


if __name__ == "__main__":
    sql(SCHEMA)
    write(TABLE, fetch(), symbols=["symbol", "exchange", "industry"], at="timestamp")

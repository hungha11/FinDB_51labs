"""Shared QuestDB helpers. Every tables/*.py imports from here."""
from pathlib import Path

import questdb

ENV = {k.strip(): v.strip().strip("'\"") for k, v in (
    line.split("=", 1) for line in open(Path(__file__).parent / ".env") if "=" in line and line[0] != "#")}
CONF = f"ws::addr=127.0.0.1:9000;username={ENV['QDB_USER']};password={ENV['QDB_PASSWORD']};"


def sql(query):
    """Run any SQL (DDL or SELECT). SELECT returns a pandas DataFrame."""
    with questdb.connect(CONF) as db:
        return db.query(query).to_pandas()


def write(table, df, symbols="auto", at="ts"):
    """Append a DataFrame. Re-sending rows is safe if the table has DEDUP."""
    with questdb.connect(CONF) as db:
        db.dataframe(df, table_name=table, symbols=symbols, at=at)

"""Shared QuestDB helpers. Every tables/*.py imports from here."""
import questdb

CONF = "ws::addr=127.0.0.1:9000;username=51labs;password=m1m2;"


def sql(query):
    """Run any SQL (DDL or SELECT). SELECT returns a pandas DataFrame."""
    with questdb.connect(CONF) as db:
        return db.query(query).to_pandas()


def write(table, df, symbols="auto", at="ts"):
    """Append a DataFrame. Re-sending rows is safe if the table has DEDUP."""
    with questdb.connect(CONF) as db:
        db.dataframe(df, table_name=table, symbols=symbols, at=at)

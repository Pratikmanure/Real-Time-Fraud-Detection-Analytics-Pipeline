from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "fraud_detection.db"


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    event_timestamp TEXT NOT NULL,
    ingest_timestamp TEXT NOT NULL,
    card_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    card_tier TEXT NOT NULL,
    merchant_name TEXT NOT NULL,
    merchant_category TEXT NOT NULL,
    merchant_city TEXT NOT NULL,
    merchant_country TEXT NOT NULL,
    amount REAL NOT NULL,
    currency TEXT NOT NULL,
    channel TEXT NOT NULL,
    entry_mode TEXT NOT NULL,
    device_id TEXT NOT NULL,
    ip_address TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    home_city TEXT NOT NULL,
    home_country TEXT NOT NULL,
    scenario_label TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_transactions_card_time
    ON transactions(card_id, event_timestamp);

CREATE INDEX IF NOT EXISTS idx_transactions_time
    ON transactions(event_timestamp);

CREATE INDEX IF NOT EXISTS idx_transactions_city
    ON transactions(merchant_city, event_timestamp);
"""


@contextmanager
def get_connection(db_path: Path | None = None):
    path = Path(db_path or DB_PATH)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(db_path: Path | None = None) -> Path:
    path = Path(db_path or DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    with get_connection(path) as conn:
        conn.executescript(SCHEMA_SQL)
    return path


def insert_transactions(records: Iterable[dict], db_path: Path | None = None) -> int:
    rows = list(records)
    if not rows:
        return 0

    columns = [
        "transaction_id",
        "event_timestamp",
        "ingest_timestamp",
        "card_id",
        "customer_id",
        "card_tier",
        "merchant_name",
        "merchant_category",
        "merchant_city",
        "merchant_country",
        "amount",
        "currency",
        "channel",
        "entry_mode",
        "device_id",
        "ip_address",
        "latitude",
        "longitude",
        "home_city",
        "home_country",
        "scenario_label",
    ]
    placeholders = ", ".join("?" for _ in columns)
    sql = f"""
        INSERT OR REPLACE INTO transactions ({", ".join(columns)})
        VALUES ({placeholders})
    """
    values = [[row[column] for column in columns] for row in rows]
    with get_connection(db_path) as conn:
        conn.executemany(sql, values)
    return len(rows)


def read_sql(query: str, params: tuple | None = None, db_path: Path | None = None):
    import pandas as pd

    with get_connection(db_path) as conn:
        return pd.read_sql_query(query, conn, params=params or ())

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from config import get_database_url


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQLITE_PATH = PROJECT_ROOT / "investment.db"
TABLES = [
    "stocks",
    "transactions",
    "portfolio",
    "dividends",
    "snapshots",
    "news_articles",
    "news_sentiments",
    "alerts",
    "keyword_alerts",
    "backtest_results",
]


def main() -> None:
    if not SQLITE_PATH.exists():
        print(f"SQLite not found: {SQLITE_PATH}")
        return

    target_url = get_database_url()
    if target_url.startswith("sqlite:///"):
        print("DATABASE_URL is SQLite. Set Supabase/PostgreSQL URL to migrate.")
        return

    src = sqlite3.connect(SQLITE_PATH)
    dst = create_engine(target_url, future=True)

    migrated = 0
    for table in TABLES:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table}", src)
        except Exception:
            print(f"skip: {table} (read failed)")
            continue
        if df.empty:
            print(f"skip: {table} (empty)")
            continue

        # For id-based tables, delete existing rows and append.
        try:
            with dst.begin() as con:
                con.execute(text(f"DELETE FROM {table}"))
        except Exception:
            pass

        df.to_sql(table, dst, if_exists="append", index=False, method="multi")
        migrated += len(df)
        print(f"migrated: {table} rows={len(df)}")

    src.close()
    print(f"done migrated_rows={migrated}")


if __name__ == "__main__":
    main()


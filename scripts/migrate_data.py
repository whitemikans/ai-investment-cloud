from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import get_database_url


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

    for table in TABLES:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table}", src)
            if df.empty:
                print(f"skip {table}: empty")
                continue
            with dst.begin() as con:
                con.execute(text(f"DELETE FROM {table}"))
            df.to_sql(table, dst, if_exists="append", index=False)
            print(f"migrated {table}: {len(df)} rows")
        except Exception as exc:
            print(f"skip {table}: {exc}")

    src.close()
    print("migration_done")


if __name__ == "__main__":
    main()

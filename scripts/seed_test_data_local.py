from __future__ import annotations

import os
import sys
from pathlib import Path


def _set_local_sqlite() -> Path:
    root = Path(__file__).resolve().parent.parent
    db_file = root / "investment.db"
    os.environ["LOCAL_DATABASE_URL"] = f"sqlite:///{db_file.as_posix()}"
    os.environ.pop("DATABASE_URL", None)
    os.environ.pop("CLOUD_DATABASE_URL", None)
    return root


def _safe_count(con, table_name: str) -> int:
    from sqlalchemy import text

    try:
        return int(con.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar() or 0)
    except Exception:
        return 0


def main() -> None:
    root = _set_local_sqlite()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    from backtest_engine import ensure_backtest_tables
    from db.db_utils import ensure_dummy_dividends, generate_dummy_snapshots, init_db
    from db.models import engine
    from db.news_utils import init_news_tables
    from scripts.generate_dummy_transactions import main as generate_dummy_transactions_main

    print("=== Seed Test Data (LOCAL/SQLite) ===")
    print(f"DB: {os.environ.get('LOCAL_DATABASE_URL')}")

    init_db()
    init_news_tables()
    ensure_backtest_tables()
    generate_dummy_transactions_main()
    ensure_dummy_dividends()
    generate_dummy_snapshots(days=180, overwrite=True)

    with engine.connect() as con:
        tx = _safe_count(con, "transactions")
        pf = _safe_count(con, "portfolio")
        dv = _safe_count(con, "dividends")
        sn = _safe_count(con, "snapshots")
    print(f"Done: transactions={tx}, portfolio={pf}, dividends={dv}, snapshots={sn}")


if __name__ == "__main__":
    main()


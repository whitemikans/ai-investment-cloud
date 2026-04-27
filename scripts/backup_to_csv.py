from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import get_database_url

OUT_DIR = PROJECT_ROOT / "data" / "backups"
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
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = OUT_DIR / stamp
    out.mkdir(parents=True, exist_ok=True)

    engine = create_engine(get_database_url(), future=True)
    exported = 0
    for t in TABLES:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {t}", engine)
            df.to_csv(out / f"{t}.csv", index=False, encoding="utf-8-sig")
            exported += 1
            print(f"exported {t}: {len(df)} rows")
        except Exception as exc:
            print(f"skip {t}: {exc}")
    print(f"backup_done tables={exported} dir={out}")


if __name__ == "__main__":
    main()

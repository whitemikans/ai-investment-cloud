from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest_engine import ensure_backtest_tables  # noqa: E402
from db.db_utils import init_db  # noqa: E402
from db.news_utils import init_news_tables  # noqa: E402


def main() -> None:
    init_db()
    init_news_tables()
    ensure_backtest_tables()
    print("bootstrap_cloud done")


if __name__ == "__main__":
    main()


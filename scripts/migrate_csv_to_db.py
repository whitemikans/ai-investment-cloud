from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.db_utils import get_session, init_db, seed_transactions_from_csv


if __name__ == "__main__":
    init_db()
    csv_path = ROOT / "portfolio_input.csv"
    with get_session() as session:
        seed_transactions_from_csv(session, csv_path)
    print(f"Migrated from {csv_path}")

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.db_utils import init_db, record_snapshot


if __name__ == "__main__":
    init_db()
    df = record_snapshot()
    if df.empty:
        print("Snapshot record failed: empty result")
    else:
        row = df.iloc[0].to_dict()
        if bool(row.get("success", True)):
            print(
                "Snapshot recorded:",
                f"date={row.get('snapshot_date')},",
                f"total_value={row.get('total_value')},",
                f"total_cost={row.get('total_cost')},",
                f"unrealized_pl={row.get('unrealized_pl')},",
                f"realized_pl={row.get('realized_pl')}",
            )
        else:
            print(f"Snapshot record failed: {row.get('message')}")

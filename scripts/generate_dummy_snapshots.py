from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.db_utils import generate_dummy_snapshots, init_db


if __name__ == "__main__":
    init_db()
    result = generate_dummy_snapshots(days=180, overwrite=True)
    if result.empty:
        print("Failed: empty result")
    else:
        row = result.iloc[0].to_dict()
        print(f"{row.get('message')} inserted={row.get('inserted')}")

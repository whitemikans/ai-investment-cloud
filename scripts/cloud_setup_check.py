from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import get_database_url, get_setting  # noqa: E402


REQUIRED_KEYS = [
    "DATABASE_URL",
    "APP_PASSWORD",
]

OPTIONAL_KEYS = [
    "GEMINI_API_KEY",
    "DISCORD_WEBHOOK_URL",
    "FRED_API_KEY",
    "ESTAT_API_KEY",
]


def main() -> int:
    print("=== Cloud Setup Check ===")
    missing = []
    for k in REQUIRED_KEYS:
        v = get_setting(k, "")
        if not v:
            missing.append(k)
    for k in OPTIONAL_KEYS:
        v = get_setting(k, "")
        print(f"{k}: {'set' if v else 'not set'}")

    if missing:
        print(f"Missing required settings: {', '.join(missing)}")
        return 1

    db_url = get_database_url()
    print(f"DATABASE_URL: {db_url.split('@')[0]}@***")
    try:
        engine = create_engine(db_url, future=True)
        with engine.connect() as con:
            con.execute(text("SELECT 1"))
        print("DB connection: OK")
    except Exception as exc:
        print(f"DB connection: NG ({exc})")
        return 2

    print("Cloud setup looks good.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


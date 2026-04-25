from __future__ import annotations

import argparse
import os
import sys
import tomllib
from pathlib import Path


def _read_url_from_local_secrets(project_root: Path) -> str:
    secrets_path = project_root / ".streamlit" / "secrets.toml"
    if not secrets_path.exists():
        return ""
    try:
        raw = secrets_path.read_text(encoding="utf-8-sig")
        data = tomllib.loads(raw)
    except Exception:
        return ""
    for key in ("CLOUD_DATABASE_URL", "DATABASE_URL"):
        value = str(data.get(key, "") or "").strip()
        if value:
            return value
    return ""


def _resolve_db_url(args_db_url: str | None, project_root: Path) -> str:
    candidates = [
        str(args_db_url or "").strip(),
        str(os.getenv("DATABASE_URL", "") or "").strip(),
        _read_url_from_local_secrets(project_root),
    ]
    for raw in candidates:
        if raw:
            return raw
    return ""


def _validate_supabase_url(raw: str) -> str:
    value = raw.strip()
    if not value:
        raise ValueError("DATABASE_URL が未指定です。--database-url か環境変数/secret を設定してください。")
    if not (value.startswith("postgresql://") or value.startswith("postgres://")):
        raise ValueError("Supabase(PostgreSQL) URL を指定してください。")
    return value


def _set_supabase_env(db_url: str) -> None:
    os.environ["DATABASE_URL"] = db_url
    # LOCAL_DATABASE_URL が残っていると local 優先になるため除去
    os.environ.pop("LOCAL_DATABASE_URL", None)


def _safe_count(con, table_name: str) -> int:
    from sqlalchemy import text

    try:
        return int(con.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar() or 0)
    except Exception:
        return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Supabase DB にテストデータを投入します。")
    parser.add_argument("--database-url", default="", help="postgresql://... (未指定時は環境変数/secretを参照)")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    db_url = _validate_supabase_url(_resolve_db_url(args.database_url, project_root))
    _set_supabase_env(db_url)

    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from backtest_engine import ensure_backtest_tables
    from db.db_utils import ensure_dummy_dividends, generate_dummy_snapshots, init_db
    from db.models import engine
    from db.news_utils import init_news_tables
    from scripts.generate_dummy_transactions import main as generate_dummy_transactions_main

    print("=== Seed Test Data (SUPABASE/PostgreSQL) ===")
    masked = db_url.split("@")[0] + "@***" if "@" in db_url else db_url
    print(f"DB: {masked}")

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


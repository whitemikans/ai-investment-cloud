from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ai_analyzer import build_japanese_summary, translate_to_japanese

DB_PATH = ROOT / "investment.db"


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    rows = cur.execute("SELECT id, title, COALESCE(summary_ja, ''), COALESCE(content, '') FROM news_articles ORDER BY id").fetchall()
    updated = 0
    for article_id, title, summary, content in rows:
        new_title = translate_to_japanese(str(title or ""))
        new_summary = build_japanese_summary(new_title, str(content or "")) if content else translate_to_japanese(str(summary or ""))
        if new_title != (title or "") or new_summary != (summary or ""):
            cur.execute(
                "UPDATE news_articles SET title = ?, summary_ja = ?, updated_at = datetime('now') WHERE id = ?",
                (new_title, new_summary, article_id),
            )
            updated += 1
    conn.commit()
    conn.close()
    print(f"updated={updated}")


if __name__ == "__main__":
    main()

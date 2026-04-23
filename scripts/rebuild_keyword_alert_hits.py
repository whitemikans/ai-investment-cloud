from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ai_analyzer import translate_to_japanese
from db.news_utils import init_news_tables, list_keyword_alerts
from news_pipeline import _find_hit_keywords

DB_PATH = ROOT / "investment.db"


def main() -> None:
    init_news_tables()
    kw_df = list_keyword_alerts()
    active_keywords = kw_df[kw_df["is_active"] == 1]["keyword"].astype(str).tolist() if not kw_df.empty else []
    if not active_keywords:
        print("no active keywords")
        return

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    rows = con.execute("SELECT id, title, COALESCE(content,'') AS content FROM news_articles").fetchall()
    inserted = 0
    for r in rows:
        text = f"{r['title']} {r['content']}".strip()
        searchable = f"{text}\n{translate_to_japanese(text)}".lower()
        hits = _find_hit_keywords(active_keywords, searchable)
        if not hits:
            continue

        exists = con.execute(
            "SELECT 1 FROM alerts WHERE article_id = ? AND alert_type = 'keyword_reindex' LIMIT 1",
            (int(r["id"]),),
        ).fetchone()
        if exists:
            continue

        con.execute(
            """
            INSERT INTO alerts(article_id, alert_type, message, hit_keywords, created_at)
            VALUES (?, 'keyword_reindex', ?, ?, datetime('now'))
            """,
            (
                int(r["id"]),
                f"keyword hits: {','.join(hits)}",
                ",".join(hits),
            ),
        )
        inserted += 1

    con.commit()
    total = con.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
    con.close()
    print(f"reindex_inserted={inserted}")
    print(f"alerts_total={total}")


if __name__ == "__main__":
    main()


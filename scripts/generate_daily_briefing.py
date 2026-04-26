from __future__ import annotations

from datetime import datetime, timedelta

import requests
from sqlalchemy import text

from config import get_setting
from db.db_utils import init_db
from db.models import engine
from db.news_utils import init_news_tables


def build_briefing() -> str:
    init_db()
    init_news_tables()
    today = datetime.now().strftime("%Y-%m-%d")
    since = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    with engine.connect() as con:
        news = int(con.execute(text("SELECT COUNT(*) FROM news_articles WHERE published_at >= :since"), {"since": since}).scalar() or 0)
        imp = int(
            con.execute(
                text("SELECT COUNT(*) FROM news_sentiments WHERE analyzed_at >= :since AND COALESCE(importance_score,1) >= 4"),
                {"since": since},
            ).scalar()
            or 0
        )
        snaps = int(con.execute(text("SELECT COUNT(*) FROM snapshots WHERE snapshot_date = :today"), {"today": today}).scalar() or 0)
    return (
        "📊 今日のマーケットブリーフィング\n"
        f"- 24hニュース件数: {news}\n"
        f"- 重要ニュース(★4+): {imp}\n"
        f"- 本日スナップショット: {snaps}\n"
        "- 詳細はダッシュボードで確認してください。"
    )


def main() -> None:
    text_body = build_briefing()
    webhook = get_setting("DISCORD_WEBHOOK_URL", "")
    print(text_body)
    if webhook:
        try:
            requests.post(webhook, json={"content": text_body}, timeout=8)
            print("discord_sent")
        except Exception as exc:
            print(f"discord_error: {exc}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from config import get_setting
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "investment.db"


def build_briefing() -> str:
    if not DB_PATH.exists():
        return "📋 Daily Briefing\n- DB未作成のため集計なし"
    con = sqlite3.connect(DB_PATH)
    today = datetime.now().strftime("%Y-%m-%d")
    since = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    news = con.execute("SELECT COUNT(*) FROM news_articles WHERE published_at >= ?", (since,)).fetchone()[0]
    imp = con.execute(
        "SELECT COUNT(*) FROM news_sentiments WHERE analyzed_at >= ? AND COALESCE(importance_score,1) >= 4",
        (since,),
    ).fetchone()[0]
    snaps = con.execute("SELECT COUNT(*) FROM snapshots WHERE snapshot_date = ?", (today,)).fetchone()[0]
    con.close()
    return (
        "📋 今日のマーケットブリーフィング\n"
        f"- 24hニュース件数: {news}\n"
        f"- 重要ニュース(⭐4+): {imp}\n"
        f"- 本日スナップショット: {snaps}\n"
        "- 詳細はダッシュボードで確認してください。"
    )


def main() -> None:
    text = build_briefing()
    webhook = get_setting("DISCORD_WEBHOOK_URL", "")
    print(text)
    if webhook:
        try:
            requests.post(webhook, json={"content": text}, timeout=8)
            print("discord_sent")
        except Exception as exc:
            print(f"discord_error: {exc}")


if __name__ == "__main__":
    main()


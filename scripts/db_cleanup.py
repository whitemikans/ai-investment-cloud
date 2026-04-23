from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import create_engine, text

from config import get_database_url


def main() -> None:
    engine = create_engine(get_database_url(), future=True)
    cutoff_news = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")
    cutoff_alert = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d %H:%M:%S")

    with engine.begin() as con:
        # Keep latest 1 year of news and 6 months of alerts by default.
        try:
            con.execute(text("DELETE FROM alerts WHERE created_at < :cutoff"), {"cutoff": cutoff_alert})
        except Exception:
            pass
        try:
            con.execute(text("DELETE FROM news_sentiments WHERE analyzed_at < :cutoff"), {"cutoff": cutoff_news})
        except Exception:
            pass
        try:
            con.execute(text("DELETE FROM news_articles WHERE published_at < :cutoff"), {"cutoff": cutoff_news})
        except Exception:
            pass
    print("cleanup_done")


if __name__ == "__main__":
    main()


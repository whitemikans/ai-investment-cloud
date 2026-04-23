from __future__ import annotations

from datetime import datetime

import feedparser

NEWS_FEEDS = {
    "Reuters": "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best",
    "Bloomberg": "https://feeds.bloomberg.com/markets/news.rss",
    "WSJ": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "SEC EDGAR": "https://www.sec.gov/news/pressreleases.rss",
}


def _to_datetime(struct_time_obj) -> datetime:
    if not struct_time_obj:
        return datetime.now()
    try:
        return datetime(
            struct_time_obj.tm_year,
            struct_time_obj.tm_mon,
            struct_time_obj.tm_mday,
            struct_time_obj.tm_hour,
            struct_time_obj.tm_min,
            struct_time_obj.tm_sec,
        )
    except Exception:
        return datetime.now()


def fetch_rss_news(max_articles_per_source: int = 25) -> list[dict[str, str]]:
    articles: list[dict[str, str]] = []
    for source, url in NEWS_FEEDS.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:max_articles_per_source]:
                published = _to_datetime(getattr(entry, "published_parsed", None))
                articles.append(
                    {
                        "title": str(getattr(entry, "title", "")).strip(),
                        "url": str(getattr(entry, "link", "")).strip(),
                        "source": source,
                        "published_at": published.strftime("%Y-%m-%d %H:%M:%S"),
                        "content": str(getattr(entry, "summary", "")).strip(),
                    }
                )
        except Exception:
            continue
    return [a for a in articles if a["title"] and a["url"]]


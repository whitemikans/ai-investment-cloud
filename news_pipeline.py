from __future__ import annotations

import sqlite3
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from ai_analyzer import (
    analyze_sentiment,
    build_japanese_summary,
    extract_related_stocks,
    infer_sector,
    score_importance,
    translate_to_english,
    translate_to_japanese,
)
from db.news_utils import get_stock_master_tickers, init_news_tables, list_keyword_alerts
from db.models import engine
from rss_reader import fetch_rss_news

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "investment.db"

KEYWORD_ALIAS_MAP: dict[str, list[str]] = {
    "自社株買い": ["share buyback", "stock buyback", "share repurchase", "stock repurchase", "buyback"],
    "業績上方修正": ["raise guidance", "raised guidance", "upward revision", "guidance increase"],
    "業績下方修正": ["cut guidance", "lowered guidance", "downward revision"],
    "増配": ["dividend increase", "raised dividend", "dividend hike"],
    "減配": ["dividend cut", "reduced dividend"],
    "株式分割": ["stock split", "share split"],
    "上場廃止": ["delisting", "delisted"],
    "不祥事": ["scandal", "misconduct", "fraud"],
    "リストラ": ["restructuring", "layoff", "layoffs"],
}


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _is_sqlite() -> bool:
    return engine.url.get_backend_name().lower() == "sqlite"


@lru_cache(maxsize=2048)
def _keyword_variants(keyword: str) -> list[str]:
    kw = (keyword or "").strip()
    if not kw:
        return []
    variants: set[str] = {kw.lower()}

    # Alias dictionary for finance terms (JP -> EN phrases).
    for alias in KEYWORD_ALIAS_MAP.get(kw, []):
        variants.add(alias.lower())

    # Automatic translation fallback for custom keywords.
    translated_ja = translate_to_japanese(kw)
    if translated_ja:
        variants.add(translated_ja.lower())
    translated_en = translate_to_english(kw)
    if translated_en:
        variants.add(translated_en.lower())
    return [v for v in variants if v]


def _find_hit_keywords(active_keywords: list[str], searchable_text: str) -> list[str]:
    hits: list[str] = []
    for kw in active_keywords:
        variants = _keyword_variants(kw)
        if any(v in searchable_text for v in variants):
            hits.append(kw)
    return hits


def _upsert_article(conn: sqlite3.Connection, article: dict[str, str]) -> int:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = conn.execute("SELECT id FROM news_articles WHERE url = ?", (article["url"],)).fetchone()
    if row:
        article_id = int(row["id"])
        conn.execute(
            """
            UPDATE news_articles
            SET title = ?, source = ?, published_at = ?, summary_ja = ?, content = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                article["title"],
                article["source"],
                article["published_at"],
                article["summary_ja"],
                article["content"],
                now,
                article_id,
            ),
        )
        return article_id

    cur = conn.execute(
        """
        INSERT INTO news_articles(title, url, source, published_at, summary_ja, content, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            article["title"],
            article["url"],
            article["source"],
            article["published_at"],
            article["summary_ja"],
            article["content"],
            now,
            now,
        ),
    )
    return int(cur.lastrowid)


def _find_existing_article(conn: sqlite3.Connection, url: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT id, title, COALESCE(summary_ja, '') AS summary_ja, COALESCE(content, '') AS content FROM news_articles WHERE url = ?",
        (url,),
    ).fetchone()


def _upsert_sentiment(
    conn: sqlite3.Connection,
    article_id: int,
    sentiment_score: float,
    sentiment_label: str,
    importance_score: int,
    related_stocks: list[str],
    sector: str,
) -> None:
    conn.execute("DELETE FROM news_sentiments WHERE article_id = ?", (article_id,))
    conn.execute(
        """
        INSERT INTO news_sentiments(article_id, sentiment_score, sentiment_label, importance_score, related_stocks, sector, analyzed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            article_id,
            sentiment_score,
            sentiment_label,
            importance_score,
            ",".join(related_stocks),
            sector,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )


def _insert_alert(conn: sqlite3.Connection, article_id: int, hit_keywords: list[str], importance_score: int) -> None:
    if not hit_keywords and importance_score < 4:
        return
    alert_type = "keyword" if hit_keywords else "high_importance"
    message = f"重要ニュースを検知: importance={importance_score}, keywords={','.join(hit_keywords) if hit_keywords else '-'}"
    conn.execute(
        """
        INSERT INTO alerts(article_id, alert_type, message, hit_keywords, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            article_id,
            alert_type,
            message,
            ",".join(hit_keywords),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )


def _find_existing_article_sa(conn, url: str):
    return conn.execute(
        text(
            """
            SELECT id, title, COALESCE(summary_ja, '') AS summary_ja, COALESCE(content, '') AS content
            FROM news_articles
            WHERE url = :url
            """
        ),
        {"url": url},
    ).mappings().first()


def _upsert_article_sa(conn, article: dict[str, str]) -> int:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    article_id = conn.execute(
        text(
            """
            INSERT INTO news_articles(title, url, source, published_at, summary_ja, content, created_at, updated_at)
            VALUES (:title, :url, :source, :published_at, :summary_ja, :content, :created_at, :updated_at)
            ON CONFLICT (url) DO UPDATE SET
                title = EXCLUDED.title,
                source = EXCLUDED.source,
                published_at = EXCLUDED.published_at,
                summary_ja = EXCLUDED.summary_ja,
                content = EXCLUDED.content,
                updated_at = EXCLUDED.updated_at
            RETURNING id
            """
        ),
        {
            "title": article["title"],
            "url": article["url"],
            "source": article["source"],
            "published_at": article["published_at"],
            "summary_ja": article["summary_ja"],
            "content": article["content"],
            "created_at": now,
            "updated_at": now,
        },
    ).scalar_one()
    return int(article_id)


def _upsert_sentiment_sa(
    conn,
    article_id: int,
    sentiment_score: float,
    sentiment_label: str,
    importance_score: int,
    related_stocks: list[str],
    sector: str,
) -> None:
    conn.execute(text("DELETE FROM news_sentiments WHERE article_id = :article_id"), {"article_id": article_id})
    conn.execute(
        text(
            """
            INSERT INTO news_sentiments(article_id, sentiment_score, sentiment_label, importance_score, related_stocks, sector, analyzed_at)
            VALUES (:article_id, :sentiment_score, :sentiment_label, :importance_score, :related_stocks, :sector, :analyzed_at)
            """
        ),
        {
            "article_id": article_id,
            "sentiment_score": sentiment_score,
            "sentiment_label": sentiment_label,
            "importance_score": importance_score,
            "related_stocks": ",".join(related_stocks),
            "sector": sector,
            "analyzed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
    )


def _insert_alert_sa(conn, article_id: int, hit_keywords: list[str], importance_score: int) -> None:
    if not hit_keywords and importance_score < 4:
        return
    alert_type = "keyword" if hit_keywords else "high_importance"
    message = f"important news detected: importance={importance_score}, keywords={','.join(hit_keywords) if hit_keywords else '-'}"
    conn.execute(
        text(
            """
            INSERT INTO alerts(article_id, alert_type, message, hit_keywords, created_at)
            VALUES (:article_id, :alert_type, :message, :hit_keywords, :created_at)
            """
        ),
        {
            "article_id": article_id,
            "alert_type": alert_type,
            "message": message,
            "hit_keywords": ",".join(hit_keywords),
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
    )


def process_news_pipeline(max_articles_per_source: int = 20) -> pd.DataFrame:
    init_news_tables()
    stock_codes = get_stock_master_tickers()
    keywords_df = list_keyword_alerts()
    active_keywords = keywords_df[keywords_df["is_active"] == 1]["keyword"].astype(str).tolist() if not keywords_df.empty else []

    raw_articles = fetch_rss_news(max_articles_per_source=max_articles_per_source)
    if not raw_articles:
        return pd.DataFrame([{"success": False, "message": "ニュースを取得できませんでした。", "processed": 0}])

    processed = 0
    inserted_or_updated = 0
    alerts_count = 0

    if not _is_sqlite():
        with engine.begin() as conn:
            for item in raw_articles:
                existing = _find_existing_article_sa(conn, item["url"])
                text_src = f"{item['title']} {item['content']}".strip()
                searchable_text = text_src.lower()
                if existing and str(existing["content"] or "").strip() == str(item["content"] or "").strip() and str(existing["summary_ja"] or "").strip():
                    title_ja = str(existing["title"] or "").strip() or translate_to_japanese(item["title"])
                    summary = str(existing["summary_ja"] or "").strip()
                else:
                    title_ja = translate_to_japanese(item["title"])
                    summary = build_japanese_summary(title_ja, item["content"])
                sentiment_score, sentiment_label = analyze_sentiment(text_src)
                importance = score_importance(text_src, item["source"])
                related_stocks = extract_related_stocks(text_src, stock_codes)
                sector = infer_sector(text_src)

                hit_keywords = _find_hit_keywords(active_keywords, searchable_text)
                if hit_keywords:
                    importance = min(5, importance + 1)

                article_id = _upsert_article_sa(
                    conn,
                    {
                        "title": title_ja,
                        "url": item["url"],
                        "source": item["source"],
                        "published_at": item["published_at"],
                        "summary_ja": summary,
                        "content": item["content"],
                    },
                )
                _upsert_sentiment_sa(
                    conn,
                    article_id=article_id,
                    sentiment_score=sentiment_score,
                    sentiment_label=sentiment_label,
                    importance_score=importance,
                    related_stocks=related_stocks,
                    sector=sector,
                )
                before = int(conn.execute(text("SELECT COUNT(*) FROM alerts")).scalar_one())
                _insert_alert_sa(conn, article_id=article_id, hit_keywords=hit_keywords, importance_score=importance)
                after = int(conn.execute(text("SELECT COUNT(*) FROM alerts")).scalar_one())
                if after > before:
                    alerts_count += 1
                processed += 1
                inserted_or_updated += 1
        return pd.DataFrame(
            [
                {
                    "success": True,
                    "message": "ニュース処理が完了しました。",
                    "processed": processed,
                    "upserted": inserted_or_updated,
                    "alerts": alerts_count,
                }
            ]
        )

    with _connect() as conn:
        for item in raw_articles:
            existing = _find_existing_article(conn, item["url"])
            text = f"{item['title']} {item['content']}".strip()
            searchable_text = text.lower()
            if existing and str(existing["content"] or "").strip() == str(item["content"] or "").strip() and str(existing["summary_ja"] or "").strip():
                title_ja = str(existing["title"] or "").strip() or translate_to_japanese(item["title"])
                summary = str(existing["summary_ja"] or "").strip()
            else:
                title_ja = translate_to_japanese(item["title"])
                summary = build_japanese_summary(title_ja, item["content"])
            sentiment_score, sentiment_label = analyze_sentiment(text)
            importance = score_importance(text, item["source"])
            related_stocks = extract_related_stocks(text, stock_codes)
            sector = infer_sector(text)

            hit_keywords = _find_hit_keywords(active_keywords, searchable_text)
            if hit_keywords:
                importance = min(5, importance + 1)

            article_id = _upsert_article(
                conn,
                {
                    "title": title_ja,
                    "url": item["url"],
                    "source": item["source"],
                    "published_at": item["published_at"],
                    "summary_ja": summary,
                    "content": item["content"],
                },
            )
            _upsert_sentiment(
                conn,
                article_id=article_id,
                sentiment_score=sentiment_score,
                sentiment_label=sentiment_label,
                importance_score=importance,
                related_stocks=related_stocks,
                sector=sector,
            )
            before = conn.execute("SELECT COUNT(*) AS c FROM alerts").fetchone()["c"]
            _insert_alert(conn, article_id=article_id, hit_keywords=hit_keywords, importance_score=importance)
            after = conn.execute("SELECT COUNT(*) AS c FROM alerts").fetchone()["c"]
            if after > before:
                alerts_count += 1

            processed += 1
            inserted_or_updated += 1
        conn.commit()

    return pd.DataFrame(
        [
            {
                "success": True,
                "message": "ニュース処理が完了しました。",
                "processed": processed,
                "upserted": inserted_or_updated,
                "alerts": alerts_count,
            }
        ]
    )


if __name__ == "__main__":
    print(process_news_pipeline().to_dict(orient="records")[0])

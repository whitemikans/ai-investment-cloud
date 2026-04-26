from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from db.models import engine

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "investment.db"

DEFAULT_KEYWORDS = [
    ("自社株買い", "ポジティブ材料"),
    ("業績上方修正", "ポジティブ材料"),
    ("業績下方修正", "ネガティブ材料"),
    ("MBO", "ポジティブ材料"),
    ("TOB", "ポジティブ材料"),
    ("増配", "ポジティブ材料"),
    ("減配", "ネガティブ材料"),
    ("株式分割", "ポジティブ材料"),
    ("上場廃止", "ネガティブ材料"),
    ("不祥事", "ネガティブ材料"),
    ("リストラ", "調査対象"),
]

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

DEFAULT_STOCK_MASTER = [
    ("AAPL", "Apple", "Information Technology", "NASDAQ"),
    ("MSFT", "Microsoft", "Information Technology", "NASDAQ"),
    ("NVDA", "NVIDIA", "Information Technology", "NASDAQ"),
    ("GOOGL", "Alphabet", "Communication Services", "NASDAQ"),
    ("AMZN", "Amazon", "Consumer Discretionary", "NASDAQ"),
]


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _is_sqlite() -> bool:
    return engine.url.get_backend_name().lower() == "sqlite"


def _id_col_sql() -> str:
    return "INTEGER PRIMARY KEY AUTOINCREMENT" if _is_sqlite() else "BIGSERIAL PRIMARY KEY"


def _now_sql() -> str:
    return "CURRENT_TIMESTAMP"


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]) for row in rows}


def _ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_def: str) -> None:
    if column_name not in _table_columns(conn, table_name):
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")


def init_news_tables() -> None:
    if not _is_sqlite():
        _init_news_tables_sqlalchemy()
        return

    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stocks (
                stock_code TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                sector TEXT,
                market TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio (
                stock_code TEXT PRIMARY KEY,
                total_quantity INTEGER DEFAULT 0,
                avg_price REAL DEFAULT 0,
                total_cost REAL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS news_articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                url TEXT NOT NULL UNIQUE,
                source TEXT,
                published_at DATETIME,
                summary_ja TEXT,
                content TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS news_sentiments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER NOT NULL,
                sentiment_score REAL,
                sentiment_label TEXT,
                importance_score INTEGER,
                related_stocks TEXT,
                sector TEXT,
                analyzed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(article_id) REFERENCES news_articles(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER NOT NULL,
                alert_type TEXT,
                message TEXT,
                hit_keywords TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(article_id) REFERENCES news_articles(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS keyword_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT NOT NULL UNIQUE,
                category TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        # Existing course databases may have older versions of these tables.
        # CREATE TABLE IF NOT EXISTS does not add missing columns, so keep this
        # lightweight migration here to prevent sqlite3.OperationalError on pages.
        _ensure_column(conn, "news_articles", "summary_ja", "TEXT")
        _ensure_column(conn, "news_articles", "content", "TEXT")
        _ensure_column(conn, "news_articles", "created_at", "DATETIME")
        _ensure_column(conn, "news_articles", "updated_at", "DATETIME")
        _ensure_column(conn, "news_sentiments", "sentiment_score", "REAL")
        _ensure_column(conn, "news_sentiments", "sentiment_label", "TEXT")
        _ensure_column(conn, "news_sentiments", "importance_score", "INTEGER")
        _ensure_column(conn, "news_sentiments", "related_stocks", "TEXT")
        _ensure_column(conn, "news_sentiments", "sector", "TEXT")
        _ensure_column(conn, "news_sentiments", "analyzed_at", "DATETIME")
        _ensure_column(conn, "alerts", "alert_type", "TEXT")
        _ensure_column(conn, "alerts", "message", "TEXT")
        _ensure_column(conn, "alerts", "hit_keywords", "TEXT")
        _ensure_column(conn, "alerts", "created_at", "DATETIME")
        _ensure_column(conn, "keyword_alerts", "category", "TEXT DEFAULT '一般'")
        _ensure_column(conn, "keyword_alerts", "is_active", "INTEGER NOT NULL DEFAULT 1")
        _ensure_column(conn, "keyword_alerts", "created_at", "DATETIME")
        for code, name, sector, market in DEFAULT_STOCK_MASTER:
            conn.execute(
                """
                INSERT OR IGNORE INTO stocks(stock_code, company_name, sector, market)
                VALUES (?, ?, ?, ?)
                """,
                (code, name, sector, market),
            )
        conn.commit()

    seed_default_keywords()


def _init_news_tables_sqlalchemy() -> None:
    id_col = _id_col_sql()
    with engine.begin() as con:
        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS stocks (
                    stock_code TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL,
                    sector TEXT,
                    market TEXT
                )
                """
            )
        )
        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS portfolio (
                    stock_code TEXT PRIMARY KEY,
                    total_quantity INTEGER DEFAULT 0,
                    avg_price REAL DEFAULT 0,
                    total_cost REAL DEFAULT 0
                )
                """
            )
        )
        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS news_articles (
                    id __ID_COL__,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL UNIQUE,
                    source TEXT,
                    published_at TIMESTAMP,
                    summary_ja TEXT,
                    content TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP
                )
                """.replace("__ID_COL__", id_col)
            )
        )
        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS news_sentiments (
                    id __ID_COL__,
                    article_id INTEGER NOT NULL,
                    sentiment_score REAL,
                    sentiment_label TEXT,
                    importance_score INTEGER,
                    related_stocks TEXT,
                    sector TEXT,
                    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """.replace("__ID_COL__", id_col)
            )
        )
        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS alerts (
                    id __ID_COL__,
                    article_id INTEGER NOT NULL,
                    alert_type TEXT,
                    message TEXT,
                    hit_keywords TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """.replace("__ID_COL__", id_col)
            )
        )
        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS keyword_alerts (
                    id __ID_COL__,
                    keyword TEXT NOT NULL UNIQUE,
                    category TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """.replace("__ID_COL__", id_col)
            )
        )
        for code, name, sector, market in DEFAULT_STOCK_MASTER:
            con.execute(
                text(
                    """
                    INSERT INTO stocks(stock_code, company_name, sector, market)
                    VALUES (:code, :name, :sector, :market)
                    ON CONFLICT (stock_code) DO NOTHING
                    """
                ),
                {"code": code, "name": name, "sector": sector, "market": market},
            )
    seed_default_keywords()


def seed_default_keywords() -> None:
    if not _is_sqlite():
        with engine.begin() as con:
            for keyword, category in DEFAULT_KEYWORDS:
                con.execute(
                    text(
                        """
                        INSERT INTO keyword_alerts(keyword, category, is_active)
                        VALUES (:keyword, :category, 1)
                        ON CONFLICT (keyword) DO NOTHING
                        """
                    ),
                    {"keyword": keyword, "category": category},
                )
        return

    with _connect() as conn:
        for keyword, category in DEFAULT_KEYWORDS:
            conn.execute(
                """
                INSERT OR IGNORE INTO keyword_alerts(keyword, category, is_active)
                VALUES (?, ?, 1)
                """,
                (keyword, category),
            )
        conn.commit()


def get_portfolio_tickers() -> list[str]:
    init_news_tables()
    try:
        if not _is_sqlite():
            rows = pd.read_sql(text("SELECT stock_code FROM portfolio ORDER BY stock_code"), con=engine)
            return rows["stock_code"].astype(str).tolist() if not rows.empty else []
        with _connect() as conn:
            rows = conn.execute("SELECT stock_code FROM portfolio ORDER BY stock_code").fetchall()
        return [str(r["stock_code"]) for r in rows]
    except sqlite3.OperationalError:
        return []


def get_stock_master_tickers() -> list[str]:
    init_news_tables()
    try:
        if not _is_sqlite():
            rows = pd.read_sql(text("SELECT stock_code FROM stocks ORDER BY stock_code"), con=engine)
            codes = rows["stock_code"].astype(str).tolist() if not rows.empty else []
            if codes:
                return codes
        with _connect() as conn:
            rows = conn.execute("SELECT stock_code FROM stocks ORDER BY stock_code").fetchall()
        codes = [str(r["stock_code"]) for r in rows]
        if codes:
            return codes
    except sqlite3.OperationalError:
        pass
    # Hard fallback so researcher stage never fails on missing stock master.
    return [c for c, _, _, _ in DEFAULT_STOCK_MASTER]


def get_news_feed_df(
    period: str = "今日",
    sentiment: str = "すべて",
    min_importance: int = 1,
    sources: list[str] | None = None,
    portfolio_only: bool = False,
) -> pd.DataFrame:
    init_news_tables()
    now = datetime.now()
    start_map = {
        "今日": now.replace(hour=0, minute=0, second=0, microsecond=0),
        "直近3日": now - timedelta(days=3),
        "直近1週間": now - timedelta(days=7),
        "直近1ヶ月": now - timedelta(days=30),
    }
    start_dt = start_map.get(period, now - timedelta(days=1))

    if not _is_sqlite():
        where = ["a.published_at >= :start_dt"]
        params: dict[str, object] = {"start_dt": start_dt.strftime("%Y-%m-%d %H:%M:%S"), "min_importance": min_importance}
        if sentiment == "ポジティブのみ":
            where.append("s.sentiment_score > 0")
        elif sentiment == "ネガティブのみ":
            where.append("s.sentiment_score < 0")
        where.append("COALESCE(s.importance_score, 1) >= :min_importance")
        if sources:
            names = []
            for i, src in enumerate(sources):
                key = f"src{i}"
                names.append(f":{key}")
                params[key] = src
            where.append(f"a.source IN ({','.join(names)})")
        if portfolio_only:
            portfolio = get_portfolio_tickers()
            if portfolio:
                clauses = []
                for i, ticker in enumerate(portfolio):
                    key = f"pf{i}"
                    clauses.append(f"POSITION(:{key} IN COALESCE(s.related_stocks, '')) > 0")
                    params[key] = ticker
                where.append("(" + " OR ".join(clauses) + ")")
            else:
                return pd.DataFrame()
        sql = f"""
        SELECT
          a.id,
          a.title,
          a.url,
          a.source,
          a.published_at,
          a.summary_ja,
          COALESCE(s.sentiment_score, 0) AS sentiment_score,
          COALESCE(s.sentiment_label, 'neutral') AS sentiment_label,
          COALESCE(s.importance_score, 1) AS importance_score,
          COALESCE(s.related_stocks, '') AS related_stocks,
          COALESCE(s.sector, 'その他') AS sector
        FROM news_articles a
        LEFT JOIN news_sentiments s ON s.article_id = a.id
        WHERE {" AND ".join(where)}
        ORDER BY a.published_at DESC, a.id DESC
        """
        return pd.read_sql(text(sql), con=engine, params=params)

    where = ["a.published_at >= ?"]
    params: list[object] = [start_dt.strftime("%Y-%m-%d %H:%M:%S")]

    if sentiment == "ポジティブのみ":
        where.append("s.sentiment_score > 0")
    elif sentiment == "ネガティブのみ":
        where.append("s.sentiment_score < 0")

    where.append("COALESCE(s.importance_score, 1) >= ?")
    params.append(min_importance)

    if sources:
        placeholders = ",".join("?" for _ in sources)
        where.append(f"a.source IN ({placeholders})")
        params.extend(sources)

    if portfolio_only:
        portfolio = get_portfolio_tickers()
        if portfolio:
            match_clauses = ["instr(COALESCE(s.related_stocks, ''), ?) > 0" for _ in portfolio]
            where.append("(" + " OR ".join(match_clauses) + ")")
            params.extend(portfolio)
        else:
            return pd.DataFrame()

    sql = f"""
    SELECT
      a.id,
      a.title,
      a.url,
      a.source,
      a.published_at,
      a.summary_ja,
      COALESCE(s.sentiment_score, 0) AS sentiment_score,
      COALESCE(s.sentiment_label, 'neutral') AS sentiment_label,
      COALESCE(s.importance_score, 1) AS importance_score,
      COALESCE(s.related_stocks, '') AS related_stocks,
      COALESCE(s.sector, 'その他') AS sector
    FROM news_articles a
    LEFT JOIN news_sentiments s ON s.article_id = a.id
    WHERE {" AND ".join(where)}
    ORDER BY a.published_at DESC, a.id DESC
    """
    with _connect() as conn:
        return pd.read_sql_query(sql, conn, params=params)


def get_sentiment_trend_df(days: int = 30) -> pd.DataFrame:
    init_news_tables()
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    if not _is_sqlite():
        sql = """
        SELECT
          CAST(a.published_at AS DATE) AS d,
          AVG(COALESCE(s.sentiment_score, 0)) AS avg_sentiment,
          COUNT(*) AS cnt
        FROM news_articles a
        LEFT JOIN news_sentiments s ON s.article_id = a.id
        WHERE a.published_at >= :start_dt
        GROUP BY CAST(a.published_at AS DATE)
        ORDER BY d
        """
        return pd.read_sql(text(sql), con=engine, params={"start_dt": start})
    sql = """
    SELECT
      date(a.published_at) AS d,
      AVG(COALESCE(s.sentiment_score, 0)) AS avg_sentiment,
      COUNT(*) AS cnt
    FROM news_articles a
    LEFT JOIN news_sentiments s ON s.article_id = a.id
    WHERE a.published_at >= ?
    GROUP BY date(a.published_at)
    ORDER BY d
    """
    with _connect() as conn:
        return pd.read_sql_query(sql, conn, params=[start])


def get_sector_sentiment_heatmap_df(days: int = 7) -> pd.DataFrame:
    init_news_tables()
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    if not _is_sqlite():
        sql = """
        SELECT
          COALESCE(s.sector, 'その他') AS sector,
          CAST(a.published_at AS DATE) AS d,
          AVG(COALESCE(s.sentiment_score, 0)) AS avg_sentiment
        FROM news_articles a
        LEFT JOIN news_sentiments s ON s.article_id = a.id
        WHERE a.published_at >= :start_dt
        GROUP BY COALESCE(s.sector, 'その他'), CAST(a.published_at AS DATE)
        ORDER BY sector, d
        """
        return pd.read_sql(text(sql), con=engine, params={"start_dt": start})
    sql = """
    SELECT
      COALESCE(s.sector, 'その他') AS sector,
      date(a.published_at) AS d,
      AVG(COALESCE(s.sentiment_score, 0)) AS avg_sentiment
    FROM news_articles a
    LEFT JOIN news_sentiments s ON s.article_id = a.id
    WHERE a.published_at >= ?
    GROUP BY COALESCE(s.sector, 'その他'), date(a.published_at)
    ORDER BY sector, d
    """
    with _connect() as conn:
        return pd.read_sql_query(sql, conn, params=[start])


def get_stock_news_df(
    ticker: str,
    period_days: int = 30,
    sentiment: str = "すべて",
    min_importance: int = 1,
) -> pd.DataFrame:
    init_news_tables()
    start = (datetime.now() - timedelta(days=period_days)).strftime("%Y-%m-%d %H:%M:%S")
    if not _is_sqlite():
        where = [
            "a.published_at >= :start_dt",
            "POSITION(:ticker IN COALESCE(s.related_stocks, '')) > 0",
            "COALESCE(s.importance_score, 1) >= :min_importance",
        ]
        params = {"start_dt": start, "ticker": ticker.upper(), "min_importance": min_importance}
        if sentiment == "ポジティブのみ":
            where.append("s.sentiment_score > 0")
        elif sentiment == "ネガティブのみ":
            where.append("s.sentiment_score < 0")
        sql = f"""
        SELECT
          a.id,
          a.title,
          a.url,
          a.source,
          a.published_at,
          a.summary_ja,
          COALESCE(s.sentiment_score, 0) AS sentiment_score,
          COALESCE(s.importance_score, 1) AS importance_score,
          COALESCE(s.related_stocks, '') AS related_stocks,
          COALESCE(s.sector, 'その他') AS sector
        FROM news_articles a
        LEFT JOIN news_sentiments s ON s.article_id = a.id
        WHERE {" AND ".join(where)}
        ORDER BY a.published_at DESC
        """
        return pd.read_sql(text(sql), con=engine, params=params)
    where = ["a.published_at >= ?", "instr(COALESCE(s.related_stocks, ''), ?) > 0", "COALESCE(s.importance_score, 1) >= ?"]
    params: list[object] = [start, ticker.upper(), min_importance]

    if sentiment == "ポジティブのみ":
        where.append("s.sentiment_score > 0")
    elif sentiment == "ネガティブのみ":
        where.append("s.sentiment_score < 0")

    sql = f"""
    SELECT
      a.id,
      a.title,
      a.url,
      a.source,
      a.published_at,
      a.summary_ja,
      COALESCE(s.sentiment_score, 0) AS sentiment_score,
      COALESCE(s.importance_score, 1) AS importance_score,
      COALESCE(s.related_stocks, '') AS related_stocks,
      COALESCE(s.sector, 'その他') AS sector
    FROM news_articles a
    LEFT JOIN news_sentiments s ON s.article_id = a.id
    WHERE {" AND ".join(where)}
    ORDER BY a.published_at DESC
    """
    with _connect() as conn:
        return pd.read_sql_query(sql, conn, params=params)


def list_keyword_alerts() -> pd.DataFrame:
    init_news_tables()
    if not _is_sqlite():
        return pd.read_sql(
            text("SELECT id, keyword, category, is_active, created_at FROM keyword_alerts ORDER BY id DESC"),
            con=engine,
        )
    with _connect() as conn:
        return pd.read_sql_query(
            "SELECT id, keyword, category, is_active, created_at FROM keyword_alerts ORDER BY id DESC", conn
        )


def add_keyword_alert(keyword: str, category: str) -> tuple[bool, str]:
    init_news_tables()
    kw = keyword.strip()
    if not kw:
        return False, "キーワードを入力してください。"
    if not _is_sqlite():
        with engine.begin() as con:
            exists = con.execute(
                text("SELECT 1 FROM keyword_alerts WHERE keyword = :keyword LIMIT 1"),
                {"keyword": kw},
            ).first()
            if not exists:
                con.execute(
                    text(
                        """
                        INSERT INTO keyword_alerts(keyword, category, is_active)
                        VALUES (:keyword, :category, 1)
                        """
                    ),
                    {"keyword": kw, "category": category},
                )
        inserted = reindex_keyword_hits_for_keyword(kw)
        if exists:
            return True, f"同じキーワードは既に登録済みです。既存ニュースへの再反映: {inserted}件"
        return True, f"キーワードを追加しました。既存ニュースへの反映: {inserted}件"
    try:
        with _connect() as conn:
            conn.execute(
                "INSERT INTO keyword_alerts(keyword, category, is_active) VALUES (?, ?, 1)",
                (kw, category),
            )
            conn.commit()
        inserted = reindex_keyword_hits_for_keyword(kw)
        return True, f"キーワードを追加しました。既存ニュースへの反映: {inserted}件"
    except sqlite3.IntegrityError:
        inserted = reindex_keyword_hits_for_keyword(kw)
        return True, f"同じキーワードは既に登録済みです。既存ニュースへの再反映: {inserted}件"


def delete_keyword_alert(keyword_id: int) -> None:
    init_news_tables()
    if not _is_sqlite():
        with engine.begin() as con:
            con.execute(text("DELETE FROM keyword_alerts WHERE id = :id"), {"id": int(keyword_id)})
        return
    with _connect() as conn:
        conn.execute("DELETE FROM keyword_alerts WHERE id = ?", (keyword_id,))
        conn.commit()


def get_keyword_hits_df(days: int = 7) -> pd.DataFrame:
    init_news_tables()
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    if not _is_sqlite():
        sql = """
        WITH dedup AS (
          SELECT
            al.article_id,
            COALESCE(al.hit_keywords, '') AS hit_keywords,
            MAX(al.created_at) AS latest_created_at
          FROM alerts al
          WHERE al.created_at >= :start_dt
            AND COALESCE(al.hit_keywords, '') <> ''
          GROUP BY al.article_id, COALESCE(al.hit_keywords, '')
        )
        SELECT
          d.hit_keywords,
          a.title,
          a.url,
          a.published_at,
          d.latest_created_at AS created_at
        FROM dedup d
        JOIN news_articles a ON a.id = d.article_id
        ORDER BY d.latest_created_at DESC
        """
        return pd.read_sql(text(sql), con=engine, params={"start_dt": start})
    sql = """
    WITH dedup AS (
      SELECT
        al.article_id,
        COALESCE(al.hit_keywords, '') AS hit_keywords,
        MAX(al.created_at) AS latest_created_at
      FROM alerts al
      WHERE al.created_at >= ?
        AND COALESCE(al.hit_keywords, '') <> ''
      GROUP BY al.article_id, COALESCE(al.hit_keywords, '')
    )
    SELECT
      d.hit_keywords,
      a.title,
      a.url,
      a.published_at,
      d.latest_created_at AS created_at
    FROM dedup d
    JOIN news_articles a ON a.id = d.article_id
    ORDER BY d.latest_created_at DESC
    """
    with _connect() as conn:
        return pd.read_sql_query(sql, conn, params=[start])


def _keyword_variants(keyword: str) -> list[str]:
    kw = (keyword or "").strip()
    if not kw:
        return []
    variants: set[str] = {kw.lower()}
    for alias in KEYWORD_ALIAS_MAP.get(kw, []):
        variants.add(alias.lower())
    return [v for v in variants if v]


def reindex_keyword_hits_for_keyword(keyword: str) -> int:
    """Backfill keyword hits for existing articles right after keyword registration."""
    init_news_tables()
    variants = _keyword_variants(keyword)
    if not variants:
        return 0

    if not _is_sqlite():
        inserted = 0
        rows = pd.read_sql(
            text(
                """
                SELECT id, title, COALESCE(summary_ja, '') AS summary_ja, COALESCE(content, '') AS content
                FROM news_articles
                """
            ),
            con=engine,
        )
        with engine.begin() as con:
            for row in rows.itertuples(index=False):
                article_id = int(getattr(row, "id"))
                searchable = f"{getattr(row, 'title', '')} {getattr(row, 'summary_ja', '')} {getattr(row, 'content', '')}".lower()
                if not any(v in searchable for v in variants):
                    continue
                exists = con.execute(
                    text(
                        """
                        SELECT 1
                        FROM alerts
                        WHERE article_id = :article_id
                          AND LOWER(COALESCE(hit_keywords, '')) LIKE :kw
                        LIMIT 1
                        """
                    ),
                    {"article_id": article_id, "kw": f"%{keyword.lower()}%"},
                ).first()
                if exists:
                    continue
                con.execute(
                    text(
                        """
                        INSERT INTO alerts(article_id, alert_type, message, hit_keywords, created_at)
                        VALUES (:article_id, 'keyword_reindex', :message, :keyword, CURRENT_TIMESTAMP)
                        """
                    ),
                    {"article_id": article_id, "message": f"keyword hits: {keyword}", "keyword": keyword},
                )
                inserted += 1
        return inserted

    inserted = 0
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, title, COALESCE(summary_ja, '') AS summary_ja, COALESCE(content, '') AS content
            FROM news_articles
            """
        ).fetchall()

        for row in rows:
            article_id = int(row["id"])
            searchable = f"{row['title']} {row['summary_ja']} {row['content']}".lower()
            if not any(v in searchable for v in variants):
                continue

            exists = conn.execute(
                """
                SELECT 1
                FROM alerts
                WHERE article_id = ?
                  AND LOWER(COALESCE(hit_keywords, '')) LIKE ?
                LIMIT 1
                """,
                (article_id, f"%{keyword.lower()}%"),
            ).fetchone()
            if exists:
                continue

            conn.execute(
                """
                INSERT INTO alerts(article_id, alert_type, message, hit_keywords, created_at)
                VALUES (?, 'keyword_reindex', ?, ?, datetime('now'))
                """,
                (
                    article_id,
                    f"keyword hits: {keyword}",
                    keyword,
                ),
            )
            inserted += 1
        conn.commit()
    return inserted

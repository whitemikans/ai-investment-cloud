from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import text

from .models import engine


JST = ZoneInfo("Asia/Tokyo")


def _id_col_sql() -> str:
    backend = engine.url.get_backend_name().lower()
    return "INTEGER PRIMARY KEY AUTOINCREMENT" if backend == "sqlite" else "BIGSERIAL PRIMARY KEY"


def _table_columns(table_name: str) -> set[str]:
    backend = engine.url.get_backend_name().lower()
    if backend == "sqlite":
        q = text(f"PRAGMA table_info({table_name})")
        with engine.connect() as con:
            rows = con.execute(q).fetchall()
        return {str(r[1]) for r in rows}

    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = :table_name
        """
    )
    with engine.connect() as con:
        rows = con.execute(sql, {"table_name": table_name}).fetchall()
    return {str(r[0]) for r in rows}


def _ensure_tech_papers_columns() -> None:
    wanted: dict[str, str] = {
        "breakthrough_summary": "TEXT",
        "practical_years": "REAL",
        "beneficiary_summary": "TEXT",
        "action_items": "TEXT",
        "is_featured": "INTEGER",
        "analysis_model": "TEXT",
        "analysis_raw_json": "TEXT",
    }
    existing = _table_columns("tech_papers")
    with engine.begin() as con:
        for col, col_type in wanted.items():
            if col not in existing:
                con.execute(text(f"ALTER TABLE tech_papers ADD COLUMN {col} {col_type}"))


def init_tech_research_tables() -> None:
    id_col = _id_col_sql()
    with engine.begin() as con:
        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS tech_papers (
                    id __ID_COL__,
                    title TEXT NOT NULL,
                    authors TEXT,
                    summary TEXT,
                    categories TEXT,
                    published_at TEXT,
                    pdf_url TEXT,
                    source_url TEXT,
                    tech_theme TEXT,
                    impact_score REAL,
                    related_tickers TEXT,
                    recommendation TEXT,
                    breakthrough_summary TEXT,
                    practical_years REAL,
                    beneficiary_summary TEXT,
                    action_items TEXT,
                    is_featured INTEGER,
                    analysis_model TEXT,
                    analysis_raw_json TEXT,
                    created_at TEXT NOT NULL
                )
                """.replace("__ID_COL__", id_col)
            )
        )
        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS tech_hype_history (
                    id __ID_COL__,
                    as_of_date TEXT NOT NULL,
                    tech_theme TEXT NOT NULL,
                    hype_index REAL NOT NULL,
                    phase TEXT NOT NULL,
                    source_breakdown_json TEXT,
                    created_at TEXT NOT NULL
                )
                """.replace("__ID_COL__", id_col)
            )
        )
        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS tech_patent_stats (
                    id __ID_COL__,
                    as_of_date TEXT NOT NULL,
                    tech_theme TEXT NOT NULL,
                    company TEXT NOT NULL,
                    patent_count INTEGER NOT NULL,
                    innovation_score REAL NOT NULL,
                    created_at TEXT NOT NULL
                )
                """.replace("__ID_COL__", id_col)
            )
        )
        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS tech_patent_yearly (
                    id __ID_COL__,
                    as_of_date TEXT NOT NULL,
                    tech_theme TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    patent_count INTEGER NOT NULL,
                    yoy_growth_pct REAL NOT NULL,
                    created_at TEXT NOT NULL
                )
                """.replace("__ID_COL__", id_col)
            )
        )
        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS tech_weekly_reports (
                    id __ID_COL__,
                    report_date TEXT NOT NULL,
                    title TEXT NOT NULL,
                    body TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """.replace("__ID_COL__", id_col)
            )
        )
    _ensure_tech_papers_columns()


def save_tech_papers(df: pd.DataFrame) -> int:
    init_tech_research_tables()
    if df is None or df.empty:
        return 0
    work = df.copy()
    defaults = {
        "title": "",
        "authors": "",
        "summary": "",
        "categories": "",
        "published_at": "",
        "pdf_url": "",
        "source_url": "",
        "tech_theme": "",
        "impact_score": 0.0,
        "related_tickers": "",
        "recommendation": "",
        "breakthrough_summary": "",
        "practical_years": None,
        "beneficiary_summary": "",
        "action_items": "",
        "is_featured": 0,
        "analysis_model": "",
        "analysis_raw_json": "",
    }
    for col, default in defaults.items():
        if col not in work.columns:
            work[col] = default
    keep_cols = [
        "title",
        "authors",
        "summary",
        "categories",
        "published_at",
        "pdf_url",
        "source_url",
        "tech_theme",
        "impact_score",
        "related_tickers",
        "recommendation",
        "breakthrough_summary",
        "practical_years",
        "beneficiary_summary",
        "action_items",
        "is_featured",
        "analysis_model",
        "analysis_raw_json",
    ]
    work = work[keep_cols]
    work["created_at"] = datetime.now(JST).isoformat(timespec="seconds")
    work.to_sql("tech_papers", con=engine, if_exists="append", index=False)
    return int(len(work))


def get_latest_tech_papers(limit: int = 100, theme: str | None = None) -> pd.DataFrame:
    init_tech_research_tables()
    sql = """
    SELECT *
    FROM tech_papers
    WHERE (:theme IS NULL OR tech_theme = :theme)
    ORDER BY published_at DESC, id DESC
    LIMIT :limit_n
    """
    return pd.read_sql(text(sql), con=engine, params={"theme": theme, "limit_n": int(limit)})


def replace_hype_history(df: pd.DataFrame) -> int:
    init_tech_research_tables()
    if df is None or df.empty:
        return 0
    work = df.copy()
    work["created_at"] = datetime.now(JST).isoformat(timespec="seconds")
    with engine.begin() as con:
        con.execute(text("DELETE FROM tech_hype_history"))
    work.to_sql("tech_hype_history", con=engine, if_exists="append", index=False)
    return int(len(work))


def get_hype_history() -> pd.DataFrame:
    init_tech_research_tables()
    sql = """
    SELECT *
    FROM tech_hype_history
    ORDER BY as_of_date ASC, tech_theme ASC
    """
    return pd.read_sql(text(sql), con=engine)


def replace_patent_stats(df: pd.DataFrame) -> int:
    init_tech_research_tables()
    if df is None or df.empty:
        return 0
    work = df.copy()
    work["created_at"] = datetime.now(JST).isoformat(timespec="seconds")
    with engine.begin() as con:
        con.execute(text("DELETE FROM tech_patent_stats"))
    work.to_sql("tech_patent_stats", con=engine, if_exists="append", index=False)
    return int(len(work))


def get_patent_stats() -> pd.DataFrame:
    init_tech_research_tables()
    sql = """
    SELECT *
    FROM tech_patent_stats
    ORDER BY innovation_score DESC, patent_count DESC
    """
    return pd.read_sql(text(sql), con=engine)


def replace_patent_yearly(df: pd.DataFrame) -> int:
    init_tech_research_tables()
    if df is None or df.empty:
        return 0
    work = df.copy()
    work["created_at"] = datetime.now(JST).isoformat(timespec="seconds")
    with engine.begin() as con:
        con.execute(text("DELETE FROM tech_patent_yearly"))
    work.to_sql("tech_patent_yearly", con=engine, if_exists="append", index=False)
    return int(len(work))


def get_patent_yearly() -> pd.DataFrame:
    init_tech_research_tables()
    sql = """
    SELECT *
    FROM tech_patent_yearly
    ORDER BY year ASC, tech_theme ASC
    """
    return pd.read_sql(text(sql), con=engine)


def save_weekly_report(title: str, body: str) -> int:
    init_tech_research_tables()
    now = datetime.now(JST).isoformat(timespec="seconds")
    report_date = datetime.now(JST).date().isoformat()
    with engine.begin() as con:
        con.execute(
            text(
                """
                INSERT INTO tech_weekly_reports(report_date, title, body, created_at)
                VALUES(:report_date, :title, :body, :created_at)
                """
            ),
            {"report_date": report_date, "title": str(title), "body": str(body), "created_at": now},
        )
    return 1

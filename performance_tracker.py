from __future__ import annotations

import json
import re
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
from sqlalchemy import text

from db.ai_team_utils import init_ai_team_tables
from db.models import engine


BUY_WORDS = {"buy", "買い", "strong_buy", "long"}
SELL_WORDS = {"sell", "売り", "short"}


def _backend_name() -> str:
    return engine.url.get_backend_name().lower()


def init_performance_table() -> None:
    backend = _backend_name()
    is_sqlite = backend == "sqlite"
    id_col = "INTEGER PRIMARY KEY AUTOINCREMENT" if is_sqlite else "BIGSERIAL PRIMARY KEY"

    sql = (
        """
        CREATE TABLE IF NOT EXISTS agent_recommendation_performance (
            id __ID_COL__,
            recommendation_date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            source TEXT,
            ai_recommendation TEXT,
            human_decision TEXT,
            return_1w REAL,
            return_1m REAL,
            return_3m REAL,
            buy_win_1m INTEGER,
            created_at TEXT NOT NULL
        )
        """.replace("__ID_COL__", id_col)
    )
    with engine.begin() as con:
        con.execute(text(sql))


def _safe_read_sql(query: str, params: dict | None = None) -> tuple[pd.DataFrame, str | None]:
    try:
        return pd.read_sql(text(query), con=engine, params=params or {}), None
    except Exception as exc:
        return pd.DataFrame(), str(exc)


def _normalize_ticker(raw: str) -> str:
    t = re.sub(r"[^A-Za-z0-9=^._-]", "", str(raw or "").strip().upper())
    return t


def _parse_related_tickers(value: str) -> list[str]:
    parts = re.split(r"[,;/|\s]+", str(value or ""))
    out = []
    for p in parts:
        t = _normalize_ticker(p)
        if t and len(t) <= 16:
            out.append(t)
    return sorted(set(out))


def _to_dt(v: str) -> datetime | None:
    s = str(v or "").strip()
    if not s:
        return None
    try:
        ts = pd.to_datetime(s, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.to_pydatetime()
    except Exception:
        return None


def _recommend_from_sentiment(sentiment: str) -> str:
    s = str(sentiment or "").strip().lower()
    if s in {"positive", "ポジティブ", "bullish"}:
        return "buy"
    if s in {"negative", "ネガティブ", "bearish"}:
        return "sell"
    return "hold"


def load_base_recommendations(lookback_days: int = 365) -> tuple[pd.DataFrame, dict]:
    cutoff = (datetime.now() - timedelta(days=max(1, int(lookback_days)))).strftime("%Y-%m-%d")

    feedback, fb_err = _safe_read_sql(
        """
        SELECT
          COALESCE(date, created_at) AS recommendation_date,
          COALESCE(ticker, '') AS ticker,
          'feedback' AS source,
          COALESCE(ai_recommendation, 'hold') AS ai_recommendation,
          COALESCE(human_decision, '') AS human_decision
        FROM agent_feedback
        WHERE COALESCE(date, created_at) >= :cutoff
          AND COALESCE(ticker, '') <> ''
        """,
        {"cutoff": cutoff},
    )

    research, rs_err = _safe_read_sql(
        """
        SELECT
          COALESCE(date, created_at) AS recommendation_date,
          COALESCE(related_tickers, '') AS related_tickers,
          COALESCE(sentiment, 'neutral') AS sentiment
        FROM agent_research_results
        WHERE COALESCE(date, created_at) >= :cutoff
        """,
        {"cutoff": cutoff},
    )

    report_rows, rp_err = _safe_read_sql(
        """
        SELECT
          COALESCE(created_at, '') AS recommendation_date,
          COALESCE(recommendations_json, '[]') AS recommendations_json
        FROM ai_team_reports
        WHERE COALESCE(created_at, '') >= :cutoff
        ORDER BY created_at DESC
        LIMIT 300
        """,
        {"cutoff": cutoff},
    )

    rec_rows: list[dict] = []
    if not research.empty:
        for r in research.itertuples(index=False):
            dt = _to_dt(getattr(r, "recommendation_date", ""))
            if dt is None:
                continue
            rec = _recommend_from_sentiment(getattr(r, "sentiment", ""))
            for t in _parse_related_tickers(getattr(r, "related_tickers", "")):
                rec_rows.append(
                    {
                        "recommendation_date": dt.strftime("%Y-%m-%d"),
                        "ticker": t,
                        "source": "research",
                        "ai_recommendation": rec,
                        "human_decision": "",
                    }
                )

    research_expanded = pd.DataFrame(rec_rows)

    report_expanded_rows: list[dict] = []
    if not report_rows.empty:
        for rr in report_rows.itertuples(index=False):
            dt = _to_dt(getattr(rr, "recommendation_date", ""))
            if dt is None:
                continue
            raw = str(getattr(rr, "recommendations_json", "[]") or "[]")
            try:
                parsed_raw = json.loads(raw)
            except Exception:
                parsed_raw = []
            if not isinstance(parsed_raw, list) or not parsed_raw:
                continue
            for item in parsed_raw:
                if not isinstance(item, dict):
                    continue
                t = _normalize_ticker(item.get("ticker", ""))
                if not t:
                    continue
                action = str(item.get("action", "hold"))
                report_expanded_rows.append(
                    {
                        "recommendation_date": dt.strftime("%Y-%m-%d"),
                        "ticker": t,
                        "source": "ai_team_report",
                        "ai_recommendation": action,
                        "human_decision": "",
                    }
                )
    report_expanded = pd.DataFrame(report_expanded_rows)

    frames = [df for df in [feedback, research_expanded, report_expanded] if not df.empty]
    if not frames:
        return (
            pd.DataFrame(columns=["recommendation_date", "ticker", "source", "ai_recommendation", "human_decision"]),
            {
                "feedback_rows": int(len(feedback)),
                "research_rows": int(len(research)),
                "research_expanded_rows": int(len(research_expanded)),
                "report_rows": int(len(report_rows)),
                "report_expanded_rows": int(len(report_expanded)),
                "feedback_error": fb_err,
                "research_error": rs_err,
                "report_error": rp_err,
            },
        )

    base = pd.concat(frames, ignore_index=True)
    base["ticker"] = base["ticker"].map(_normalize_ticker)
    base = base[base["ticker"] != ""].copy()
    base = base.drop_duplicates(subset=["recommendation_date", "ticker", "source", "ai_recommendation"]).reset_index(drop=True)
    return (
        base,
        {
            "feedback_rows": int(len(feedback)),
            "research_rows": int(len(research)),
            "research_expanded_rows": int(len(research_expanded)),
            "report_rows": int(len(report_rows)),
            "report_expanded_rows": int(len(report_expanded)),
            "feedback_error": fb_err,
            "research_error": rs_err,
            "report_error": rp_err,
        },
    )


def _history_for_ticker(ticker: str, start: datetime, end: datetime, cache: dict[str, pd.Series]) -> pd.Series:
    if ticker in cache:
        return cache[ticker]

    try:
        hist = yf.Ticker(ticker).history(start=start.date().isoformat(), end=(end + timedelta(days=3)).date().isoformat())
        if hist is None or hist.empty:
            cache[ticker] = pd.Series(dtype=float)
            return cache[ticker]
        close = pd.to_numeric(hist["Close"], errors="coerce").dropna()
        close.index = pd.to_datetime(close.index).tz_localize(None)
        cache[ticker] = close
        return close
    except Exception:
        cache[ticker] = pd.Series(dtype=float)
        return cache[ticker]


def _forward_return(close: pd.Series, base_date: datetime, days: int) -> float | None:
    if close.empty:
        return None

    idx_base = close.index.searchsorted(pd.Timestamp(base_date.date()), side="left")
    if idx_base >= len(close):
        return None
    entry = float(close.iloc[idx_base])
    if entry == 0:
        return None

    target_dt = pd.Timestamp((base_date + timedelta(days=int(days))).date())
    idx_t = close.index.searchsorted(target_dt, side="left")
    if idx_t >= len(close):
        return None
    target = float(close.iloc[idx_t])
    return float((target / entry - 1.0) * 100.0)


def track_recommendation_performance(lookback_days: int = 365) -> dict:
    init_performance_table()
    try:
        init_ai_team_tables()
    except Exception:
        pass

    base, src = load_base_recommendations(lookback_days=lookback_days)
    if base.empty:
        return {
            "tracked": 0,
            "message": "no recommendations",
            "source_status": src,
        }

    base_dates = pd.to_datetime(base["recommendation_date"], errors="coerce").dropna()
    if base_dates.empty:
        return {"tracked": 0, "message": "no valid dates"}
    start = base_dates.min().to_pydatetime() - timedelta(days=7)
    end = datetime.now() + timedelta(days=2)

    cache: dict[str, pd.Series] = {}
    rows: list[dict] = []
    now = datetime.now().isoformat(timespec="seconds")

    for r in base.itertuples(index=False):
        dt = _to_dt(getattr(r, "recommendation_date", ""))
        ticker = _normalize_ticker(getattr(r, "ticker", ""))
        if dt is None or not ticker:
            continue

        close = _history_for_ticker(ticker, start, end, cache)
        ret_1w = _forward_return(close, dt, 7)
        ret_1m = _forward_return(close, dt, 30)
        ret_3m = _forward_return(close, dt, 90)

        ai_rec = str(getattr(r, "ai_recommendation", "") or "").strip().lower()
        buy_win_1m = None
        if ai_rec in BUY_WORDS and ret_1m is not None:
            buy_win_1m = 1 if float(ret_1m) > 0 else 0

        rows.append(
            {
                "recommendation_date": dt.strftime("%Y-%m-%d"),
                "ticker": ticker,
                "source": str(getattr(r, "source", "") or ""),
                "ai_recommendation": str(getattr(r, "ai_recommendation", "") or ""),
                "human_decision": str(getattr(r, "human_decision", "") or ""),
                "return_1w": ret_1w,
                "return_1m": ret_1m,
                "return_3m": ret_3m,
                "buy_win_1m": buy_win_1m,
                "created_at": now,
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return {"tracked": 0, "message": "no computed rows", "source_status": src}

    with engine.begin() as con:
        con.execute(text("DELETE FROM agent_recommendation_performance"))
    out.to_sql("agent_recommendation_performance", con=engine, if_exists="append", index=False)

    buy_df = out[out["ai_recommendation"].astype(str).str.lower().isin(BUY_WORDS)]
    buy_win_rate = float(buy_df["buy_win_1m"].dropna().mean() * 100.0) if not buy_df.empty else 0.0
    return {
        "tracked": int(len(out)),
        "buy_recommendations": int(len(buy_df)),
        "buy_win_rate_1m": round(buy_win_rate, 2),
        "source_status": src,
    }


def load_performance_data(limit: int = 2000) -> pd.DataFrame:
    init_performance_table()
    df, _err = _safe_read_sql(
        """
        SELECT *
        FROM agent_recommendation_performance
        ORDER BY recommendation_date DESC, id DESC
        LIMIT :n
        """,
        {"n": int(limit)},
    )
    return df


def summarize_recent_accuracy(days: int = 90) -> dict:
    df = load_performance_data(limit=5000)
    if df.empty:
        return {"window_days": int(days), "buy_win_rate_1m": None, "samples": 0}

    df["recommendation_date"] = pd.to_datetime(df["recommendation_date"], errors="coerce")
    cutoff = pd.Timestamp(datetime.now() - timedelta(days=int(days)))
    use = df[df["recommendation_date"] >= cutoff].copy()
    if use.empty:
        return {"window_days": int(days), "buy_win_rate_1m": None, "samples": 0}

    buy = use[use["ai_recommendation"].astype(str).str.lower().isin(BUY_WORDS)].copy()
    if buy.empty:
        return {"window_days": int(days), "buy_win_rate_1m": None, "samples": int(len(use))}

    win = buy["buy_win_1m"].dropna()
    if win.empty:
        return {"window_days": int(days), "buy_win_rate_1m": None, "samples": int(len(use))}

    return {
        "window_days": int(days),
        "buy_win_rate_1m": float(win.mean() * 100.0),
        "samples": int(len(buy)),
    }


if __name__ == "__main__":
    result = track_recommendation_performance(lookback_days=540)
    print(result)

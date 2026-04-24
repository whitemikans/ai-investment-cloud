from __future__ import annotations

import json
from datetime import datetime

import feedparser
import pandas as pd
import yfinance as yf

from db.ai_team_utils import init_ai_team_tables
from db.models import engine
from utils.fire_simulator import simulate_fire_monte_carlo  # noqa: F401 (keeps dependency graph warm)


try:
    from crewai.tools import tool
except Exception:
    def tool(_name: str):  # type: ignore
        def deco(func):
            return func
        return deco


@tool("RSSニュース取得")
def fetch_news_from_rss(feed_urls: str) -> str:
    """指定されたRSS URL(カンマ区切り)からニュースを取得してJSON文字列で返します。"""
    urls = [u.strip() for u in str(feed_urls).split(",") if u.strip()]
    rows: list[dict] = []
    for url in urls:
        try:
            feed = feedparser.parse(url)
            for e in (feed.entries or [])[:10]:
                rows.append(
                    {
                        "title": str(e.get("title", "")),
                        "url": str(e.get("link", "")),
                        "published_at": str(e.get("published", "")),
                        "summary": str(e.get("summary", ""))[:260],
                        "source_url": url,
                    }
                )
        except Exception:
            continue
    return json.dumps(rows, ensure_ascii=False)


@tool("株価データ取得")
def fetch_stock_data(ticker: str) -> str:
    """指定ティッカーの直近30日データとテクニカル概要を返します。"""
    code = str(ticker).strip().upper()
    if not code:
        return json.dumps({"error": "ticker is empty"}, ensure_ascii=False)

    df = yf.Ticker(code).history(period="1mo")
    if df is None or df.empty:
        return json.dumps({"ticker": code, "error": "data not found"}, ensure_ascii=False)

    close = pd.to_numeric(df["Close"], errors="coerce").dropna()
    vol = pd.to_numeric(df["Volume"], errors="coerce").dropna()
    ma5 = float(close.rolling(5).mean().iloc[-1]) if len(close) >= 5 else float(close.iloc[-1])
    ma25 = float(close.rolling(25).mean().iloc[-1]) if len(close) >= 25 else float(close.mean())
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss.replace(0, pd.NA)
    rsi = float((100 - (100 / (1 + rs))).fillna(50).iloc[-1])

    result = {
        "ticker": code,
        "as_of": datetime.now().isoformat(timespec="seconds"),
        "current_price": float(close.iloc[-1]),
        "change_1d_pct": float((close.iloc[-1] / close.iloc[-2] - 1) * 100) if len(close) >= 2 else 0.0,
        "change_1m_pct": float((close.iloc[-1] / close.iloc[0] - 1) * 100) if len(close) >= 2 else 0.0,
        "ma5": ma5,
        "ma25": ma25,
        "rsi14": rsi,
        "volume_avg": float(vol.mean()) if not vol.empty else 0.0,
        "trend": "上昇" if close.iloc[-1] > ma5 else "下降",
    }
    return json.dumps(result, ensure_ascii=False)


@tool("データベース保存")
def save_to_database(table_name: str, data: str) -> str:
    """指定テーブルへJSONデータを保存して、保存件数を返します。"""
    init_ai_team_tables()
    table = str(table_name).strip()
    if not table:
        return "table_nameが空です。"

    try:
        parsed = json.loads(data)
    except Exception:
        return "dataはJSON形式で指定してください。"
    rows = parsed if isinstance(parsed, list) else [parsed]
    if not rows:
        return "保存対象データが0件です。"

    df = pd.DataFrame(rows)
    if df.empty:
        return "保存対象データが空です。"
    df.to_sql(table, con=engine, if_exists="append", index=False)
    return f"✅ {len(df)}件を {table} に保存しました。"


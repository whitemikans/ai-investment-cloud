from __future__ import annotations

from datetime import datetime
import os

import pandas as pd
import requests
import yfinance as yf
from sqlalchemy import create_engine, text

from config import get_database_url


def _send_discord(message: str) -> None:
    webhook = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not webhook:
        print("[INFO] DISCORD_WEBHOOK_URL is not set. Skip notify.")
        return
    try:
        requests.post(webhook, json={"content": message}, timeout=8)
    except Exception as exc:
        print(f"[WARN] Discord notify failed: {exc}")


def _ensure_tables(engine) -> None:
    ddl_targets = """
    CREATE TABLE IF NOT EXISTS portfolio_targets (
        id INTEGER PRIMARY KEY,
        ticker VARCHAR(16) NOT NULL UNIQUE,
        target_weight FLOAT NOT NULL,
        quantity FLOAT NOT NULL DEFAULT 0,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """
    ddl_hist = """
    CREATE TABLE IF NOT EXISTS rebalance_history (
        id INTEGER PRIMARY KEY,
        checked_at TIMESTAMP NOT NULL,
        threshold FLOAT NOT NULL,
        needs_rebalance BOOLEAN NOT NULL,
        details TEXT NOT NULL DEFAULT ''
    )
    """
    with engine.begin() as con:
        con.execute(text(ddl_targets))
        con.execute(text(ddl_hist))


def _load_targets(engine) -> pd.DataFrame:
    with engine.connect() as con:
        df = pd.read_sql("SELECT ticker, target_weight, quantity FROM portfolio_targets ORDER BY ticker", con)
    return df


def _price_of(ticker: str) -> float:
    t = yf.Ticker(ticker)
    hist = t.history(period="5d")
    if hist.empty:
        return 0.0
    return float(hist["Close"].iloc[-1])


def check_rebalance(threshold: float = 0.05) -> tuple[bool, str]:
    engine = create_engine(get_database_url(), future=True)
    _ensure_tables(engine)
    targets = _load_targets(engine)
    if targets.empty:
        return False, "portfolio_targets が空です。"

    rows = []
    total_value = 0.0
    for _, row in targets.iterrows():
        ticker = str(row["ticker"]).upper()
        qty = float(row["quantity"])
        price = _price_of(ticker)
        value = qty * price
        total_value += value
        rows.append(
            {
                "ticker": ticker,
                "target_weight": float(row["target_weight"]),
                "quantity": qty,
                "price": price,
                "value": value,
            }
        )
    data = pd.DataFrame(rows)
    if total_value <= 0:
        return False, "評価額合計が0です。"

    data["actual_weight"] = data["value"] / total_value
    data["deviation"] = data["actual_weight"] - data["target_weight"]
    data["is_over"] = data["deviation"].abs() > threshold

    hits = data[data["is_over"]]
    needs_rebalance = not hits.empty
    if needs_rebalance:
        details = ", ".join([f"{r.ticker}:{r.deviation:+.2%}" for r in hits.itertuples()])
        message = (
            f"⚠️ リバランス警告 ({datetime.now():%Y-%m-%d %H:%M})\n"
            f"閾値 {threshold:.1%} を超過: {details}"
        )
    else:
        details = ""
        message = f"✅ 本日のリバランスチェック完了。閾値 {threshold:.1%} 超過なし。"

    with engine.begin() as con:
        con.execute(
            text(
                """
                INSERT INTO rebalance_history (checked_at, threshold, needs_rebalance, details)
                VALUES (:checked_at, :threshold, :needs_rebalance, :details)
                """
            ),
            {
                "checked_at": datetime.now(),
                "threshold": threshold,
                "needs_rebalance": needs_rebalance,
                "details": details,
            },
        )
    _send_discord(message)
    return needs_rebalance, message


if __name__ == "__main__":
    env_threshold = os.getenv("REBALANCE_THRESHOLD", "0.05").strip()
    try:
        threshold = float(env_threshold)
    except Exception:
        threshold = 0.05
    _, output = check_rebalance(threshold=threshold)
    print(output)


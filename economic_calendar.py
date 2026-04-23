from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Callable

import pandas as pd
import requests


# FRED API key guide:
# 1) Open https://fred.stlouisfed.org/docs/api/api_key.html
# 2) Sign in and request an API key
# 3) Set environment variable: FRED_API_KEY=<your_key>

FRED_API_KEY_ENV = "FRED_API_KEY"
ESTAT_API_KEY_ENV = "ESTAT_API_KEY"
DISCORD_WEBHOOK_ENV = "DISCORD_WEBHOOK_URL"

FRED_SERIES = {
    "GDP成長率（米国）": "A191RL1Q225SBEA",  # Real GDP Percent Change (annual rate)
    "非農業部門雇用者数（米国）": "PAYEMS",  # Total Nonfarm Payrolls
    "CPI（米国）": "CPIAUCSL",  # Consumer Price Index
    "FF金利（米国）": "FEDFUNDS",  # Federal Funds Effective Rate
    "10年国債利回り（米国）": "DGS10",  # 10-Year Treasury Constant Maturity Rate
}

# e-Stat IDs can vary by dataset version; these are common examples.
ESTAT_SERIES = {
    "GDP成長率（日本）": {"statsDataId": "0003448236", "label": "四半期GDP（参考）"},
    "CPI（日本）": {"statsDataId": "0003427113", "label": "全国CPI（参考）"},
    "完全失業率（日本）": {"statsDataId": "0003289170", "label": "労働力調査（参考）"},
}


@dataclass
class IndicatorPoint:
    indicator: str
    latest_value: float | None
    latest_date: str | None
    previous_value: float | None
    previous_date: str | None
    source: str
    note: str = ""


def _safe_float(v: str | float | int | None) -> float | None:
    try:
        if v is None or v == "." or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _next_weekday_of_month(year: int, month: int, weekday: int, nth: int) -> date:
    d = date(year, month, 1)
    while d.weekday() != weekday:
        d += timedelta(days=1)
    d += timedelta(days=7 * (nth - 1))
    return d


def _next_monthly_day(base: date, day: int) -> date:
    y, m = base.year, base.month
    target = date(y, m, min(day, 28))
    if target < base:
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
        target = date(y, m, min(day, 28))
    return target


def _next_quarter_release(base: date, months: list[int], day: int) -> date:
    year = base.year
    candidates = [date(year, m, min(day, 28)) for m in months]
    future = [d for d in candidates if d >= base]
    if future:
        return min(future)
    return date(year + 1, months[0], min(day, 28))


def _build_release_schedule(today: date | None = None) -> pd.DataFrame:
    t = today or date.today()
    rows = [
        {"指標": "GDP成長率（米国）", "国": "米国", "次回発表日": _next_quarter_release(t, [1, 4, 7, 10], 25), "注目": "⭐"},
        {"指標": "非農業部門雇用者数（米国）", "国": "米国", "次回発表日": _next_weekday_of_month(t.year + (1 if t.month == 12 and _next_weekday_of_month(t.year, t.month, 4, 1) < t else 0), (t.month % 12) + (1 if _next_weekday_of_month(t.year, t.month, 4, 1) < t else 0), 4, 1) if False else None},
    ]
    # explicit monthly rules
    payroll = _next_weekday_of_month(t.year, t.month, 4, 1)
    if payroll < t:
        nm = t.month + 1
        ny = t.year + (1 if nm > 12 else 0)
        nm = 1 if nm > 12 else nm
        payroll = _next_weekday_of_month(ny, nm, 4, 1)

    rows = [
        {"指標": "GDP成長率（米国）", "国": "米国", "次回発表日": _next_quarter_release(t, [1, 4, 7, 10], 25), "注目": "⭐"},
        {"指標": "非農業部門雇用者数（米国）", "国": "米国", "次回発表日": payroll, "注目": "⭐"},
        {"指標": "CPI（米国）", "国": "米国", "次回発表日": _next_monthly_day(t, 12), "注目": ""},
        {"指標": "FF金利（米国）", "国": "米国", "次回発表日": _next_monthly_day(t, 20), "注目": "⭐"},
        {"指標": "10年国債利回り（米国）", "国": "米国", "次回発表日": t + timedelta(days=1), "注目": ""},
        {"指標": "GDP成長率（日本）", "国": "日本", "次回発表日": _next_quarter_release(t, [2, 5, 8, 11], 15), "注目": ""},
        {"指標": "CPI（日本）", "国": "日本", "次回発表日": _next_monthly_day(t, 25), "注目": ""},
        {"指標": "完全失業率（日本）", "国": "日本", "次回発表日": _next_monthly_day(t, 30), "注目": ""},
    ]
    df = pd.DataFrame(rows)
    df["次回発表日"] = pd.to_datetime(df["次回発表日"]).dt.date
    return df.sort_values("次回発表日").reset_index(drop=True)


def _fetch_fred_observations(series_id: str, api_key: str, timeout_sec: int = 8) -> tuple[dict | None, dict | None]:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 12,
    }
    r = requests.get(url, params=params, timeout=timeout_sec)
    r.raise_for_status()
    data = r.json()
    obs = data.get("observations", [])
    valid = [o for o in obs if o.get("value") not in {".", "", None}]
    if not valid:
        return None, None
    latest = valid[0]
    prev = valid[1] if len(valid) > 1 else None
    return latest, prev


def fetch_fred_indicators() -> pd.DataFrame:
    api_key = os.getenv(FRED_API_KEY_ENV, "").strip()
    rows: list[IndicatorPoint] = []
    for name, series in FRED_SERIES.items():
        if not api_key:
            rows.append(IndicatorPoint(name, None, None, None, None, "FRED", "FRED_API_KEY未設定"))
            continue
        try:
            latest, prev = _fetch_fred_observations(series, api_key)
            rows.append(
                IndicatorPoint(
                    indicator=name,
                    latest_value=_safe_float(latest.get("value") if latest else None),
                    latest_date=latest.get("date") if latest else None,
                    previous_value=_safe_float(prev.get("value") if prev else None),
                    previous_date=prev.get("date") if prev else None,
                    source="FRED",
                    note="",
                )
            )
        except Exception as exc:
            rows.append(IndicatorPoint(name, None, None, None, None, "FRED", f"取得失敗: {exc}"))
    return pd.DataFrame([r.__dict__ for r in rows])


def _parse_estat_values(payload: dict) -> tuple[tuple[float | None, str | None], tuple[float | None, str | None]]:
    # e-Stat payload structure differs by dataset. This parser is defensive.
    data_obj = payload.get("GET_STATS_DATA", {}).get("STATISTICAL_DATA", {}).get("DATA_INF", {}).get("VALUE", [])
    if isinstance(data_obj, dict):
        data_obj = [data_obj]
    values = []
    for row in data_obj:
        v = _safe_float(row.get("$"))
        if v is None:
            continue
        dt = row.get("@time") or row.get("@tab") or row.get("@cat01")
        values.append((dt, v))
    if not values:
        return (None, None), (None, None)
    values.sort(key=lambda x: str(x[0]), reverse=True)
    latest = values[0]
    prev = values[1] if len(values) > 1 else (None, None)
    return (latest[1], str(latest[0]) if latest[0] else None), (prev[1], str(prev[0]) if prev[0] else None)


def fetch_estat_indicators() -> pd.DataFrame:
    app_id = os.getenv(ESTAT_API_KEY_ENV, "").strip()
    rows: list[IndicatorPoint] = []
    for name, meta in ESTAT_SERIES.items():
        if not app_id:
            rows.append(IndicatorPoint(name, None, None, None, None, "e-Stat", "ESTAT_API_KEY未設定"))
            continue
        try:
            url = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"
            params = {
                "appId": app_id,
                "statsDataId": meta["statsDataId"],
                "limit": 20,
            }
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            payload = r.json()
            (latest_v, latest_d), (prev_v, prev_d) = _parse_estat_values(payload)
            rows.append(
                IndicatorPoint(
                    indicator=name,
                    latest_value=latest_v,
                    latest_date=latest_d,
                    previous_value=prev_v,
                    previous_date=prev_d,
                    source="e-Stat",
                    note="",
                )
            )
        except Exception as exc:
            rows.append(IndicatorPoint(name, None, None, None, None, "e-Stat", f"取得失敗: {exc}"))
    return pd.DataFrame([r.__dict__ for r in rows])


def fetch_all_indicators() -> pd.DataFrame:
    us = fetch_fred_indicators()
    jp = fetch_estat_indicators()
    return pd.concat([us, jp], ignore_index=True)


def get_economic_calendar() -> pd.DataFrame:
    return _build_release_schedule()


def build_upcoming_alert_message(days_ahead: int = 2) -> str:
    cal = get_economic_calendar()
    today = date.today()
    threshold = today + timedelta(days=days_ahead)
    upcoming = cal[(cal["次回発表日"] >= today) & (cal["次回発表日"] <= threshold)].copy()
    if upcoming.empty:
        return ""
    lines = [f"📅 経済指標 直近{days_ahead}日アラート"]
    for r in upcoming.itertuples(index=False):
        lines.append(f"- {r.次回発表日} {r.国} {r.指標} {r.注目}")
    return "\n".join(lines)


def send_discord_webhook(message: str) -> tuple[bool, str]:
    url = os.getenv(DISCORD_WEBHOOK_ENV, "").strip()
    if not url:
        return False, f"{DISCORD_WEBHOOK_ENV} が未設定です。"
    if not message.strip():
        return False, "送信メッセージが空です。"
    try:
        resp = requests.post(url, json={"content": message}, timeout=8)
        if 200 <= resp.status_code < 300:
            return True, "Discord通知を送信しました。"
        return False, f"Discord通知失敗: status={resp.status_code}"
    except Exception as exc:
        return False, f"Discord通知例外: {exc}"


def notify_upcoming_economic_events(days_ahead: int = 2) -> tuple[bool, str]:
    msg = build_upcoming_alert_message(days_ahead=days_ahead)
    if not msg:
        return False, f"直近{days_ahead}日以内の発表予定はありません。"
    return send_discord_webhook(msg)


def _with_fallback(df: pd.DataFrame, fallback_builder: Callable[[], pd.DataFrame]) -> pd.DataFrame:
    if df.empty:
        return fallback_builder()
    return df


def build_demo_indicators() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "indicator": "GDP成長率（米国）",
                "latest_value": 2.4,
                "latest_date": "2025-12-31",
                "previous_value": 2.1,
                "previous_date": "2025-09-30",
                "source": "FRED",
                "note": "デモ値",
            },
            {
                "indicator": "非農業部門雇用者数（米国）",
                "latest_value": 235.0,
                "latest_date": "2026-03-01",
                "previous_value": 210.0,
                "previous_date": "2026-02-01",
                "source": "FRED",
                "note": "デモ値（千人）",
            },
            {
                "indicator": "CPI（米国）",
                "latest_value": 313.1,
                "latest_date": "2026-03-01",
                "previous_value": 312.4,
                "previous_date": "2026-02-01",
                "source": "FRED",
                "note": "デモ値",
            },
            {
                "indicator": "FF金利（米国）",
                "latest_value": 4.5,
                "latest_date": "2026-03-31",
                "previous_value": 4.5,
                "previous_date": "2026-02-28",
                "source": "FRED",
                "note": "デモ値",
            },
            {
                "indicator": "10年国債利回り（米国）",
                "latest_value": 4.3,
                "latest_date": "2026-04-22",
                "previous_value": 4.2,
                "previous_date": "2026-04-21",
                "source": "FRED",
                "note": "デモ値",
            },
            {
                "indicator": "GDP成長率（日本）",
                "latest_value": 1.2,
                "latest_date": "2025Q4",
                "previous_value": 0.9,
                "previous_date": "2025Q3",
                "source": "e-Stat",
                "note": "デモ値",
            },
            {
                "indicator": "CPI（日本）",
                "latest_value": 2.8,
                "latest_date": "2026-03",
                "previous_value": 2.7,
                "previous_date": "2026-02",
                "source": "e-Stat",
                "note": "デモ値",
            },
            {
                "indicator": "完全失業率（日本）",
                "latest_value": 2.5,
                "latest_date": "2026-03",
                "previous_value": 2.6,
                "previous_date": "2026-02",
                "source": "e-Stat",
                "note": "デモ値",
            },
        ]
    )


def get_indicators_with_fallback() -> pd.DataFrame:
    real = fetch_all_indicators()
    # If all values are missing, show demo rows for dashboard continuity.
    if real.empty or real["latest_value"].notna().sum() == 0:
        return build_demo_indicators()
    return real


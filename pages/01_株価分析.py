from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import streamlit as st
import yfinance as yf

from utils.chart_builder import advanced_candlestick_with_volume, financial_trend_chart, single_radar
from utils.common import (
    apply_global_ui_tweaks,
    log_event,
    render_footer,
    render_last_data_update,
    show_download_button,
    touch_last_data_update,
)
from utils.data_fetcher import (
    calculate_bollinger_bands,
    calculate_moving_averages,
    fetch_financial_trend,
    fetch_price_data_by_dates,
    five_axis_scores,
    get_company_name,
)


def fmt_num(value: float, digits: int = 2, suffix: str = "") -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "-"
    return f"{value:,.{digits}f}{suffix}"


def fmt_man_shares(value: float) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "-"
    return f"{value / 10000:,.2f} 万株"


def fmt_jpy_large(value: float) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "-"
    abs_value = abs(value)
    if abs_value >= 1_0000_0000_0000:
        return f"{value / 1_0000_0000_0000:,.1f}兆円"
    if abs_value >= 1_0000_0000:
        return f"{value / 1_0000_0000:,.1f}億円"
    if abs_value >= 1_0000:
        return f"{value / 1_0000:,.1f}万円"
    return f"{value:,.0f}円"


def per_highlight_html(per_value: float) -> str:
    if per_value is None or (isinstance(per_value, float) and np.isnan(per_value)):
        return "<span style='color:#94a3b8;'>PER判定: -</span>"
    if per_value >= 20:
        return "<span style='color:#f87171;font-weight:700;'>PER判定: 割高ゾーン (20倍以上)</span>"
    if per_value <= 15:
        return "<span style='color:#4ade80;font-weight:700;'>PER判定: 割安ゾーン (15倍以下)</span>"
    return "<span style='color:#cbd5e1;'>PER判定: 中立ゾーン</span>"


@st.cache_data(ttl=3600)
def get_usdjpy_rate() -> float:
    try:
        fx = yf.Ticker("JPY=X").history(period="5d")
        if fx.empty:
            return 150.0
        return float(fx["Close"].iloc[-1])
    except Exception:
        return 150.0


def build_metrics(info: dict, hist) -> dict[str, float]:
    latest_close = float(hist["Close"].iloc[-1]) if not hist.empty else np.nan
    prev_close = float(hist["Close"].iloc[-2]) if len(hist) > 1 else latest_close
    change = latest_close - prev_close if not np.isnan(latest_close) and not np.isnan(prev_close) else np.nan
    change_pct = (change / prev_close * 100.0) if prev_close not in (0, np.nan) else np.nan

    def _safe(v) -> float:
        try:
            if v is None:
                return np.nan
            return float(v)
        except Exception:
            return np.nan

    return {
        "current": latest_close,
        "change": change,
        "change_pct": change_pct,
        "volume": float(hist["Volume"].iloc[-1]) if not hist.empty else np.nan,
        "per": _safe(info.get("trailingPE")),
        "pbr": _safe(info.get("priceToBook")),
        "div_yield": _safe(info.get("dividendYield")) * 100,
        "market_cap": _safe(info.get("marketCap")),
    }


st.title("📈 株価分析")
apply_global_ui_tweaks()
with st.spinner("ページを読み込み中..."):
    st.empty()
st.caption("ローソク足・テクニカル指標・業績推移・5軸スコアを表示します。")

watchlist = {
    "Apple (AAPL)": "AAPL",
    "Microsoft (MSFT)": "MSFT",
    "NVIDIA (NVDA)": "NVDA",
    "Alphabet (GOOGL)": "GOOGL",
    "Amazon (AMZN)": "AMZN",
}

selected_watch = st.sidebar.selectbox("注目銘柄（プリセット）", options=list(watchlist.keys()))
ticker = watchlist[selected_watch]

use_custom = st.sidebar.checkbox("カスタム銘柄", value=False)
if use_custom:
    custom_ticker = st.sidebar.text_input("銘柄コードを自由入力", value=ticker).strip().upper()
    if custom_ticker:
        ticker = custom_ticker

period_options = ["1か月", "3か月", "6か月", "1年", "3年", "5年"]
period_label = st.sidebar.selectbox("期間プリセット", options=period_options, index=3)

today = date.today()
period_days_map = {"1か月": 30, "3か月": 90, "6か月": 180, "1年": 365, "3年": 365 * 3, "5年": 365 * 5}
default_start = today - timedelta(days=period_days_map.get(period_label, 365))
slider_min = today - timedelta(days=365 * 10)
start_date, end_date = st.sidebar.slider(
    "期間選択（日付スライダー）",
    min_value=slider_min,
    max_value=today,
    value=(default_start, today),
    format="YYYY-MM-DD",
)

st.sidebar.markdown("---")
if st.sidebar.button("データ更新", use_container_width=True):
    st.cache_data.clear()
    st.cache_resource.clear()
    log_event("manual_refresh", ticker)
    st.rerun()

render_last_data_update()

if not ticker:
    st.warning("銘柄コードを入力してください。")
    st.stop()

with st.spinner("株価データ取得中..."):
    try:
        hist, info = fetch_price_data_by_dates(ticker, start_date, end_date)
    except Exception as exc:
        msg = str(exc)
        if "YFRateLimitError" in msg or "rate limit" in msg.lower() or "too many requests" in msg.lower():
            st.error("yfinanceの取得上限に達しました。数分待ってから再実行してください。")
            st.info("一時的な制限です。期間を短くするか、時間を空けると改善します。")
            log_event("stock_rate_limit", f"{ticker} {start_date} to {end_date}")
            st.stop()
        st.error(f"データ取得中にエラーが発生しました: {exc}")
        log_event("stock_error", f"{ticker} {start_date} to {end_date} {exc}")
        st.stop()

if hist.empty:
    st.error("株価データを取得できませんでした。銘柄コードや期間を確認してください。")
    st.info("yfinanceのレート制限中は同様の表示になります。数分待って再実行してください。")
    log_event("stock_error", f"{ticker} {start_date} to {end_date}")
    st.stop()

touch_last_data_update()

hist = calculate_moving_averages(hist)
hist = calculate_bollinger_bands(hist, window=20, num_std=2.0)
company_name = get_company_name(info, ticker)
st.subheader(f"{company_name} ({ticker})")

metrics = build_metrics(info, hist)

row1 = st.columns(3)
row1[0].metric("現在株価", fmt_num(metrics["current"], 2, " USD"))
row1[1].metric("前日比", fmt_num(metrics["change"], 2, " USD"), delta=fmt_num(metrics["change_pct"], 2, "%"))
row1[2].metric("出来高", fmt_man_shares(metrics["volume"]))

row2 = st.columns(4)
with row2[0]:
    st.metric("PER", fmt_num(metrics["per"], 2))
    st.markdown(per_highlight_html(metrics["per"]), unsafe_allow_html=True)
row2[1].metric("PBR", fmt_num(metrics["pbr"], 2))
row2[2].metric("配当利回り", fmt_num(metrics["div_yield"], 2, "%"))
usdjpy = get_usdjpy_rate()
market_cap_jpy = metrics["market_cap"] * usdjpy if not np.isnan(metrics["market_cap"]) else np.nan
row2[3].metric("時価総額", fmt_jpy_large(market_cap_jpy))

st.plotly_chart(advanced_candlestick_with_volume(hist, ticker), use_container_width=True)

try:
    financial_df = fetch_financial_trend(ticker, years=5)
except Exception as exc:
    msg = str(exc)
    if "YFRateLimitError" in msg or "rate limit" in msg.lower() or "too many requests" in msg.lower():
        financial_df = None
        st.warning("業績データはレート制限のため一時的に取得できませんでした。")
    else:
        raise

if financial_df is not None and not financial_df.empty:
    financial_jpy = financial_df.copy()
    financial_jpy["Revenue"] = financial_jpy["Revenue"] * usdjpy
    financial_jpy["OperatingIncome"] = financial_jpy["OperatingIncome"] * usdjpy
    st.plotly_chart(financial_trend_chart(financial_jpy), use_container_width=True)
else:
    st.info("業績推移データ（financials）を取得できませんでした。")

scores = five_axis_scores(info, hist)
st.plotly_chart(single_radar(scores, ticker), use_container_width=True)

export_df = hist[["Date", "Open", "High", "Low", "Close", "Volume", "MA5", "MA25", "MA75", "BB_MID", "BB_UPPER", "BB_LOWER"]].copy()
export_df["Ticker"] = ticker
show_download_button(export_df, "stock_analysis")

with st.expander("生データ"):
    st.dataframe(export_df, use_container_width=True)

render_footer()
log_event("open_stock_analysis", f"{ticker} {start_date} to {end_date}")

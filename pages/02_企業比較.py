from __future__ import annotations

import streamlit as st
import yfinance as yf

from utils.chart_builder import comparison_heatmap, multi_radar
from utils.common import (
    apply_global_ui_tweaks,
    log_event,
    render_footer,
    render_last_data_update,
    show_download_button,
    touch_last_data_update,
)
from utils.data_fetcher import build_compare_table, fetch_price_data, five_axis_scores


st.title("🏢 企業比較")
apply_global_ui_tweaks()
with st.spinner("ページを読み込み中..."):
    st.empty()
st.caption("複数銘柄の財務指標比較と、5軸スコアの重ね合わせを表示します。")

tickers_input = st.sidebar.text_input("比較銘柄（カンマ区切り）", value="AAPL, AMZN, META")
tickers = [t.strip().upper() for t in tickers_input.split(",") if t.strip()]
render_last_data_update()

if len(tickers) < 2:
    st.warning("比較には2銘柄以上を入力してください。")
    st.stop()

with st.spinner("比較データ取得中..."):
    compare_df = build_compare_table(tickers)

if compare_df.empty:
    st.error("企業比較データを取得できませんでした。")
    log_event("compare_error", tickers_input)
    st.stop()
touch_last_data_update()

st.plotly_chart(comparison_heatmap(compare_df), use_container_width=True)

st.subheader("比較テーブル")
st.dataframe(compare_df, use_container_width=True)

score_map: dict[str, dict[str, float]] = {}
for ticker in compare_df["Ticker"].tolist():
    hist, info = fetch_price_data(ticker, "1年")
    if hist.empty:
        info = yf.Ticker(ticker).info or {}
    score_map[ticker] = five_axis_scores(info, hist)

st.plotly_chart(multi_radar(score_map), use_container_width=True)

show_download_button(compare_df, "company_compare")

render_footer()
log_event("open_company_compare", ",".join(tickers))

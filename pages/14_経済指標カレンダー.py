from __future__ import annotations

import pandas as pd
import streamlit as st

from db.db_utils import init_db
from db.news_utils import init_news_tables
from economic_calendar import (
    ESTAT_API_KEY_ENV,
    FRED_API_KEY_ENV,
    DISCORD_WEBHOOK_ENV,
    get_economic_calendar,
    get_indicators_with_fallback,
    notify_upcoming_economic_events,
)
from utils.common import apply_global_ui_tweaks, render_footer, render_last_data_update


def _fmt_value(v: float | None) -> str:
    if v is None or pd.isna(v):
        return "-"
    if abs(v) >= 1000:
        return f"{v:,.0f}"
    return f"{v:,.2f}"


st.title("📅 経済指標カレンダー")
apply_global_ui_tweaks()
st.caption("FRED/e-Stat の主要指標、最新値・前回値、次回発表予定を表示します。")

with st.spinner("DB初期化中..."):
    init_db()
    init_news_tables()
render_last_data_update()

with st.spinner("経済指標データ取得中..."):
    indicators = get_indicators_with_fallback()
    calendar_df = get_economic_calendar()

st.info(
    f"APIキー設定: `{FRED_API_KEY_ENV}` / `{ESTAT_API_KEY_ENV}` / Discord通知 `{DISCORD_WEBHOOK_ENV}`"
)

st.subheader("最新の経済指標（最新値 vs 前回値）")
view = indicators.copy()
view["変化"] = view.apply(
    lambda r: (r["latest_value"] - r["previous_value"])
    if pd.notna(r["latest_value"]) and pd.notna(r["previous_value"])
    else None,
    axis=1,
)
view = view.rename(
    columns={
        "indicator": "指標",
        "latest_value": "最新値",
        "latest_date": "最新日時",
        "previous_value": "前回値",
        "previous_date": "前回日時",
        "source": "ソース",
        "note": "備考",
    }
)
st.dataframe(
    view[["指標", "ソース", "最新値", "前回値", "変化", "最新日時", "前回日時", "備考"]],
    use_container_width=True,
    column_config={
        "最新値": st.column_config.NumberColumn("最新値", format="%.4f"),
        "前回値": st.column_config.NumberColumn("前回値", format="%.4f"),
        "変化": st.column_config.NumberColumn("変化", format="%.4f"),
    },
)

st.subheader("発表スケジュール（カレンダー）")
cal_view = calendar_df.copy()
cal_view["次回発表日"] = pd.to_datetime(cal_view["次回発表日"])
cal_view["発表日"] = cal_view["次回発表日"].dt.strftime("%Y-%m-%d")
cal_view["あと何日"] = (cal_view["次回発表日"].dt.date - pd.Timestamp.today().date()).apply(lambda x: x.days)
st.dataframe(
    cal_view[["発表日", "あと何日", "国", "指標", "注目"]],
    use_container_width=True,
)

st.subheader("Discord事前通知")
col1, col2 = st.columns([1, 2])
with col1:
    days = st.selectbox("通知対象", options=[1, 2, 3, 7], index=1, format_func=lambda x: f"直近{x}日")
with col2:
    if st.button("📣 近い発表予定をDiscord通知", use_container_width=True):
        ok, msg = notify_upcoming_economic_events(days_ahead=int(days))
        if ok:
            st.success(msg)
        else:
            st.warning(msg)

render_footer()


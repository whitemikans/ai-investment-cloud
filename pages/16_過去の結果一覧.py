from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from backtest_engine import get_backtest_history
from db.db_utils import init_db
from db.news_utils import init_news_tables
from utils.common import apply_global_ui_tweaks, render_footer, render_last_data_update


st.title("📋 過去の結果一覧")
apply_global_ui_tweaks()
st.caption("#05対応: バックテスト履歴の検索と推移確認")

with st.spinner("初期化中..."):
    init_db()
    init_news_tables()
render_last_data_update()

with st.spinner("履歴読み込み中..."):
    base_df = get_backtest_history()

if base_df.empty:
    st.info("保存済みのバックテスト結果がありません。まず「📊 バックテスト」ページで実行してください。")
    render_footer()
    st.stop()

strategy_options = ["すべて"] + sorted(base_df["strategy_name"].dropna().unique().tolist())
ticker_options = ["すべて"] + sorted(base_df["ticker"].dropna().unique().tolist())

st.sidebar.subheader("フィルター")
strategy = st.sidebar.selectbox("戦略名", strategy_options, index=0)
ticker = st.sidebar.selectbox("銘柄", ticker_options, index=0)
date_from = st.sidebar.date_input("作成日(開始)", value=date.today() - timedelta(days=90))
date_to = st.sidebar.date_input("作成日(終了)", value=date.today())

df = get_backtest_history(
    strategy_name=strategy,
    ticker=ticker,
    date_from=f"{date_from} 00:00:00",
    date_to=f"{date_to} 23:59:59",
)

if df.empty:
    st.warning("条件に一致する履歴がありません。")
    render_footer()
    st.stop()

st.dataframe(
    df[
        [
            "id",
            "created_at",
            "strategy_name",
            "ticker",
            "start_date",
            "end_date",
            "total_return_pct",
            "max_drawdown_pct",
            "win_rate_pct",
            "sharpe_ratio",
            "profit_factor",
            "trades",
            "params_json",
        ]
    ],
    use_container_width=True,
)

st.subheader("同じ戦略のパラメータ変更による成績推移")
plot_df = df.copy()
plot_df["created_at"] = pd.to_datetime(plot_df["created_at"])
line = px.line(
    plot_df.sort_values("created_at"),
    x="created_at",
    y="total_return_pct",
    color="strategy_name",
    markers=True,
    template="plotly_dark",
    title="総リターン推移",
)
line.update_layout(height=360, xaxis_title="実行日時", yaxis_title="総リターン(%)")
st.plotly_chart(line, use_container_width=True)

render_footer()


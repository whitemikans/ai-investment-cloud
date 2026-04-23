from __future__ import annotations

from datetime import date, timedelta

import plotly.express as px
import streamlit as st

from db.db_utils import get_monthly_trade_count, get_transactions_df, init_db, list_stocks
from utils.common import apply_global_ui_tweaks, render_footer, render_last_data_update, show_download_button


st.title("📒 取引履歴")
apply_global_ui_tweaks()
st.caption("#03対応: transactionsテーブルから履歴を表示します。")

with st.spinner("DBを読み込み中..."):
    init_db()

stocks = list_stocks()
stock_options = ["ALL"] + [s.stock_code for s in stocks]

st.sidebar.subheader("フィルター")
default_start = date.today() - timedelta(days=365)
start_date = st.sidebar.date_input("開始日", value=default_start)
end_date = st.sidebar.date_input("終了日", value=date.today())
stock_code = st.sidebar.selectbox("銘柄", options=stock_options)
trade_type = st.sidebar.selectbox("売買区分", options=["すべて", "買", "売"])
render_last_data_update()

with st.spinner("取引履歴を取得中..."):
    df = get_transactions_df(
        stock_code=None if stock_code == "ALL" else stock_code,
        start_date=start_date,
        end_date=end_date,
        trade_type=trade_type,
    )

if df.empty:
    st.info("該当する取引はありません。")
    render_footer()
    st.stop()

buy_total = float(df.loc[df["売買"] == "買", "金額"].sum())
sell_total = float(df.loc[df["売買"] == "売", "金額"].sum())
fee_total = float(df["手数料"].sum())

c1, c2, c3, c4 = st.columns(4)
c1.metric("取引件数", f"{len(df):,}件")
c2.metric("買い付け合計", f"¥{buy_total:,.0f}")
c3.metric("売却合計", f"¥{sell_total:,.0f}")
c4.metric("手数料合計", f"¥{fee_total:,.0f}")

st.subheader("取引一覧")

def highlight_amount_by_trade(row):
    styles = [""] * len(row)
    if "売買" in row.index and "金額" in row.index:
        amount_idx = list(row.index).index("金額")
        if row["売買"] == "買":
            styles[amount_idx] = "color: #60a5fa; font-weight: 700;"  # blue
        elif row["売買"] == "売":
            styles[amount_idx] = "color: #f87171; font-weight: 700;"  # red
    return styles

styled_df = df.style.apply(highlight_amount_by_trade, axis=1).format({"金額": "{:,.2f}", "単価": "{:,.2f}", "手数料": "{:,.2f}"})
st.dataframe(styled_df, use_container_width=True)

monthly = get_monthly_trade_count(df)
if not monthly.empty:
    fig = px.bar(monthly, x="month", y="count", title="月別取引回数", template="plotly_dark")
    fig.update_layout(xaxis_title="月", yaxis_title="回数", height=320)
    st.plotly_chart(fig, use_container_width=True)

show_download_button(df, "trade_history")

render_footer()

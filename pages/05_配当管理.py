from __future__ import annotations

from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st

from db.db_utils import add_dividend, ensure_dummy_dividends, get_dividends_df, get_portfolio_base_df, init_db, list_stocks
from utils.common import apply_global_ui_tweaks, render_footer, render_last_data_update, show_download_button, touch_last_data_update


def jpy(v: float) -> str:
    return f"¥{v:,.0f}"


st.title("💰 配当管理")
apply_global_ui_tweaks()
st.caption("#03対応: dividendsテーブルで配当実績を管理します。")

with st.spinner("DBを初期化中..."):
    init_db()
    ensure_dummy_dividends()

stocks = list_stocks()
stock_options = [f"{s.stock_code} - {s.company_name}" for s in stocks]

st.sidebar.subheader("配当登録フォーム")
with st.sidebar.form("div_form", clear_on_submit=True):
    selected = st.selectbox("銘柄", options=stock_options)
    dividend_ps = st.number_input("1株配当金額 (USD)", min_value=0.0, value=0.25, step=0.01)
    shares = st.number_input("受取株数", min_value=1, value=100, step=1)
    tax = st.number_input("源泉徴収税額 (USD)", min_value=0.0, value=0.0, step=0.01)
    record_date = st.date_input("権利確定日", value=date.today())
    payment_date = st.date_input("支払日", value=date.today())
    submitted = st.form_submit_button("配当登録")

render_last_data_update()

if submitted:
    stock_code = selected.split(" - ", 1)[0]
    total_amount = float(dividend_ps) * int(shares)
    result = add_dividend(
        stock_code=stock_code,
        amount_per_share=float(dividend_ps),
        total_amount=total_amount,
        tax_amount=float(tax),
        ex_date=record_date,
        payment_date=payment_date,
    )
    ok = bool(result.iloc[0]["success"]) if not result.empty else False
    msg = str(result.iloc[0]["message"]) if not result.empty else "配当登録に失敗しました。"
    if ok:
        touch_last_data_update()
        st.success(msg)
        st.rerun()
    else:
        st.error(msg)

year_options = sorted({date.today().year, date.today().year - 1, date.today().year - 2}, reverse=True)
selected_year = st.selectbox("年度", options=year_options, index=0)

df = get_dividends_df(year=selected_year)
if df.empty:
    st.info("この年度の配当データはありません。")
    render_footer()
    st.stop()

before_tax = float(df["税引前"].sum())
after_tax = float(df["税引後"].sum())
progress = min(after_tax / 1_000_000, 1.0)

c1, c2 = st.columns([2, 1])
with c1:
    st.metric("年間配当金（税引前）", jpy(before_tax))
    st.metric("年間配当金（税引後）", jpy(after_tax))
with c2:
    st.metric("年間配当100万円への進捗", f"{progress*100:,.1f}%")
    st.progress(progress)

work = df.copy()
work["支払日"] = pd.to_datetime(work["支払日"])
monthly = work.assign(月=work["支払日"].dt.strftime("%Y-%m")).groupby("月", as_index=False)["税引後"].sum()
bar = px.bar(monthly, x="月", y="税引後", title="月別配当金（税引後）", template="plotly_dark", color_discrete_sequence=["#22c55e"])
bar.update_layout(xaxis_title="月", yaxis_title="金額")
st.plotly_chart(bar, use_container_width=True)

portfolio = get_portfolio_base_df()
if not portfolio.empty:
    by_stock = work.groupby(["銘柄コード", "企業名"], as_index=False)["税引後"].sum().rename(columns={"税引後": "年間配当金"})
    merged = by_stock.merge(portfolio[["ticker", "avg_cost", "shares"]], left_on="銘柄コード", right_on="ticker", how="left")
    merged["取得原価"] = merged["avg_cost"].fillna(0) * merged["shares"].fillna(0)
    merged["配当利回り(%)"] = merged.apply(
        lambda r: (r["年間配当金"] / r["取得原価"] * 100) if r["取得原価"] > 0 else 0,
        axis=1,
    )
    rank = merged[["銘柄コード", "企業名", "年間配当金", "配当利回り(%)"]].sort_values("配当利回り(%)", ascending=False)
    st.subheader("銘柄別配当利回りランキング")
    max_yield = float(rank["配当利回り(%)"].max()) if not rank.empty else 0.0
    st.dataframe(
        rank,
        use_container_width=True,
        column_config={
            "年間配当金": st.column_config.NumberColumn("年間配当金", format="¥%.0f"),
            "配当利回り(%)": st.column_config.ProgressColumn(
                "配当利回り(%)",
                format="%.2f%%",
                min_value=0.0,
                max_value=max(5.0, max_yield * 1.2),
                help="横棒で利回りを可視化",
            ),
        },
    )

st.subheader("配当履歴")
st.dataframe(df, use_container_width=True)
show_download_button(df, "dividends")

render_footer()

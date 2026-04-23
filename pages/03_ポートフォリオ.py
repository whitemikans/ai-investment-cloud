from __future__ import annotations

from datetime import date

import numpy as np
import streamlit as st

from db.db_utils import add_transaction, get_portfolio_base_df, init_db, list_stocks
from utils.chart_builder import allocation_pie, dividend_bar
from utils.common import (
    apply_global_ui_tweaks,
    log_event,
    render_footer,
    render_last_data_update,
    show_download_button,
    touch_last_data_update,
)
from utils.data_fetcher import enrich_portfolio


def jpy(value: float) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "-"
    return f"¥{value:,.0f}"


st.title("💼 ポートフォリオ（DB統合版）")
apply_global_ui_tweaks()
st.caption("#03対応: 取引登録はSQLite（investment.db）へ保存されます。")

with st.spinner("DBを初期化中..."):
    init_db()

stocks = list_stocks()
stock_options = [f"{s.stock_code} - {s.company_name}" for s in stocks]

st.sidebar.subheader("取引登録フォーム（DB保存）")
with st.sidebar.form("trade_form", clear_on_submit=True):
    selected = st.selectbox("銘柄", options=stock_options)
    trade_type = st.radio("売買区分", options=["買", "売"], horizontal=True)
    shares = st.number_input("株数", min_value=1, value=10, step=1)
    price = st.number_input("約定単価 (USD)", min_value=0.0, value=100.0, step=0.1)
    commission = st.number_input("手数料 (USD)", min_value=0.0, value=0.0, step=0.1)
    trade_date = st.date_input("約定日", value=date.today())
    memo = st.text_area("メモ", value="")
    submitted = st.form_submit_button("登録")

render_last_data_update()

if submitted:
    stock_code = selected.split(" - ", 1)[0]
    result = add_transaction(stock_code, trade_type, int(shares), float(price), float(commission), trade_date, memo)
    ok = bool(result.iloc[0]["success"]) if not result.empty else False
    msg = str(result.iloc[0]["message"]) if not result.empty else "取引登録に失敗しました。"
    needs_confirm = bool(result.iloc[0]["requires_confirmation"]) if (not result.empty and "requires_confirmation" in result.columns) else False
    has_warning = bool(result.iloc[0]["has_warning"]) if (not result.empty and "has_warning" in result.columns) else False
    warning_message = str(result.iloc[0]["warning_message"]) if (not result.empty and "warning_message" in result.columns) else ""
    if ok:
        touch_last_data_update()
        log_event("db_trade_add", f"{stock_code} {trade_type} {shares}")
        st.success(msg)
        if has_warning and warning_message:
            st.warning(warning_message)
        st.rerun()
    else:
        if needs_confirm:
            st.warning(msg)
        else:
            st.error(msg)

with st.spinner("保有データを読み込み中..."):
    portfolio_base = get_portfolio_base_df()

st.subheader("保有一覧（DB）")
if portfolio_base.empty:
    st.info("保有銘柄はまだありません。左サイドバーから取引を登録してください。")
    render_footer()
    st.stop()

st.dataframe(
    portfolio_base.rename(
        columns={
            "ticker": "銘柄コード",
            "company_name": "企業名",
            "sector": "セクター",
            "avg_cost": "平均取得単価",
            "shares": "保有株数",
        }
    ),
    use_container_width=True,
)

with st.spinner("時価・セクター・配当情報を取得中..."):
    enriched = enrich_portfolio(portfolio_base[["ticker", "avg_cost", "shares"]])

if enriched.empty:
    st.warning("評価データがありません。")
    render_footer()
    st.stop()

touch_last_data_update()

invested = float(enriched["投資元本"].sum())
market_value = float(enriched["評価額"].sum())
pnl = market_value - invested
pnl_pct = (pnl / invested * 100) if invested > 0 else 0.0

top_cols = st.columns(3)
top_cols[0].metric("投資元本", jpy(invested))
top_cols[1].metric("現在評価額", jpy(market_value))
top_cols[2].metric("評価損益", jpy(pnl), delta=f"{pnl_pct:,.2f}%")

left, right = st.columns(2)
with left:
    by_ticker = enriched.groupby("Ticker", as_index=False)["評価額"].sum()
    st.plotly_chart(allocation_pie(by_ticker, "Ticker", "評価額", "銘柄別 構成比"), use_container_width=True)
with right:
    by_sector = enriched.groupby("セクター", as_index=False)["評価額"].sum()
    st.plotly_chart(allocation_pie(by_sector, "セクター", "評価額", "セクター別 構成比"), use_container_width=True)

st.subheader("銘柄別損益テーブル")
pnl_table = enriched[["Ticker", "企業名", "保有数", "取得単価", "現在値", "評価損益", "評価損益率(%)"]].copy()
pnl_table["判定"] = np.where(
    pnl_table["評価損益"] > 0,
    "利益",
    np.where(pnl_table["評価損益"] < 0, "損失", "±0"),
)
st.dataframe(pnl_table, use_container_width=True)

st.subheader("配当分析")
st.plotly_chart(dividend_bar(enriched), use_container_width=True)
annual_dividend = float(enriched["年間配当(予想)"].sum())
after_tax = annual_dividend * (1 - 0.20315)
div_yield = (annual_dividend / invested * 100) if invested > 0 else 0.0

div_cols = st.columns(3)
div_cols[0].metric("年間配当収入 (予想)", jpy(annual_dividend))
div_cols[1].metric("税引後手取り配当 (20.315%)", jpy(after_tax))
div_cols[2].metric("配当利回り", f"{div_yield:,.2f}%")
st.info("#03では配当の実績値は『05_配当管理』ページでDB管理します。")

show_download_button(enriched.copy(), "portfolio_db")

render_footer()
log_event("open_portfolio_db", f"rows={len(enriched)}")

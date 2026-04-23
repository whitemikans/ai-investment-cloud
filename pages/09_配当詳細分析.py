from __future__ import annotations

from math import isfinite, log

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from db.db_utils import get_dividends, get_portfolio_base_df, init_db
from utils.common import apply_global_ui_tweaks, render_footer, render_last_data_update


def _jpy(value: float) -> str:
    return f"¥{value:,.0f}"


def _build_reinvestment_series(div_df: pd.DataFrame, portfolio_df: pd.DataFrame) -> pd.DataFrame:
    if div_df.empty:
        return pd.DataFrame(columns=["payment_date", "cum_without_reinvest", "cum_with_reinvest"])

    events = div_df.copy()
    events["payment_date"] = pd.to_datetime(events["payment_date"])
    events = events.sort_values("payment_date")

    base_shares = portfolio_df.set_index("ticker")["shares"].to_dict() if not portfolio_df.empty else {}
    avg_cost = portfolio_df.set_index("ticker")["avg_cost"].to_dict() if not portfolio_df.empty else {}

    state_shares: dict[str, float] = {}
    for code in events["stock_code"].unique():
        fallback_shares = float(events.loc[events["stock_code"] == code, "shares"].median())
        if not np.isfinite(fallback_shares) or fallback_shares <= 0:
            fallback_shares = 100.0
        state_shares[code] = float(base_shares.get(code, fallback_shares))

    without_sum = 0.0
    with_sum = 0.0
    rows: list[dict[str, object]] = []
    for row in events.itertuples(index=False):
        code = str(row.stock_code)
        dps = float(row.amount_per_share)
        if dps <= 0:
            continue
        base_qty = float(base_shares.get(code, row.shares))
        if not np.isfinite(base_qty) or base_qty <= 0:
            base_qty = float(row.shares) if float(row.shares) > 0 else 100.0
        cur_qty = float(state_shares.get(code, base_qty))
        if not np.isfinite(cur_qty) or cur_qty <= 0:
            cur_qty = base_qty
        reinvest_price = float(avg_cost.get(code, row.amount_per_share * 120))
        if not np.isfinite(reinvest_price) or reinvest_price <= 0:
            reinvest_price = row.amount_per_share * 120

        div_without = base_qty * dps
        div_with = cur_qty * dps
        without_sum += div_without
        with_sum += div_with
        state_shares[code] = cur_qty + (div_with / reinvest_price)

        rows.append(
            {
                "payment_date": pd.Timestamp(row.payment_date),
                "cum_without_reinvest": without_sum,
                "cum_with_reinvest": with_sum,
            }
        )
    return pd.DataFrame(rows)


st.title("💹 配当金の詳細分析")
apply_global_ui_tweaks()
st.caption("年間推移・月別カレンダー・利回りランキング・再投資シミュレーション・100万円進捗を表示します。")

with st.spinner("DBを読み込み中..."):
    init_db()
render_last_data_update()

with st.spinner("配当データを取得中..."):
    div_df = get_dividends()
    portfolio_df = get_portfolio_base_df()

if div_df.empty:
    st.info("配当データがありません。")
    render_footer()
    st.stop()

div_df["payment_date"] = pd.to_datetime(div_df["payment_date"])
div_df["year"] = div_df["payment_date"].dt.year
div_df["month"] = div_df["payment_date"].dt.month

st.subheader("1. 年間配当金の推移")
annual = (
    div_df.groupby("year", as_index=False)
    .agg(gross_amount=("total_amount", "sum"), net_amount=("net_amount", "sum"), tax_amount=("tax_amount", "sum"))
    .sort_values("year")
)
bar = go.Figure()
bar.add_trace(go.Bar(x=annual["year"], y=annual["gross_amount"], name="税引前", marker_color="#3b82f6"))
bar.add_trace(go.Bar(x=annual["year"], y=annual["net_amount"], name="税引後", marker_color="#22c55e"))
bar.update_layout(template="plotly_dark", barmode="group", xaxis_title="年", yaxis_title="配当金", height=360)
st.plotly_chart(bar, use_container_width=True)

st.subheader("2. 月別配当金カレンダー")
cal = (
    div_df.groupby(["year", "month"], as_index=False)["total_amount"]
    .sum()
    .pivot(index="year", columns="month", values="total_amount")
    .fillna(0.0)
    .sort_index()
)
cal = cal.reindex(columns=list(range(1, 13)), fill_value=0.0)
heat = go.Figure(
    data=go.Heatmap(
        z=cal.values,
        x=[f"{m}月" for m in cal.columns],
        y=[str(y) for y in cal.index],
        colorscale="Blues",
        text=np.vectorize(lambda x: f"{x:,.0f}")(cal.values),
        texttemplate="%{text}",
        hovertemplate="年:%{y}<br>月:%{x}<br>配当:%{z:,.0f}<extra></extra>",
    )
)
heat.update_layout(template="plotly_dark", height=360)
st.plotly_chart(heat, use_container_width=True)

st.subheader("3. 配当利回りランキング")
selected_year = st.selectbox("集計年度", options=sorted(annual["year"].unique(), reverse=True))
year_df = div_df[div_df["year"] == selected_year].copy()
yield_df = (
    year_df.groupby(["stock_code", "company_name"], as_index=False)["total_amount"]
    .sum()
    .rename(columns={"total_amount": "annual_dividend"})
)
if not portfolio_df.empty:
    cost_df = portfolio_df.assign(total_cost=lambda d: d["avg_cost"] * d["shares"])[["ticker", "total_cost"]]
    yield_df = yield_df.merge(cost_df, left_on="stock_code", right_on="ticker", how="left")
else:
    yield_df["total_cost"] = np.nan
yield_df["dividend_yield_pct"] = np.where(
    yield_df["total_cost"].fillna(0) > 0, yield_df["annual_dividend"] / yield_df["total_cost"] * 100, 0.0
)
yield_df = yield_df.sort_values("dividend_yield_pct", ascending=False)
st.dataframe(
    yield_df[["stock_code", "company_name", "annual_dividend", "dividend_yield_pct"]].rename(
        columns={
            "stock_code": "銘柄コード",
            "company_name": "企業名",
            "annual_dividend": "年間配当金",
            "dividend_yield_pct": "配当利回り(%)",
        }
    ),
    use_container_width=True,
    column_config={
        "年間配当金": st.column_config.NumberColumn("年間配当金", format="¥%.0f"),
        "配当利回り(%)": st.column_config.ProgressColumn(
            "配当利回り(%)",
            format="%.2f%%",
            min_value=0.0,
            max_value=max(5.0, float(yield_df["dividend_yield_pct"].max()) * 1.2 if not yield_df.empty else 5.0),
        ),
    },
)

st.subheader("4. 配当金再投資シミュレーション")
sim_df = _build_reinvestment_series(div_df, portfolio_df)
if sim_df.empty:
    st.info("再投資シミュレーションに必要なデータが不足しています。")
else:
    sim_fig = go.Figure()
    sim_fig.add_trace(
        go.Scatter(
            x=sim_df["payment_date"],
            y=sim_df["cum_without_reinvest"],
            mode="lines",
            name="再投資なし 累計配当",
            line=dict(color="#94a3b8", width=2),
        )
    )
    sim_fig.add_trace(
        go.Scatter(
            x=sim_df["payment_date"],
            y=sim_df["cum_with_reinvest"],
            mode="lines",
            name="再投資あり 累計配当",
            line=dict(color="#22c55e", width=3),
        )
    )
    sim_fig.update_layout(template="plotly_dark", height=360, xaxis_title="日付", yaxis_title="累計配当")
    st.plotly_chart(sim_fig, use_container_width=True)

st.subheader("5. 年間配当金100万円への道")
current_annual = float(annual["net_amount"].iloc[-1]) if not annual.empty else 0.0
target = 1_000_000.0
progress = min(current_annual / target, 1.0) if target > 0 else 0.0

growth = 0.0
if len(annual) >= 2:
    start_val = float(annual["net_amount"].iloc[max(0, len(annual) - 3)])
    end_val = float(annual["net_amount"].iloc[-1])
    years = max(1, len(annual.iloc[max(0, len(annual) - 3) :]) - 1)
    if start_val > 0 and end_val > 0:
        growth = (end_val / start_val) ** (1 / years) - 1

years_to_target = None
if current_annual > 0 and growth > 0:
    years_to_target = max(log(target / current_annual) / log(1 + growth), 0)
    if not isfinite(years_to_target):
        years_to_target = None

c1, c2, c3 = st.columns(3)
c1.metric("現在の年間配当金(税引後)", _jpy(current_annual))
c2.metric("100万円目標進捗", f"{progress * 100:.1f}%")
c3.metric("到達予想年数", f"{years_to_target:.1f}年" if years_to_target is not None else "算出不可")
st.progress(progress)

render_footer()

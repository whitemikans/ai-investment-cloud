from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from db.db_utils import get_transactions_df_sql, init_db
from utils.common import apply_global_ui_tweaks, render_footer, render_last_data_update


@dataclass
class BuyLot:
    qty: float
    price: float
    trade_date: pd.Timestamp


def _jpy(value: float) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "-"
    return f"¥{value:,.0f}"


def _analyze_trade_pairs(tx_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if tx_df.empty:
        empty_pair = pd.DataFrame(
            columns=[
                "stock_code",
                "company_name",
                "sector",
                "sell_date",
                "sell_price",
                "avg_buy_price",
                "matched_qty",
                "cost_basis",
                "realized_pnl",
                "return_pct",
                "is_win",
                "holding_days_weighted_sum",
            ]
        )
        return empty_pair, pd.DataFrame(), pd.DataFrame()

    work = tx_df.copy()
    work["trade_date"] = pd.to_datetime(work["trade_date"])
    work = work.sort_values(["stock_code", "trade_date", "id"]).reset_index(drop=True)

    pair_rows: list[dict[str, object]] = []
    for code, g in work.groupby("stock_code", sort=False):
        lots: list[BuyLot] = []
        for row in g.itertuples(index=False):
            qty = float(row.quantity)
            price = float(row.price)
            commission = float(row.commission)
            trade_date = pd.Timestamp(row.trade_date)

            if row.trade_type == "買":
                lots.append(BuyLot(qty=qty, price=price, trade_date=trade_date))
                continue

            total_qty_before = sum(lot.qty for lot in lots)
            total_cost_before = sum(lot.qty * lot.price for lot in lots)
            if total_qty_before <= 0:
                continue

            avg_buy_price = total_cost_before / total_qty_before
            sell_qty_remaining = qty
            matched_qty = 0.0
            cost_basis = 0.0
            holding_days_weighted_sum = 0.0

            while sell_qty_remaining > 0 and lots:
                head = lots[0]
                take = min(sell_qty_remaining, head.qty)
                matched_qty += take
                cost_basis += take * head.price
                holding_days_weighted_sum += take * max((trade_date - head.trade_date).days, 0)

                head.qty -= take
                sell_qty_remaining -= take
                if head.qty <= 1e-9:
                    lots.pop(0)

            if matched_qty <= 0:
                continue

            sell_commission_alloc = commission * (matched_qty / qty) if qty > 0 else 0.0
            sell_amount = matched_qty * price - sell_commission_alloc
            realized_pnl = sell_amount - cost_basis
            return_pct = (realized_pnl / cost_basis * 100) if cost_basis > 0 else 0.0

            pair_rows.append(
                {
                    "stock_code": code,
                    "company_name": row.company_name,
                    "sector": row.sector,
                    "sell_date": trade_date,
                    "sell_price": price,
                    "avg_buy_price": avg_buy_price,
                    "matched_qty": matched_qty,
                    "cost_basis": cost_basis,
                    "realized_pnl": realized_pnl,
                    "return_pct": return_pct,
                    "is_win": bool(price > avg_buy_price),
                    "holding_days_weighted_sum": holding_days_weighted_sum,
                }
            )

    pair_df = pd.DataFrame(pair_rows)
    if pair_df.empty:
        return pair_df, pd.DataFrame(), pd.DataFrame()

    ticker_summary = (
        pair_df.groupby(["stock_code", "company_name", "sector"], as_index=False)
        .agg(
            trades=("stock_code", "count"),
            wins=("is_win", "sum"),
            total_qty=("matched_qty", "sum"),
            total_cost=("cost_basis", "sum"),
            realized_pnl=("realized_pnl", "sum"),
            holding_days_weighted_sum=("holding_days_weighted_sum", "sum"),
        )
        .sort_values("realized_pnl", ascending=False)
    )
    ticker_summary["losses"] = ticker_summary["trades"] - ticker_summary["wins"]
    ticker_summary["win_rate"] = np.where(
        ticker_summary["trades"] > 0, ticker_summary["wins"] / ticker_summary["trades"] * 100, 0.0
    )
    ticker_summary["return_pct"] = np.where(
        ticker_summary["total_cost"] > 0, ticker_summary["realized_pnl"] / ticker_summary["total_cost"] * 100, 0.0
    )
    ticker_summary["avg_holding_days"] = np.where(
        ticker_summary["total_qty"] > 0, ticker_summary["holding_days_weighted_sum"] / ticker_summary["total_qty"], 0.0
    )

    sector_summary = (
        ticker_summary.groupby("sector", as_index=False)
        .agg(
            total_cost=("total_cost", "sum"),
            realized_pnl=("realized_pnl", "sum"),
            trades=("trades", "sum"),
            wins=("wins", "sum"),
        )
        .sort_values("realized_pnl", ascending=False)
    )
    sector_summary["return_pct"] = np.where(
        sector_summary["total_cost"] > 0, sector_summary["realized_pnl"] / sector_summary["total_cost"] * 100, 0.0
    )
    sector_summary["win_rate"] = np.where(
        sector_summary["trades"] > 0, sector_summary["wins"] / sector_summary["trades"] * 100, 0.0
    )
    return pair_df, ticker_summary, sector_summary


st.title("🎯 取引分析ダッシュボード")
apply_global_ui_tweaks()
st.caption("勝率・保有期間・銘柄別損益・セクター別リターンを分析します。")

with st.spinner("DBを読み込み中..."):
    init_db()
render_last_data_update()

with st.spinner("取引データを分析中..."):
    tx_df = get_transactions_df_sql()
    pair_df, ticker_summary, sector_summary = _analyze_trade_pairs(tx_df)

if pair_df.empty:
    st.info("分析可能な売却データがありません。")
    render_footer()
    st.stop()

st.subheader("1. 投資の勝率")
win_count = int(pair_df["is_win"].sum())
total_count = int(len(pair_df))
loss_count = total_count - win_count
win_rate = (win_count / total_count * 100) if total_count else 0.0

c1, c2 = st.columns([1.1, 1])
with c1:
    gauge = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=win_rate,
            number={"suffix": "%", "font": {"size": 42}},
            title={"text": "勝率"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": "#22c55e" if win_rate >= 50 else "#f87171"},
                "steps": [
                    {"range": [0, 40], "color": "#3f1d1d"},
                    {"range": [40, 60], "color": "#30333a"},
                    {"range": [60, 100], "color": "#0f2f1f"},
                ],
            },
        )
    )
    gauge.update_layout(template="plotly_dark", height=320, margin=dict(l=10, r=10, t=50, b=0))
    st.plotly_chart(gauge, use_container_width=True)
with c2:
    pie = px.pie(
        pd.DataFrame({"結果": ["勝ち", "負け"], "件数": [win_count, loss_count]}),
        names="結果",
        values="件数",
        color="結果",
        color_discrete_map={"勝ち": "#22c55e", "負け": "#ef4444"},
        template="plotly_dark",
        title="勝ち負け件数",
    )
    pie.update_layout(height=320, margin=dict(l=10, r=10, t=50, b=0))
    st.plotly_chart(pie, use_container_width=True)

st.dataframe(
    ticker_summary[
        ["stock_code", "company_name", "sector", "trades", "wins", "losses", "win_rate", "realized_pnl", "return_pct"]
    ].rename(
        columns={
            "stock_code": "銘柄コード",
            "company_name": "企業名",
            "sector": "セクター",
            "trades": "件数",
            "wins": "勝ち",
            "losses": "負け",
            "win_rate": "勝率(%)",
            "realized_pnl": "実現損益",
            "return_pct": "リターン(%)",
        }
    ),
    use_container_width=True,
    column_config={
        "勝率(%)": st.column_config.NumberColumn("勝率(%)", format="%.1f"),
        "実現損益": st.column_config.NumberColumn("実現損益", format="¥%.0f"),
        "リターン(%)": st.column_config.NumberColumn("リターン(%)", format="%.2f"),
    },
)

st.subheader("2. 銘柄別の平均保有期間")
ticker_holding = ticker_summary[
    ["stock_code", "company_name", "sector", "trades", "avg_holding_days"]
].rename(
    columns={
        "stock_code": "銘柄コード",
        "company_name": "企業名",
        "sector": "セクター",
        "trades": "売却件数",
        "avg_holding_days": "平均保有日数",
    }
)
sector_holding = (
    ticker_summary.groupby("sector", as_index=False)
    .agg(
        sell_trades=("trades", "sum"),
        weighted_days=("holding_days_weighted_sum", "sum"),
        total_qty=("total_qty", "sum"),
    )
    .assign(avg_holding_days=lambda d: np.where(d["total_qty"] > 0, d["weighted_days"] / d["total_qty"], 0.0))
    .rename(columns={"sector": "セクター", "sell_trades": "売却件数", "avg_holding_days": "平均保有日数"})
)

c3, c4 = st.columns(2)
with c3:
    st.markdown("**銘柄別**")
    st.dataframe(
        ticker_holding.sort_values("平均保有日数", ascending=False),
        use_container_width=True,
        column_config={"平均保有日数": st.column_config.NumberColumn("平均保有日数", format="%.1f日")},
    )
with c4:
    st.markdown("**セクター別**")
    st.dataframe(
        sector_holding[["セクター", "売却件数", "平均保有日数"]].sort_values("平均保有日数", ascending=False),
        use_container_width=True,
        column_config={"平均保有日数": st.column_config.NumberColumn("平均保有日数", format="%.1f日")},
    )

hist = px.histogram(
    pair_df.assign(保有日数=np.where(pair_df["matched_qty"] > 0, pair_df["holding_days_weighted_sum"] / pair_df["matched_qty"], 0)),
    x="保有日数",
    nbins=30,
    template="plotly_dark",
    title="売却トレード単位の保有日数ヒストグラム",
    color_discrete_sequence=["#38bdf8"],
)
hist.update_layout(xaxis_title="保有日数", yaxis_title="件数", height=320)
st.plotly_chart(hist, use_container_width=True)

st.subheader("3. 損益ランキング")
top_profit = ticker_summary[ticker_summary["realized_pnl"] > 0].head(10).copy()
top_loss = ticker_summary[ticker_summary["realized_pnl"] < 0].sort_values("realized_pnl", ascending=True).head(5).copy()

c5, c6 = st.columns(2)
with c5:
    st.markdown("**利益TOP10**")
    st.dataframe(
        top_profit[["stock_code", "company_name", "realized_pnl", "return_pct"]].rename(
            columns={"stock_code": "銘柄コード", "company_name": "企業名", "realized_pnl": "金額", "return_pct": "割合(%)"}
        ),
        use_container_width=True,
        column_config={
            "金額": st.column_config.NumberColumn("金額", format="¥%.0f"),
            "割合(%)": st.column_config.NumberColumn("割合(%)", format="%.2f"),
        },
    )
with c6:
    st.markdown("**損失TOP5**")
    st.dataframe(
        top_loss[["stock_code", "company_name", "realized_pnl", "return_pct"]].rename(
            columns={"stock_code": "銘柄コード", "company_name": "企業名", "realized_pnl": "金額", "return_pct": "割合(%)"}
        ),
        use_container_width=True,
        column_config={
            "金額": st.column_config.NumberColumn("金額", format="¥%.0f"),
            "割合(%)": st.column_config.NumberColumn("割合(%)", format="%.2f"),
        },
    )

ranking_df = pd.concat([top_profit.assign(kind="利益"), top_loss.assign(kind="損失")], ignore_index=True)
if not ranking_df.empty:
    bar = go.Figure(
        go.Bar(
            x=ranking_df["stock_code"],
            y=ranking_df["realized_pnl"],
            marker_color=["#22c55e" if v >= 0 else "#ef4444" for v in ranking_df["realized_pnl"]],
            text=[f"{v:,.0f}" for v in ranking_df["realized_pnl"]],
            textposition="outside",
        )
    )
    bar.update_layout(template="plotly_dark", title="損益ランキング（金額）", xaxis_title="銘柄", yaxis_title="実現損益", height=360)
    st.plotly_chart(bar, use_container_width=True)

st.subheader("4. セクター別投資リターン")
if sector_summary.empty:
    st.info("セクター別集計データがありません。")
else:
    heat = go.Figure(
        data=go.Heatmap(
            z=[sector_summary["return_pct"].tolist()],
            x=sector_summary["sector"].tolist(),
            y=["リターン(%)"],
            zmid=0,
            colorscale=[[0.0, "#ef4444"], [0.5, "#111827"], [1.0, "#22c55e"]],
            text=[[f"{v:.2f}%" for v in sector_summary["return_pct"].tolist()]],
            texttemplate="%{text}",
            hovertemplate="セクター:%{x}<br>リターン:%{z:.2f}%<extra></extra>",
        )
    )
    heat.update_layout(template="plotly_dark", height=260, title="セクター別リターンヒートマップ")
    st.plotly_chart(heat, use_container_width=True)
    st.dataframe(
        sector_summary.rename(
            columns={
                "sector": "セクター",
                "trades": "件数",
                "wins": "勝ち",
                "win_rate": "勝率(%)",
                "realized_pnl": "実現損益",
                "return_pct": "リターン(%)",
            }
        )[["セクター", "件数", "勝ち", "勝率(%)", "実現損益", "リターン(%)"]],
        use_container_width=True,
        column_config={
            "勝率(%)": st.column_config.NumberColumn("勝率(%)", format="%.1f"),
            "実現損益": st.column_config.NumberColumn("実現損益", format="¥%.0f"),
            "リターン(%)": st.column_config.NumberColumn("リターン(%)", format="%.2f"),
        },
    )

st.metric("全体勝率", f"{win_rate:.1f}%")
st.metric("全体実現損益", _jpy(float(pair_df['realized_pnl'].sum())))

render_footer()

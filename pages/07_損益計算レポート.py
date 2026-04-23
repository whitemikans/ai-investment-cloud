from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from db.db_utils import get_portfolio_df_with_price, get_snapshots, get_transactions_df_sql, init_db
from utils.common import apply_global_ui_tweaks, render_footer, render_last_data_update


def _jpy(value: float) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "-"
    return f"¥{value:,.0f}"


def _build_realized_pnl_table(tx_df: pd.DataFrame) -> pd.DataFrame:
    if tx_df.empty:
        return pd.DataFrame(columns=["銘柄コード", "企業名", "売却株数", "平均取得単価", "平均売却単価", "実現損益"])

    rows: list[dict[str, object]] = []
    for code, g in tx_df.groupby("stock_code"):
        buys = g[g["trade_type"] == "買"].copy()
        sells = g[g["trade_type"] == "売"].copy()
        if buys.empty or sells.empty:
            continue

        buy_qty = float(buys["quantity"].sum())
        if buy_qty <= 0:
            continue
        buy_cost = float((buys["quantity"] * buys["price"] + buys["commission"]).sum())
        avg_buy = buy_cost / buy_qty

        sold_qty = float(sells["quantity"].sum())
        if sold_qty <= 0:
            continue
        sell_proceeds = float((sells["quantity"] * sells["price"] - sells["commission"]).sum())
        avg_sell = sell_proceeds / sold_qty
        realized = sell_proceeds - avg_buy * sold_qty

        rows.append(
            {
                "銘柄コード": code,
                "企業名": str(g["company_name"].iloc[0]),
                "売却株数": sold_qty,
                "平均取得単価": avg_buy,
                "平均売却単価": avg_sell,
                "実現損益": realized,
            }
        )

    return pd.DataFrame(rows).sort_values("実現損益", ascending=False) if rows else pd.DataFrame()


def _build_trend_df(snap_df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if snap_df.empty:
        return pd.DataFrame(columns=["period", "realized_pl", "unrealized_pl", "total_pl"])

    work = snap_df.copy()
    work["snapshot_date"] = pd.to_datetime(work["snapshot_date"])

    if mode == "yearly":
        work["period"] = work["snapshot_date"].dt.strftime("%Y")
    else:
        work["period"] = work["snapshot_date"].dt.strftime("%Y-%m")

    grouped = work.sort_values("snapshot_date").groupby("period", as_index=False).last()
    grouped["total_pl"] = grouped["realized_pl"] + grouped["unrealized_pl"]
    return grouped[["period", "realized_pl", "unrealized_pl", "total_pl"]]


def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _build_simple_pdf(lines: list[str]) -> bytes:
    # Minimal one-page PDF generator using standard Helvetica (ASCII text).
    y = 800
    content_parts: list[str] = ["BT", "/F1 11 Tf", "50 820 Td"]
    for i, line in enumerate(lines):
        safe = _pdf_escape(line)
        if i == 0:
            content_parts.append(f"({safe}) Tj")
        else:
            content_parts.append(f"0 -16 Td ({safe}) Tj")
        y -= 16
        if y < 60:
            break
    content_parts.append("ET")
    content_stream = "\n".join(content_parts).encode("ascii", errors="ignore")

    objects: list[bytes] = []
    objects.append(b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n")
    objects.append(b"2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n")
    objects.append(
        b"3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] "
        b"/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >> endobj\n"
    )
    objects.append(b"4 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj\n")
    objects.append(
        f"5 0 obj << /Length {len(content_stream)} >> stream\n".encode("ascii")
        + content_stream
        + b"\nendstream endobj\n"
    )

    pdf = bytearray(b"%PDF-1.4\n")
    xref_positions = [0]
    for obj in objects:
        xref_positions.append(len(pdf))
        pdf.extend(obj)

    xref_start = len(pdf)
    pdf.extend(f"xref\n0 {len(xref_positions)}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for pos in xref_positions[1:]:
        pdf.extend(f"{pos:010d} 00000 n \n".encode("ascii"))
    pdf.extend(
        f"trailer << /Size {len(xref_positions)} /Root 1 0 R >>\nstartxref\n{xref_start}\n%%EOF\n".encode("ascii")
    )
    return bytes(pdf)


st.title("🧾 損益計算レポート")
apply_global_ui_tweaks()
st.caption("確定損益・含み損益・損益推移を統合表示し、PDFレポートを出力します。")

with st.spinner("DBを読み込み中..."):
    init_db()

render_last_data_update()

with st.spinner("取引・保有・スナップショットを取得中..."):
    tx_df = get_transactions_df_sql()
    portfolio_df = get_portfolio_df_with_price()
    snap_df = get_snapshots(period="daily")

realized_df = _build_realized_pnl_table(tx_df)
realized_total = float(realized_df["実現損益"].sum()) if not realized_df.empty else 0.0

st.subheader("1. 確定損益（売却済み銘柄）")
st.metric("合計確定損益", _jpy(realized_total))
if realized_df.empty:
    st.info("売却取引がないため、確定損益データがありません。")
else:
    st.dataframe(
        realized_df,
        use_container_width=True,
        column_config={
            "売却株数": st.column_config.NumberColumn("売却株数", format="%.0f"),
            "平均取得単価": st.column_config.NumberColumn("平均取得単価", format="%.2f"),
            "平均売却単価": st.column_config.NumberColumn("平均売却単価", format="%.2f"),
            "実現損益": st.column_config.NumberColumn("実現損益", format="%.0f"),
        },
    )

st.subheader("2. 含み損益（現在保有中の銘柄）")
if portfolio_df.empty:
    st.info("保有銘柄がありません。")
    unrealized_total = 0.0
else:
    unrealized_total = float(portfolio_df["unrealized_pl"].fillna(0).sum())
    st.metric("合計含み損益", _jpy(unrealized_total))

    unrealized_view = portfolio_df[
        [
            "stock_code",
            "company_name",
            "total_quantity",
            "avg_price",
            "current_price",
            "total_cost",
            "market_value",
            "unrealized_pl",
            "unrealized_pl_pct",
        ]
    ].rename(
        columns={
            "stock_code": "銘柄コード",
            "company_name": "企業名",
            "total_quantity": "保有株数",
            "avg_price": "平均取得単価",
            "current_price": "現在株価",
            "total_cost": "投資額",
            "market_value": "評価額",
            "unrealized_pl": "含み損益",
            "unrealized_pl_pct": "含み損益率(%)",
        }
    )
    st.dataframe(unrealized_view, use_container_width=True)

    heat = go.Figure(
        data=go.Heatmap(
            z=[portfolio_df["unrealized_pl"].fillna(0).tolist()],
            x=portfolio_df["stock_code"].tolist(),
            y=["含み損益"],
            zmid=0,
            colorscale=[[0.0, "#ef4444"], [0.5, "#111827"], [1.0, "#22c55e"]],
            text=[[f"{v:,.0f}" for v in portfolio_df["unrealized_pl"].fillna(0).tolist()]],
            texttemplate="%{text}",
            hovertemplate="銘柄:%{x}<br>含み損益:%{z:,.0f}<extra></extra>",
        )
    )
    heat.update_layout(template="plotly_dark", title="含み損益ヒートマップ（緑=利益 / 赤=損失）", height=240)
    st.plotly_chart(heat, use_container_width=True)

st.subheader("3. 月別/年別の損益推移")
mode = st.radio("集計単位", options=["monthly", "yearly"], horizontal=True)
trend_df = _build_trend_df(snap_df, mode=mode)
if trend_df.empty:
    st.info("損益推移データがありません。")
else:
    trend_fig = go.Figure()
    trend_fig.add_trace(
        go.Bar(
            x=trend_df["period"],
            y=trend_df["realized_pl"],
            name="確定損益",
            marker_color="#38bdf8",
            opacity=0.6,
        )
    )
    trend_fig.add_trace(
        go.Bar(
            x=trend_df["period"],
            y=trend_df["unrealized_pl"],
            name="含み損益",
            marker_color="#22c55e",
            opacity=0.55,
        )
    )
    trend_fig.add_trace(
        go.Scatter(
            x=trend_df["period"],
            y=trend_df["total_pl"],
            mode="lines+markers",
            name="合計損益",
            line=dict(color="#f59e0b", width=3),
        )
    )
    trend_fig.update_layout(
        template="plotly_dark",
        barmode="group",
        title="損益推移",
        xaxis_title="期間",
        yaxis_title="損益（円）",
        height=420,
    )
    st.plotly_chart(trend_fig, use_container_width=True)

st.subheader("4. PDFレポート")
total_value = float(portfolio_df["market_value"].fillna(0).sum()) if not portfolio_df.empty else 0.0
total_cost = float(portfolio_df["total_cost"].fillna(0).sum()) if not portfolio_df.empty else 0.0
unrealized_total = float(portfolio_df["unrealized_pl"].fillna(0).sum()) if not portfolio_df.empty else 0.0
report_lines = [
    "PnL Report (AI Investment Dashboard)",
    f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    "",
    f"Realized PnL: {realized_total:,.0f} JPY",
    f"Unrealized PnL: {unrealized_total:,.0f} JPY",
    f"Total Market Value: {total_value:,.0f} JPY",
    f"Total Cost: {total_cost:,.0f} JPY",
    "",
    "Top Realized PnL by Ticker:",
]
if not realized_df.empty:
    for _, row in realized_df.head(10).iterrows():
        report_lines.append(f"- {row['銘柄コード']}: {float(row['実現損益']):,.0f} JPY")
else:
    report_lines.append("- No realized trades")

pdf_bytes = _build_simple_pdf(report_lines)
st.download_button(
    "📄 PDFレポートをダウンロード",
    data=pdf_bytes,
    file_name=f"pnl_report_{datetime.now().strftime('%Y%m%d')}.pdf",
    mime="application/pdf",
    use_container_width=True,
)

render_footer()

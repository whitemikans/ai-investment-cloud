from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st

from db.db_utils import get_dividends, get_transactions_df_sql, init_db
from utils.common import apply_global_ui_tweaks, dataframe_to_csv_bytes, render_footer, render_last_data_update

TAX_RATE = 0.20315


def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _build_simple_pdf(lines: list[str]) -> bytes:
    content_parts: list[str] = ["BT", "/F1 11 Tf", "50 820 Td"]
    y = 800
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


def _calc_yearly_realized_pnl(tx_df: pd.DataFrame, year: int) -> float:
    if tx_df.empty:
        return 0.0
    work = tx_df.copy()
    work["trade_date"] = pd.to_datetime(work["trade_date"])
    work = work.sort_values(["stock_code", "trade_date", "id"])

    realized = 0.0
    holdings: dict[str, dict[str, float]] = {}
    for row in work.itertuples(index=False):
        code = str(row.stock_code)
        qty = float(row.quantity)
        price = float(row.price)
        comm = float(row.commission)
        dt = pd.Timestamp(row.trade_date)
        state = holdings.setdefault(code, {"qty": 0.0, "cost": 0.0})

        if row.trade_type == "買":
            state["qty"] += qty
            state["cost"] += qty * price + comm
            continue

        if state["qty"] <= 0:
            continue
        matched_qty = min(qty, state["qty"])
        avg_cost = state["cost"] / state["qty"] if state["qty"] > 0 else 0.0
        sell_comm = comm * (matched_qty / qty) if qty > 0 else 0.0
        pnl = matched_qty * price - sell_comm - avg_cost * matched_qty

        state["qty"] -= matched_qty
        state["cost"] -= avg_cost * matched_qty

        if dt.year == year:
            realized += pnl
    return realized


st.title("🧾 確定申告用 税金計算レポート")
apply_global_ui_tweaks()
st.caption("年度別に売却益・配当金・税額・損益通算を計算し、CSV/PDFを出力します。")

with st.spinner("DBを読み込み中..."):
    init_db()
render_last_data_update()

with st.spinner("取引・配当データを取得中..."):
    tx_df = get_transactions_df_sql()
    div_df = get_dividends()

if tx_df.empty and div_df.empty:
    st.info("取引または配当データがありません。")
    render_footer()
    st.stop()

year_candidates: set[int] = set()
if not tx_df.empty:
    year_candidates.update(pd.to_datetime(tx_df["trade_date"]).dt.year.astype(int).tolist())
if not div_df.empty:
    year_candidates.update(pd.to_datetime(div_df["payment_date"]).dt.year.astype(int).tolist())
year_options = sorted(year_candidates, reverse=True) if year_candidates else [datetime.now().year]

selected_year = st.selectbox("年度を選択", options=year_options, index=0)

realized_pnl = _calc_yearly_realized_pnl(tx_df, selected_year)
year_div = div_df[pd.to_datetime(div_df["payment_date"]).dt.year == selected_year].copy() if not div_df.empty else pd.DataFrame()
gross_dividend = float(year_div["total_amount"].sum()) if not year_div.empty else 0.0
withheld_tax = float(year_div["tax_amount"].sum()) if not year_div.empty else 0.0

capital_gain_tax = max(realized_pnl, 0.0) * TAX_RATE
offset_adjusted_income = realized_pnl + gross_dividend
offset_taxable_income = max(offset_adjusted_income, 0.0)
offset_amount = min(abs(min(realized_pnl, 0.0)), gross_dividend)
tax_after_offset = offset_taxable_income * TAX_RATE

summary_df = pd.DataFrame(
    [
        {"項目": "年度", "値": selected_year},
        {"項目": "年間の株式売却益（確定損益）", "値": round(realized_pnl, 2)},
        {"項目": "年間の配当金受取額（税引前）", "値": round(gross_dividend, 2)},
        {"項目": "年間の源泉徴収税額（配当）", "値": round(withheld_tax, 2)},
        {"項目": "売却益に対する税額（20.315%）", "値": round(capital_gain_tax, 2)},
        {"項目": "損益通算による相殺額", "値": round(offset_amount, 2)},
        {"項目": "損益通算後の課税対象額", "値": round(offset_taxable_income, 2)},
        {"項目": "損益通算後の想定税額（20.315%）", "値": round(tax_after_offset, 2)},
    ]
)

c1, c2, c3 = st.columns(3)
c1.metric("年間確定損益", f"¥{realized_pnl:,.0f}")
c2.metric("年間配当金(税引前)", f"¥{gross_dividend:,.0f}")
c3.metric("源泉徴収税額", f"¥{withheld_tax:,.0f}")

c4, c5, c6 = st.columns(3)
c4.metric("売却益課税額", f"¥{capital_gain_tax:,.0f}")
c5.metric("損益通算相殺額", f"¥{offset_amount:,.0f}")
c6.metric("損益通算後想定税額", f"¥{tax_after_offset:,.0f}")

st.subheader("税金計算サマリー")
st.dataframe(
    summary_df,
    use_container_width=True,
    column_config={"値": st.column_config.NumberColumn("値", format="%.2f")},
)

csv_bytes = dataframe_to_csv_bytes(summary_df)
st.download_button(
    "📥 税金レポートCSVをダウンロード",
    data=csv_bytes,
    file_name=f"tax_report_{selected_year}.csv",
    mime="text/csv",
    use_container_width=True,
)

pdf_lines = [
    f"Tax Report Year: {selected_year}",
    f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    "",
    f"Realized PnL: {realized_pnl:,.0f} JPY",
    f"Gross Dividend: {gross_dividend:,.0f} JPY",
    f"Withheld Tax: {withheld_tax:,.0f} JPY",
    f"Capital Gain Tax (20.315%): {capital_gain_tax:,.0f} JPY",
    f"Offset Amount: {offset_amount:,.0f} JPY",
    f"Taxable After Offset: {offset_taxable_income:,.0f} JPY",
    f"Estimated Tax After Offset: {tax_after_offset:,.0f} JPY",
]
pdf_bytes = _build_simple_pdf(pdf_lines)
st.download_button(
    "📄 税金レポートPDFをダウンロード",
    data=pdf_bytes,
    file_name=f"tax_report_{selected_year}.pdf",
    mime="application/pdf",
    use_container_width=True,
)

render_footer()

from __future__ import annotations

import json
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf
from sqlalchemy import text

from db.ai_team_utils import (
    get_ai_team_agent_logs,
    get_ai_team_report_history,
    get_latest_ai_team_report,
    init_ai_team_tables,
)
from db.models import engine
from run_investment_crew import run_investment_crew
from utils.common import apply_global_ui_tweaks, render_footer


st.set_page_config(page_title="AIチーム分析", page_icon="🤖", layout="wide")
apply_global_ui_tweaks()
st.title("🤖 AIチーム分析")
st.caption("リサーチャー/アナリスト/リスクマネージャー/レポーターの実行結果を統合表示")

init_ai_team_tables()


@st.cache_data(ttl=3600)
def fetch_auto_1m_return(ticker: str) -> float | None:
    code = str(ticker or "").strip().upper()
    if not code:
        return None
    try:
        hist = yf.Ticker(code).history(period="2mo")
        if hist is None or hist.empty or len(hist) < 20:
            return None
        close = pd.to_numeric(hist["Close"], errors="coerce").dropna()
        if len(close) < 20:
            return None
        latest = float(close.iloc[-1])
        ref = float(close.iloc[max(0, len(close) - 22)])
        if ref == 0:
            return None
        return float((latest / ref - 1.0) * 100.0)
    except Exception:
        return None

top1, top2 = st.columns([1, 2])
with top1:
    if st.button("AIチームを今すぐ実行", use_container_width=True, type="primary"):
        with st.spinner("AI投資チームを実行中..."):
            run_investment_crew()
        st.success("AIチーム実行が完了しました。")
        st.rerun()
with top2:
    st.page_link("pages/20_AI分析.py", label="AI分析ページへ", icon="🧠")

history_df = get_ai_team_report_history(limit=120)
if history_df.empty:
    st.info("AIチームレポート履歴がありません。まず実行してください。")
    render_footer()
    st.stop()

selected_run_id = st.selectbox(
    "表示するレポート日時",
    options=history_df["run_id"].tolist(),
    format_func=lambda x: f"{x} | {history_df.loc[history_df['run_id'] == x, 'created_at'].iloc[0]}",
)

current_row = history_df[history_df["run_id"] == selected_run_id].iloc[0]
latest_df = get_latest_ai_team_report()

report_sql = """
SELECT *
FROM ai_team_reports
WHERE run_id = :run_id
ORDER BY id DESC
LIMIT 1
"""
report_df = pd.read_sql(text(report_sql), con=engine, params={"run_id": selected_run_id})
if report_df.empty:
    st.warning("対象レポートを読み込めませんでした。")
    render_footer()
    st.stop()

report = report_df.iloc[0]
logs_df = get_ai_team_agent_logs(selected_run_id)

k1, k2, k3 = st.columns(3)
k1.metric("選択Run", str(selected_run_id))
k2.metric("リスクレベル", str(report.get("risk_level", "")))
k3.metric("最終更新", str(report.get("created_at", "")))

st.markdown("### 最新デイリーレポート")
st.write(str(report.get("full_report", "")).strip() or "レポート本文なし")

st.markdown("### エージェント別実行ログ")
if logs_df.empty:
    st.info("エージェントログがありません。")
else:
    for row in logs_df.itertuples(index=False):
        with st.expander(f"{row.agent_name} | {row.created_at}", expanded=False):
            st.write(str(row.output_text or "").strip() or "テキスト出力なし")
            if str(row.output_json or "").strip():
                try:
                    st.json(json.loads(row.output_json))
                except Exception:
                    st.code(str(row.output_json))

st.markdown("### 銘柄別5軸評価（レーダー）")
recs = []
try:
    recs = json.loads(str(report.get("recommendations_json", "[]")))
except Exception:
    recs = []

if not recs:
    st.info("5軸評価データがありません。")
else:
    axes = ["ニュース", "テクニカル", "バリュエーション", "成長性", "リスク"]
    selected_ticker = st.selectbox("銘柄", options=[str(r.get("ticker", "-")) for r in recs], index=0)
    item = next((r for r in recs if str(r.get("ticker")) == selected_ticker), recs[0])
    tech = float(((item.get("technical") or {}).get("technical_score")) or 3)
    val = float(((item.get("fundamental") or {}).get("valuation_score")) or 3)
    growth = float(((item.get("fundamental") or {}).get("growth_score")) or 3)
    stars = float(item.get("stars", 3))
    risk_inv = max(1.0, 6.0 - stars)
    news = stars
    values = [news, tech, val, growth, risk_inv]
    fig = go.Figure()
    fig.add_trace(
        go.Scatterpolar(r=values + [values[0]], theta=axes + [axes[0]], fill="toself", name=selected_ticker)
    )
    fig.update_layout(template="plotly_dark", polar=dict(radialaxis=dict(visible=True, range=[1, 5])), height=420)
    st.plotly_chart(fig, use_container_width=True)

st.markdown("### 人間フィードバック")
with st.form("feedback_form"):
    f1, f2, f3 = st.columns(3)
    ticker = f1.text_input("Ticker", value="AAPL")
    ai_rec = f2.selectbox("AI推奨", ["買い", "保持", "売り"], index=1)
    decision = f3.selectbox("人間判断", ["承認", "却下", "保留"], index=0)
    c1, c2 = st.columns(2)
    reason = c1.text_area("理由（却下時など）", value="", height=80)
    action_taken = c2.checkbox("実際に売買実行", value=False)
    auto_ret = st.checkbox("1ヶ月後リターンを自動取得", value=True)
    ret1m = st.number_input("1ヶ月後リターン(%)", value=0.0, step=0.1, disabled=auto_ret)
    submit = st.form_submit_button("フィードバック保存", use_container_width=True)
    if submit:
        resolved_ret1m = ret1m
        if auto_ret:
            auto_val = fetch_auto_1m_return(ticker)
            if auto_val is None:
                st.warning("自動取得に失敗したため、0.0%で保存します。")
                resolved_ret1m = 0.0
            else:
                resolved_ret1m = float(auto_val)
        with engine.begin() as con:
            con.execute(
                text(
                    """
                    INSERT INTO agent_feedback(date, created_at, run_id, agent_name, ticker, ai_recommendation, human_decision, human_reason, action_taken, actual_return_1m)
                    VALUES(:date, :created_at, :run_id, :agent_name, :ticker, :ai_rec, :decision, :reason, :action_taken, :ret)
                    """
                ),
                {
                    "date": datetime.now().date().isoformat(),
                    "created_at": datetime.now().isoformat(timespec="seconds"),
                    "run_id": selected_run_id,
                    "agent_name": "analyst",
                    "ticker": ticker.strip().upper(),
                    "ai_rec": ai_rec,
                    "decision": decision,
                    "reason": reason.strip(),
                    "action_taken": 1 if action_taken else 0,
                    "ret": float(resolved_ret1m),
                },
            )
        st.success("保存しました。")
        st.rerun()

stats_sql = """
SELECT
  COUNT(*) AS total_count,
  AVG(CASE WHEN human_decision='承認' THEN 1.0 ELSE 0.0 END) AS approve_rate,
  AVG(CASE WHEN human_decision='承認' THEN actual_return_1m END) AS approve_ret,
  AVG(CASE WHEN human_decision='却下' THEN actual_return_1m END) AS reject_ret
FROM agent_feedback
"""
stats_df = pd.read_sql(text(stats_sql), con=engine)
if not stats_df.empty:
    s = stats_df.iloc[0]
    c1, c2, c3 = st.columns(3)
    c1.metric("フィードバック件数", f"{int(s.get('total_count') or 0)}")
    c2.metric("AI推奨承認率", f"{float((s.get('approve_rate') or 0.0) * 100):.1f}%")
    diff = float((s.get("approve_ret") or 0.0) - (s.get("reject_ret") or 0.0))
    c3.metric("承認-却下 平均1Mリターン差", f"{diff:+.2f}pt")

st.markdown("### 銘柄別・エージェント別 承認率")
ticker_stats_sql = """
SELECT
  ticker,
  COUNT(*) AS total_count,
  AVG(CASE WHEN human_decision='承認' THEN 1.0 ELSE 0.0 END) AS approve_rate,
  AVG(actual_return_1m) AS avg_return_1m
FROM agent_feedback
GROUP BY ticker
ORDER BY total_count DESC, ticker
"""
agent_stats_sql = """
SELECT
  COALESCE(agent_name, 'unknown') AS agent_name,
  COUNT(*) AS total_count,
  AVG(CASE WHEN human_decision='承認' THEN 1.0 ELSE 0.0 END) AS approve_rate,
  AVG(actual_return_1m) AS avg_return_1m
FROM agent_feedback
GROUP BY COALESCE(agent_name, 'unknown')
ORDER BY total_count DESC
"""
col_t, col_a = st.columns(2)
with col_t:
    ticker_df = pd.read_sql(text(ticker_stats_sql), con=engine)
    if ticker_df.empty:
        st.info("銘柄別統計データがありません。")
    else:
        st.dataframe(
            ticker_df.style.format({"approve_rate": "{:.1%}", "avg_return_1m": "{:+.2f}%"}),
            use_container_width=True,
            height=220,
        )
with col_a:
    agent_df = pd.read_sql(text(agent_stats_sql), con=engine)
    if agent_df.empty:
        st.info("エージェント別統計データがありません。")
    else:
        st.dataframe(
            agent_df.style.format({"approve_rate": "{:.1%}", "avg_return_1m": "{:+.2f}%"}),
            use_container_width=True,
            height=220,
        )

st.markdown("### AIに従った場合 vs 人間判断の比較")
compare_sql = """
SELECT
  AVG(CASE WHEN human_decision='承認' AND action_taken=1 THEN actual_return_1m END) AS follow_ai_ret,
  AVG(CASE WHEN human_decision='却下' AND action_taken=1 THEN actual_return_1m END) AS human_override_ret
FROM agent_feedback
"""
cmp_df = pd.read_sql(text(compare_sql), con=engine)
if not cmp_df.empty:
    c = cmp_df.iloc[0]
    v1 = float(c.get("follow_ai_ret") or 0.0)
    v2 = float(c.get("human_override_ret") or 0.0)
    x1, x2, x3 = st.columns(3)
    x1.metric("AIに従った平均1Mリターン", f"{v1:+.2f}%")
    x2.metric("人間判断（却下）平均1Mリターン", f"{v2:+.2f}%")
    x3.metric("差分", f"{(v1 - v2):+.2f}pt")

render_footer()

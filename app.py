from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf

from db.db_utils import get_portfolio_df_with_price, init_db
from db.news_utils import init_news_tables
from utils.auth import ensure_login, render_logout
from utils.common import apply_global_ui_tweaks, log_event, render_last_data_update


PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "investment.db"


@st.cache_data(ttl=300)
def load_portfolio_market_df() -> pd.DataFrame:
    try:
        return get_portfolio_df_with_price()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_news_top3() -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame(columns=["title", "url", "source", "published_at", "summary_ja"])
    sql = """
    SELECT title, url, source, published_at, COALESCE(summary_ja, '') AS summary_ja
    FROM news_articles
    ORDER BY published_at DESC, id DESC
    LIMIT 3
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            return pd.read_sql_query(sql, conn)
    except Exception:
        return pd.DataFrame(columns=["title", "url", "source", "published_at", "summary_ja"])


@st.cache_data(ttl=300)
def calc_today_pnl(positions: tuple[tuple[str, float], ...]) -> float:
    total = 0.0
    for code, qty in positions:
        try:
            hist = yf.Ticker(code).history(period="5d", interval="1d")
            if hist is None or hist.empty or len(hist) < 2:
                continue
            close = hist["Close"].dropna()
            if len(close) < 2:
                continue
            diff = float(close.iloc[-1] - close.iloc[-2])
            total += diff * float(qty)
        except Exception:
            continue
    return float(total)


def calc_fire_metrics(total_assets: float) -> tuple[float, float]:
    fire_type = st.session_state.get("FIREタイプ", "Fat")
    swr_map = {"Fat": 0.04, "Lean": 0.035, "Barista": 0.04, "Coast": 0.04}
    swr = float(swr_map.get(str(fire_type), 0.04))

    annual_expense = float(st.session_state.get("年間支出（円）", 3_600_000))
    part_time_monthly = float(st.session_state.get("Barista収入（月額円）", 0))
    part_time_annual = part_time_monthly * 12.0

    fire_target = max(0.0, (annual_expense - part_time_annual) / max(1e-9, swr))
    life_sim = st.session_state.get("life_sim_result")
    if isinstance(life_sim, dict) and isinstance(life_sim.get("mc"), dict):
        fire_prob = float(life_sim["mc"].get("fire_probability", 0.0))
    else:
        fire_prob = min(0.99, (total_assets / fire_target * 0.9) if fire_target > 0 else 0.0)
    return fire_target, fire_prob


def calc_optimization_score(portfolio_df: pd.DataFrame) -> float:
    opt_payload = st.session_state.get("opt_payload")
    if isinstance(opt_payload, dict):
        selected = opt_payload.get("selected", {}) or {}
        sharpe = float(selected.get("sharpe", 0.0))
        weights = selected.get("weights", {}) or {}
        w = pd.Series(weights, dtype=float)
        if not w.empty and float(w.sum()) > 0:
            w = w / float(w.sum())
            effective_n = 1.0 / float((w**2).sum())
            div_score = min(100.0, effective_n / max(1.0, len(w)) * 120.0)
        else:
            div_score = 50.0
        sharpe_score = max(0.0, min(100.0, (sharpe + 0.2) / 1.6 * 100.0))
        return float(0.6 * sharpe_score + 0.4 * div_score)

    if portfolio_df.empty:
        return 0.0
    mv = portfolio_df["market_value"].fillna(0.0)
    total = float(mv.sum())
    if total <= 0:
        return 0.0
    weights = mv / total
    concentration = float((weights**2).sum())
    return float(max(0.0, min(100.0, (1.0 - concentration) * 130.0)))


def build_rebalance_alerts(portfolio_df: pd.DataFrame) -> pd.DataFrame:
    if portfolio_df.empty:
        return pd.DataFrame(columns=["銘柄", "現在配分", "目標配分", "乖離", "提案"])
    work = portfolio_df.copy()
    work["market_value"] = work["market_value"].fillna(0.0)
    total = float(work["market_value"].sum())
    if total <= 0:
        return pd.DataFrame(columns=["銘柄", "現在配分", "目標配分", "乖離", "提案"])

    work["current_weight"] = work["market_value"] / total
    target = 1.0 / max(1, len(work))
    work["target_weight"] = target
    work["diff"] = work["current_weight"] - work["target_weight"]
    alerts = work[work["diff"].abs() >= 0.05].copy().sort_values("diff", key=lambda s: s.abs(), ascending=False)
    if alerts.empty:
        return pd.DataFrame(columns=["銘柄", "現在配分", "目標配分", "乖離", "提案"])

    alerts["提案"] = alerts["diff"].apply(lambda v: "売却寄り" if v > 0 else "購入寄り")
    return alerts.rename(
        columns={
            "stock_code": "銘柄",
            "current_weight": "現在配分",
            "target_weight": "目標配分",
            "diff": "乖離",
        }
    )[["銘柄", "現在配分", "目標配分", "乖離", "提案"]]


def get_upcoming_life_events(limit: int = 2) -> pd.DataFrame:
    events = st.session_state.get("life_events", [])
    if not events:
        return pd.DataFrame(columns=["name", "age", "event_type", "amount"])
    try:
        current_age = int(st.session_state.get("現在の年齢", 35))
    except Exception:
        current_age = 35

    df = pd.DataFrame(events)
    if df.empty or "age" not in df.columns:
        return pd.DataFrame(columns=["name", "age", "event_type", "amount"])
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df = df.dropna(subset=["age"])
    df = df[df["age"] >= current_age].sort_values("age").head(limit)
    return df[[c for c in ["name", "age", "event_type", "amount"] if c in df.columns]]


st.set_page_config(page_title="統合AI投資ダッシュボード", page_icon="🏠", layout="wide")
ensure_login()
apply_global_ui_tweaks()

with st.spinner("初期化中..."):
    init_db()
    init_news_tables()

render_last_data_update()
render_logout()

st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@400;600;800&display=swap');
html, body, [class*="css"] { font-family: 'Noto Sans JP', sans-serif; }
.hero {
  border: 1px solid rgba(59,130,246,.32);
  border-radius: 14px;
  padding: 16px 18px;
  background: linear-gradient(135deg, rgba(37,99,235,.16), rgba(16,185,129,.10));
}
.kpi-card {
  border: 1px solid rgba(148,163,184,.28);
  border-radius: 12px;
  padding: 10px;
  background: rgba(15,23,42,.25);
}
.footer-v2 {
  margin-top: 1.2rem;
  padding-top: .8rem;
  border-top: 1px solid rgba(148,163,184,.25);
  text-align: center;
  color: #94a3b8;
}
</style>
""",
    unsafe_allow_html=True,
)

st.markdown("<div class='hero'><h2>🏠 統合ダッシュボード</h2><div>講座 #02 #04 #05 #07 #08 の主要指標を1画面で確認</div></div>", unsafe_allow_html=True)

portfolio_df = load_portfolio_market_df()
portfolio_df = portfolio_df.copy() if isinstance(portfolio_df, pd.DataFrame) else pd.DataFrame()
if portfolio_df.empty:
    positions = tuple()
    total_assets = 0.0
else:
    portfolio_df["market_value"] = pd.to_numeric(portfolio_df["market_value"], errors="coerce").fillna(0.0)
    portfolio_df["total_quantity"] = pd.to_numeric(portfolio_df["total_quantity"], errors="coerce").fillna(0.0)
    total_assets = float(portfolio_df["market_value"].sum())
    positions = tuple((str(r["stock_code"]), float(r["total_quantity"])) for _, r in portfolio_df.iterrows())

today_pnl = calc_today_pnl(positions)
fire_target, fire_probability = calc_fire_metrics(total_assets)
opt_score = calc_optimization_score(portfolio_df)

k1, k2, k3, k4 = st.columns(4)
k1.metric("総資産額", f"¥{total_assets:,.0f}")
k2.metric("今日の損益", f"¥{today_pnl:,.0f}")
k3.metric("FIRE達成確率", f"{fire_probability * 100:.1f}%")
k4.metric("最適化スコア", f"{opt_score:.1f}")

left, right = st.columns([1.2, 1])
with left:
    st.markdown("### ポートフォリオ概要")
    if portfolio_df.empty:
        st.info("ポートフォリオデータがありません。")
    else:
        pie_df = portfolio_df[["stock_code", "market_value"]].copy()
        pie_df = pie_df[pie_df["market_value"] > 0]
        fig_pie = px.pie(pie_df, values="market_value", names="stock_code", hole=0.45, template="plotly_dark")
        fig_pie.update_layout(height=380, margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig_pie, use_container_width=True)

with right:
    st.markdown("### FIREゲージ")
    progress = (total_assets / fire_target) if fire_target > 0 else 0.0
    progress = max(0.0, min(1.5, progress))
    gauge = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=progress * 100,
            number={"suffix": "%"},
            title={"text": "資産 / FIRE目標"},
            gauge={
                "axis": {"range": [0, 150]},
                "bar": {"color": "#22c55e"},
                "steps": [
                    {"range": [0, 60], "color": "#1f2937"},
                    {"range": [60, 100], "color": "#334155"},
                    {"range": [100, 150], "color": "#065f46"},
                ],
            },
        )
    )
    gauge.update_layout(template="plotly_dark", height=300, margin=dict(l=20, r=20, t=40, b=10))
    st.plotly_chart(gauge, use_container_width=True)
    st.caption(f"目標資産: ¥{fire_target:,.0f}")

st.markdown("### 今日のAIニュースサマリー（上位3件）")
news_df = load_news_top3()
if news_df.empty:
    st.info("ニュースデータがありません。")
else:
    for i, row in news_df.reset_index(drop=True).iterrows():
        summary = str(row.get("summary_ja", "")).strip() or "要約なし"
        source = str(row.get("source", ""))
        published = str(row.get("published_at", ""))
        title = str(row.get("title", "(タイトルなし)"))
        url = str(row.get("url", "")).strip()
        with st.container(border=True):
            st.markdown(f"**{i + 1}. {title}**")
            st.caption(f"{source} | {published}")
            st.write(summary)
            if url:
                st.link_button("ニュースを開く", url)

b1, b2 = st.columns(2)
with b1:
    st.markdown("### 直近のリバランスアラート")
    alert_df = build_rebalance_alerts(portfolio_df)
    if alert_df.empty:
        st.success("大きな配分乖離はありません。")
    else:
        st.dataframe(
            alert_df.style.format({"現在配分": "{:.1%}", "目標配分": "{:.1%}", "乖離": "{:+.1%}"}),
            use_container_width=True,
            height=220,
        )

with b2:
    st.markdown("### 今後のライフイベント")
    next_events = get_upcoming_life_events(limit=2)
    if next_events.empty:
        st.info("登録済みイベントがありません。")
    else:
        show = next_events.rename(columns={"name": "イベント", "age": "年齢", "event_type": "タイプ", "amount": "金額"})
        st.dataframe(show.style.format({"金額": "{:,.0f}"}), use_container_width=True, height=220)

st.markdown("<div class='footer-v2'>Powered by AI Investment Team | v2.0</div>", unsafe_allow_html=True)
log_event("open_top", f"dashboard_v2_loaded {datetime.now().isoformat(timespec='seconds')}")

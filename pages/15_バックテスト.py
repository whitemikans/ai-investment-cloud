from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from backtest_engine import (
    build_ai_strategy_report,
    compare_strategies,
    fetch_price_data,
    optimize_golden_cross,
    run_backtest,
    save_backtest_result,
    save_comparison_results,
)
from db.db_utils import init_db
from db.news_utils import init_news_tables
from utils.common import apply_global_ui_tweaks, render_footer, render_last_data_update

PRESET_TICKERS = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA"]
STRATEGIES = [
    "ゴールデンクロス（SMA）",
    "RSIリバーサル",
    "MACDクロスオーバー",
    "ボリンジャーバンドブレイクアウト",
    "複合条件（RSI + MACD）",
]


def _strategy_key(label: str) -> str:
    return {
        "ゴールデンクロス（SMA）": "golden_cross",
        "RSIリバーサル": "rsi_reversal",
        "MACDクロスオーバー": "macd_cross",
        "ボリンジャーバンドブレイクアウト": "bb_breakout",
        "複合条件（RSI + MACD）": "combo",
    }[label]


def _monthly_heatmap(curve: pd.DataFrame) -> pd.DataFrame:
    w = curve.copy()
    w["Date"] = pd.to_datetime(w["Date"])
    w["ret"] = w["Equity"].pct_change().fillna(0.0)
    w["Year"] = w["Date"].dt.year
    w["Month"] = w["Date"].dt.month
    month_ret = (1 + w.groupby(["Year", "Month"])["ret"].apply(lambda x: (1 + x).prod() - 1)).reset_index(name="mret")
    return month_ret.pivot(index="Year", columns="Month", values="mret").sort_index()


st.title("📊 バックテスト")
apply_global_ui_tweaks()
st.caption("#05対応: 戦略検証・最適化・比較をGUIで実行")

with st.spinner("初期化中..."):
    init_db()
    init_news_tables()
render_last_data_update()

st.sidebar.subheader("バックテスト設定")
preset = st.sidebar.selectbox("よく使う銘柄", PRESET_TICKERS, index=0)
ticker = st.sidebar.text_input("銘柄選択（ティッカー）", value=preset).upper().strip()
start = st.sidebar.date_input("開始日", value=date.today() - timedelta(days=3650))
end = st.sidebar.date_input("終了日", value=date.today())
strategy_label = st.sidebar.selectbox("戦略選択", STRATEGIES, index=0)
strategy_key = _strategy_key(strategy_label)

params: dict[str, object] = {}
if strategy_key == "golden_cross":
    st.sidebar.markdown("**パラメータ（SMA）**")
    params["short"] = st.sidebar.slider("短期SMA", 5, 50, 15)
    params["long"] = st.sidebar.slider("長期SMA", 30, 200, 60)
elif strategy_key == "rsi_reversal":
    st.sidebar.markdown("**パラメータ（RSI）**")
    params["rsi_low"] = st.sidebar.slider("RSI買い閾値", 10, 45, 30)
    params["rsi_high"] = st.sidebar.slider("RSI売り閾値", 55, 90, 70)
elif strategy_key == "combo":
    st.sidebar.markdown("**パラメータ（複合）**")
    params["rsi_low"] = st.sidebar.slider("RSI買い閾値", 10, 45, 35)
    params["rsi_high"] = st.sidebar.slider("RSI売り閾値", 55, 90, 65)

st.sidebar.markdown("**リスク管理設定**")
stop_loss = st.sidebar.number_input("ストップロス(%)", min_value=0.0, value=0.0, step=0.5)
take_profit = st.sidebar.number_input("テイクプロフィット(%)", min_value=0.0, value=0.0, step=0.5)
sizing_method_label = st.sidebar.selectbox("ポジションサイジング方法", ["全額投入", "資金の固定割合", "リスク一定割合"])
if sizing_method_label == "資金の固定割合":
    sizing_method = "fixed_pct"
    sizing_value = st.sidebar.slider("投入割合", 0.05, 1.0, 0.5, 0.05)
elif sizing_method_label == "リスク一定割合":
    sizing_method = "risk_pct"
    sizing_value = st.sidebar.slider("許容リスク割合", 0.005, 0.1, 0.02, 0.005)
else:
    sizing_method = "all_in"
    sizing_value = 1.0

initial_cash = st.sidebar.number_input("初期資金", min_value=100_000, value=1_000_000, step=100_000)
commission = st.sidebar.number_input("手数料率", min_value=0.0, max_value=0.02, value=0.001, step=0.0005, format="%.4f")
run_bt = st.sidebar.button("▶ バックテスト実行", type="primary", use_container_width=True)

with st.spinner("株価データ取得中..."):
    price_df = fetch_price_data(ticker=ticker, start=start, end=end)

if price_df.empty:
    st.error("株価データを取得できませんでした。銘柄や期間を確認してください。")
    render_footer()
    st.stop()

tab_result, tab_opt, tab_cmp = st.tabs(["結果表示", "パラメータ最適化", "戦略比較レポート"])

with tab_result:
    if run_bt:
        result = run_backtest(
            price_df,
            ticker=ticker,
            strategy_name=strategy_key,
            params=params,
            initial_cash=float(initial_cash),
            commission=float(commission),
            stop_loss_pct=float(stop_loss) if stop_loss > 0 else None,
            take_profit_pct=float(take_profit) if take_profit > 0 else None,
            position_sizing_method=sizing_method,
            position_size_value=float(sizing_value),
        )
        save_backtest_result(result)
        st.session_state["last_backtest_result"] = result
    else:
        result = st.session_state.get("last_backtest_result")

    if result is None:
        st.info("サイドバーの「▶ バックテスト実行」を押してください。")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("総リターン", f"{result.total_return_pct:.1f}%")
        c2.metric("Buy&Hold", f"{result.buy_hold_return_pct:.1f}%")
        c3.metric("最大DD", f"{result.max_drawdown_pct:.1f}%")
        c4, c5, c6 = st.columns(3)
        c4.metric("勝率", f"{result.win_rate_pct:.1f}%")
        c5.metric("シャープ", f"{result.sharpe_ratio:.2f}")
        c6.metric("PF", f"{result.profit_factor:.2f}")

        chart_tab1, chart_tab2, chart_tab3, chart_tab4 = st.tabs(
            ["売買チャート", "エクイティカーブ", "ドローダウン", "月次ヒートマップ"]
        )

        with chart_tab1:
            fig = go.Figure()
            fig.add_trace(
                go.Candlestick(
                    x=price_df["Date"],
                    open=price_df["Open"],
                    high=price_df["High"],
                    low=price_df["Low"],
                    close=price_df["Close"],
                    name="価格",
                    increasing_line_color="#22c55e",
                    decreasing_line_color="#ef4444",
                )
            )
            tlog = result.trade_log.copy() if not result.trade_log.empty else pd.DataFrame()
            if not tlog.empty:
                buy_df = tlog[tlog["type"] == "BUY"]
                sell_df = tlog[tlog["type"] == "SELL"]
                fig.add_trace(go.Scatter(x=buy_df["date"], y=buy_df["price"], mode="markers", marker=dict(color="#3b82f6", size=9), name="BUY"))
                fig.add_trace(go.Scatter(x=sell_df["date"], y=sell_df["price"], mode="markers", marker=dict(color="#f59e0b", size=9), name="SELL"))
            fig.update_layout(template="plotly_dark", height=500, xaxis_title="日付", yaxis_title="価格")
            st.plotly_chart(fig, use_container_width=True)

        with chart_tab2:
            curve = result.equity_curve.copy()
            curve["BuyHold"] = float(initial_cash) * (price_df["Close"] / float(price_df["Close"].iloc[0]))
            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(x=curve["Date"], y=curve["Equity"], name="戦略資産", line=dict(color="#22c55e", width=2)))
            fig2.add_trace(go.Scatter(x=curve["Date"], y=curve["BuyHold"], name="Buy&Hold", line=dict(color="#f59e0b", width=2)))
            fig2.update_layout(template="plotly_dark", height=420, xaxis_title="日付", yaxis_title="資産")
            st.plotly_chart(fig2, use_container_width=True)

        with chart_tab3:
            curve = result.equity_curve.copy()
            curve["rolling_max"] = curve["Equity"].cummax()
            curve["drawdown_pct"] = (curve["Equity"] / curve["rolling_max"] - 1) * 100
            fig3 = px.area(curve, x="Date", y="drawdown_pct", template="plotly_dark", title="ドローダウン(%)")
            fig3.update_traces(line_color="#ef4444", fillcolor="rgba(239,68,68,0.35)")
            fig3.update_layout(height=320, xaxis_title="日付", yaxis_title="DD(%)")
            st.plotly_chart(fig3, use_container_width=True)

        with chart_tab4:
            heat = _monthly_heatmap(result.equity_curve)
            if heat.empty:
                st.info("月次ヒートマップ表示に必要なデータが不足しています。")
            else:
                fig4 = px.imshow(heat, aspect="auto", color_continuous_scale="RdYlGn", title="月次リターンヒートマップ")
                fig4.update_layout(template="plotly_dark", height=360)
                st.plotly_chart(fig4, use_container_width=True)

        st.subheader("トレード一覧")
        if result.trade_log.empty:
            st.info("トレードがありません。")
        else:
            trade_view = result.trade_log.copy()
            trade_view["ticker"] = ticker
            trade_view = trade_view.rename(
                columns={"date": "日付", "ticker": "銘柄", "type": "売買", "price": "価格", "pnl": "損益", "shares": "株数", "reason": "理由"}
            )
            st.dataframe(trade_view[["日付", "銘柄", "売買", "価格", "株数", "損益", "理由"]], use_container_width=True)

with tab_opt:
    st.markdown("パラメータ最適化（ゴールデンクロス）")
    col1, col2 = st.columns(2)
    short_min = col1.number_input("短期SMA最小", 5, 60, 10)
    short_max = col1.number_input("短期SMA最大", 6, 80, 30)
    long_min = col2.number_input("長期SMA最小", 20, 180, 50)
    long_max = col2.number_input("長期SMA最大", 21, 220, 120)
    total_combos = sum(
        1
        for s in range(int(short_min), int(short_max) + 1, 2)
        for l in range(int(long_min), int(long_max) + 1, 5)
        if s < l
    )
    st.caption(f"試行予定の組み合わせ数: {total_combos:,} 通り")
    run_opt = st.button("🔧 最適化実行", use_container_width=True)
    if run_opt:
        progress_bar = st.progress(0)
        progress_text = st.empty()

        def _on_progress(done: int, total: int) -> None:
            ratio = 0 if total <= 0 else min(max(done / total, 0.0), 1.0)
            progress_bar.progress(ratio)
            progress_text.caption(f"最適化実行中... {done:,}/{total:,}")

        with st.spinner("最適化を実行中です..."):
            opt_df = optimize_golden_cross(
                price_df,
                ticker=ticker,
                short_range=range(int(short_min), int(short_max) + 1, 2),
                long_range=range(int(long_min), int(long_max) + 1, 5),
                progress_callback=_on_progress,
            )
        progress_bar.progress(1.0)
        progress_text.caption("最適化完了")
        if opt_df.empty:
            st.warning("最適化結果がありません。範囲を見直してください。")
        else:
            heat = opt_df.pivot_table(index="short", columns="long", values="sharpe", aggfunc="mean")
            fig = px.imshow(heat, aspect="auto", color_continuous_scale="Viridis", title="パラメータ感度ヒートマップ（Sharpe）")
            fig.update_layout(template="plotly_dark", height=360)
            st.plotly_chart(fig, use_container_width=True)
            st.markdown("**上位10パラメータ**")
            st.dataframe(opt_df.head(10), use_container_width=True)

with tab_cmp:
    st.markdown("5戦略の一括比較")
    if st.button("📈 5戦略を比較", use_container_width=True):
        cmp_df = compare_strategies(price_df, ticker=ticker)
        st.dataframe(cmp_df, use_container_width=True)
        bar = px.bar(
            cmp_df.sort_values("total_return_pct", ascending=False),
            x="strategy_name",
            y="total_return_pct",
            color="strategy_name",
            template="plotly_dark",
            title="戦略別リターン比較",
        )
        st.plotly_chart(bar, use_container_width=True)
        report = build_ai_strategy_report(cmp_df)
        st.text_area("AI戦略比較レポート", value=report, height=170)
        inserted = save_comparison_results(ticker=ticker, start_date=str(start), end_date=str(end), df=cmp_df)
        st.info(f"比較結果をDBに保存しました: {inserted}件")

render_footer()

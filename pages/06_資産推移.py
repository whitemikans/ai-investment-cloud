from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from db.db_utils import generate_dummy_snapshots, get_snapshots, init_db, record_snapshot
from utils.common import apply_global_ui_tweaks, render_footer, render_last_data_update, show_download_button, touch_last_data_update


st.title("📉 資産推移")
apply_global_ui_tweaks()
st.caption("#03対応: snapshotsテーブルの資産履歴を可視化します。")

with st.spinner("DBを初期化中..."):
    init_db()

st.sidebar.subheader("データ操作")
if st.sidebar.button("現在値でスナップショット記録", use_container_width=True):
    with st.spinner("最新スナップショットを記録中..."):
        result = record_snapshot()
    if not result.empty and bool(result.iloc[0].get("success", True)):
        touch_last_data_update()
        st.success("スナップショットを記録しました。")
        st.rerun()
    else:
        st.error(str(result.iloc[0].get("message", "記録に失敗しました。")) if not result.empty else "記録に失敗しました。")

if st.sidebar.button("ダミー6ヶ月分を生成", use_container_width=True):
    with st.spinner("ダミースナップショットを生成中..."):
        result = generate_dummy_snapshots(days=180, overwrite=True)
    if not result.empty and bool(result.iloc[0].get("success", False)):
        touch_last_data_update()
        st.success(f"ダミーデータを生成しました（{int(result.iloc[0].get('inserted', 0))}件）。")
        st.rerun()
    else:
        st.error(str(result.iloc[0].get("message", "生成に失敗しました。")) if not result.empty else "生成に失敗しました。")

period = st.sidebar.radio("集計粒度", options=["daily", "weekly", "monthly"], index=2, horizontal=True)
render_last_data_update()

with st.spinner("資産推移データを取得中..."):
    df = get_snapshots(period=period)

if df.empty:
    st.info("スナップショットがまだありません。サイドバーの『ダミー6ヶ月分を生成』または『現在値でスナップショット記録』を実行してください。")
    render_footer()
    st.stop()

latest = df.iloc[-1]
c1, c2, c3 = st.columns(3)
c1.metric("総資産評価額", f"¥{float(latest['total_value']):,.0f}")
c2.metric("投資額", f"¥{float(latest['total_cost']):,.0f}")
c3.metric("含み損益", f"¥{float(latest['unrealized_pl']):,.0f}")

x = df["snapshot_date"]

fig = go.Figure()
fig.add_trace(
    go.Scatter(
        x=x,
        y=df["total_value"],
        mode="lines",
        name="総資産評価額",
        line=dict(color="#22d3ee", width=3),
        hovertemplate="日付:%{x}<br>総資産評価額:¥%{y:,.0f}<extra></extra>",
    )
)
fig.add_trace(
    go.Scatter(
        x=x,
        y=df["total_cost"],
        mode="lines",
        name="投資額",
        line=dict(color="#fbbf24", width=2, dash="dot"),
        hovertemplate="日付:%{x}<br>投資額:¥%{y:,.0f}<extra></extra>",
    )
)

# 含み損益エリア（グラデーション風: 2層重ね）
fig.add_trace(
    go.Scatter(
        x=x,
        y=df["unrealized_pl"] * 0.6,
        mode="lines",
        line=dict(width=0),
        fill="tozeroy",
        fillcolor="rgba(34,197,94,0.08)",
        name="含み損益(下層)",
        showlegend=False,
        yaxis="y2",
        hoverinfo="skip",
    )
)
fig.add_trace(
    go.Scatter(
        x=x,
        y=df["unrealized_pl"],
        mode="lines",
        line=dict(color="rgba(34,197,94,0.75)", width=1.5),
        fill="tonexty",
        fillcolor="rgba(34,197,94,0.22)",
        name="含み損益",
        yaxis="y2",
        hovertemplate="日付:%{x}<br>含み損益:¥%{y:,.0f}<extra></extra>",
    )
)

fig.update_layout(
    template="plotly_dark",
    height=540,
    title="資産推移",
    xaxis_title="日付",
    yaxis=dict(title="金額（円）"),
    yaxis2=dict(title="含み損益（円）", overlaying="y", side="right", showgrid=False),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(l=40, r=40, t=70, b=40),
)

st.plotly_chart(fig, use_container_width=True)

show_download_button(df, f"asset_history_{period}")

render_footer()

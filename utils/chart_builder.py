from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from plotly.subplots import make_subplots


@st.cache_resource
def candlestick_with_ma(df: pd.DataFrame, ticker: str) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Candlestick(
            x=df["Date"],
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            name=ticker,
        )
    )
    for window, color in [(5, "#00d4ff"), (25, "#2dd4bf"), (75, "#38bdf8")]:
        col = f"MA{window}"
        if col in df.columns:
            fig.add_trace(go.Scatter(x=df["Date"], y=df[col], mode="lines", name=col, line=dict(color=color, width=2)))
    fig.update_layout(
        title=f"{ticker} 株価チャート",
        template="plotly_dark",
        xaxis_title="日付",
        yaxis_title="価格",
        height=520,
    )
    return fig


@st.cache_resource
def advanced_candlestick_with_volume(df: pd.DataFrame, ticker: str) -> go.Figure:
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.71, 0.29],
        subplot_titles=("", ""),
    )

    fig.add_trace(
        go.Candlestick(
            x=df["Date"],
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            name="ローソク足",
            increasing_line_color="#22c55e",
            decreasing_line_color="#ef4444",
            increasing_fillcolor="#22c55e",
            decreasing_fillcolor="#ef4444",
        ),
        row=1,
        col=1,
    )

    if {"BB_UPPER", "BB_LOWER"}.issubset(df.columns):
        fig.add_trace(
            go.Scatter(
                x=df["Date"],
                y=df["BB_UPPER"],
                mode="lines",
                line=dict(color="rgba(180,180,180,0.35)", width=1),
                name="ボリンジャー上限(+2σ)",
                hovertemplate="日付:%{x|%Y-%m-%d}<br>上限:%{y:.2f}<extra></extra>",
                showlegend=False,
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["Date"],
                y=df["BB_LOWER"],
                mode="lines",
                line=dict(color="rgba(180,180,180,0.35)", width=1),
                name="ボリンジャー下限(-2σ)",
                fill="tonexty",
                fillcolor="rgba(180,180,180,0.16)",
                hovertemplate="日付:%{x|%Y-%m-%d}<br>下限:%{y:.2f}<extra></extra>",
            ),
            row=1,
            col=1,
        )

    for col, label, color in [
        ("MA5", "移動平均5日", "#facc15"),
        ("MA25", "移動平均25日", "#3b82f6"),
        ("MA75", "移動平均75日", "#a855f7"),
    ]:
        if col in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df["Date"],
                    y=df[col],
                    mode="lines",
                    name=label,
                    line=dict(color=color, width=2),
                    hovertemplate="日付:%{x|%Y-%m-%d}<br>" + label + ":%{y:.2f}<extra></extra>",
                ),
                row=1,
                col=1,
            )

    volume_colors = np.where(df["Close"] >= df["Open"], "#22c55e", "#ef4444")
    fig.add_trace(
        go.Bar(
            x=df["Date"],
            y=df["Volume"],
            name="出来高",
            marker_color=volume_colors,
            hovertemplate="日付:%{x|%Y-%m-%d}<br>出来高:%{y:,.0f}<extra></extra>",
        ),
        row=2,
        col=1,
    )

    fig.update_layout(
        title=f"{ticker} テクニカルチャート",
        template="plotly_dark",
        height=700,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=20, t=80, b=30),
    )
    fig.update_yaxes(title_text="株価 (USD)", row=1, col=1)
    fig.update_yaxes(title_text="出来高", row=2, col=1)
    fig.update_xaxes(title_text="日付", row=2, col=1, rangeslider=dict(visible=True, thickness=0.1))
    fig.update_xaxes(rangeslider=dict(visible=False), row=1, col=1)
    return fig


def _fmt_jpy_short(value: float) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "-"
    abs_value = abs(value)
    if abs_value >= 1_0000_0000_0000:
        return f"{value / 1_0000_0000_0000:,.1f}兆円"
    if abs_value >= 1_0000_0000:
        return f"{value / 1_0000_0000:,.1f}億円"
    if abs_value >= 1_0000:
        return f"{value / 1_0000:,.1f}万円"
    return f"{value:,.0f}円"


@st.cache_resource
def financial_trend_chart(df: pd.DataFrame) -> go.Figure:
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    rev_text = [
        f"前年比 {v:+.1f}%"
        if pd.notna(v) else "前年比 -"
        for v in df["RevenueYoY"]
    ]
    op_text = [
        f"前年比 {v:+.1f}%"
        if pd.notna(v) else "前年比 -"
        for v in df["OperatingIncomeYoY"]
    ]
    rev_money = [_fmt_jpy_short(v) for v in df["Revenue"]]
    op_money = [_fmt_jpy_short(v) for v in df["OperatingIncome"]]

    fig.add_trace(
        go.Bar(
            x=df["Year"],
            y=df["Revenue"],
            name="売上高",
            marker_color="#3b82f6",
            text=rev_text,
            textposition="outside",
            customdata=np.array(rev_money, dtype=object),
            hovertemplate="年度:%{x}<br>売上高:%{customdata}<br>%{text}<extra></extra>",
        ),
        secondary_y=False,
    )

    fig.add_trace(
        go.Bar(
            x=df["Year"],
            y=df["OperatingIncome"],
            name="営業利益",
            marker_color="#22c55e",
            text=op_text,
            textposition="outside",
            customdata=np.array(op_money, dtype=object),
            hovertemplate="年度:%{x}<br>営業利益:%{customdata}<br>%{text}<extra></extra>",
        ),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(
            x=df["Year"],
            y=df["OperatingMargin"],
            name="営業利益率",
            mode="lines+markers",
            line=dict(color="#f59e0b", width=3),
            marker=dict(size=8),
            hovertemplate="年度:%{x}<br>営業利益率:%{y:.1f}%<extra></extra>",
        ),
        secondary_y=True,
    )

    fig.update_layout(
        template="plotly_dark",
        title="業績推移（売上高・営業利益・営業利益率）",
        barmode="group",
        height=520,
        xaxis_title="年度",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=40, t=70, b=40),
    )
    fig.update_yaxes(title_text="金額（円）", secondary_y=False)
    fig.update_yaxes(title_text="営業利益率（%）", secondary_y=True, tickformat=".1f")
    return fig


def single_radar(scores: dict[str, float], name: str) -> go.Figure:
    labels = list(scores.keys())
    values = list(scores.values())
    labels_closed = labels + [labels[0]]
    values_closed = values + [values[0]]
    fig = go.Figure()
    fig.add_trace(
        go.Scatterpolar(
            r=values_closed,
            theta=labels_closed,
            fill="toself",
            name=name,
            line=dict(color="#00d4ff"),
        )
    )
    fig.update_layout(
        template="plotly_dark",
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=True,
        height=420,
        title="5軸評価レーダーチャート",
    )
    return fig


def multi_radar(score_map: dict[str, dict[str, float]]) -> go.Figure:
    fig = go.Figure()
    palette = ["#00d4ff", "#2dd4bf", "#a3e635", "#f59e0b", "#fb7185"]
    for i, (ticker, scores) in enumerate(score_map.items()):
        labels = list(scores.keys())
        values = list(scores.values())
        labels_closed = labels + [labels[0]]
        values_closed = values + [values[0]]
        fig.add_trace(
            go.Scatterpolar(
                r=values_closed,
                theta=labels_closed,
                fill="toself",
                opacity=0.45,
                name=ticker,
                line=dict(color=palette[i % len(palette)], width=2),
            )
        )
    fig.update_layout(
        template="plotly_dark",
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        title="5軸比較レーダー",
        height=460,
    )
    return fig


def comparison_heatmap(df: pd.DataFrame) -> go.Figure:
    numeric_cols = ["PER", "PBR", "ROE(%)", "配当利回り(%)", "時価総額"]
    work = df[["Ticker"] + numeric_cols].copy()
    z = work[numeric_cols].to_numpy(dtype=float)
    z_norm = np.zeros_like(z, dtype=float)
    for idx in range(z.shape[1]):
        col = z[:, idx]
        finite = np.isfinite(col)
        if finite.sum() < 2:
            z_norm[:, idx] = 0.5
            continue
        vmin, vmax = np.nanmin(col), np.nanmax(col)
        if vmax == vmin:
            z_norm[:, idx] = 0.5
        else:
            z_norm[:, idx] = (col - vmin) / (vmax - vmin)
    text = np.where(np.isfinite(z), np.round(z, 2).astype(str), "-")
    fig = go.Figure(
        data=go.Heatmap(
            z=z_norm,
            x=numeric_cols,
            y=work["Ticker"],
            text=text,
            texttemplate="%{text}",
            colorscale="Tealgrn",
            hovertemplate="銘柄:%{y}<br>指標:%{x}<br>値:%{text}<extra></extra>",
        )
    )
    fig.update_layout(template="plotly_dark", title="企業比較ヒートマップ", height=420)
    return fig


def allocation_pie(df: pd.DataFrame, group_col: str, value_col: str, title: str) -> go.Figure:
    fig = px.pie(
        df,
        names=group_col,
        values=value_col,
        title=title,
        hole=0.35,
        color_discrete_sequence=["#00d4ff", "#2dd4bf", "#4ade80", "#facc15", "#fb7185", "#a78bfa"],
    )
    fig.update_layout(template="plotly_dark", height=380)
    return fig


def dividend_bar(df: pd.DataFrame) -> go.Figure:
    fig = px.bar(
        df.sort_values("年間配当(予想)", ascending=False),
        x="Ticker",
        y="年間配当(予想)",
        color="年間配当(予想)",
        color_continuous_scale="Tealgrn",
        title="銘柄別 年間予想配当",
    )
    fig.update_layout(template="plotly_dark", height=380, coloraxis_showscale=False)
    return fig

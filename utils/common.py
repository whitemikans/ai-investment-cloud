from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_FILE = PROJECT_ROOT / "dashboard_events.log"


def log_event(event: str, details: str = "") -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().isoformat(timespec="seconds")
    line = f"{timestamp}\t{event}\t{details}\n"
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line)


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def show_download_button(df: pd.DataFrame, prefix: str, label: str = "CSVダウンロード") -> None:
    today = datetime.now().strftime("%Y%m%d")
    st.download_button(
        label=f"📥 {label}",
        data=dataframe_to_csv_bytes(df),
        file_name=f"{prefix}_{today}.csv",
        mime="text/csv",
        use_container_width=True,
    )


def render_footer() -> None:
    st.markdown(
        "<div style='margin-top:1.2rem;opacity:.75;text-align:center;'>"
        "© 2026 AI Investment Team | Built with Streamlit"
        "</div>",
        unsafe_allow_html=True,
    )


def render_unified_sidebar_navigation() -> None:
    with st.sidebar:
        st.markdown("## ナビゲーション")
        st.page_link("app.py", label="🏠 ダッシュボード（トップ）")

        with st.expander("📱 ポートフォリオ概要", expanded=True):
            st.page_link("pages/01_株価分析.py", label="株価分析")
            st.page_link("pages/02_企業比較.py", label="企業比較")
            st.page_link("pages/03_ポートフォリオ.py", label="ポートフォリオ")

        with st.expander("📒 取引概要", expanded=False):
            st.page_link("pages/04_取引履歴.py", label="取引履歴")
            st.page_link("pages/05_配当管理.py", label="配当管理")
            st.page_link("pages/06_資産推移.py", label="資産推移")
            st.page_link("pages/07_損益計算レポート.py", label="損益計算レポート")

        with st.expander("📰 AIニュースフィード", expanded=False):
            st.page_link("pages/11_ニュースフィード.py", label="ニュースフィード")
            st.page_link("pages/12_銘柄別ニュース.py", label="銘柄別ニュース")
            st.page_link("pages/13_キーワードアラート.py", label="キーワードアラート")
            st.page_link("pages/14_経済指標カレンダー.py", label="経済指標カレンダー")

        with st.expander("📊 バックテスト", expanded=False):
            st.page_link("pages/15_バックテスト.py", label="バックテスト")
            st.page_link("pages/16_過去の結果一覧.py", label="過去の結果一覧")

        with st.expander("⚖️ ポートフォリオ最適化", expanded=False):
            st.page_link("pages/18_ポートフォリオ最適化.py", label="ポートフォリオ最適化")

        with st.expander("📋 ライフプラン", expanded=False):
            st.page_link("pages/19_ライフプラン.py", label="ライフプラン")

        with st.expander("🤖 AI分析", expanded=False):
            st.page_link("pages/20_AI分析.py", label="AI分析")
            st.page_link("pages/21_AIチーム分析.py", label="AIチーム分析")
            st.page_link("pages/22_AI推奨精度追跡.py", label="AI推奨精度追跡")

        with st.expander("🏥 システムヘルスチェック", expanded=False):
            st.page_link("pages/17_管理者ヘルスチェック.py", label="管理者ヘルスチェック")


def apply_global_ui_tweaks() -> None:
    # Enforce authentication on every page in #06 cloud mode.
    try:
        from utils.auth import ensure_login

        ensure_login()
    except Exception:
        pass

    st.markdown(
        """
<style>
/* Hide Streamlit default multipage links */
[data-testid="stSidebarNav"] { display: none !important; }

/* DataFrame cells: show full text with wrapping */
[data-testid="stDataFrame"] [role="gridcell"],
[data-testid="stDataFrame"] [role="columnheader"] {
  white-space: normal !important;
  overflow: visible !important;
  text-overflow: clip !important;
  line-height: 1.2 !important;
}

/* Metric cards: avoid value/label truncation with ellipsis */
[data-testid="stMetricLabel"],
[data-testid="stMetricLabel"] *,
[data-testid="stMetricValue"],
[data-testid="stMetricValue"] *,
[data-testid="stMetricDelta"],
[data-testid="stMetricDelta"] * {
  white-space: normal !important;
  overflow: visible !important;
  text-overflow: clip !important;
  word-break: break-word !important;
}
</style>
""",
        unsafe_allow_html=True,
    )
    render_unified_sidebar_navigation()


def touch_last_data_update() -> None:
    st.session_state["last_data_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def render_last_data_update() -> None:
    st.sidebar.markdown("---")
    last = st.session_state.get("last_data_update", "未取得")
    st.sidebar.caption(f"最終データ更新時刻: {last}")

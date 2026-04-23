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
/* Sidebar page menu: avoid ellipsis and allow wrapping */
[data-testid="stSidebarNav"] a,
[data-testid="stSidebarNav"] a * {
  white-space: normal !important;
  overflow: visible !important;
  text-overflow: clip !important;
  line-height: 1.25 !important;
}

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


def touch_last_data_update() -> None:
    st.session_state["last_data_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def render_last_data_update() -> None:
    st.sidebar.markdown("---")
    last = st.session_state.get("last_data_update", "未取得")
    st.sidebar.caption(f"最終データ更新時刻: {last}")

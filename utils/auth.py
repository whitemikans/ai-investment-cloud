from __future__ import annotations

import streamlit as st

from config import get_app_password


def ensure_login() -> None:
    if st.session_state.get("authenticated", False):
        return

    st.title("🔐 ログイン")
    st.caption("クラウド運用版ダッシュボードにアクセスするには認証が必要です。")
    password = st.text_input("パスワード", type="password")
    if st.button("ログイン", type="primary", use_container_width=True):
        if password == get_app_password():
            st.session_state["authenticated"] = True
            st.success("ログインしました。")
            st.rerun()
        else:
            st.error("パスワードが違います。")
    st.stop()


def render_logout() -> None:
    st.sidebar.markdown("---")
    if st.sidebar.button("🚪 ログアウト", use_container_width=True):
        st.session_state["authenticated"] = False
        st.rerun()


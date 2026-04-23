from __future__ import annotations

import os
import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

from config import get_database_url, get_setting
from db.db_utils import init_db
from db.news_utils import init_news_tables
from utils.auth import ensure_login
from utils.common import apply_global_ui_tweaks, render_footer, render_last_data_update


def _sqlite_path_from_url(url: str) -> Path | None:
    if not url.startswith("sqlite:///"):
        return None
    raw = url.replace("sqlite:///", "", 1)
    return Path(raw)


def _db_health() -> tuple[bool, str]:
    db_url = get_database_url()
    sqlite_path = _sqlite_path_from_url(db_url)
    if sqlite_path is not None:
        if not sqlite_path.exists():
            return False, f"SQLiteファイルが見つかりません: {sqlite_path}"
        try:
            con = sqlite3.connect(sqlite_path)
            con.execute("SELECT 1")
            con.close()
            return True, "DB接続OK（SQLite）"
        except Exception as exc:
            return False, f"DB接続NG（SQLite）: {exc}"
    return True, "DB接続URL設定あり（PostgreSQL/Supabase想定）"


def _table_count(table: str) -> int:
    db_url = get_database_url()
    sqlite_path = _sqlite_path_from_url(db_url)
    if sqlite_path is None or (not sqlite_path.exists()):
        return 0
    with sqlite3.connect(sqlite_path) as con:
        try:
            return int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        except Exception:
            return 0


def _latest_dt(table: str, column: str) -> str:
    db_url = get_database_url()
    sqlite_path = _sqlite_path_from_url(db_url)
    if sqlite_path is None or (not sqlite_path.exists()):
        return "-"
    with sqlite3.connect(sqlite_path) as con:
        try:
            row = con.execute(f"SELECT MAX({column}) FROM {table}").fetchone()
            return str(row[0]) if row and row[0] is not None else "-"
        except Exception:
            return "-"


def _github_actions_runs(limit: int = 10) -> pd.DataFrame:
    repo = get_setting("GITHUB_REPOSITORY", "")
    token = get_setting("GITHUB_TOKEN", "")
    if not repo:
        return pd.DataFrame([{"status": "GITHUB_REPOSITORY 未設定"}])
    if not token:
        return pd.DataFrame([{"status": "GITHUB_TOKEN 未設定"}])
    try:
        url = f"https://api.github.com/repos/{repo}/actions/runs"
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params={"per_page": limit}, timeout=8)
        resp.raise_for_status()
        data = resp.json().get("workflow_runs", [])
        rows = [
            {
                "name": r.get("name", ""),
                "event": r.get("event", ""),
                "status": r.get("status", ""),
                "conclusion": r.get("conclusion", ""),
                "created_at": r.get("created_at", ""),
                "html_url": r.get("html_url", ""),
            }
            for r in data
        ]
        return pd.DataFrame(rows)
    except Exception as exc:
        return pd.DataFrame([{"status": f"取得失敗: {exc}"}])


def _discord_ping() -> tuple[bool, str]:
    webhook = get_setting("DISCORD_WEBHOOK_URL", "")
    if not webhook:
        return False, "DISCORD_WEBHOOK_URL 未設定"
    try:
        r = requests.post(webhook, json={"content": "✅ ヘルスチェックページからの通知テスト"}, timeout=8)
        if 200 <= r.status_code < 300:
            return True, "通知テスト送信OK"
        return False, f"通知失敗: HTTP {r.status_code}"
    except Exception as exc:
        return False, f"通知例外: {exc}"


ensure_login()
st.title("🩺 管理者ヘルスチェック")
apply_global_ui_tweaks()
st.caption("#06対応: 運用健全性の確認（DB・更新時刻・API使用量・Actionsログ）")

with st.spinner("初期化中..."):
    init_db()
    init_news_tables()
render_last_data_update()

db_ok, db_msg = _db_health()
news_count = _table_count("news_articles")
sent_count = _table_count("news_sentiments")
snap_count = _table_count("snapshots")
today_sent = 0

db_url = get_database_url()
sqlite_path = _sqlite_path_from_url(db_url)
if sqlite_path is not None and sqlite_path.exists():
    with sqlite3.connect(sqlite_path) as con:
        try:
            today_sent = int(
                con.execute(
                    "SELECT COUNT(*) FROM news_sentiments WHERE date(analyzed_at)=date('now', 'localtime')"
                ).fetchone()[0]
            )
        except Exception:
            today_sent = 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("DB接続", "OK" if db_ok else "NG")
c2.metric("ニュース件数", f"{news_count:,}")
c3.metric("センチメント件数", f"{sent_count:,}")
c4.metric("スナップショット件数", f"{snap_count:,}")
st.info(db_msg)

st.subheader("最終更新時刻")
u1, u2, u3 = st.columns(3)
u1.metric("ニュース最終", _latest_dt("news_articles", "published_at"))
u2.metric("分析最終", _latest_dt("news_sentiments", "analyzed_at"))
u3.metric("資産最終", _latest_dt("snapshots", "snapshot_date"))

st.subheader("API使用量（概算）")
api_limit = 1500
ratio = min(today_sent / api_limit, 1.0) if api_limit > 0 else 0.0
st.metric("Gemini API（推定）", f"{today_sent:,} / {api_limit:,}")
st.progress(ratio)

st.subheader("GitHub Actions 実行ログ")
runs_df = _github_actions_runs(limit=10)
st.dataframe(runs_df, use_container_width=True)

st.subheader("通知テスト")
if st.button("Discord通知テスト", use_container_width=True):
    ok, msg = _discord_ping()
    if ok:
        st.success(msg)
    else:
        st.warning(msg)

render_footer()


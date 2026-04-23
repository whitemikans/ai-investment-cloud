from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from db.db_utils import init_db
from db.news_utils import (
    add_keyword_alert,
    delete_keyword_alert,
    get_keyword_hits_df,
    init_news_tables,
    list_keyword_alerts,
)
from utils.common import apply_global_ui_tweaks, render_footer, render_last_data_update

CATEGORIES = ["ポジティブ材料", "ネガティブ材料", "調査対象"]

st.title("🔔 キーワードアラート設定")
apply_global_ui_tweaks()
st.caption("キーワード登録・削除、ヒット履歴、キーワード別集計を表示します。")

with st.spinner("DB初期化中..."):
    init_db()
    init_news_tables()
render_last_data_update()

st.subheader("新規キーワード追加")
with st.form("add_keyword", clear_on_submit=True):
    keyword = st.text_input("キーワード")
    category = st.selectbox("カテゴリ", CATEGORIES, index=0)
    submit = st.form_submit_button("追加")

if submit:
    ok, msg = add_keyword_alert(keyword, category)
    if ok:
        st.success(msg)
        st.rerun()
    else:
        st.error(msg)

st.subheader("登録済みキーワード")
kw_df = list_keyword_alerts()
if kw_df.empty:
    st.info("キーワードは未登録です。")
else:
    st.dataframe(
        kw_df.rename(columns={"keyword": "キーワード", "category": "カテゴリ", "is_active": "有効", "created_at": "登録日時"}),
        use_container_width=True,
    )
    del_col1, del_col2 = st.columns([3, 2])
    with del_col1:
        target = st.selectbox(
            "削除するキーワードを選択",
            options=kw_df["id"].tolist(),
            format_func=lambda x: f"{kw_df.loc[kw_df['id'] == x, 'keyword'].iloc[0]} (id={x})",
        )
    with del_col2:
        if st.button("削除", type="secondary", use_container_width=True):
            delete_keyword_alert(int(target))
            st.success("削除しました。")
            st.rerun()

days = st.selectbox("集計期間", options=[7, 30, 90], index=0, format_func=lambda x: f"直近{x}日")

st.subheader(f"キーワードヒット履歴（直近{days}日）")
hits_df = get_keyword_hits_df(days=days)
if hits_df.empty:
    st.info("ヒット履歴はありません。")
else:
    st.dataframe(
        hits_df.rename(
            columns={
                "hit_keywords": "ヒットキーワード",
                "title": "タイトル",
                "url": "URL",
                "published_at": "記事日時",
                "created_at": "検知日時",
            }
        ),
        use_container_width=True,
    )

st.subheader("キーワード別の集計グラフ")
if kw_df.empty:
    st.info("キーワードを登録すると集計グラフを表示できます。")
else:
    base = kw_df[["keyword"]].copy().rename(columns={"keyword": "hit_keywords"})
    base["count"] = 0

    if hits_df.empty:
        count_df = base
    else:
        exploded = hits_df.assign(hit_keywords=hits_df["hit_keywords"].fillna("").str.split(",")).explode("hit_keywords")
        exploded["hit_keywords"] = exploded["hit_keywords"].astype(str).str.strip()
        exploded = exploded[exploded["hit_keywords"] != ""]
        if exploded.empty:
            count_df = base
        else:
            actual = exploded.groupby("hit_keywords", as_index=False).size().rename(columns={"size": "count"})
            count_df = base.merge(actual, on="hit_keywords", how="left")
            count_df["count"] = count_df["count_y"].fillna(0).astype(int)
            count_df = count_df[["hit_keywords", "count"]]

    bar = px.bar(
        count_df.sort_values("count", ascending=False),
        x="hit_keywords",
        y="count",
        template="plotly_dark",
        title=f"キーワード別ヒット件数（直近{days}日）",
        color_discrete_sequence=["#22c55e"],
    )
    bar.update_layout(xaxis_title="キーワード", yaxis_title="件数", height=320)
    st.plotly_chart(bar, use_container_width=True)

render_footer()

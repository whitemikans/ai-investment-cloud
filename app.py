import streamlit as st

from db.db_utils import init_db
from db.news_utils import init_news_tables
from utils.auth import ensure_login, render_logout
from utils.common import apply_global_ui_tweaks, log_event, render_footer, render_last_data_update


st.set_page_config(page_title="AIポートフォリオ最適化ダッシュボード", page_icon="📊", layout="wide")
ensure_login()
apply_global_ui_tweaks()
with st.spinner("ページを読み込み中..."):
    init_db()
    init_news_tables()
    st.empty()
render_last_data_update()
render_logout()


st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@400;600;800&display=swap');
html, body, [class*="css"] { font-family: 'Noto Sans JP', sans-serif; }
.hero-title {
  font-size: clamp(1.8rem, 4.5vw, 3.1rem);
  font-weight: 800;
  background: linear-gradient(90deg, #00d4ff 0%, #2dd4bf 40%, #a3e635 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  margin: 0 0 .4rem 0;
}
.hero-sub { color: #cbd5e1; margin-bottom: 1.0rem; }
.nav-card {
  border: 1px solid rgba(0, 212, 255, 0.35);
  border-radius: 14px;
  padding: 16px;
  background: linear-gradient(145deg, rgba(0,212,255,0.08), rgba(45,212,191,0.06));
}
</style>
""",
    unsafe_allow_html=True,
)

st.markdown("<h1 class='hero-title'>AIライフプラン統合ダッシュボード #08</h1>", unsafe_allow_html=True)
st.markdown(
    "<div class='hero-sub'>Powered by AI × Python × Streamlit × GitHub Actions × Supabase/PostgreSQL</div>",
    unsafe_allow_html=True,
)

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.markdown("<div class='nav-card'><h4>📈 株価分析</h4><p>ローソク足・移動平均・5軸評価</p></div>", unsafe_allow_html=True)
    st.page_link("pages/01_株価分析.py", label="株価分析ページへ", icon="📈")
with col2:
    st.markdown("<div class='nav-card'><h4>🏢 企業比較</h4><p>ヒートマップ・比較表・重ねレーダー</p></div>", unsafe_allow_html=True)
    st.page_link("pages/02_企業比較.py", label="企業比較ページへ", icon="🏢")
with col3:
    st.markdown("<div class='nav-card'><h4>💼 ポートフォリオ</h4><p>DB保存の取引登録と保有管理</p></div>", unsafe_allow_html=True)
    st.page_link("pages/03_ポートフォリオ.py", label="ポートフォリオページへ", icon="💼")
with col4:
    st.markdown("<div class='nav-card'><h4>📒 取引履歴</h4><p>フィルター・集計・月次回数</p></div>", unsafe_allow_html=True)
    st.page_link("pages/04_取引履歴.py", label="取引履歴ページへ", icon="📒")
with col5:
    st.markdown("<div class='nav-card'><h4>💰 配当管理</h4><p>配当登録・年次集計・進捗</p></div>", unsafe_allow_html=True)
    st.page_link("pages/05_配当管理.py", label="配当管理ページへ", icon="💰")

st.info("左サイドバーまたは上記リンクからページを切り替えてください。")

st.markdown("### #03 追加ページ")
st.page_link("pages/06_資産推移.py", label="資産推移ページへ", icon="📉")
st.page_link("pages/07_損益計算レポート.py", label="損益計算レポートページへ", icon="🧾")
st.page_link("pages/08_取引分析.py", label="取引分析ダッシュボードへ", icon="🎯")
st.page_link("pages/09_配当詳細分析.py", label="配当詳細分析ページへ", icon="💹")
st.page_link("pages/10_税金計算レポート.py", label="税金計算レポートページへ", icon="🧾")

st.markdown("### #04 ニュース分析ページ")
st.page_link("pages/11_ニュースフィード.py", label="ニュースフィードページへ", icon="📰")
st.page_link("pages/12_銘柄別ニュース.py", label="銘柄別ニュースビューへ", icon="🏢")
st.page_link("pages/13_キーワードアラート.py", label="キーワードアラート設定へ", icon="🔔")
st.page_link("pages/14_経済指標カレンダー.py", label="経済指標カレンダーページへ", icon="📅")

st.markdown("### #05 バックテストページ")
st.page_link("pages/15_バックテスト.py", label="バックテストページへ", icon="📊")
st.page_link("pages/16_過去の結果一覧.py", label="過去の結果一覧ページへ", icon="📋")

st.markdown("### #06 クラウド運用ページ")
st.page_link("pages/17_管理者ヘルスチェック.py", label="管理者ヘルスチェックへ", icon="🩺")

st.markdown("### #07 ポートフォリオ最適化ページ")
st.page_link("pages/18_ポートフォリオ最適化.py", label="ポートフォリオ最適化ページへ", icon="📊")

st.markdown("### #08 ライフプラン/FIREページ")
st.page_link("pages/19_ライフプラン.py", label="ライフプラン/FIREシミュレーターへ", icon="🔥")

render_footer()
log_event("open_top", "app.py loaded")

from __future__ import annotations

import json
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from ai_financial_advisor import generate_financial_advice, generate_whatif_comparison_comment
from utils.common import apply_global_ui_tweaks, render_footer
from utils.fire_simulator import (
    EVENT_TYPE_INCOME_CHANGE,
    EVENT_TYPE_ONE_TIME_EXPENSE,
    EVENT_TYPE_RECURRING_EXPENSE,
    FireSimulationInput,
    build_what_if_scenarios,
    simulate_fire_deterministic,
    simulate_fire_monte_carlo,
)
from utils.pension_simulator import build_pension_table, calc_break_even_age, calc_pension_monthly


@st.cache_data(ttl=300, show_spinner=False)
def _run_simulations_cached(
    sim_params_json: str,
    events_json: str,
    pension_annual: float,
    n_sims: int,
    return_std: float,
) -> tuple[dict, dict]:
    sim_params = json.loads(sim_params_json)
    events = json.loads(events_json)

    sim_input = FireSimulationInput(
        current_age=int(sim_params["current_age"]),
        annual_income=float(sim_params["annual_income"]),
        annual_expense=float(sim_params["annual_expense"]),
        current_assets=float(sim_params["current_assets"]),
        annual_return=float(sim_params["annual_return"]),
        inflation_rate=float(sim_params["inflation_rate"]),
        safe_withdrawal_rate=float(sim_params["safe_withdrawal_rate"]),
        part_time_income_annual=float(sim_params["part_time_income_annual"]),
    )
    events_df = pd.DataFrame(events) if events else pd.DataFrame(
        columns=["name", "age", "event_type", "amount", "duration_years", "memo"]
    )

    det = simulate_fire_deterministic(sim_input=sim_input, events_df=events_df, pension_annual=float(pension_annual))
    mc = simulate_fire_monte_carlo(
        sim_input=sim_input,
        events_df=events_df,
        pension_annual=float(pension_annual),
        n_sims=int(n_sims),
        return_std=float(return_std),
    )
    return det, mc


def _calc_max_drawdown(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    roll_max = series.cummax()
    drawdown = (series / roll_max) - 1.0
    return abs(float(drawdown.min()))


def _build_scenario_params_and_events(
    scenario: dict,
    base_params: dict,
    base_events: list[dict],
) -> tuple[dict, list[dict]]:
    overrides = scenario.get("overrides", {}) or {}
    params = dict(base_params)
    events = [dict(e) for e in base_events]

    if "annual_income" in overrides:
        params["annual_income"] = float(overrides["annual_income"])
    if "annual_return" in overrides:
        params["annual_return"] = float(overrides["annual_return"])
    if "part_time_income_annual" in overrides:
        params["part_time_income_annual"] = float(overrides["part_time_income_annual"])
    if "current_assets_add" in overrides:
        params["current_assets"] = max(0.0, float(params["current_assets"]) + float(overrides["current_assets_add"]))
    if "annual_expense_add" in overrides:
        params["annual_expense"] = max(0.0, float(params["annual_expense"]) + float(overrides["annual_expense_add"]))
    if overrides.get("early_retire_at_55"):
        annual_income = float(params["annual_income"])
        events.append(
            {
                "name": "What-If: 55歳早期退職",
                "age": 55,
                "event_type": EVENT_TYPE_INCOME_CHANGE,
                "amount": -annual_income,
                "amount_unit": "円",
                "frequency": "年額",
                "duration_years": 1,
                "memo": "55歳以降の給与収入をゼロ化",
            }
        )
    return params, events


st.set_page_config(page_title="ライフプラン/FIREシミュレーター", page_icon="🔥", layout="wide")
apply_global_ui_tweaks()
st.title("🔥 #08 ライフプラン・FIREシミュレーター")
st.caption("新NISA・FIRE・ライフイベント・年金・What-Ifを統合して将来資産を可視化")


if "life_events" not in st.session_state:
    st.session_state["life_events"] = []
if "life_ai_report" not in st.session_state:
    st.session_state["life_ai_report"] = ""
if "life_status_message" not in st.session_state:
    st.session_state["life_status_message"] = ""
if "life_run_what_if" not in st.session_state:
    st.session_state["life_run_what_if"] = False
if "life_whatif_selected" not in st.session_state:
    st.session_state["life_whatif_selected"] = ["ベースケース", "もし年収が100万円上がったら？", "もしリターンが5%しかなかったら？"]
if "life_whatif_ai_comment" not in st.session_state:
    st.session_state["life_whatif_ai_comment"] = ""
if "life_sim_result" not in st.session_state:
    st.session_state["life_sim_result"] = None
if "life_sim_signature" not in st.session_state:
    st.session_state["life_sim_signature"] = ""
if "life_events_editor_version" not in st.session_state:
    st.session_state["life_events_editor_version"] = 0

if st.session_state["life_status_message"]:
    st.info(st.session_state["life_status_message"])
for ev in st.session_state["life_events"]:
    ev.setdefault("amount_unit", "円")
    ev.setdefault("frequency", "年額")


with st.sidebar:
    st.subheader("プロフィール")
    current_age = st.slider("現在の年齢", min_value=20, max_value=60, value=35)
    annual_income = st.number_input("年収（円）", min_value=0, value=6_000_000, step=100_000)
    annual_expense = st.number_input("年間支出（円）", min_value=0, value=3_600_000, step=100_000)
    current_assets = st.number_input("現在の総資産（円）", min_value=0, value=12_000_000, step=100_000)

    st.subheader("シミュレーション設定")
    annual_return = st.slider("想定年率リターン(%)", min_value=3.0, max_value=10.0, value=7.0, step=0.1) / 100.0
    inflation_rate = st.slider("想定インフレ率(%)", min_value=0.0, max_value=5.0, value=2.0, step=0.1) / 100.0
    fire_type = st.selectbox("FIREタイプ", ["Fat", "Lean", "Barista", "Coast"], index=0)

    fire_swr_map = {"Fat": 0.04, "Lean": 0.035, "Barista": 0.04, "Coast": 0.04}
    safe_withdrawal_rate = fire_swr_map.get(fire_type, 0.04)
    part_time_income_monthly = st.number_input("Barista収入（月額円）", min_value=0, value=100_000 if fire_type == "Barista" else 0, step=10_000)

    n_sims = st.slider("モンテカルロ試行回数", min_value=1000, max_value=10000, value=5000, step=500)
    return_std = st.slider("年率ボラティリティ(%)", min_value=5.0, max_value=35.0, value=15.0, step=1.0) / 100.0

    st.subheader("年金設定")
    pension_type = st.selectbox("加入種別", ["国民年金のみ", "厚生年金"], index=1)
    pension_years = st.slider("加入年数", min_value=20, max_value=40, value=38)
    pension_income = st.number_input("平均年収（円, 厚生年金用）", min_value=0, value=6_000_000, step=100_000)
    pension_start_age = st.slider("受給開始年齢", min_value=60, max_value=75, value=65)
    run_sim = st.button("シミュレーション実行", use_container_width=True, type="primary")


sim_input = FireSimulationInput(
    current_age=current_age,
    annual_income=float(annual_income),
    annual_expense=float(annual_expense),
    current_assets=float(current_assets),
    annual_return=float(annual_return),
    inflation_rate=float(inflation_rate),
    safe_withdrawal_rate=float(safe_withdrawal_rate),
    part_time_income_annual=float(part_time_income_monthly * 12),
)

events_df = pd.DataFrame(st.session_state["life_events"]) if st.session_state["life_events"] else pd.DataFrame(
    columns=["name", "age", "event_type", "amount", "amount_unit", "frequency", "duration_years", "memo"]
)

pension_monthly = calc_pension_monthly(pension_type, pension_years, pension_income, pension_start_age)
pension_annual = pension_monthly * 12.0

sim_params = {
    "current_age": current_age,
    "annual_income": float(annual_income),
    "annual_expense": float(annual_expense),
    "current_assets": float(current_assets),
    "annual_return": float(annual_return),
    "inflation_rate": float(inflation_rate),
    "safe_withdrawal_rate": float(safe_withdrawal_rate),
    "part_time_income_annual": float(part_time_income_monthly * 12),
}
current_signature = json.dumps(
    {
        "sim_params": sim_params,
        "events": st.session_state["life_events"],
        "pension_annual": float(pension_annual),
        "n_sims": int(n_sims),
        "return_std": float(return_std),
    },
    ensure_ascii=False,
    sort_keys=True,
)

if run_sim:
    with st.spinner("シミュレーション計算中..."):
        det_new, mc_new = _run_simulations_cached(
            sim_params_json=json.dumps(sim_params, ensure_ascii=False, sort_keys=True),
            events_json=json.dumps(st.session_state["life_events"], ensure_ascii=False, sort_keys=True),
            pension_annual=float(pension_annual),
            n_sims=int(n_sims),
            return_std=float(return_std),
        )
    st.session_state["life_sim_result"] = {"det": det_new, "mc": mc_new}
    st.session_state["life_sim_signature"] = current_signature
    st.session_state["life_status_message"] = "シミュレーションを更新しました。"

sim_result = st.session_state.get("life_sim_result")
det = sim_result["det"] if sim_result else None
mc = sim_result["mc"] if sim_result else None
st.session_state["life_status_message"] = ""

fire_target_now = (sim_input.annual_expense - sim_input.part_time_income_annual - pension_annual) / max(1e-9, sim_input.safe_withdrawal_rate)
fire_target_now = max(0.0, fire_target_now)
remaining_gap = max(0.0, fire_target_now - sim_input.current_assets)

if st.session_state.get("life_sim_signature") and st.session_state["life_sim_signature"] != current_signature:
    st.warning("設定が変更されています。最新値を反映するには『シミュレーション実行』を押してください。")
elif mc is None:
    st.info("『シミュレーション実行』を押すと結果を計算します。")

c1, c2, c3, c4 = st.columns(4)
c1.metric("FIRE達成確率", "-" if mc is None else f"{mc['fire_probability'] * 100:.1f}%")
c2.metric("FIRE達成年齢(中央値)", "-" if mc is None else ("未達" if mc["fire_age_median"] is None else f"{int(mc['fire_age_median'])}歳"))
c3.metric("FIRE必要資産（現在基準）", f"¥{fire_target_now:,.0f}")
c4.metric("現在との差額", f"¥{remaining_gap:,.0f}")

tabs = st.tabs(["📈 FIREシミュレーション", "📋 ライフイベント", "🏖️ 年金", "🔁 What-If比較", "🤖 AIアドバイザー"])

with tabs[0]:
    if det is None or mc is None:
        st.info("シミュレーション未実行です。サイドバーの『シミュレーション実行』を押してください。")
    else:
        det_df = det["records"].copy()

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=det_df["age"], y=det_df["assets"], mode="lines", name="確定シミュレーション資産", line=dict(color="#22c55e", width=3)))
        fig.add_trace(go.Scatter(x=det_df["age"], y=det_df["fire_target"], mode="lines", name="FIRE必要資産", line=dict(color="#f97316", width=2, dash="dash")))
        if det["fire_age"] is not None:
            fire_idx = det_df[det_df["age"] == det["fire_age"]]
            if not fire_idx.empty:
                fig.add_trace(go.Scatter(x=fire_idx["age"], y=fire_idx["assets"], mode="markers", name="FIRE達成", marker=dict(size=12, color="#eab308", symbol="star")))
        fig.update_layout(template="plotly_dark", height=450, xaxis_title="年齢", yaxis_title="資産(円)")
        st.plotly_chart(fig, use_container_width=True)

        pct_df = mc["percentiles"]
        fig_mc = go.Figure()
        fig_mc.add_trace(go.Scatter(x=pct_df["age"], y=pct_df["p95"], line=dict(color="rgba(59,130,246,0.0)"), showlegend=False, hoverinfo="skip"))
        fig_mc.add_trace(go.Scatter(x=pct_df["age"], y=pct_df["p5"], fill="tonexty", fillcolor="rgba(59,130,246,0.15)", line=dict(color="rgba(59,130,246,0.0)"), name="5-95%", hoverinfo="skip"))
        fig_mc.add_trace(go.Scatter(x=pct_df["age"], y=pct_df["p75"], line=dict(color="rgba(16,185,129,0.0)"), showlegend=False, hoverinfo="skip"))
        fig_mc.add_trace(go.Scatter(x=pct_df["age"], y=pct_df["p25"], fill="tonexty", fillcolor="rgba(16,185,129,0.25)", line=dict(color="rgba(16,185,129,0.0)"), name="25-75%", hoverinfo="skip"))
        fig_mc.add_trace(go.Scatter(x=pct_df["age"], y=pct_df["p50"], mode="lines", name="中央値", line=dict(color="#22d3ee", width=3)))
        fig_mc.update_layout(template="plotly_dark", height=450, xaxis_title="年齢", yaxis_title="資産(円)", title="モンテカルロ信頼区間")
        st.plotly_chart(fig_mc, use_container_width=True)

        table_df = det_df[
            ["age", "assets", "annual_cashflow", "annual_return_amount", "one_time_expense", "recurring_expense", "income_delta", "is_fire_phase"]
        ].copy()
        table_df["event_total"] = table_df["one_time_expense"] + table_df["recurring_expense"]
        table_df = table_df[
            ["age", "assets", "annual_cashflow", "annual_return_amount", "one_time_expense", "recurring_expense", "event_total", "income_delta", "is_fire_phase"]
        ]
        table_df.columns = [
            "年齢",
            "資産(円)",
            "年間収支(円)",
            "年間リターン(円)",
            "一時イベント支出(円)",
            "継続イベント支出(円)",
            "イベント支出合計(円)",
            "収入変化(円)",
            "FIREフェーズ",
        ]
        st.dataframe(
            table_df.style.format(
                {
                    "資産(円)": "¥{:,.0f}",
                    "年間収支(円)": "¥{:,.0f}",
                    "年間リターン(円)": "¥{:,.0f}",
                    "一時イベント支出(円)": "¥{:,.0f}",
                    "継続イベント支出(円)": "¥{:,.0f}",
                    "イベント支出合計(円)": "¥{:,.0f}",
                    "収入変化(円)": "¥{:,.0f}",
                }
            ),
            use_container_width=True,
            height=340,
        )

with tabs[1]:
    st.markdown("#### プリセットイベント")
    preset_cols = st.columns(4)
    presets = [
        ("💍 結婚", 35, EVENT_TYPE_ONE_TIME_EXPENSE, 350, "万円", "年額", 1),
        ("👶 出産", 36, EVENT_TYPE_ONE_TIME_EXPENSE, 50, "万円", "年額", 1),
        ("🏠 住宅ローン", 40, EVENT_TYPE_RECURRING_EXPENSE, 12, "万円", "月額", 35),
        ("🎓 大学", 56, EVENT_TYPE_RECURRING_EXPENSE, 100, "万円", "年額", 4),
        ("🚗 車購入", 42, EVENT_TYPE_ONE_TIME_EXPENSE, 250, "万円", "年額", 1),
        ("🏥 介護", 60, EVENT_TYPE_RECURRING_EXPENSE, 100, "万円", "年額", 5),
        ("📈 昇給", 40, EVENT_TYPE_INCOME_CHANGE, 50, "万円", "年額", 1),
        ("🏖️ 年金開始", 65, EVENT_TYPE_INCOME_CHANGE, pension_annual, "円", "年額", 1),
    ]
    for i, p in enumerate(presets):
        with preset_cols[i % 4]:
            if st.button(p[0], key=f"preset_{i}", use_container_width=True):
                st.session_state["life_events"].append(
                    {
                        "name": p[0],
                        "age": p[1],
                        "event_type": p[2],
                        "amount": p[3],
                        "amount_unit": p[4],
                        "frequency": p[5],
                        "duration_years": p[6],
                        "memo": "",
                    }
                )
                st.session_state["life_events_editor_version"] += 1
                st.session_state["life_status_message"] = f"{p[0]} を追加しました。シミュレーションを再計算しています..."
                st.rerun()

    st.markdown("#### イベント追加")
    with st.form("event_add_form"):
        ec1, ec2, ec3 = st.columns(3)
        name = ec1.text_input("イベント名", value="")
        age = ec2.number_input("発生年齢", min_value=20, max_value=90, value=current_age)
        ev_type = ec3.selectbox("タイプ", [EVENT_TYPE_ONE_TIME_EXPENSE, EVENT_TYPE_RECURRING_EXPENSE, EVENT_TYPE_INCOME_CHANGE])
        ec4, ec5, ec6 = st.columns(3)
        amount = ec4.number_input("金額", min_value=0.0, value=50.0, step=10.0)
        amount_unit = ec5.selectbox("単位", ["万円", "円"], index=0)
        frequency = ec6.selectbox("頻度", ["年額", "月額"], index=0)
        duration = st.number_input("期間(年, 継続支出用)", min_value=1, max_value=50, value=1)
        memo = st.text_input("メモ")
        submitted = st.form_submit_button("イベント追加", use_container_width=True)
        if submitted:
            st.session_state["life_events"].append(
                {
                    "name": name or "イベント",
                    "age": int(age),
                    "event_type": ev_type,
                    "amount": float(amount),
                    "amount_unit": amount_unit,
                    "frequency": frequency,
                    "duration_years": int(duration),
                    "memo": memo,
                }
            )
            st.session_state["life_events_editor_version"] += 1
            st.session_state["life_status_message"] = "イベントを追加しました。シミュレーションを再計算しています..."
            st.rerun()

    st.markdown("#### 登録済みイベント")
    if st.session_state["life_events"]:
        edit_df = pd.DataFrame(st.session_state["life_events"])
        view_df = edit_df.copy()
        view_df["換算額(円/年)"] = view_df.apply(
            lambda r: (
                (float(r.get("amount", 0.0)) * (10_000.0 if str(r.get("amount_unit", "円")) == "万円" else 1.0))
                * (12.0 if str(r.get("frequency", "年額")) == "月額" else 1.0)
            ),
            axis=1,
        )
        st.caption("換算額(円/年) がシミュレーション計算に使われます。")
        st.dataframe(
            view_df.style.format({"換算額(円/年)": "¥{:,.0f}"}),
            use_container_width=True,
            height=220,
        )
        editor_key = f"life_events_editor_{st.session_state['life_events_editor_version']}"
        edited = st.data_editor(edit_df, num_rows="dynamic", use_container_width=True, key=editor_key)
        st.session_state["life_events"] = edited.to_dict(orient="records")
        if st.button("イベントを全削除", type="secondary"):
            st.session_state["life_events"] = []
            st.session_state["life_events_editor_version"] += 1
            st.rerun()

        timeline_df = pd.DataFrame(st.session_state["life_events"]).copy()
        timeline_df["方向"] = timeline_df["event_type"].map(
            {
                EVENT_TYPE_ONE_TIME_EXPENSE: "支出",
                EVENT_TYPE_RECURRING_EXPENSE: "支出",
                EVENT_TYPE_INCOME_CHANGE: "収入",
            }
        )
        timeline_df["Y"] = timeline_df["方向"].map({"支出": -1, "収入": 1})
        fig_tl = px.scatter(
            timeline_df,
            x="age",
            y="Y",
            color="方向",
            hover_name="name",
            hover_data={"amount": True, "duration_years": True, "Y": False},
            color_discrete_map={"支出": "#ef4444", "収入": "#22c55e"},
            template="plotly_dark",
        )
        fig_tl.update_yaxes(tickvals=[-1, 1], ticktext=["支出", "収入"])
        fig_tl.update_layout(height=300, xaxis_title="年齢", yaxis_title="種別")
        st.plotly_chart(fig_tl, use_container_width=True)
    else:
        st.info("イベントは未登録です。")

with tabs[2]:
    st.markdown("#### 年金受給シミュレーション")
    pension_table = build_pension_table(pension_type, pension_years, pension_income)
    st.dataframe(
        pension_table.style.format({"月額(円)": "¥{:,.0f}", "年額(円)": "¥{:,.0f}", "65歳比": "{:+.1%}"}),
        use_container_width=True,
        height=420,
    )

    m65 = calc_pension_monthly(pension_type, pension_years, pension_income, 65)
    m70 = calc_pension_monthly(pension_type, pension_years, pension_income, 70)
    break_even = calc_break_even_age(m65, 65, m70, 70)
    st.metric("現在設定の想定年金（月額）", f"¥{pension_monthly:,.0f}")
    st.caption("65歳開始 vs 70歳開始の損益分岐年齢: " + (f"{break_even}歳" if break_even else "100歳超"))

with tabs[3]:
    st.markdown("#### What-Ifシナリオ比較")
    base_scenario_params = {
        "current_age": current_age,
        "annual_income": float(annual_income),
        "annual_expense": float(annual_expense),
        "current_assets": float(current_assets),
        "annual_return": float(annual_return),
        "inflation_rate": float(inflation_rate),
        "safe_withdrawal_rate": float(safe_withdrawal_rate),
        "part_time_income_annual": float(part_time_income_monthly * 12),
    }
    scenarios = build_what_if_scenarios(
        {
            "annual_income": annual_income,
            "annual_expense": annual_expense,
            "annual_return": annual_return,
            "part_time_income_annual": part_time_income_monthly * 12,
        }
    )
    scenario_map = {s["name"]: s for s in scenarios}
    scenario_names = [s["name"] for s in scenarios]
    current_selected = [n for n in st.session_state["life_whatif_selected"] if n in scenario_names]
    if not current_selected:
        current_selected = ["ベースケース", "もし年収が100万円上がったら？", "もしリターンが5%しかなかったら？"]
    current_selected = current_selected[:3]

    col_add_1, col_add_2 = st.columns([2, 1])
    add_target = col_add_1.selectbox("追加するシナリオ", options=scenario_names, index=0)
    if col_add_2.button("What-Ifシナリオを追加", use_container_width=True):
        current = list(st.session_state["life_whatif_selected"])
        if add_target in current:
            st.warning("そのシナリオはすでに追加済みです。")
        elif len(current) >= 3:
            st.warning("同時比較は最大3シナリオです。既存を外してから追加してください。")
        else:
            current.append(add_target)
            if "ベースケース" not in current:
                current[0] = "ベースケース"
            st.session_state["life_whatif_selected"] = current

    selected_names = st.multiselect(
        "比較するシナリオ（最大3）",
        options=scenario_names,
        default=current_selected,
        max_selections=3,
    )
    if "ベースケース" not in selected_names:
        selected_names = (["ベースケース"] + selected_names)[:3]
    st.session_state["life_whatif_selected"] = selected_names

    if st.button("What-Ifを計算/更新", use_container_width=True):
        st.session_state["life_run_what_if"] = True

    if not st.session_state["life_run_what_if"]:
        st.info("What-Ifは重い処理のため、ボタン押下時のみ計算します。")
    else:
        compare_rows = []
        fig_compare = go.Figure()
        with st.spinner("What-Ifシナリオを計算中..."):
            for name in selected_names:
                scenario = scenario_map.get(name, {"name": name, "overrides": {}})
                scenario_params, scenario_events = _build_scenario_params_and_events(
                    scenario=scenario,
                    base_params=base_scenario_params,
                    base_events=st.session_state["life_events"],
                )

                _, scenario_mc = _run_simulations_cached(
                    sim_params_json=json.dumps(scenario_params, ensure_ascii=False, sort_keys=True),
                    events_json=json.dumps(scenario_events, ensure_ascii=False, sort_keys=True),
                    pension_annual=float(pension_annual),
                    n_sims=2500,
                    return_std=float(return_std),
                )
                pct = scenario_mc["percentiles"]
                fig_compare.add_trace(go.Scatter(x=pct["age"], y=pct["p50"], mode="lines", name=name))

                max_risk = _calc_max_drawdown(pct["p50"])
                compare_rows.append(
                    {
                        "シナリオ": name,
                        "FIRE確率": float(scenario_mc["fire_probability"]),
                        "FIRE年齢": None if scenario_mc["fire_age_median"] is None else int(scenario_mc["fire_age_median"]),
                        "90歳時資産": float(pct["p50"].iloc[-1]),
                        "最大リスク": max_risk,
                    }
                )

        fig_compare.update_layout(template="plotly_dark", height=450, xaxis_title="年齢", yaxis_title="資産(中央値, 円)")
        st.plotly_chart(fig_compare, use_container_width=True)

        if compare_rows:
            compare_df = pd.DataFrame(compare_rows)
            base_row = compare_df[compare_df["シナリオ"] == "ベースケース"]
            if base_row.empty:
                base_row = compare_df.iloc[[0]]
            base_prob = float(base_row["FIRE確率"].iloc[0])
            base_asset = float(base_row["90歳時資産"].iloc[0])
            base_risk = float(base_row["最大リスク"].iloc[0])

            compare_df["FIRE確率差分"] = compare_df["FIRE確率"] - base_prob
            compare_df["90歳時資産差分"] = compare_df["90歳時資産"] - base_asset
            compare_df["最大リスク差分"] = compare_df["最大リスク"] - base_risk

            def _diff_color(v: float) -> str:
                if pd.isna(v) or abs(float(v)) < 1e-12:
                    return ""
                return "background-color: rgba(34,197,94,0.28);" if float(v) > 0 else "background-color: rgba(239,68,68,0.28);"

            styled = compare_df.style.format(
                {
                    "FIRE確率": "{:.1%}",
                    "90歳時資産": "¥{:,.0f}",
                    "最大リスク": "{:.1%}",
                    "FIRE確率差分": "{:+.1%}",
                    "90歳時資産差分": "{:+,.0f}",
                    "最大リスク差分": "{:+.1%}",
                }
            )
            risk_color_fn = (
                lambda v: "background-color: rgba(34,197,94,0.28);"
                if pd.notna(v) and float(v) < 0
                else ("background-color: rgba(239,68,68,0.28);" if pd.notna(v) and float(v) > 0 else "")
            )
            if hasattr(styled, "map"):
                styled = styled.map(_diff_color, subset=["FIRE確率差分", "90歳時資産差分"])
                styled = styled.map(risk_color_fn, subset=["最大リスク差分"])
            else:
                styled = styled.applymap(_diff_color, subset=["FIRE確率差分", "90歳時資産差分"])
                styled = styled.applymap(risk_color_fn, subset=["最大リスク差分"])
            st.dataframe(styled, use_container_width=True)

            cmt_col1, cmt_col2 = st.columns([1, 4])
            if cmt_col1.button("AI比較コメント生成", use_container_width=True):
                with st.spinner("AIがシナリオ比較を分析中..."):
                    st.session_state["life_whatif_ai_comment"] = generate_whatif_comparison_comment(compare_rows)
            cmt_col2.markdown("##### AIによるシナリオ比較コメント")
            cmt_col2.write(st.session_state.get("life_whatif_ai_comment", "") or "未生成です。")

with tabs[4]:
    st.markdown("#### AIファイナンシャルアドバイザー")
    det_data = det if det is not None else {"fire_achieved": False, "fire_age": None}
    mc_data = mc if mc is not None else {"fire_probability": 0.0, "fire_age_median": None, "fire_age_p10": None, "fire_age_p90": None}

    plan_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "profile": {
            "current_age": current_age,
            "annual_income": annual_income,
            "annual_expense": annual_expense,
            "current_assets": current_assets,
            "fire_type": fire_type,
            "safe_withdrawal_rate": safe_withdrawal_rate,
        },
        "fire_deterministic": {
            "fire_achieved": bool(det_data["fire_achieved"]),
            "fire_age": det_data["fire_age"],
            "fire_target_now": fire_target_now,
        },
        "monte_carlo": {
            "fire_probability": mc_data["fire_probability"],
            "fire_age_median": mc_data["fire_age_median"],
            "fire_age_p10": mc_data["fire_age_p10"],
            "fire_age_p90": mc_data["fire_age_p90"],
        },
        "life_events": st.session_state["life_events"],
        "pension": {
            "pension_type": pension_type,
            "start_age": pension_start_age,
            "monthly": pension_monthly,
            "annual": pension_annual,
        },
    }

    if st.button("AI診断を実行", type="primary", use_container_width=True):
        if mc is None:
            st.warning("先に『シミュレーション実行』を押してください。")
        else:
            with st.spinner("AIが診断中..."):
                try:
                    st.session_state["life_ai_report"] = generate_financial_advice(plan_payload)
                except TypeError as exc:
                    st.session_state["life_ai_report"] = f"AI診断データの形式エラーです。入力値をJSON化できませんでした: {exc}"
                except Exception as exc:
                    st.session_state["life_ai_report"] = f"AI診断の実行中にエラーが発生しました: {exc}"

    st.write(st.session_state.get("life_ai_report") or "AI診断は未実行です。")

render_footer()

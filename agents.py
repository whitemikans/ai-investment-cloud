from __future__ import annotations

from dataclasses import dataclass
from sqlalchemy import text

from db.models import engine

from tools.analysis_tools import fundamental_analysis, technical_analysis
from tools.notification_tools import send_discord_message
from tools.research_tools import fetch_news_from_rss, fetch_stock_data, save_to_database
from tools.risk_tools import portfolio_risk_check, stress_test


try:
    from crewai import Agent
except Exception:
    Agent = None  # type: ignore


@dataclass
class LocalAgent:
    role: str
    goal: str
    backstory: str
    tools: list


def _mk_agent(role: str, goal: str, backstory: str, tools: list):
    if Agent is None:
        return LocalAgent(role=role, goal=goal, backstory=backstory, tools=tools)
    return Agent(role=role, goal=goal, backstory=backstory, tools=tools, verbose=True)


def _feedback_hint_for_analyst() -> str:
    """Build a short prompt hint from historical human feedback."""
    try:
        sql = """
        SELECT
          SUM(CASE WHEN ai_recommendation='買い' AND human_decision='却下' THEN 1 ELSE 0 END) AS rejected_buy_count,
          SUM(CASE WHEN ai_recommendation='買い' AND human_decision='却下' AND LOWER(COALESCE(human_reason,'')) LIKE '%per%' THEN 1 ELSE 0 END) AS rejected_buy_per_count
        FROM agent_feedback
        """
        with engine.begin() as con:
            row = con.execute(text(sql)).mappings().first()
        if not row:
            return ""
        rejected_buy = int(row.get("rejected_buy_count") or 0)
        rejected_buy_per = int(row.get("rejected_buy_per_count") or 0)
        if rejected_buy_per >= 3:
            return "過去のフィードバックで、高PER銘柄の買い推奨は却下率が高い傾向があります。バリュエーションをより厳格に評価してください。"
        if rejected_buy >= 5:
            return "過去のフィードバックで買い推奨の却下が一定数あります。推奨前にリスクと割高感の説明を強化してください。"
    except Exception:
        return ""
    return ""


def create_researcher():
    return _mk_agent(
        role="シニアマーケットリサーチャー",
        goal="日本の金融市場に関する最新ニュースと市場データを収集し、投資に影響を与える重要な情報を特定する",
        backstory=(
            "あなたはWSJ新聞で10年間記者として活躍した後、独立系リサーチ会社を設立した市場調査のプロフェッショナルです。"
            "情報収集の速さと正確さに定評があり、他のアナリストが見逃す小さなシグナルも見逃しません。"
            "日本市場特有の構造（クロスシェアリング、持ち合い、メインバンク制度）にも精通しています。"
        ),
        tools=[fetch_news_from_rss, fetch_stock_data, save_to_database],
    )


def create_analyst():
    feedback_hint = _feedback_hint_for_analyst()
    return _mk_agent(
        role="チーフ投資アナリスト",
        goal="リサーチャーから受け取った情報を分析し、具体的な投資推奨を行う",
        backstory=(
            "あなたはゴールドマンサックス東京支社で15年間株式アナリストとして活躍した後、"
            "ヘッジファンドのCIOを務めた経験を持つ投資のプロフェッショナルです。"
            "CFA資格を保有し、ファンダメンタルズ分析とテクニカル分析の両方に精通しています。"
            "過去10年の推奨銘柄の勝率は68%を誇ります。"
            + (f" {feedback_hint}" if feedback_hint else "")
        ),
        tools=[technical_analysis, fundamental_analysis],
    )


def create_risk_manager():
    return _mk_agent(
        role="チーフリスクオフィサー",
        goal="アナリストの投資推奨をポートフォリオ全体のリスク観点から評価し、セクター集中リスク、配分上限超過、相関リスクなどを検出して警告する",
        backstory=(
            "あなたは大手銀行のリスク管理部門で20年勤務した後、独立系リスクコンサルタントとなった専門家です。"
            "2008年のリーマンショックでは、いち早くリスクの高まりを検知し、クライアントの損失を最小限に抑えた実績があります。"
            "楽観的な分析に必ず冷や水を浴びせる『悪役』ですが、それがポートフォリオを守る最後の砦です。"
            "常に最悪のシナリオから考え、リスクを定量的に評価します。"
        ),
        tools=[portfolio_risk_check, stress_test],
    )


def create_reporter():
    return _mk_agent(
        role="シニアインベストメントレポーター",
        goal="リサーチャー、アナリスト、リスクマネージャーの全出力を統合し、個人投資家が5分で読めるデイリーレポートを生成する",
        backstory=(
            "あなたはWSJマネー誌で10年間編集長を務めた後、個人投資家向けの情報サービスを立ち上げたレポーティングの専門家です。"
            "複雑な金融情報を、初心者でも理解できる言葉で伝える能力に長けています。"
            "情報の優先順位付けが得意で、『今日最も重要な3つのこと』を的確に絞り込みます。"
        ),
        tools=[send_discord_message, save_to_database],
    )

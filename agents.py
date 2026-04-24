from __future__ import annotations

from dataclasses import dataclass

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
    return _mk_agent(
        role="チーフ投資アナリスト",
        goal="リサーチャーから受け取った情報を分析し、具体的な投資推奨を行う",
        backstory=(
            "あなたはゴールドマンサックス東京支社で15年間株式アナリストとして活躍した後、"
            "ヘッジファンドのCIOを務めた経験を持つ投資のプロフェッショナルです。"
            "CFA資格を保有し、ファンダメンタルズ分析とテクニカル分析の両方に精通しています。"
            "過去10年の推奨銘柄の勝率は68%を誇ります。"
        ),
        tools=[technical_analysis, fundamental_analysis],
    )


def create_risk_manager():
    return _mk_agent(
        role="チーフリスクオフィサー",
        goal="ポートフォリオ全体の下振れリスクを検出する",
        backstory="最悪シナリオから逆算して、集中・相関・ストレス耐性を定量評価する。",
        tools=[portfolio_risk_check, stress_test],
    )


def create_reporter():
    return _mk_agent(
        role="シニアインベストメントレポーター",
        goal="各エージェント結果を統合し短時間で読めるレポートを作る",
        backstory="複雑な金融情報を個人投資家向けに平易に要約する。",
        tools=[send_discord_message, save_to_database],
    )

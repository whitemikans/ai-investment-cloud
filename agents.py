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
        role="マーケットリサーチャー",
        goal="ニュースと市場データを迅速に収集する",
        backstory="日本語で思考し、事実ベースで情報を整理するリサーチ専門家。",
        tools=[fetch_news_from_rss, fetch_stock_data, save_to_database],
    )


def create_analyst():
    return _mk_agent(
        role="投資アナリスト",
        goal="5軸評価で銘柄推奨を行う",
        backstory=(
            "5軸評価（ニュース/テクニカル/バリュエーション/成長性/リスク）で客観分析する。"
            "分析では必ず以下の3フレームワークを使用する: "
            "1. SWOT分析（強み/弱み/機会/脅威） "
            "2. ポーターの5フォース（業界の競争環境） "
            "3. 逆張りの視点（市場のコンセンサスに対する反論）。"
            "投資推奨を出す際は、必ず「なぜ市場がまだこの価値を織り込んでいないか」を説明すること。"
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

from __future__ import annotations

from dataclasses import dataclass


try:
    from crewai import Task
except Exception:
    Task = None  # type: ignore


@dataclass
class LocalTask:
    description: str
    expected_output: str
    agent: object
    context: list | None = None


def _mk_task(description: str, expected_output: str, agent: object, context: list | None = None):
    if Task is None:
        return LocalTask(description=description, expected_output=expected_output, agent=agent, context=context or [])
    return Task(description=description, expected_output=expected_output, agent=agent, context=context or [])


def create_research_task(researcher: object):
    return _mk_task(
        description=(
            "今日の日本の金融市場に関する重要ニュースを調査してください。"
            "以下の観点で情報を整理してください: "
            "(1)市場全体の動向 "
            "(2)注目すべき個別銘柄のニュース "
            "(3)マクロ経済指標の発表 "
            "(4)海外市場からの影響。"
            "各ニュースにセンチメント（ポジティブ/ネガティブ/ニュートラル）を付与し、"
            "投資への影響度（高/中/低）を評価してください。"
        ),
        expected_output="JSON形式のニュースリスト（タイトル、要約、センチメント、影響度、関連銘柄）",
        agent=researcher,
    )


def create_analysis_task(analyst: object, research_task: object):
    return _mk_task(
        description=(
            "リサーチャーが収集したニュースを基に、保有銘柄への影響を分析してください。"
            "各銘柄について"
            "(1)ニュースの影響評価 "
            "(2)テクニカル分析の状況 "
            "(3)投資推奨（買い/保持/売り） "
            "(4)推奨の根拠を記述してください。"
        ),
        expected_output="各銘柄の投資評価レポート（推奨度1〜5、推奨アクション、根拠）",
        agent=analyst,
        context=[research_task],
    )


def create_risk_task(risk_manager: object, research_task: object, analysis_task: object):
    return _mk_task(
        description=(
            "アナリストの投資推奨を受け取り、以下の観点でリスク評価を行ってください: "
            "(1)推奨通りに売買した場合のポートフォリオ全体のリスク変化 "
            "(2)セクター集中リスクの有無 "
            "(3)ストレステスト結果（リーマン級暴落時の想定損失） "
            "(4)最終的なGo/No-Go判定（承認/条件付き承認/却下）"
        ),
        expected_output="リスク評価レポート（リスク変化、セクター集中、ストレステスト、Go/No-Go判定）",
        agent=risk_manager,
        context=[research_task, analysis_task],
    )


def create_report_task(reporter: object, research_task: object, analysis_task: object, risk_task: object):
    return _mk_task(
        description="全結果を統合し、5分で読めるデイリーレポートを作成し、レポートをDiscordに送信してください。",
        expected_output="エグゼクティブサマリー、推奨アクション、免責事項付きレポート",
        agent=reporter,
        context=[research_task, analysis_task, risk_task],
    )

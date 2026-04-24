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
        description="日本株/米国株ニュースと主要市場データを収集し、注目トピックを抽出してください。",
        expected_output="ニュース一覧、注目トピック、主要指数の要約",
        agent=researcher,
    )


def create_analysis_task(analyst: object, research_task: object):
    return _mk_task(
        description="収集ニュースの注目銘柄について、テクニカル/ファンダ分析を行い5軸評価を作成してください。",
        expected_output="銘柄別5軸評価、推奨度、推奨アクション",
        agent=analyst,
        context=[research_task],
    )


def create_risk_task(risk_manager: object, research_task: object, analysis_task: object):
    return _mk_task(
        description="推奨売買を反映した場合のリスク変化とストレステスト結果を評価してください。",
        expected_output="リスク警告、Go/No-Go、推定損失",
        agent=risk_manager,
        context=[research_task, analysis_task],
    )


def create_report_task(reporter: object, research_task: object, analysis_task: object, risk_task: object):
    return _mk_task(
        description="全結果を統合し、5分で読めるデイリーレポートを作成して通知してください。",
        expected_output="エグゼクティブサマリー、推奨アクション、免責事項付きレポート",
        agent=reporter,
        context=[research_task, analysis_task, risk_task],
    )


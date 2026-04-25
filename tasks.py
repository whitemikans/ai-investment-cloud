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
            "(1)市場全体の動向 (2)注目すべき個別銘柄のニュース "
            "(3)マクロ経済指標の発表 (4)海外市場からの影響。"
            "各ニュースにセンチメント（ポジティブ/ネガティブ/ニュートラル）を付与し、"
            "投資への影響度（高/中/低）を評価してください。"
            "収集結果をデータベースに保存してください。"
        ),
        expected_output="JSON形式のニュースリスト（タイトル、要約、センチメント、影響度、関連銘柄）",
        agent=researcher,
    )


def create_analysis_task(analyst: object, research_task: object):
    return _mk_task(
        description=(
            "リサーチャーが収集したニュースを基に、保有銘柄への影響を分析してください。"
            "各銘柄について(1)ニュースの影響評価 (2)テクニカル分析の状況 "
            "(3)投資推奨（買い/保持/売り） (4)推奨の根拠を記述してください。"
            "さらに、ニュース/テクニカル/バリュエーション/成長性/リスクの5軸評価スコアを算出し、"
            "総合推奨度（1〜5）を決定してください。"
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
        expected_output="リスク評価レポート（集中リスク、ストレス損失、Go/No-Go判定）",
        agent=risk_manager,
        context=[research_task, analysis_task],
    )


def create_report_task(reporter: object, research_task: object, analysis_task: object, risk_task: object):
    return _mk_task(
        description=(
            "全エージェントの出力を統合して、個人投資家向けのデイリーレポートを作成し、Discordに送信してください。"
            "加えて、過去3ヶ月の推奨精度データ（買い推奨1ヶ月勝率、サンプル数、直近の精度トレンド）を必ず含め、"
            "自分たちの過去の推奨精度も報告してください。"
        ),
        expected_output="エグゼクティブサマリー、推奨アクション、リスク警告、過去3ヶ月推奨精度を含むレポート",
        agent=reporter,
        context=[research_task, analysis_task, risk_task],
    )


def create_technology_research_task(tech_researcher: object):
    return _mk_task(
        description=(
            "6大技術領域（AI/量子/バイオ/宇宙/エネルギー/ロボティクス）の最新動向を調査してください。"
            "arXivの最新論文を収集し、投資インパクトが高い論文を特定。"
            "各技術のハイプサイクル上の位置づけを評価し、投資機会として報告してください。"
        ),
        expected_output="技術領域別の注目論文リスト + ハイプサイクル位置 + 投資推奨",
        agent=tech_researcher,
    )

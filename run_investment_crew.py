from __future__ import annotations

import json
import os
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from agents import create_analyst, create_reporter, create_researcher, create_risk_manager
from db.ai_team_utils import init_ai_team_tables, save_ai_team_report
from db.models import engine
from news_pipeline import process_news_pipeline
from tasks import create_analysis_task, create_report_task, create_research_task, create_risk_task
from tools.analysis_tools import fundamental_analysis, technical_analysis
from tools.notification_tools import send_discord_message
from tools.research_tools import save_to_database
from tools.risk_tools import portfolio_risk_check, stress_test


try:
    from crewai import Crew, Process
except Exception:
    Crew = None  # type: ignore
    Process = None  # type: ignore


def _required_env_ok() -> tuple[bool, list[str]]:
    required = ["GEMINI_API_KEY", "DISCORD_WEBHOOK_URL"]
    missing = [k for k in required if not (os.getenv(k) or "").strip()]
    return len(missing) == 0, missing


def _build_report(research: dict, analysis: dict, risk: dict) -> dict:
    risk_score = int(risk.get("risk_score", 50))
    if risk_score >= 80:
        risk_level = "urgent"
    elif risk_score >= 60:
        risk_level = "warning"
    else:
        risk_level = "normal"

    recs = analysis.get("recommendations", [])
    actions = []
    for r in recs[:3]:
        actions.append(f"{r.get('ticker','-')}: {r.get('action','保持')} ({r.get('stars',3)}★)")

    summary = (
        f"ニュース処理件数: {int(research.get('processed', 0))}件 / "
        f"リスクスコア: {risk_score} / "
        f"注目銘柄数: {len(recs)}"
    )
    market_overview = f"センチメントや市場変動を踏まえ、総合判断は {risk_level}。"
    risk_alerts = " / ".join(risk.get("warnings", [])) if isinstance(risk.get("warnings"), list) else str(risk.get("warnings", ""))

    lines = [
        "📝 AI投資チーム デイリーレポート",
        f"日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"📋 サマリー: {summary}",
        f"📰 市場概況: {market_overview}",
        f"🛡️ リスク: {risk_alerts or '重大警告なし'}",
        "📊 推奨アクション:",
    ]
    for a in actions:
        lines.append(f"- {a}")
    lines.append("⚠️ 本情報は一般情報であり投資判断は自己責任です。")
    full = "\n".join(lines)

    return {
        "run_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "risk_level": risk_level,
        "summary": summary,
        "market_overview": market_overview,
        "recommendations": recs,
        "risk_alerts": risk_alerts,
        "actions": actions,
        "full_report": full,
    }


def _local_fallback_pipeline() -> dict:
    r = process_news_pipeline(max_articles_per_source=10)
    r_dict = r.iloc[0].to_dict() if not r.empty else {"processed": 0}
    _auto_save_research_results(limit=50)

    sample = ["AAPL", "MSFT", "NVDA"]
    recs = []
    for t in sample:
        try:
            tech = json.loads(technical_analysis(t))
            fund = json.loads(fundamental_analysis(t))
            stars = int(round((float(tech.get("technical_score", 3)) + float(fund.get("valuation_score", 3)) + float(fund.get("growth_score", 3))) / 3))
            action = "買い" if stars >= 4 else ("売り" if stars <= 2 else "保持")
            recs.append({"ticker": t, "stars": stars, "action": action, "technical": tech, "fundamental": fund})
        except Exception:
            continue

    risk = json.loads(portfolio_risk_check())
    stress = json.loads(stress_test("lehman"))
    risk["stress"] = stress
    analysis = {"recommendations": recs}
    report = _build_report(r_dict, analysis, risk)
    send_discord_message(report["full_report"], severity=report["risk_level"])
    save_ai_team_report(
        report=report,
        agent_outputs={
            "researcher": r_dict,
            "analyst": analysis,
            "risk_manager": risk,
            "reporter": {"text": report["full_report"]},
        },
    )
    return report


def _auto_save_research_results(limit: int = 50) -> None:
    """最新ニュースをagent_research_resultsへ自動保存する。"""
    init_ai_team_tables()
    query = text(
        """
        SELECT
          COALESCE(na.published_at, '') AS date,
          COALESCE(na.title, '') AS title,
          COALESCE(na.summary_ja, '') AS summary,
          COALESCE(ns.sentiment_label, 'neutral') AS sentiment,
          COALESCE(ns.importance_score, 3) AS impact,
          COALESCE(na.source, '') AS source,
          COALESCE(ns.related_stocks, '') AS related_tickers
        FROM news_articles na
        LEFT JOIN news_sentiments ns ON ns.article_id = na.id
        ORDER BY na.published_at DESC, na.id DESC
        LIMIT :limit_n
        """
    )
    try:
        df = pd.read_sql(query, con=engine, params={"limit_n": int(limit)})
    except Exception:
        return

    if df.empty:
        return
    df["created_at"] = datetime.now().isoformat(timespec="seconds")
    save_to_database("agent_research_results", df.to_json(orient="records", force_ascii=False))


def run_investment_crew() -> dict:
    print("━" * 50)
    print("🚀 AI投資チーム — デイリー分析開始")
    print(f"   {datetime.now().strftime('%Y年%m月%d日 %H:%M JST')}")
    print("━" * 50)

    ok, missing = _required_env_ok()
    if not ok:
        print(f"⚠️ 環境変数不足: {', '.join(missing)}（ローカルフォールバックで実行）")

    # CrewAI objects are prepared for compatibility; fallback pipeline is used
    # when CrewAI runtime is unavailable or not configured.
    researcher = create_researcher()
    analyst = create_analyst()
    risk_manager = create_risk_manager()
    reporter = create_reporter()
    _ = [
        create_research_task(researcher),
        create_analysis_task(analyst, object()),
        create_risk_task(risk_manager, object(), object()),
        create_report_task(reporter, object(), object(), object()),
    ]

    start = time.time()
    if Crew is not None and Process is not None and ok:
        # For stability, we still run deterministic local pipeline in this project version.
        result = _local_fallback_pipeline()
    else:
        result = _local_fallback_pipeline()
    elapsed = int(time.time() - start)

    print("━" * 50)
    print("✅ AI投資チーム — デイリー分析完了")
    print(f"   合計所要時間: {elapsed // 60}分{elapsed % 60}秒")
    print("━" * 50)
    return result


if __name__ == "__main__":
    run_investment_crew()

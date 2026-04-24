from __future__ import annotations

import json
import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime
from typing import Callable

import pandas as pd
import yfinance as yf
from google import genai
from sqlalchemy import text

from agents import create_analyst, create_reporter, create_researcher, create_risk_manager
from db.ai_team_utils import init_ai_team_tables, save_ai_team_report
from db.db_utils import init_db
from db.db_utils import get_portfolio_df_with_price
from db.models import engine
from llm_config import resolve_gemini_api_key, resolve_model_name
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


def _required_env_status() -> dict[str, bool]:
    return {
        "GEMINI_API_KEY": bool(resolve_gemini_api_key()),
        "DATABASE_URL": bool((os.getenv("DATABASE_URL") or "").strip()),
        "DISCORD_WEBHOOK_URL": bool((os.getenv("DISCORD_WEBHOOK_URL") or "").strip()),
    }


def _test_db_connection() -> bool:
    with engine.connect() as con:
        con.execute(text("SELECT 1"))
    return True


def _test_api_connection() -> bool:
    key = resolve_gemini_api_key()
    if not key:
        raise RuntimeError("GEMINI_API_KEY が未設定です。")
    model = resolve_model_name()
    client = genai.Client(api_key=key)
    resp = client.models.generate_content(model=model, contents="ping")
    text_resp = str(getattr(resp, "text", "") or "").strip()
    if not text_resp:
        raise RuntimeError("Gemini API応答が空です。")
    return True


def _run_with_retry(
    func: Callable[[], dict | bool | str],
    stage_name: str,
    retries: int = 3,
    timeout_sec: int = 300,
):
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with ThreadPoolExecutor(max_workers=1) as ex:
                fut = ex.submit(func)
                return fut.result(timeout=timeout_sec)
        except FuturesTimeoutError as exc:
            last_err = TimeoutError(f"{stage_name} timed out after {timeout_sec}s")
            print(f"⚠️ {stage_name} タイムアウト ({attempt}/{retries})")
        except Exception as exc:
            last_err = exc
            print(f"⚠️ {stage_name} 失敗 ({attempt}/{retries}): {exc}")
        if attempt < retries:
            time.sleep(2)
    raise RuntimeError(f"{stage_name} failed after {retries} retries: {last_err}")


def _build_report(research: dict, analysis: dict, risk: dict) -> dict:
    risk_score = int(risk.get("risk_score", 50))
    risk_warnings = risk.get("warnings", [])
    if risk_score >= 80:
        risk_level = "urgent"
    elif isinstance(risk_warnings, list) and len(risk_warnings) > 0:
        risk_level = "warning"
    else:
        risk_level = "normal"

    recs = analysis.get("recommendations", [])
    actions = [f"{r.get('ticker','-')}: {r.get('action','保持')} ({r.get('stars',3)}★)" for r in recs[:3]]

    market = _get_market_snapshot()
    sentiment = "やや強気" if risk_level == "normal" else ("中立" if risk_level == "warning" else "警戒")
    summary_line1 = f"ニュース処理件数 {int(research.get('processed', 0))}件、注目銘柄 {len(recs)}件。"
    summary_line2 = f"リスクスコア {risk_score}（{risk_level}）。"
    summary_line3 = "本日の重要ポイントは上位3点に要約しました。"
    summary = "\n".join([summary_line1, summary_line2, summary_line3])
    market_overview = (
        f"S&P500: {market.get('sp500_text', 'N/A')} / "
        f"為替(ドル円): {market.get('usdjpy_text', 'N/A')} / "
        f"VIX: {market.get('vix_text', 'N/A')} / "
        f"センチメント: {sentiment}"
    )
    risk_alerts = " / ".join(risk_warnings) if isinstance(risk_warnings, list) else str(risk_warnings or "")
    notable_lines = [f"- {r.get('ticker','-')}: 推奨度 {r.get('stars',3)} / {r.get('action','保持')}" for r in recs[:3]]

    lines = [
        "📝 AI投資チーム デイリーレポート",
        f"日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "📋 エグゼクティブサマリー（3行）",
        summary_line1,
        summary_line2,
        summary_line3,
        f"📰 市場概況: {market_overview}",
        "📊 注目銘柄評価:",
    ]
    lines.extend(notable_lines or ["- 該当なし"])
    lines.extend([f"🛡️ リスク: {risk_alerts or '重大警告なし'}", "📊 推奨アクション:"])
    lines.extend([f"- {a}" for a in actions])
    lines.append("⚠️ 免責事項: 本情報は一般情報であり、投資判断はご自身の責任でお願いします。")
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


def _get_market_snapshot() -> dict:
    out = {"sp500_text": "N/A", "vix_text": "N/A", "usdjpy_text": "N/A"}
    symbols = {"sp500_text": "^GSPC", "vix_text": "^VIX", "usdjpy_text": "JPY=X"}
    for key, ticker in symbols.items():
        try:
            hist = yf.Ticker(ticker).history(period="5d")
            if hist is None or hist.empty:
                continue
            close = pd.to_numeric(hist["Close"], errors="coerce").dropna()
            if close.empty:
                continue
            cur = float(close.iloc[-1])
            prev = float(close.iloc[-2]) if len(close) >= 2 else cur
            diff = ((cur / prev) - 1.0) * 100.0 if prev else 0.0
            out[key] = f"{cur:,.2f} ({diff:+.2f}%)"
        except Exception:
            continue
    return out


def _auto_save_research_results(limit: int = 50) -> None:
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
    df = pd.read_sql(query, con=engine, params={"limit_n": int(limit)})
    if df.empty:
        return
    df["created_at"] = datetime.now().isoformat(timespec="seconds")
    save_to_database("agent_research_results", df.to_json(orient="records", force_ascii=False))


def _research_stage() -> dict:
    r = process_news_pipeline(max_articles_per_source=10)
    _auto_save_research_results(limit=50)
    return r.iloc[0].to_dict() if not r.empty else {"processed": 0}


def _analysis_stage() -> dict:
    pf = get_portfolio_df_with_price()
    candidates = pf["stock_code"].astype(str).head(3).tolist() if not pf.empty else ["AAPL", "MSFT", "NVDA"]
    recs = []
    for t in candidates:
        tech = json.loads(technical_analysis(t))
        fund = json.loads(fundamental_analysis(t))
        stars = int(round((float(tech.get("technical_score", 3)) + float(fund.get("valuation_score", 3)) + float(fund.get("growth_score", 3))) / 3))
        action = "買い" if stars >= 4 else ("売り" if stars <= 2 else "保持")
        recs.append({"ticker": t, "stars": stars, "action": action, "technical": tech, "fundamental": fund})
    return {"recommendations": recs}


def _risk_stage(analysis: dict) -> dict:
    rec_json = json.dumps(analysis.get("recommendations", []), ensure_ascii=False)
    risk = json.loads(portfolio_risk_check(rec_json))
    risk["stress"] = json.loads(stress_test("lehman"))
    return risk


def _report_stage(research: dict, analysis: dict, risk: dict) -> dict:
    report = _build_report(research, analysis, risk)
    send_discord_message(report["full_report"], severity=report["risk_level"])
    save_ai_team_report(
        report=report,
        agent_outputs={
            "researcher": research,
            "analyst": analysis,
            "risk_manager": risk,
            "reporter": {"text": report["full_report"]},
        },
    )
    return report


def _kickoff_crewai_best_effort() -> None:
    if Crew is None or Process is None:
        return
    researcher = create_researcher()
    analyst = create_analyst()
    risk_manager = create_risk_manager()
    reporter = create_reporter()
    t1 = create_research_task(researcher)
    t2 = create_analysis_task(analyst, t1)
    t3 = create_risk_task(risk_manager, t1, t2)
    t4 = create_report_task(reporter, t1, t2, t3)
    crew = Crew(
        agents=[researcher, analyst, risk_manager, reporter],
        tasks=[t1, t2, t3, t4],
        process=Process.sequential,
        verbose=True,
        memory=True,
    )
    _run_with_retry(lambda: str(crew.kickoff()), "Crew kickoff", retries=3, timeout_sec=300)


def run_investment_crew() -> dict:
    print("━" * 50)
    print("🚀 AI投資チーム — デイリー分析開始")
    print(f"   {datetime.now().strftime('%Y年%m月%d日 %H:%M JST')}")
    print("━" * 50)

    try:
        # Ensure core DB schema (including stocks table) exists before any stage.
        init_db()
        env_status = _required_env_status()
        print(f"ENV CHECK: {env_status}")
        db_ok = bool(_run_with_retry(_test_db_connection, "DB接続テスト", retries=3, timeout_sec=60))
        api_ok = bool(_run_with_retry(_test_api_connection, "API接続テスト", retries=3, timeout_sec=60))
        print(f"PRECHECK: db_ok={db_ok}, api_ok={api_ok}")

        if all(env_status.values()) and db_ok and api_ok:
            try:
                _kickoff_crewai_best_effort()
            except Exception as exc:
                send_discord_message(f"🔴 Crew kickoff failed: {exc}", severity="urgent")

        start = time.time()
        research = _run_with_retry(_research_stage, "Researcher", retries=3, timeout_sec=300)
        analysis = _run_with_retry(_analysis_stage, "Analyst", retries=3, timeout_sec=300)
        risk = _run_with_retry(lambda: _risk_stage(analysis), "Risk Manager", retries=3, timeout_sec=300)
        result = _run_with_retry(lambda: _report_stage(research, analysis, risk), "Reporter", retries=3, timeout_sec=300)
        elapsed = int(time.time() - start)

        print("━" * 50)
        print("✅ AI投資チーム — デイリー分析完了")
        print(f"   合計所要時間: {elapsed // 60}分{elapsed % 60}秒")
        print("━" * 50)
        return result
    except Exception as exc:
        err = f"致命的エラー: {exc}\n{traceback.format_exc(limit=2)}"
        send_discord_message(err[:1800], severity="urgent")
        raise


if __name__ == "__main__":
    run_investment_crew()

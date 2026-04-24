from __future__ import annotations

import json
from datetime import datetime

from agents import create_analyst, create_researcher
from tasks import create_analysis_task, create_research_task
from tools.analysis_tools import fundamental_analysis, technical_analysis


def _safe_json(text: str) -> dict:
    try:
        return json.loads(text)
    except Exception:
        return {}


def run_research_analyst_link_test() -> dict:
    """リサーチャー→アナリスト連携（context受け渡し）を検証する。"""
    researcher = create_researcher()
    analyst = create_analyst()

    research_task = create_research_task(researcher)
    analysis_task = create_analysis_task(analyst, research_task)

    # リサーチャー出力（テスト用サンプル）
    research_output = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "headlines": [
            {"title": "NVIDIA launches new AI chips", "ticker": "NVDA", "sentiment": 0.7},
            {"title": "Microsoft cloud growth remains strong", "ticker": "MSFT", "sentiment": 0.5},
            {"title": "Apple iPhone demand mixed in Q2", "ticker": "AAPL", "sentiment": 0.1},
        ],
    }

    tickers = [row["ticker"] for row in research_output["headlines"]]
    sentiment_map = {row["ticker"]: float(row["sentiment"]) for row in research_output["headlines"]}

    recommendations = []
    for ticker in tickers:
        tech = _safe_json(technical_analysis(ticker))
        fund = _safe_json(fundamental_analysis(ticker))

        technical_score = float(tech.get("technical_score", 3))
        valuation_score = float(fund.get("valuation_score", 3))
        growth_score = float(fund.get("growth_score", 3))
        news_score = max(1.0, min(5.0, 3.0 + sentiment_map.get(ticker, 0.0) * 2.0))
        risk_score = 3.0

        total_score = round(
            0.2 * news_score
            + 0.2 * technical_score
            + 0.2 * valuation_score
            + 0.25 * growth_score
            + 0.15 * risk_score,
            2,
        )
        if total_score >= 4.0:
            action = "買い"
        elif total_score <= 2.5:
            action = "売り"
        else:
            action = "保持"

        recommendations.append(
            {
                "ticker": ticker,
                "scores": {
                    "ニュース": news_score,
                    "テクニカル": technical_score,
                    "バリュエーション": valuation_score,
                    "成長性": growth_score,
                    "リスク": risk_score,
                },
                "total_score_1to5": total_score,
                "action": action,
            }
        )

    return {
        "research_task_description": getattr(research_task, "description", ""),
        "analysis_task_description": getattr(analysis_task, "description", ""),
        "analysis_context_count": len(getattr(analysis_task, "context", []) or []),
        "analysis_context_has_research_task": any(
            c is research_task for c in (getattr(analysis_task, "context", []) or [])
        ),
        "research_output": research_output,
        "analysis_output": recommendations,
    }


if __name__ == "__main__":
    result = run_research_analyst_link_test()
    print(json.dumps(result, ensure_ascii=False, indent=2))

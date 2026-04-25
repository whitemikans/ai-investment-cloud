from __future__ import annotations

import json

import pandas as pd

from db.tech_research_utils import get_latest_tech_papers, save_tech_papers


try:
    from crewai.tools import tool
except Exception:
    def tool(_name: str):  # type: ignore
        def deco(func):
            return func
        return deco


KEYWORD_MAP = {
    "NVIDIA": ["gpu", "cuda", "transformer", "llm"],
    "MSFT": ["copilot", "azure", "openai", "enterprise ai"],
    "GOOGL": ["deepmind", "gemini", "alphafold"],
    "IONQ": ["quantum", "qubit", "fault-tolerant"],
    "AAPL": ["wearable", "health", "battery"],
    "TSLA": ["robot", "autonomous", "battery"],
}


def _score_text(text: str) -> tuple[float, list[str]]:
    src = str(text or "").lower()
    picks: list[str] = []
    score = 1.0
    for t, kws in KEYWORD_MAP.items():
        for kw in kws:
            if kw in src:
                score += 0.7
                if t not in picks:
                    picks.append(t)
    return min(5.0, score), picks


def analyze_papers_for_investment(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    scores: list[float] = []
    tickers: list[str] = []
    recs: list[str] = []

    for r in work.itertuples(index=False):
        text = f"{getattr(r, 'title', '')} {getattr(r, 'summary', '')}"
        score, picks = _score_text(text)
        scores.append(float(score))
        tickers.append(",".join(picks))
        if score >= 4.2:
            rec = "Invest"
        elif score >= 3.0:
            rec = "Watch"
        else:
            rec = "Research"
        recs.append(rec)

    work["impact_score"] = scores
    work["related_tickers"] = tickers
    work["recommendation"] = recs
    return work


@tool("論文投資翻訳")
def analyze_latest_papers_tool(limit: str = "80") -> str:
    n = int(float(limit or 80))
    src = get_latest_tech_papers(limit=max(10, min(300, n)))
    analyzed = analyze_papers_for_investment(src)
    if analyzed.empty:
        return json.dumps({"rows": 0, "saved": 0}, ensure_ascii=False)
    saved = save_tech_papers(analyzed)
    top = analyzed.sort_values("impact_score", ascending=False).head(5)[["title", "impact_score", "related_tickers"]]
    return json.dumps(
        {"rows": int(len(analyzed)), "saved": int(saved), "top5": top.to_dict(orient="records")},
        ensure_ascii=False,
    )


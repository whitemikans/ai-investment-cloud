from __future__ import annotations

import json
import os
import re
import time
from typing import Any

import pandas as pd

from config import get_setting
from db.tech_research_utils import get_latest_tech_papers, save_tech_papers


try:
    from crewai.tools import tool
except Exception:
    def tool(_name: str):  # type: ignore
        def deco(func):
            return func
        return deco


DEFAULT_MODEL = "gemini-2.5-flash"
KEYWORD_MAP = {
    "NVIDIA": ["gpu", "cuda", "transformer", "llm", "inference"],
    "MSFT": ["copilot", "azure", "openai", "enterprise ai", "cloud"],
    "GOOGL": ["deepmind", "gemini", "alphafold", "search"],
    "IONQ": ["quantum", "qubit", "fault-tolerant"],
    "AAPL": ["wearable", "health", "battery", "on-device ai"],
    "TSLA": ["robot", "autonomous", "battery"],
}


def _fallback_score_text(text: str) -> tuple[float, list[str]]:
    src = str(text or "").lower()
    picks: list[str] = []
    score = 1.0
    for ticker, kws in KEYWORD_MAP.items():
        for kw in kws:
            if kw in src:
                score += 0.7
                if ticker not in picks:
                    picks.append(ticker)
    return min(5.0, score), picks


def _recommendation_from_score(score: float) -> str:
    if score >= 4.0:
        return "注目"
    if score >= 3.0:
        return "監視"
    return "調査"


def _extract_json(text: str) -> dict[str, Any]:
    src = str(text or "").strip()
    if not src:
        return {}
    try:
        return json.loads(src)
    except Exception:
        pass
    m = re.search(r"\{.*\}", src, flags=re.S)
    if not m:
        return {}
    try:
        return json.loads(m.group(0))
    except Exception:
        return {}


def _parse_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _build_prompt(title: str, summary: str) -> str:
    return f"""
以下の論文情報を投資家向けに分析してください。

タイトル:
{title}

要約:
{summary}

必ずJSONのみで回答してください。説明文は不要です。
JSONスキーマ:
{{
  "breakthrough_summary": "技術的ブレイクスルー要約（2行以内）",
  "impact_score": 1-5の数値,
  "practical_years": 実用化までの推定年数(数値),
  "beneficiary_summary": "恩恵を受けるセクター/企業（米国株ティッカー含む）",
  "related_tickers": ["AAPL","MSFT"],
  "action_items": ["投資家向けアクション1","アクション2","アクション3"]
}}
""".strip()


def _gemini_client() -> tuple[Any, str]:
    api_key = (get_setting("GEMINI_API_KEY", "") or os.getenv("GEMINI_API_KEY", "")).strip()
    model = (get_setting("GEMINI_MODEL", DEFAULT_MODEL) or DEFAULT_MODEL).strip()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY missing")
    from google import genai
    return genai.Client(api_key=api_key), model


def _analyze_one_with_gemini(client: Any, model_name: str, title: str, summary: str) -> dict[str, Any]:
    from google.genai import types

    system = (
        "あなたは米国株テック投資に強いアナリストです。"
        "論文内容を投資機会に翻訳し、必ずJSONのみを返してください。"
    )
    prompt = _build_prompt(title=title, summary=summary)
    resp = client.models.generate_content(
        model=model_name,
        contents=prompt,
        config=types.GenerateContentConfig(system_instruction=system, temperature=0.2),
    )
    data = _extract_json(getattr(resp, "text", "") or "")
    if not data:
        raise RuntimeError("invalid gemini json response")
    return data


def analyze_papers_for_investment(
    df: pd.DataFrame,
    max_items: int = 60,
    sleep_seconds: float = 1.0,
    impact_threshold: float = 4.0,
    use_gemini: bool = True,
) -> pd.DataFrame:
    """Analyze arXiv papers and translate summaries into investment opportunities.

    Pipeline:
    1. Input: paper title + summary.
    2. Request Gemini analysis (JSON).
    3. Batch process up to `max_items` with 1-second interval by default.
    4. Tag featured papers where impact_score >= impact_threshold.
    5. Return enriched dataframe (can be persisted to Supabase via save_tech_papers).
    """
    if df is None or df.empty:
        return pd.DataFrame()

    work = df.copy().head(max(1, int(max_items))).reset_index(drop=True)
    rows: list[dict[str, Any]] = []

    use_gemini_flag = bool(use_gemini)
    gemini_error = ""
    client = None
    model_name = ""
    if use_gemini_flag:
        try:
            client, model_name = _gemini_client()
        except Exception as e:
            use_gemini_flag = False
            gemini_error = str(e)

    for idx, r in enumerate(work.itertuples(index=False), start=1):
        title = str(getattr(r, "title", "") or "")
        summary = str(getattr(r, "summary", "") or "")
        text = f"{title} {summary}"

        analysis: dict[str, Any] = {}
        if use_gemini_flag and client is not None:
            try:
                analysis = _analyze_one_with_gemini(client, model_name=model_name, title=title, summary=summary)
            except Exception as e:
                analysis = {"_error": str(e)}

        if not analysis or analysis.get("_error"):
            score, picks = _fallback_score_text(text)
            analysis = {
                "breakthrough_summary": (summary[:180] if summary else title[:180]),
                "impact_score": score,
                "practical_years": 3.0 if score >= 4 else 5.0,
                "beneficiary_summary": ", ".join(picks) if picks else "US Tech Broad Market",
                "related_tickers": picks,
                "action_items": [
                    "関連企業の決算で技術言及を確認",
                    "次四半期の研究投資額を追跡",
                    "過熱時は分割エントリーで管理",
                ],
            }

        score = min(5.0, max(1.0, _parse_float(analysis.get("impact_score", 1.0), 1.0)))
        years = _parse_float(analysis.get("practical_years", 0.0), 0.0)
        raw_tickers = analysis.get("related_tickers", [])
        if isinstance(raw_tickers, str):
            tickers = [x.strip().upper() for x in raw_tickers.split(",") if x.strip()]
        elif isinstance(raw_tickers, list):
            tickers = [str(x).strip().upper() for x in raw_tickers if str(x).strip()]
        else:
            tickers = []
        tickers = sorted(set(tickers))
        action_items = analysis.get("action_items", [])
        if isinstance(action_items, str):
            action_items = [x.strip() for x in action_items.split("\n") if x.strip()]
        if not isinstance(action_items, list):
            action_items = []

        row = {
            "title": title,
            "authors": str(getattr(r, "authors", "") or ""),
            "summary": summary,
            "categories": str(getattr(r, "categories", "") or ""),
            "published_at": str(getattr(r, "published_at", "") or ""),
            "pdf_url": str(getattr(r, "pdf_url", "") or ""),
            "source_url": str(getattr(r, "source_url", "") or ""),
            "tech_theme": str(getattr(r, "tech_theme", "") or ""),
            "impact_score": score,
            "related_tickers": ",".join(tickers),
            "recommendation": _recommendation_from_score(score),
            "breakthrough_summary": str(analysis.get("breakthrough_summary", "") or ""),
            "practical_years": years,
            "beneficiary_summary": str(analysis.get("beneficiary_summary", "") or ""),
            "action_items": json.dumps(action_items, ensure_ascii=False),
            "is_featured": 1 if score >= float(impact_threshold) else 0,
            "analysis_model": model_name if use_gemini_flag else "fallback",
            "analysis_raw_json": json.dumps(analysis, ensure_ascii=False),
        }
        rows.append(row)

        if idx < len(work):
            time.sleep(max(0.0, float(sleep_seconds)))

    out = pd.DataFrame(rows)
    # Keep diagnostic info in attrs without affecting DB schema.
    out.attrs["featured_count"] = int((out["is_featured"] == 1).sum()) if not out.empty else 0
    out.attrs["gemini_used"] = bool(use_gemini_flag)
    out.attrs["gemini_error"] = gemini_error
    return out


@tool("論文投資分析")
def analyze_latest_papers_tool(limit: str = "60") -> str:
    """Analyze latest papers in batch and persist results to configured database."""
    n = int(float(limit or 60))
    src = get_latest_tech_papers(limit=max(10, min(300, n)))
    analyzed = analyze_papers_for_investment(src, max_items=min(60, n), sleep_seconds=1.0, impact_threshold=4.0)
    if analyzed.empty:
        return json.dumps({"rows": 0, "saved": 0, "featured": 0}, ensure_ascii=False)
    saved = save_tech_papers(analyzed)
    featured = analyzed[analyzed["is_featured"] == 1][["title", "impact_score", "related_tickers"]]
    return json.dumps(
        {
            "rows": int(len(analyzed)),
            "saved": int(saved),
            "featured": int(len(featured)),
            "featured_top": featured.sort_values("impact_score", ascending=False).head(10).to_dict(orient="records"),
            "gemini_used": bool(analyzed.attrs.get("gemini_used", False)),
            "gemini_error": str(analyzed.attrs.get("gemini_error", "") or ""),
        },
        ensure_ascii=False,
    )

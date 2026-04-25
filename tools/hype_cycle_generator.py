from __future__ import annotations

import json
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from db.models import engine
from db.tech_research_utils import replace_hype_history
from tools.paper_trends import get_paper_trends


try:
    from crewai.tools import tool
except Exception:
    def tool(_name: str):  # type: ignore
        def deco(func):
            return func
        return deco


THEMES = ["AI", "Quantum", "Biotech", "Space", "Energy", "Robotics"]


def _phase_from_index(index_value: float, slope: float) -> str:
    if index_value >= 70 and slope > 2:
        return "②過度な期待"
    if slope < -1.5:
        return "③幻滅の谷"
    if 45 <= index_value <= 75 and slope > 0.5:
        return "④啓蒙活動期"
    if index_value >= 65 and abs(slope) <= 1.2:
        return "⑤安定期"
    return "①黎明期"


def _news_counts_by_theme() -> dict[str, int]:
    mapping = {
        "AI": ["ai", "artificial intelligence", "llm", "agent"],
        "Quantum": ["quantum", "qubit"],
        "Biotech": ["biotech", "drug", "gene", "mrna"],
        "Space": ["space", "satellite", "rocket"],
        "Energy": ["battery", "energy", "fusion", "solar"],
        "Robotics": ["robot", "robotics", "humanoid"],
    }
    counts = {k: 0 for k in THEMES}
    for theme, kws in mapping.items():
        where = " OR ".join([f"LOWER(COALESCE(title,'')) LIKE :kw{i}" for i, _ in enumerate(kws)])
        params = {f"kw{i}": f"%{kw.lower()}%" for i, kw in enumerate(kws)}
        sql = text(f"SELECT COUNT(*) FROM news_articles WHERE {where}")
        try:
            with engine.connect() as con:
                counts[theme] = int(con.execute(sql, params).scalar() or 0)
        except Exception:
            counts[theme] = 0
    return counts


def generate_hype_cycle() -> pd.DataFrame:
    pt = get_paper_trends(months=12)
    news = _news_counts_by_theme()
    if pt.empty:
        rows = []
        for t in THEMES:
            idx = min(100.0, float(news.get(t, 0) * 1.4))
            rows.append(
                {
                    "as_of_date": datetime.now().date().isoformat(),
                    "tech_theme": t,
                    "hype_index": round(idx, 2),
                    "phase": _phase_from_index(idx, slope=0.0),
                    "source_breakdown_json": json.dumps({"search": 0, "papers": 0, "news": int(news.get(t, 0))}),
                }
            )
        return pd.DataFrame(rows)

    latest_month = pt["month"].max()
    prev_month = sorted(pt["month"].unique())[-2] if len(pt["month"].unique()) >= 2 else latest_month

    rows = []
    for t in THEMES:
        p_latest = int(pt[(pt["tech_theme"] == t) & (pt["month"] == latest_month)]["paper_count"].sum())
        p_prev = int(pt[(pt["tech_theme"] == t) & (pt["month"] == prev_month)]["paper_count"].sum())
        search_vol = min(100.0, float(p_latest * 7.0))
        paper_norm = min(100.0, float(p_latest * 8.0))
        news_norm = min(100.0, float(news.get(t, 0) * 1.6))
        hype = 0.4 * search_vol + 0.3 * paper_norm + 0.3 * news_norm
        slope = float(p_latest - p_prev)
        rows.append(
            {
                "as_of_date": datetime.now().date().isoformat(),
                "tech_theme": t,
                "hype_index": round(float(hype), 2),
                "phase": _phase_from_index(float(hype), slope=slope),
                "source_breakdown_json": json.dumps(
                    {"search": round(search_vol, 2), "papers": p_latest, "news": int(news.get(t, 0))}
                ),
            }
        )
    return pd.DataFrame(rows)


@tool("ハイプサイクル生成")
def generate_hype_cycle_tool() -> str:
    df = generate_hype_cycle()
    saved = replace_hype_history(df)
    return json.dumps({"rows": int(len(df)), "saved": int(saved)}, ensure_ascii=False)


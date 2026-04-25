from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from db.tech_research_utils import get_hype_history, get_latest_tech_papers, save_weekly_report
from tools.arxiv_collector import collect_arxiv_papers
from tools.hype_cycle_generator import generate_hype_cycle
from tools.paper_analyzer import analyze_papers_for_investment
from tools.patent_analyzer import build_patent_stats
from tools.tech_radar import build_tech_radar
from tools.notification_tools import send_discord_message


JST = ZoneInfo("Asia/Tokyo")


def _fmt_line(prefix: str, value: str) -> str:
    return f"- {prefix}: {value}"


def run_weekly_tech_report() -> dict:
    raw = collect_arxiv_papers(max_results_per_theme=10, days_back=21)
    analyzed = analyze_papers_for_investment(raw)
    from db.tech_research_utils import save_tech_papers, replace_hype_history, replace_patent_stats

    save_tech_papers(analyzed)
    replace_hype_history(generate_hype_cycle())
    replace_patent_stats(build_patent_stats())

    latest = get_latest_tech_papers(limit=200)
    radar = build_tech_radar()
    hype = get_hype_history()

    top = pd.DataFrame()
    if not latest.empty:
        latest["impact_score"] = pd.to_numeric(latest["impact_score"], errors="coerce").fillna(0.0)
        top = latest.sort_values(["impact_score", "published_at"], ascending=[False, False]).head(5)

    lines = [
        "🔬 AI投資チーム 週次テクノロジーレポート",
        f"日時: {datetime.now(JST).strftime('%Y-%m-%d %H:%M JST')}",
        "",
        f"収集論文: {len(raw)}件 / 分析済み: {len(analyzed)}件",
        "",
        "今週の注目論文 TOP5:",
    ]
    if top.empty:
        lines.append("- 該当なし")
    else:
        for i, row in enumerate(top.itertuples(index=False), start=1):
            title = str(getattr(row, "title", ""))[:120]
            score = float(getattr(row, "impact_score", 0.0))
            theme = str(getattr(row, "tech_theme", ""))
            tickers = str(getattr(row, "related_tickers", "") or "-")
            lines.append(f"{i}. [{theme}] ⭐{score:.1f} {title}")
            lines.append(f"   関連銘柄: {tickers}")

    lines.append("")
    lines.append("テクノロジーレーダー:")
    if radar.empty:
        lines.append("- データなし")
    else:
        for r in radar.itertuples(index=False):
            lines.append(_fmt_line(str(getattr(r, "tech_theme", "")), f"{getattr(r, 'radar_stage', '')} / {getattr(r, 'phase', '')}"))

    lines.append("")
    lines.append("ハイプ指数(最新):")
    if hype.empty:
        lines.append("- データなし")
    else:
        latest_h = hype.sort_values(["as_of_date", "tech_theme"]).groupby("tech_theme", as_index=False).tail(1)
        for r in latest_h.itertuples(index=False):
            lines.append(_fmt_line(str(getattr(r, "tech_theme", "")), f"{float(getattr(r, 'hype_index', 0.0)):.1f}"))

    body = "\n".join(lines)
    save_weekly_report("週次テクノロジーレポート", body)
    send_discord_message(body, severity="normal")
    return {"papers_collected": int(len(raw)), "papers_analyzed": int(len(analyzed)), "radar_rows": int(len(radar))}


if __name__ == "__main__":
    print(run_weekly_tech_report())


from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from db.tech_research_utils import (
    get_hype_history,
    get_latest_tech_papers,
    save_weekly_report,
)
from tools.arxiv_collector import collect_arxiv_papers
from tools.hype_cycle_generator import generate_hype_cycle
from tools.notification_tools import send_discord_message
from tools.paper_analyzer import analyze_papers_for_investment
from tools.patent_analyzer import build_patent_stats, build_patent_yearly_stats
from tools.tech_radar import build_tech_radar


JST = ZoneInfo("Asia/Tokyo")


def _fmt_num(value: object, digits: int = 1) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return "-"


def _short(text: object, limit: int = 120) -> str:
    src = str(text or "").replace("\n", " ").strip()
    return src if len(src) <= limit else src[: limit - 1] + "…"


def _parse_items(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    src = str(value or "").strip()
    if not src:
        return []
    try:
        data = json.loads(src)
        if isinstance(data, list):
            return [str(x).strip() for x in data if str(x).strip()]
    except Exception:
        pass
    return [x.strip() for x in src.replace(";", ",").split(",") if x.strip()]


def _latest_and_previous_hype(hype: pd.DataFrame) -> pd.DataFrame:
    if hype is None or hype.empty:
        return pd.DataFrame(columns=["tech_theme", "hype_index", "prev_hype_index", "phase", "prev_phase", "delta"])
    work = hype.copy()
    work["hype_index"] = pd.to_numeric(work["hype_index"], errors="coerce").fillna(0.0)
    work = work.sort_values(["tech_theme", "as_of_date"])
    rows: list[dict[str, object]] = []
    for theme, grp in work.groupby("tech_theme"):
        tail = grp.tail(2).reset_index(drop=True)
        latest = tail.iloc[-1]
        prev = tail.iloc[-2] if len(tail) >= 2 else latest
        current = float(latest.get("hype_index", 0.0) or 0.0)
        previous = float(prev.get("hype_index", current) or current)
        rows.append(
            {
                "tech_theme": theme,
                "hype_index": current,
                "prev_hype_index": previous,
                "phase": str(latest.get("phase", "")),
                "prev_phase": str(prev.get("phase", "")),
                "delta": current - previous,
            }
        )
    return pd.DataFrame(rows)


def _change_label(delta: object, phase: str, prev_phase: str) -> str:
    try:
        d = float(delta)
    except Exception:
        d = 0.0
    phase_changed = str(phase) != str(prev_phase)
    if abs(d) < 1.0 and not phase_changed:
        return "変化なし"
    direction = "上昇" if d > 0 else "低下" if d < 0 else "横ばい"
    suffix = "、フェーズ変更あり" if phase_changed else ""
    return f"変化あり（{direction} {d:+.1f}pt{suffix}）"


def _stage_from_phase(phase: str, hype_index: object) -> str:
    p = str(phase or "")
    h = float(hype_index or 0.0)
    if "安定" in p or ("啓蒙" in p and h >= 55):
        return "Invest"
    if "過度" in p or h >= 65:
        return "Watch"
    if "幻滅" in p or h >= 35:
        return "Research"
    return "Hold"


def _radar_change_rows(radar: pd.DataFrame, hype_summary: pd.DataFrame) -> list[str]:
    if radar is None or radar.empty:
        return ["- レーダーデータなし"]
    prev_map = {
        str(r.tech_theme): _stage_from_phase(str(r.prev_phase), getattr(r, "prev_hype_index", 0.0))
        for r in hype_summary.itertuples(index=False)
    }
    lines: list[str] = []
    for row in radar.sort_values("tech_theme").itertuples(index=False):
        theme = str(getattr(row, "tech_theme", ""))
        current = str(getattr(row, "radar_stage", ""))
        previous = prev_map.get(theme, current)
        marker = "変更あり" if current != previous else "変更なし"
        if current != previous:
            lines.append(f"- {theme}: {previous} → {current}（{marker}）")
        else:
            lines.append(f"- {theme}: {current}（{marker}）")
    return lines


def _related_stock_impact(top: pd.DataFrame) -> list[str]:
    if top is None or top.empty:
        return ["- 関連銘柄データなし"]
    scores: dict[str, list[float]] = defaultdict(list)
    themes: dict[str, Counter[str]] = defaultdict(Counter)
    for row in top.itertuples(index=False):
        score = float(getattr(row, "impact_score", 0.0) or 0.0)
        theme = str(getattr(row, "tech_theme", "") or "Unknown")
        for ticker in _parse_items(getattr(row, "related_tickers", "")):
            scores[ticker].append(score)
            themes[ticker][theme] += 1
    if not scores:
        return ["- 関連銘柄データなし"]
    lines: list[str] = []
    for ticker, vals in sorted(scores.items(), key=lambda kv: sum(kv[1]) / max(1, len(kv[1])), reverse=True)[:8]:
        avg = sum(vals) / max(1, len(vals))
        theme = themes[ticker].most_common(1)[0][0] if themes[ticker] else "-"
        action = "重点監視" if avg >= 4.0 else "監視継続" if avg >= 3.0 else "参考情報"
        lines.append(f"- {ticker}: 平均インパクト {_fmt_num(avg)} / 主テーマ {theme} / {action}")
    return lines


def _action_items(top: pd.DataFrame, radar: pd.DataFrame, hype_summary: pd.DataFrame) -> list[str]:
    actions: list[str] = []
    if top is not None and not top.empty:
        for row in top.itertuples(index=False):
            for item in _parse_items(getattr(row, "action_items", "")):
                if item and item not in actions:
                    actions.append(item)
                if len(actions) >= 3:
                    break
            if len(actions) >= 3:
                break
    if len(actions) < 3 and radar is not None and not radar.empty:
        invest = radar[radar["radar_stage"].astype(str).eq("Invest")]
        if not invest.empty:
            theme = str(invest.sort_values("radar_score", ascending=False).iloc[0].get("tech_theme", ""))
            actions.append(f"{theme}関連銘柄の決算・研究開発発表を重点確認する")
    if len(actions) < 3 and hype_summary is not None and not hype_summary.empty:
        mover = hype_summary.reindex(hype_summary["delta"].abs().sort_values(ascending=False).index).head(1)
        if not mover.empty:
            theme = str(mover.iloc[0].get("tech_theme", ""))
            actions.append(f"ハイプ指数の変化が大きい{theme}のニュース量と特許動向を再確認する")
    if not actions:
        actions = ["高インパクト論文と関連銘柄をウォッチリストに追加する"]
    return [f"- {a}" for a in actions[:3]]


def _technology_researcher_stage() -> dict[str, object]:
    from db.tech_research_utils import (
        replace_hype_history,
        replace_patent_stats,
        replace_patent_yearly,
        save_tech_papers,
    )

    raw = collect_arxiv_papers(max_results_per_theme=10, days_back=21)
    analyzed = analyze_papers_for_investment(raw)
    saved_papers = save_tech_papers(analyzed)

    hype_df = generate_hype_cycle()
    saved_hype = replace_hype_history(hype_df)

    patent_stats = build_patent_stats()
    patent_yearly = build_patent_yearly_stats(start_year=2018)
    saved_patents = replace_patent_stats(patent_stats.drop(columns=["source"], errors="ignore"))
    saved_patent_yearly = replace_patent_yearly(patent_yearly)

    return {
        "raw": raw,
        "analyzed": analyzed,
        "hype_df": hype_df,
        "patent_stats": patent_stats,
        "patent_yearly": patent_yearly,
        "saved_papers": saved_papers,
        "saved_hype": saved_hype,
        "saved_patents": saved_patents,
        "saved_patent_yearly": saved_patent_yearly,
    }


def _reporter_stage(research: dict[str, object]) -> str:
    latest = get_latest_tech_papers(limit=300)
    radar = build_tech_radar()
    hype = get_hype_history()
    hype_summary = _latest_and_previous_hype(hype)
    patent_stats = research.get("patent_stats")
    patent_yearly = research.get("patent_yearly")
    if not isinstance(patent_stats, pd.DataFrame):
        patent_stats = pd.DataFrame()
    if not isinstance(patent_yearly, pd.DataFrame):
        patent_yearly = pd.DataFrame()

    top = pd.DataFrame()
    if latest is not None and not latest.empty:
        work = latest.copy()
        work["impact_score"] = pd.to_numeric(work["impact_score"], errors="coerce").fillna(0.0)
        top = work.sort_values(["impact_score", "published_at"], ascending=[False, False]).head(5)

    lines: list[str] = [
        "🔬 AI投資チーム 週次テクノロジーレポート",
        f"作成時刻: {datetime.now(JST).strftime('%Y-%m-%d %H:%M JST')}",
        "実行フロー: テクノロジーリサーチャー → レポーター",
        "",
        f"収集論文: {len(research.get('raw', []))}件 / 分析済み: {len(research.get('analyzed', []))}件",
        "",
        "## 今週の注目論文TOP5（投資インパクト順）",
    ]

    if top.empty:
        lines.append("- 該当論文なし")
    else:
        for i, row in enumerate(top.itertuples(index=False), start=1):
            title = _short(getattr(row, "title", ""), 120)
            score = _fmt_num(getattr(row, "impact_score", 0.0))
            theme = str(getattr(row, "tech_theme", "") or "-")
            tickers = str(getattr(row, "related_tickers", "") or "-")
            summary = _short(getattr(row, "breakthrough_summary", "") or getattr(row, "summary", ""), 110)
            lines.append(f"{i}. [{theme}] impact {score}: {title}")
            lines.append(f"   - 技術要点: {summary}")
            lines.append(f"   - 関連銘柄: {tickers}")

    lines.extend(["", "## 技術領域別ハイプサイクル更新（前週比）"])
    if hype_summary.empty:
        lines.append("- ハイプサイクルデータなし")
    else:
        for row in hype_summary.sort_values("tech_theme").itertuples(index=False):
            label = _change_label(getattr(row, "delta", 0.0), str(getattr(row, "phase", "")), str(getattr(row, "prev_phase", "")))
            lines.append(
                f"- {row.tech_theme}: {_fmt_num(row.prev_hype_index)} → {_fmt_num(row.hype_index)} / {row.phase} / {label}"
            )

    lines.extend(["", "## 特許動向（今週の新規出願で注目すべきもの）"])
    if patent_yearly.empty:
        lines.append("- 特許データなし")
    else:
        latest_year = int(pd.to_numeric(patent_yearly["year"], errors="coerce").max())
        py = patent_yearly[patent_yearly["year"].astype(int).eq(latest_year)].copy()
        py["yoy_growth_pct"] = pd.to_numeric(py["yoy_growth_pct"], errors="coerce").fillna(0.0)
        for row in py.sort_values("yoy_growth_pct", ascending=False).head(5).itertuples(index=False):
            lines.append(
                f"- {row.tech_theme}: {latest_year}年 {int(row.patent_count)}件 / 前年比 {_fmt_num(row.yoy_growth_pct)}%"
            )
    if not patent_stats.empty:
        top_company = patent_stats.sort_values("patent_count", ascending=False).head(5)
        lines.append("- 注目企業: " + ", ".join(f"{r.company}({int(r.patent_count)}件)" for r in top_company.itertuples(index=False)))

    lines.extend(["", "## テクノロジーレーダー更新（分類変更）"])
    lines.extend(_radar_change_rows(radar, hype_summary))

    lines.extend(["", "## 関連銘柄への影響評価"])
    lines.extend(_related_stock_impact(top))

    lines.extend(["", "## 今週のアクションアイテム"])
    lines.extend(_action_items(top, radar, hype_summary))

    lines.extend([
        "",
        "## 免責事項",
        "このレポートは教育目的の自動分析であり、投資助言ではありません。最終判断はご自身で行ってください。",
    ])
    return "\n".join(lines)


def _tech_webhook_url() -> str:
    return (os.getenv("DISCORD_TECH_WEBHOOK_URL") or os.getenv("DISCORD_WEBHOOK_URL") or "").strip()


def run_weekly_tech_report() -> dict[str, object]:
    research = _technology_researcher_stage()
    body = _reporter_stage(research)
    save_weekly_report("週次テクノロジーレポート", body)
    discord_result = send_discord_message(body, severity="normal", webhook_url=_tech_webhook_url())
    return {
        "papers_collected": int(len(research.get("raw", []))),
        "papers_analyzed": int(len(research.get("analyzed", []))),
        "papers_saved": int(research.get("saved_papers", 0) or 0),
        "hype_rows_saved": int(research.get("saved_hype", 0) or 0),
        "patent_rows_saved": int(research.get("saved_patents", 0) or 0),
        "discord": discord_result,
    }


if __name__ == "__main__":
    print(run_weekly_tech_report())

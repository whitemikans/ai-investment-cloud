from __future__ import annotations

import json
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET

import pandas as pd
import requests

from db.tech_research_utils import save_tech_papers


try:
    from crewai.tools import tool
except Exception:
    def tool(_name: str):  # type: ignore
        def deco(func):
            return func
        return deco


ARXIV_API = "http://export.arxiv.org/api/query"

THEME_QUERIES = {
    "AI": 'cat:cs.AI OR cat:cs.LG OR all:"large language model"',
    "Quantum": 'cat:quant-ph OR all:"quantum computing"',
    "Biotech": 'cat:q-bio OR all:"gene therapy" OR all:"drug discovery"',
    "Space": 'all:"satellite" OR all:"space technology" OR all:"reusable rocket"',
    "Energy": 'all:"solid-state battery" OR all:"perovskite" OR all:"fusion energy"',
    "Robotics": 'cat:cs.RO OR all:"humanoid robot" OR all:"autonomous robot"',
}


def _parse_authors(entry: ET.Element, ns: dict[str, str]) -> str:
    names: list[str] = []
    for a in entry.findall("atom:author", ns):
        n = (a.findtext("atom:name", default="", namespaces=ns) or "").strip()
        if n:
            names.append(n)
    return ", ".join(names[:8])


def _entry_to_row(entry: ET.Element, theme: str, ns: dict[str, str]) -> dict:
    title = (entry.findtext("atom:title", default="", namespaces=ns) or "").replace("\n", " ").strip()
    summary = (entry.findtext("atom:summary", default="", namespaces=ns) or "").replace("\n", " ").strip()
    published = (entry.findtext("atom:published", default="", namespaces=ns) or "").strip()
    categories = [c.attrib.get("term", "") for c in entry.findall("atom:category", ns)]
    pdf_url = ""
    source_url = (entry.findtext("atom:id", default="", namespaces=ns) or "").strip()
    for link in entry.findall("atom:link", ns):
        if link.attrib.get("title") == "pdf":
            pdf_url = link.attrib.get("href", "")
            break

    return {
        "title": title,
        "authors": _parse_authors(entry, ns),
        "summary": summary,
        "categories": ",".join([c for c in categories if c]),
        "published_at": published,
        "pdf_url": pdf_url,
        "source_url": source_url,
        "tech_theme": theme,
        "impact_score": None,
        "related_tickers": "",
        "recommendation": "",
    }


def collect_arxiv_papers(max_results_per_theme: int = 10, days_back: int = 30) -> pd.DataFrame:
    rows: list[dict] = []
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    cutoff = datetime.utcnow() - timedelta(days=max(1, int(days_back)))

    for theme, query in THEME_QUERIES.items():
        params = {
            "search_query": query,
            "start": 0,
            "max_results": int(max_results_per_theme),
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }
        try:
            resp = requests.get(ARXIV_API, params=params, timeout=20)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
        except Exception:
            continue

        for entry in root.findall("atom:entry", ns):
            row = _entry_to_row(entry, theme, ns)
            try:
                p = pd.to_datetime(row["published_at"], errors="coerce")
                if pd.isna(p):
                    continue
                if p.to_pydatetime().replace(tzinfo=None) < cutoff:
                    continue
            except Exception:
                continue
            rows.append(row)

    if not rows:
        return pd.DataFrame(
            columns=[
                "title",
                "authors",
                "summary",
                "categories",
                "published_at",
                "pdf_url",
                "source_url",
                "tech_theme",
                "impact_score",
                "related_tickers",
                "recommendation",
            ]
        )
    df = pd.DataFrame(rows).drop_duplicates(subset=["title", "published_at"]).reset_index(drop=True)
    return df


@tool("arXiv論文収集")
def collect_arxiv_papers_tool(max_results_per_theme: str = "10") -> str:
    n = int(float(max_results_per_theme or 10))
    df = collect_arxiv_papers(max_results_per_theme=max(3, min(25, n)))
    inserted = save_tech_papers(df)
    return json.dumps({"rows": int(len(df)), "inserted": int(inserted)}, ensure_ascii=False)


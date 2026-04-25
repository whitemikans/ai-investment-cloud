from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import networkx as nx
import pandas as pd
import plotly.graph_objects as go


COUNTRY_MAP = {
    "IBM": "US",
    "Google": "US",
    "Microsoft": "US",
    "Intel": "US",
    "Apple": "US",
    "Commonwealth Fusion": "US",
    "TAE Technologies": "US",
    "Boston Dynamics": "US",
    "テスラ": "US",
    "東芝": "JP",
    "NEC": "JP",
    "パナソニック": "JP",
    "ファナック": "JP",
    "京都フュージョニアリング": "JP",
    "三星SDI": "CN",
    "CATL": "CN",
}

COUNTRY_COLOR = {
    "JP": "#ef4444",   # red
    "US": "#3b82f6",   # blue
    "CN": "#eab308",   # yellow
    "EU": "#22c55e",   # green
    "Other": "#94a3b8",
}


@dataclass
class NetworkArtifacts:
    graph: nx.Graph
    centrality_df: pd.DataFrame
    clusters_df: pd.DataFrame
    hubs_df: pd.DataFrame


def _country_of(company: str) -> str:
    c = COUNTRY_MAP.get(str(company), "Other")
    return c if c in COUNTRY_COLOR else "Other"


def build_mock_citation_edges(patent_stats: pd.DataFrame) -> pd.DataFrame:
    """Create mock citation edges (source -> target) with weighted strengths."""
    if patent_stats is None or patent_stats.empty:
        return pd.DataFrame(columns=["source", "target", "citations"])

    work = (
        patent_stats[["tech_theme", "company", "patent_count"]]
        .copy()
        .sort_values(["tech_theme", "patent_count"], ascending=[True, False])
    )
    rows: list[dict[str, Any]] = []

    for _, g in work.groupby("tech_theme"):
        companies = g["company"].tolist()
        counts = g["patent_count"].astype(float).tolist()
        n = len(companies)
        if n < 2:
            continue
        # Dense-but-weighted mock graph inside each theme.
        for i in range(n):
            for j in range(i + 1, n):
                c1, c2 = str(companies[i]), str(companies[j])
                p1, p2 = float(counts[i]), float(counts[j])
                base = max(1.0, (min(p1, p2) ** 0.5) / 2.2)
                dist_penalty = 1.0 / (1.0 + abs(i - j))
                w = int(round(base * (1.8 + 4.0 * dist_penalty)))
                rows.append({"source": c1, "target": c2, "citations": max(1, w)})

    if not rows:
        return pd.DataFrame(columns=["source", "target", "citations"])
    return pd.DataFrame(rows)


def analyze_patent_citation_network(patent_stats: pd.DataFrame) -> NetworkArtifacts:
    """Analyze mock citation network with centrality, clustering, and hubs."""
    if patent_stats is None or patent_stats.empty:
        empty = pd.DataFrame()
        return NetworkArtifacts(nx.Graph(), empty, empty, empty)

    edges = build_mock_citation_edges(patent_stats)
    g = nx.Graph()

    node_df = patent_stats.groupby("company", as_index=False)["patent_count"].sum()
    for r in node_df.itertuples(index=False):
        company = str(getattr(r, "company"))
        pcount = int(getattr(r, "patent_count"))
        g.add_node(company, patent_count=pcount, country=_country_of(company))

    for r in edges.itertuples(index=False):
        s = str(getattr(r, "source"))
        t = str(getattr(r, "target"))
        w = int(getattr(r, "citations"))
        if s == t:
            continue
        if g.has_edge(s, t):
            g[s][t]["weight"] += w
        else:
            g.add_edge(s, t, weight=w)

    if g.number_of_nodes() == 0:
        empty = pd.DataFrame()
        return NetworkArtifacts(g, empty, empty, empty)

    deg_cent = nx.degree_centrality(g)
    bet_cent = nx.betweenness_centrality(g, weight="weight", normalized=True)
    eig_cent = nx.eigenvector_centrality(g, weight="weight", max_iter=1000)

    cent_rows = []
    for n in g.nodes():
        cent_rows.append(
            {
                "company": n,
                "country": g.nodes[n].get("country", "Other"),
                "patent_count": int(g.nodes[n].get("patent_count", 0)),
                "degree_centrality": float(deg_cent.get(n, 0.0)),
                "betweenness_centrality": float(bet_cent.get(n, 0.0)),
                "eigenvector_centrality": float(eig_cent.get(n, 0.0)),
            }
        )
    centrality_df = pd.DataFrame(cent_rows).sort_values("degree_centrality", ascending=False).reset_index(drop=True)

    communities = list(nx.algorithms.community.greedy_modularity_communities(g, weight="weight"))
    cluster_rows = []
    for idx, cset in enumerate(communities, start=1):
        for c in sorted(cset):
            cluster_rows.append({"company": c, "cluster_id": idx})
    clusters_df = pd.DataFrame(cluster_rows)

    merged = centrality_df.merge(clusters_df, on="company", how="left")
    topn = max(1, int(round(len(merged) * 0.2)))
    hubs_df = merged.sort_values("degree_centrality", ascending=False).head(topn).copy()
    hubs_df["is_hub"] = 1

    return NetworkArtifacts(g, merged, clusters_df, hubs_df)


def build_patent_citation_network_figure(patent_stats: pd.DataFrame) -> tuple[go.Figure, pd.DataFrame, pd.DataFrame]:
    """Build interactive Plotly + NetworkX network figure and analysis tables."""
    art = analyze_patent_citation_network(patent_stats)
    g = art.graph
    if g.number_of_nodes() == 0:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="特許引用ネットワーク", height=520)
        return fig, pd.DataFrame(), pd.DataFrame()

    pos = nx.spring_layout(g, seed=42, weight="weight", k=1.1)

    # Edges
    edge_traces = []
    max_w = max((float(g[u][v].get("weight", 1.0)) for u, v in g.edges()), default=1.0)
    for u, v in g.edges():
        x0, y0 = pos[u]
        x1, y1 = pos[v]
        w = float(g[u][v].get("weight", 1.0))
        width = 0.8 + (w / max_w) * 4.8
        edge_traces.append(
            go.Scatter(
                x=[x0, x1],
                y=[y0, y1],
                mode="lines",
                line={"width": width, "color": "rgba(148,163,184,0.45)"},
                hoverinfo="text",
                text=[f"{u} ↔ {v}<br>引用強度: {int(w)}", f"{u} ↔ {v}<br>引用強度: {int(w)}"],
                showlegend=False,
            )
        )

    cent = art.centrality_df.copy()
    hub_set = set(art.hubs_df["company"].tolist()) if not art.hubs_df.empty else set()

    # Nodes per country color
    node_traces = []
    for country in ["JP", "US", "CN", "EU", "Other"]:
        sub = cent[cent["country"] == country]
        if sub.empty:
            continue
        xs, ys, txt, size = [], [], [], []
        for r in sub.itertuples(index=False):
            c = str(getattr(r, "company"))
            x, y = pos[c]
            xs.append(x)
            ys.append(y)
            pcount = int(getattr(r, "patent_count"))
            dcent = float(getattr(r, "degree_centrality"))
            bcent = float(getattr(r, "betweenness_centrality"))
            label = " (Hub)" if c in hub_set else ""
            txt.append(
                f"{c}{label}<br>国籍={country}<br>特許数={pcount}<br>"
                f"Degree={dcent:.3f}<br>Betweenness={bcent:.3f}"
            )
            size.append(10 + (pcount ** 0.5) * 1.8)
        node_traces.append(
            go.Scatter(
                x=xs,
                y=ys,
                mode="markers+text",
                text=sub["company"],
                textposition="top center",
                hoverinfo="text",
                hovertext=txt,
                marker={
                    "size": size,
                    "color": COUNTRY_COLOR[country],
                    "line": {"width": 1, "color": "#0f172a"},
                },
                name={"JP": "日本", "US": "米国", "CN": "中国", "EU": "欧州", "Other": "その他"}[country],
            )
        )

    fig = go.Figure(data=edge_traces + node_traces)
    fig.update_layout(
        template="plotly_dark",
        title="特許引用ネットワーク（模擬データ）",
        height=560,
        xaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
        yaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
        margin={"l": 10, "r": 10, "t": 55, "b": 10},
        legend={"orientation": "h", "y": 1.02, "x": 0},
    )

    centrality_table = cent.sort_values("degree_centrality", ascending=False).reset_index(drop=True)
    cluster_table = art.clusters_df.merge(cent[["company", "country"]], on="company", how="left")
    return fig, centrality_table, cluster_table


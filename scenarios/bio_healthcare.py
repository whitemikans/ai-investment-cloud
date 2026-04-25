from __future__ import annotations

import pandas as pd


def get_bio_subthemes() -> pd.DataFrame:
    """Three bio-healthcare investment subthemes."""
    return pd.DataFrame(
        [
            {
                "subtheme": "AI創薬（AI-driven Drug Discovery）",
                "thesis": "従来10年の新薬開発を2-3年へ短縮できる可能性",
                "companies": "Recursion(RXRX), Exscientia, 中外製薬(4519)",
            },
            {
                "subtheme": "遺伝子治療・mRNA",
                "thesis": "CRISPR遺伝子編集の治療応用が拡大",
                "companies": "Moderna(MRNA), CRISPR Therapeutics(CRSP), 第一三共(4568)",
            },
            {
                "subtheme": "デジタルヘルス・遠隔医療",
                "thesis": "ウェアラブル + AI診断で早期検知・予防医療が進む",
                "companies": "テルモ(4543), オムロン(6645), Apple(AAPL)",
            },
        ]
    )


def get_bio_pipeline_mock() -> pd.DataFrame:
    """Mock pipeline data. Phase-3 weighted heavily for commercialization proximity."""
    return pd.DataFrame(
        [
            {"company": "RXRX", "subtheme": "AI創薬", "phase1_count": 4, "phase2_count": 3, "phase3_count": 1},
            {"company": "Exscientia", "subtheme": "AI創薬", "phase1_count": 5, "phase2_count": 2, "phase3_count": 0},
            {"company": "4519", "subtheme": "AI創薬", "phase1_count": 6, "phase2_count": 4, "phase3_count": 2},
            {"company": "MRNA", "subtheme": "遺伝子治療mRNA", "phase1_count": 8, "phase2_count": 6, "phase3_count": 3},
            {"company": "CRSP", "subtheme": "遺伝子治療mRNA", "phase1_count": 5, "phase2_count": 3, "phase3_count": 2},
            {"company": "4568", "subtheme": "遺伝子治療mRNA", "phase1_count": 7, "phase2_count": 5, "phase3_count": 2},
            {"company": "4543", "subtheme": "デジタルヘルス遠隔医療", "phase1_count": 3, "phase2_count": 2, "phase3_count": 1},
            {"company": "6645", "subtheme": "デジタルヘルス遠隔医療", "phase1_count": 4, "phase2_count": 2, "phase3_count": 1},
            {"company": "AAPL", "subtheme": "デジタルヘルス遠隔医療", "phase1_count": 2, "phase2_count": 2, "phase3_count": 1},
        ]
    )


def evaluate_bio_pipeline() -> pd.DataFrame:
    """Evaluate future potential using pipeline volume and progression depth.

    Rule:
    - Phase3 is nearest to commercialization, so weighted highest.
    - Score = P1*1 + P2*2 + P3*4
    """
    df = get_bio_pipeline_mock().copy()
    df["pipeline_score"] = (
        df["phase1_count"] * 1.0 + df["phase2_count"] * 2.0 + df["phase3_count"] * 4.0
    )
    df["phase3_flag"] = (df["phase3_count"] > 0).astype(int)
    df["commercialization_view"] = df["phase3_flag"].map({1: "商用化近い", 0: "中長期"})
    out = df.sort_values(["subtheme", "pipeline_score"], ascending=[True, False]).reset_index(drop=True)
    return out


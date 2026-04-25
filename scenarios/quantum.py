from __future__ import annotations

import pandas as pd


def get_quantum_milestones() -> pd.DataFrame:
    """Technology milestones and investment triggers for quantum computing."""
    return pd.DataFrame(
        [
            {
                "milestone": "論理量子ビット1,000個の達成",
                "expected_window": "2027-2030",
                "trigger": "確認されたら本格投資開始",
                "investment_judgement": "強気へシフト",
            },
            {
                "milestone": "量子超越性の実用的証明",
                "expected_window": "2026-2032",
                "trigger": "特定計算で古典計算を継続的に上回る",
                "investment_judgement": "関連ETF（QTUM等）を段階導入",
            },
            {
                "milestone": "量子暗号通信の商用化",
                "expected_window": "2028-2032",
                "trigger": "商用案件の継続受注が可視化",
                "investment_judgement": "日本企業（東芝・NEC）比率を引き上げ",
            },
        ]
    )


def get_quantum_investment_universe() -> pd.DataFrame:
    """Investable names by region/type for quantum scenario analysis."""
    return pd.DataFrame(
        [
            {"region": "米国", "asset_type": "個別株", "name": "IonQ", "ticker": "IONQ", "theme": "量子ハードウェア/サービス"},
            {"region": "米国", "asset_type": "個別株", "name": "Rigetti", "ticker": "RGTI", "theme": "超伝導量子"},
            {"region": "米国", "asset_type": "個別株", "name": "IBM", "ticker": "IBM", "theme": "量子エンタープライズ"},
            {"region": "日本", "asset_type": "個別株", "name": "東芝", "ticker": "6502", "theme": "量子暗号通信"},
            {"region": "日本", "asset_type": "個別株", "name": "NEC", "ticker": "6701", "theme": "量子 + AI"},
            {"region": "日本", "asset_type": "個別株", "name": "富士通", "ticker": "6702", "theme": "量子インフラ"},
            {"region": "米国", "asset_type": "ETF", "name": "Defiance Quantum ETF", "ticker": "QTUM", "theme": "量子テーマ分散"},
        ]
    )


def get_quantum_risks() -> pd.DataFrame:
    """Major investment risks for quantum computing theme."""
    return pd.DataFrame(
        [
            {
                "risk": "技術的失敗リスク",
                "description": "量子デコヒーレンス問題が解決せず実用性能が伸びない",
                "monitoring_point": "エラー訂正・論理量子ビット進捗",
            },
            {
                "risk": "代替技術リスク",
                "description": "古典コンピュータ/近似アルゴリズム進化で量子優位性が縮小",
                "monitoring_point": "古典HPC性能とコスト曲線",
            },
            {
                "risk": "長期商用化リスク",
                "description": "収益化まで10年以上かかり、資金繰り負担が増える",
                "monitoring_point": "受注残・資金調達・提携状況",
            },
        ]
    )


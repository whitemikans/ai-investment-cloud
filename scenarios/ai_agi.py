from __future__ import annotations

import pandas as pd


def get_ai_agi_market_scenarios() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"scenario": "楽観", "market_2040_usd_t": 3.0, "cagr_pct": 21.0, "agi_year": 2030},
            {"scenario": "標準", "market_2040_usd_t": 2.7, "cagr_pct": 20.0, "agi_year": 2035},
            {"scenario": "悲観", "market_2040_usd_t": 1.5, "cagr_pct": 15.0, "agi_year": None},
        ]
    )


def get_ai_agi_layers() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"layer": "インフラ層", "tickers": "NVDA, AMD, 8035.T", "risk": "低", "strategy": "ツルハシ戦略"},
            {"layer": "モデル層", "tickers": "GOOGL, MSFT, META", "risk": "中", "strategy": "競争優位の継続確認"},
            {"layer": "アプリ層", "tickers": "CRM, NOW, 6098.T", "risk": "高", "strategy": "選別投資"},
            {"layer": "データ層", "tickers": "SNOW, MDB", "risk": "中", "strategy": "長期監視"},
        ]
    )


def get_theme_stock_table() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"theme": "AI", "ticker": "NVDA", "company": "NVIDIA", "per": 49.0, "patent_count": 1240, "judgement": "Invest"},
            {"theme": "Quantum", "ticker": "IONQ", "company": "IonQ", "per": 0.0, "patent_count": 240, "judgement": "Watch"},
            {"theme": "Biotech", "ticker": "MRNA", "company": "Moderna", "per": 18.0, "patent_count": 620, "judgement": "Watch"},
            {"theme": "Space", "ticker": "RKLB", "company": "Rocket Lab", "per": 0.0, "patent_count": 210, "judgement": "Research"},
            {"theme": "Energy", "ticker": "7203.T", "company": "Toyota", "per": 12.0, "patent_count": 1350, "judgement": "Invest"},
            {"theme": "Robotics", "ticker": "TSLA", "company": "Tesla", "per": 61.0, "patent_count": 560, "judgement": "Watch"},
        ]
    )


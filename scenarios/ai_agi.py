from __future__ import annotations

import pandas as pd


def get_ai_agi_market_scenarios() -> pd.DataFrame:
    """AI/AGI market scenarios: optimistic / base / pessimistic."""
    return pd.DataFrame(
        [
            {
                "scenario": "楽観",
                "agi_timing": "AGIが2030年に実現",
                "market_usd_t": 3.0,
                "agi_year": 2030,
                "note": "汎用AIが主要産業へ急速浸透",
            },
            {
                "scenario": "標準",
                "agi_timing": "AGIは2035年",
                "market_usd_t": 2.7,
                "agi_year": 2035,
                "note": "段階的に企業導入が進む",
            },
            {
                "scenario": "悲観",
                "agi_timing": "AGI実現せず（特化型AI中心）",
                "market_usd_t": 1.5,
                "agi_year": None,
                "note": "ユースケース限定で成長",
            },
        ]
    )


def get_ai_agi_layers() -> pd.DataFrame:
    """Layer-based investable map."""
    return pd.DataFrame(
        [
            {
                "layer": "インフラ層",
                "focus": "GPU/チップ",
                "companies": "NVIDIA, AMD, 東京エレクトロン(8035.T)",
                "risk": "中",
                "strategy": "設備投資サイクルを確認しつつ分散",
            },
            {
                "layer": "モデル層",
                "focus": "LLMプロバイダー",
                "companies": "Google, OpenAI/Microsoft, Meta",
                "risk": "中",
                "strategy": "規制対応力と推論コスト競争力を重視",
            },
            {
                "layer": "アプリ層",
                "focus": "AI活用企業",
                "companies": "Salesforce, ServiceNow, リクルート",
                "risk": "中高",
                "strategy": "業務浸透率と継続課金の質を評価",
            },
            {
                "layer": "データ層",
                "focus": "データプロバイダー",
                "companies": "Snowflake, MongoDB",
                "risk": "中",
                "strategy": "データ基盤ロックインを重視",
            },
        ]
    )


def get_japan_ai_related_stocks() -> pd.DataFrame:
    """Japanese AI-related stock list (including one unlisted candidate)."""
    return pd.DataFrame(
        [
            {"name": "ソフトバンクG", "code": "9984", "category": "AI投資持株会社", "listed": "上場"},
            {"name": "NEC", "code": "6701", "category": "量子 + AI", "listed": "上場"},
            {"name": "富士通", "code": "6702", "category": "AIインフラ", "listed": "上場"},
            {"name": "PKSHA Technology", "code": "3993", "category": "国内AIスタートアップ", "listed": "上場"},
            {"name": "Preferred Networks", "code": "-", "category": "非上場AI企業（IPO期待）", "listed": "非上場"},
        ]
    )


def get_ai_investment_milestones() -> pd.DataFrame:
    """Key milestones investors should monitor."""
    return pd.DataFrame(
        [
            {
                "milestone": "Google/DeepMindがAGI定義を満たすモデルを発表",
                "signal": "技術ブレイクスルー",
                "impact": "モデル層・インフラ層が再評価される可能性",
            },
            {
                "milestone": "AI規制法案の可決",
                "signal": "規制イベント",
                "impact": "参入障壁上昇で既存大企業優位",
            },
            {
                "milestone": "日本政府のAIインフラ投資予算確定",
                "signal": "政策イベント",
                "impact": "国内インフラ/実装企業の需要拡大",
            },
        ]
    )


def get_theme_stock_table() -> pd.DataFrame:
    """Theme stock table used by the technology research page."""
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


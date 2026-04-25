from __future__ import annotations

import pandas as pd


def get_nextgen_energy_subthemes() -> pd.DataFrame:
    """Next-generation energy investment scenarios by subtheme."""
    return pd.DataFrame(
        [
            {
                "subtheme": "核融合",
                "thesis": "究極のクリーンエネルギー。実用化は2035-2040年が視野",
                "companies": "Commonwealth Fusion Systems, TAE Technologies, 京都フュージョニアリング(非上場), 浜松ホトニクス(6965)",
                "judgement": "🟡 Research",
                "reason": "技術リスクは大きいが、実現時インパクトは桁違い",
                "window": "2035-2040",
            },
            {
                "subtheme": "ペロブスカイト太陽電池",
                "thesis": "シリコンより安価・軽量・柔軟。日本が先行",
                "companies": "積水化学(4204), パナソニック(6752), エネコートテクノロジーズ",
                "judgement": "🟢 Invest",
                "reason": "日本発の世界技術。2028年の量産開始が追い風",
                "window": "2028-",
            },
            {
                "subtheme": "全固体電池",
                "thesis": "EV航続距離2倍、充電時間1/3の可能性",
                "companies": "Apple(7203), 村田製作所(6981), 出光興産(5019)",
                "judgement": "🟢 Invest",
                "reason": "2027-2028年の量産開始が主要カタリスト",
                "window": "2027-2028",
            },
        ]
    )


def get_nextgen_energy_milestones() -> pd.DataFrame:
    """Operational milestones to monitor for each subtheme."""
    return pd.DataFrame(
        [
            {"subtheme": "核融合", "milestone": "ネットエネルギー利得(Q>1)の再現性", "trigger": "商用炉設計の実証開始"},
            {"subtheme": "核融合", "milestone": "主要プレイヤーの大型資金調達", "trigger": "実装ロードマップの前倒し"},
            {"subtheme": "ペロブスカイト太陽電池", "milestone": "量産ライン立ち上げ", "trigger": "設備投資の収益化局面へ移行"},
            {"subtheme": "ペロブスカイト太陽電池", "milestone": "耐久性・劣化率の標準達成", "trigger": "商用採用拡大"},
            {"subtheme": "全固体電池", "milestone": "量産セルの歩留まり改善", "trigger": "自動車向け搭載が本格化"},
            {"subtheme": "全固体電池", "milestone": "EVメーカーの量産採用契約", "trigger": "需給見通し上方修正"},
        ]
    )


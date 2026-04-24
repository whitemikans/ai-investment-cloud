from __future__ import annotations

import json
import os
from datetime import datetime

from config import get_setting


def _fmt_pct(v: float) -> str:
    try:
        return f"{float(v) * 100:.2f}%"
    except Exception:
        return "0.00%"


def _local_fallback_report(portfolio_data: dict) -> str:
    """Build a simple rule-based diagnosis when Gemini is unavailable."""
    current = portfolio_data.get("current_weights", {}) or {}
    recommended = portfolio_data.get("recommended_weights", {}) or {}
    adjusted = portfolio_data.get("market_adjusted_weights", {}) or {}
    tickers = list((portfolio_data.get("tickers", []) or []))
    normal_stats = portfolio_data.get("normal_opt_stats", {}) or {}
    adjusted_stats = portfolio_data.get("market_adjusted_stats", {}) or {}

    diffs = []
    for t in tickers:
        cur = float(current.get(t, 0.0))
        rec = float(recommended.get(t, 0.0))
        adj = float(adjusted.get(t, rec))
        diffs.append((t, cur, rec, adj, adj - rec))
    diffs_sorted = sorted(diffs, key=lambda x: abs(x[4]), reverse=True)

    score = 72
    if diffs_sorted:
        max_dev = abs(diffs_sorted[0][4])
        if max_dev <= 0.01:
            score = 84
        elif max_dev <= 0.03:
            score = 78
        elif max_dev <= 0.06:
            score = 72
        else:
            score = 66

    lines = [
        "🤖 AI診断（ローカル代替レポート）",
        "",
        f"📊 総合スコア: {score} / 100",
        "Gemini APIが利用できないため、ルールベース診断を表示しています。",
        "",
        "💪 強み:",
        "1. 最適化（最小分散・最大シャープ）による配分比較ができています。",
        "2. 市場環境シグナルを配分調整に反映できています。",
        "3. リバランス閾値を用いた運用ルールが明確です。",
        "",
        "⚠️ 弱み・リスク:",
        "1. 市場急変時は相関構造が変わるため、過去データ依存に注意が必要です。",
        "2. 乖離が大きい銘柄は想定以上の下振れ要因になります。",
        "3. ニュース件数が少ないセクターはセンチメント推定が不安定です。",
        "",
        "📋 市場環境を考慮した配分調整提案:",
    ]

    for i, (t, cur, rec, adj, d) in enumerate(diffs_sorted[:5], start=1):
        direction = "増やす" if d > 0 else "減らす"
        lines.append(
            f"{i}. {t}: 通常最適 {_fmt_pct(rec)} → 市場調整後 {_fmt_pct(adj)} "
            f"（{direction} {_fmt_pct(abs(d))} / 現在 {_fmt_pct(cur)}）"
        )

    if normal_stats and adjusted_stats:
        lines.extend(
            [
                "",
                "📈 効果見込み（通常最適 vs 市場調整後）:",
                f"- リターン: {_fmt_pct(normal_stats.get('return', 0.0))} → {_fmt_pct(adjusted_stats.get('return', 0.0))}",
                f"- リスク: {_fmt_pct(normal_stats.get('risk', 0.0))} → {_fmt_pct(adjusted_stats.get('risk', 0.0))}",
                f"- シャープ: {normal_stats.get('sharpe', 0.0):.2f} → {adjusted_stats.get('sharpe', 0.0):.2f}",
            ]
        )

    lines.extend(
        [
            "",
            "※本内容は一般的な情報提供であり、投資判断は自己責任で行ってください。",
        ]
    )
    return "\n".join(lines)


def _build_prompt(portfolio_data: dict) -> tuple[str, str]:
    system_prompt = (
        "あなたはCFA資格を持つ日本のファイナンシャルアドバイザーです。"
        "個人投資家向けに分かりやすく、具体的なアドバイスを日本語で提供してください。"
        "専門用語には短い補足を付け、断定的な売買指示は避けてください。"
        "最後に一般的情報であり投資判断は自己責任である旨を明記してください。"
    )

    user_prompt = f"""以下のポートフォリオデータを分析し、診断レポートを作成してください。

生成日時: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

【入力データ(JSON)】
{json.dumps(portfolio_data, ensure_ascii=False, indent=2)}

以下の構成で出力してください:
1. 総合スコア（100点満点）と一言
2. 現在のポートフォリオの強み（3点）
3. 現在のポートフォリオの弱み・リスク（3点）
4. 市場環境を踏まえた具体的な改善提案（3〜5件、優先度順）
5. 「通常時の最適配分」と「市場環境調整済み配分」の比較コメント
6. 今後3か月の推奨アクション

注意:
- セクター別ニュースセンチメントとキーワード頻度を考慮する
- S&P500/VIX/米10年債/USDJPY のシグナルを考慮する
- 最後に「投資判断は自己責任」である旨を記載する
"""
    return system_prompt, user_prompt


def generate_portfolio_diagnosis(portfolio_data: dict) -> str:
    """Generate Japanese portfolio diagnosis text with Gemini."""
    api_key = (get_setting("GEMINI_API_KEY", "") or os.getenv("GEMINI_API_KEY", "")).strip()
    if not api_key:
        return "GEMINI_API_KEY が未設定のため、AI診断を実行できません。"

    try:
        from google import genai
        from google.genai import types
    except Exception as exc:
        return f"google-genai が利用できません: {exc}"

    system_prompt, user_prompt = _build_prompt(portfolio_data)

    try:
        model_name = (get_setting("GEMINI_MODEL", "gemini-2.0-flash") or "gemini-2.0-flash").strip()
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=model_name,
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=0.3,
            ),
        )
        return (response.text or "").strip() or "AI診断の応答が空でした。"
    except Exception as exc:
        msg = str(exc)
        if "RESOURCE_EXHAUSTED" in msg or "429" in msg:
            return _local_fallback_report(portfolio_data)
        if "503" in msg or "UNAVAILABLE" in msg:
            return (
                "Gemini側が混雑中です（503 / UNAVAILABLE）。\n"
                "対処: 1) 1〜2分待って再実行 2) 連打を避ける 3) 必要ならモデルを切替"
            )
        if "404" in msg or "NOT_FOUND" in msg:
            return (
                "指定モデルが見つかりません（404 / NOT_FOUND）。\n"
                "対処: secrets.toml の GEMINI_MODEL を見直してください。"
            )
        if "401" in msg or "403" in msg or "API_KEY_INVALID" in msg or "invalid api key" in msg.lower():
            return (
                "APIキーが無効、または権限不足です（401/403）。\n"
                "対処: GEMINI_API_KEY を再発行して設定し直してください。"
            )
        if "10013" in msg or "アクセス許可" in msg:
            return (
                "ネットワーク接続がブロックされています。\n"
                "対処: ファイアウォール/プロキシ/VPN設定を確認してください。"
            )
        return f"AI診断の実行中にエラーが発生しました: {exc}"

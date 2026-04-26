from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

from config import get_setting


def _json_safe(value: Any) -> Any:
    """Convert pandas/numpy/NaN values into JSON-serializable plain Python values."""
    try:
        import math

        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
    except Exception:
        pass
    try:
        import numpy as np

        if isinstance(value, np.integer):
            return int(value)
        if isinstance(value, np.floating):
            v = float(value)
            return None if not np.isfinite(v) else v
        if isinstance(value, np.bool_):
            return bool(value)
    except Exception:
        pass
    try:
        import pandas as pd

        if value is pd.NA or value is pd.NaT:
            return None
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, pd.Series):
            return [_json_safe(v) for v in value.tolist()]
        if isinstance(value, pd.DataFrame):
            return [_json_safe(r) for r in value.to_dict(orient="records")]
    except Exception:
        pass
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return value


def _local_advice(data: dict) -> str:
    profile = data.get("profile", {})
    mc = data.get("monte_carlo", {})
    prob = float(mc.get("fire_probability", 0.0))
    score = int(min(95, max(40, 50 + prob * 50)))

    lines = [
        "🤖 AIファイナンシャル診断（ローカル簡易版）",
        f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"① 現状評価: {score} / 100",
        f"FIRE達成確率は {prob * 100:.1f}% です。",
        "",
        "② 強み",
        "- 収支と資産を数値で管理できています。",
        "- FIRE確率をモンテカルロで確認できています。",
        "- ライフイベントを織り込んだ意思決定ができます。",
        "",
        "③ リスク・弱み",
        "- リターン前提が変わると達成年齢がずれる可能性があります。",
        "- 大型支出イベントの追加で確率が下がる可能性があります。",
        "- 生活費上振れ時の安全余裕資金が不足しやすいです。",
        "",
        "④ 改善提案",
        "1. 生活費6か月分の現金バッファを最優先で確保",
        "2. 年間投資額を増やす/固定費削減で貯蓄率を+5%改善",
        "3. 年1回、年金受給開始年齢を見直し",
        "4. 主要What-If(収入減/教育費増)を四半期ごとに再計算",
        "5. 新NISA枠の配分を年初に再最適化",
        "",
        "⑤ 今月やること",
        "- 固定費の見直し（通信・保険）",
        "- ライフイベントの更新",
        "- 次回リバランス条件の明文化",
        "",
        "⑥ 長期注意点",
        "- 税制・インフレ・為替前提は定期的に更新してください。",
        "",
        "※本内容は一般情報であり、最終判断はご自身で行ってください。",
    ]
    return "\n".join(lines)


def _build_prompt(data: dict) -> tuple[str, str]:
    system_prompt = (
        "あなたはCFP資格を持つ日本のFP専門家です。"
        "クライアントのシミュレーションデータを分析し、"
        "具体的で実行可能な助言を日本語で提供してください。"
        "法律・税務の断定的助言は避け、一般情報として記述してください。"
    )
    user_prompt = f"""
次のデータを分析して、以下の形式で日本語レポートを返してください。

【データJSON】
{json.dumps(_json_safe(data), ensure_ascii=False, indent=2, default=str)}

【出力形式】
① 現状評価（100点満点）
② 強み（3点）
③ リスク・弱み（3点）
④ 改善提案（優先度順5項目）
⑤ 今月やること（3つ）
⑥ 長期注意点

最後に「投資判断は自己責任」と明記してください。
"""
    return system_prompt, user_prompt


def generate_financial_advice(data: dict) -> str:
    data = _json_safe(data)
    api_key = (get_setting("GEMINI_API_KEY", "") or os.getenv("GEMINI_API_KEY", "")).strip()
    if not api_key:
        return "GEMINI_API_KEY が未設定のため、AI診断を実行できません。"

    try:
        from google import genai
        from google.genai import types
    except Exception as exc:
        return f"google-genai が利用できません: {exc}"

    model_name = (get_setting("GEMINI_MODEL", "gemini-2.5-flash") or "gemini-2.5-flash").strip()
    system_prompt, user_prompt = _build_prompt(data)

    try:
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=model_name,
            contents=user_prompt,
            config=types.GenerateContentConfig(system_instruction=system_prompt, temperature=0.3),
        )
        text = (response.text or "").strip()
        return text or "AI診断の応答が空でした。"
    except Exception as exc:
        msg = str(exc)
        if "429" in msg or "RESOURCE_EXHAUSTED" in msg:
            return "Gemini APIの利用上限に達しています（429 / RESOURCE_EXHAUSTED）。\n" + _local_advice(data)
        if "503" in msg or "UNAVAILABLE" in msg:
            return "Gemini APIが混雑中です（503 / UNAVAILABLE）。時間を置いて再実行してください。"
        if "404" in msg or "NOT_FOUND" in msg:
            return "指定モデルが見つかりません（404 / NOT_FOUND）。GEMINI_MODEL を確認してください。"
        if "401" in msg or "403" in msg or "API_KEY_INVALID" in msg:
            return "APIキーが無効です（401/403）。GEMINI_API_KEY を再設定してください。"
        return f"AI診断の実行中にエラーが発生しました: {exc}"


def generate_whatif_comparison_comment(compare_rows: list[dict]) -> str:
    """Generate short AI comment for what-if comparison table."""
    compare_rows = _json_safe(compare_rows)
    if not compare_rows:
        return "比較データがないためコメントを生成できません。"

    base_row = next((r for r in compare_rows if r.get("シナリオ") == "ベースケース"), compare_rows[0])
    base_prob = float(base_row.get("FIRE確率", 0.0))
    base_assets = float(base_row.get("90歳時資産", 0.0))

    ranked = sorted(compare_rows, key=lambda r: (float(r.get("FIRE確率", 0.0)), float(r.get("90歳時資産", 0.0))), reverse=True)
    best = ranked[0]

    local_comment = (
        f"最有力シナリオは「{best.get('シナリオ', '-') }」です。"
        f"FIRE確率 {float(best.get('FIRE確率', 0.0)) * 100:.1f}%、"
        f"90歳時資産 ¥{float(best.get('90歳時資産', 0.0)):,.0f}。"
        f"ベースケース比で確率 {((float(best.get('FIRE確率', 0.0)) - base_prob) * 100):+.1f}pt、"
        f"資産 {float(best.get('90歳時資産', 0.0) - base_assets):+,.0f} 円です。"
    )

    api_key = (get_setting("GEMINI_API_KEY", "") or os.getenv("GEMINI_API_KEY", "")).strip()
    if not api_key:
        return local_comment

    try:
        from google import genai
        from google.genai import types
    except Exception:
        return local_comment

    model_name = (get_setting("GEMINI_MODEL", "gemini-2.5-flash") or "gemini-2.5-flash").strip()
    system_prompt = (
        "あなたは日本のFPです。What-If比較表を基に、"
        "意思決定に使える短いコメントを日本語で3〜5行で出力してください。"
    )
    user_prompt = f"""
以下のWhat-If比較データを分析してコメントしてください。

{json.dumps(compare_rows, ensure_ascii=False, indent=2, default=str)}

出力要件:
- 最も改善効果が高いシナリオ
- 最も悪化するシナリオ
- 次に試すべき追加検証を1つ
- 最後に「投資判断は自己責任」と明記
"""
    try:
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=model_name,
            contents=user_prompt,
            config=types.GenerateContentConfig(system_instruction=system_prompt, temperature=0.3),
        )
        text = (response.text or "").strip()
        return text or local_comment
    except Exception:
        return local_comment

from __future__ import annotations

import json
import os
from datetime import datetime


def _build_prompt(portfolio_data: dict) -> tuple[str, str]:
    system_prompt = (
        "あなたはCFA資格を持つ日本のファイナンシャルアドバイザーです。"
        "個人投資家向けに分かりやすく、具体的なアドバイスを日本語で提供してください。"
        "専門用語には短い補足を付けてください。"
        "投資助言に該当しない一般的情報であることを明記してください。"
    )
    user_prompt = f"""以下のポートフォリオデータを分析し、診断レポートを作成してください。

生成日時: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

【ポートフォリオデータ】
{json.dumps(portfolio_data, ensure_ascii=False, indent=2)}

以下の構成で作成してください:
1. 総合スコア（100点満点）と一言
2. 強み（3点）
3. 弱み・リスク（3点）
4. 改善提案（優先度順で3〜5件）
5. 市場環境を踏まえた注意点
6. 今後3か月の推奨アクション

最後に「投資判断は自己責任」である旨を記載してください。"""
    return system_prompt, user_prompt


def generate_portfolio_diagnosis(portfolio_data: dict) -> str:
    """Generate Japanese portfolio diagnosis text with Gemini."""
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        return "GEMINI_API_KEY が未設定のため、AI診断を実行できません。"

    try:
        from google import genai
        from google.genai import types
    except Exception as exc:
        return f"google-genai が利用できません: {exc}"

    system_prompt, user_prompt = _build_prompt(portfolio_data)
    try:
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=0.3,
            ),
        )
        return (response.text or "").strip() or "AI診断の応答が空でした。"
    except Exception as exc:
        return f"AI診断の実行中にエラーが発生しました: {exc}"


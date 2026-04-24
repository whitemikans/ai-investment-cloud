from __future__ import annotations

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import get_setting


def classify_error(message: str) -> str:
    msg = (message or "").lower()
    if "resource_exhausted" in msg or "429" in msg:
        return "AUTH_OK_BUT_QUOTA_EXCEEDED"
    if "api_key_invalid" in msg or "invalid api key" in msg or "401" in msg or "403" in msg:
        return "KEY_INVALID_OR_FORBIDDEN"
    if "not_found" in msg or "404" in msg or "model" in msg and "not" in msg and "found" in msg:
        return "MODEL_NOT_FOUND"
    if "10013" in msg or "アクセス許可" in msg:
        return "NETWORK_BLOCKED"
    return "ERROR"


def main() -> int:
    api_key = (get_setting("GEMINI_API_KEY", "") or "").strip()
    model = (get_setting("GEMINI_MODEL", "gemini-2.0-flash") or "gemini-2.0-flash").strip()

    if not api_key:
        print("RESULT: NO_KEY")
        print("GEMINI_API_KEY が未設定です。")
        return 1

    try:
        from google import genai
        from google.genai import types
    except Exception as exc:
        print("RESULT: SDK_MISSING")
        print(f"google-genai が見つかりません: {exc}")
        return 1

    try:
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=model,
            contents="Ping",
            config=types.GenerateContentConfig(temperature=0),
        )
        text = (response.text or "").strip().replace("\n", " ")
        print("RESULT: OK")
        print(f"MODEL: {model}")
        print(f"TEXT: {text[:160]}")
        return 0
    except Exception as exc:
        msg = str(exc)
        result = classify_error(msg)
        print(f"RESULT: {result}")
        print(f"ERROR: {msg[:600]}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

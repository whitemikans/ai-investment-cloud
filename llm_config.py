from __future__ import annotations

import os
from pathlib import Path

try:
    import tomllib  # Python 3.11+
except Exception:  # pragma: no cover
    tomllib = None  # type: ignore


DEFAULT_MODEL_NAME = "gemini-2.5-flash"
PROJECT_ROOT = Path(__file__).resolve().parent
SECRETS_PATH = PROJECT_ROOT / ".streamlit" / "secrets.toml"


def _read_secrets_file() -> dict:
    if tomllib is None or not SECRETS_PATH.exists():
        return {}
    try:
        return tomllib.loads(SECRETS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def resolve_gemini_api_key() -> str:
    """GEMINI_API_KEYを env -> streamlit secrets -> .streamlit/secrets.toml の順で解決する。"""
    key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if key:
        return key

    try:
        import streamlit as st

        sec_key = str(st.secrets.get("GEMINI_API_KEY", "")).strip()
        if sec_key:
            return sec_key
    except Exception:
        pass

    sec = _read_secrets_file()
    return str(sec.get("GEMINI_API_KEY", "")).strip()


def resolve_model_name() -> str:
    """MODEL_NAMEを env -> streamlit secrets -> .streamlit/secrets.toml の順で解決する。"""
    name = (os.getenv("MODEL_NAME") or "").strip()
    if name:
        return name

    try:
        import streamlit as st

        sec_name = str(st.secrets.get("MODEL_NAME", "")).strip()
        if sec_name:
            return sec_name
    except Exception:
        pass

    sec = _read_secrets_file()
    return str(sec.get("MODEL_NAME", "")).strip() or DEFAULT_MODEL_NAME


def get_llm():
    """CrewAI用Gemini LLMを返す。"""
    from crewai import LLM

    api_key = resolve_gemini_api_key()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY が未設定です（env / secrets.toml を確認してください）")
    model_name = resolve_model_name()
    return LLM(
        model=f"gemini/{model_name}",
        api_key=api_key,
        temperature=0.3,
    )


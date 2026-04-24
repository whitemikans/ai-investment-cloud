from __future__ import annotations

import os
from pathlib import Path
import tomllib


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_SQLITE_URL = f"sqlite:///{(PROJECT_ROOT / 'investment.db').as_posix()}"


def _read_streamlit_secret(key: str) -> str | None:
    try:
        import streamlit as st  # lazy import

        if key in st.secrets:
            value = st.secrets[key]
            if value is None:
                return None
            return str(value).strip()
    except Exception:
        return None
    return None


def _read_local_secrets_file(key: str) -> str | None:
    secrets_path = PROJECT_ROOT / ".streamlit" / "secrets.toml"
    if not secrets_path.exists():
        return None
    try:
        raw = secrets_path.read_text(encoding="utf-8-sig")
        data = tomllib.loads(raw)
        if key in data and data[key] is not None:
            return str(data[key]).strip()
    except Exception:
        return None
    return None


def get_setting(key: str, default: str | None = None) -> str | None:
    # For Gemini settings, prefer Streamlit/local secrets over environment
    # to avoid stale shell environment variables overriding updated keys.
    if key in {"GEMINI_API_KEY", "GEMINI_MODEL"}:
        secret_value = _read_streamlit_secret(key)
        if secret_value is not None and secret_value != "":
            return secret_value
        file_secret_value = _read_local_secrets_file(key)
        if file_secret_value is not None and file_secret_value != "":
            return file_secret_value
        value = os.getenv(key)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
        return default

    value = os.getenv(key)
    if value is not None and str(value).strip() != "":
        return str(value).strip()
    secret_value = _read_streamlit_secret(key)
    if secret_value is not None and secret_value != "":
        return secret_value
    file_secret_value = _read_local_secrets_file(key)
    if file_secret_value is not None and file_secret_value != "":
        return file_secret_value
    return default


def is_streamlit_cloud() -> bool:
    sharing_mode = os.getenv("STREAMLIT_SHARING_MODE", "").strip().lower()
    if sharing_mode in {"streamlit-app", "sharing"}:
        return True
    explicit_flag = os.getenv("IS_STREAMLIT_CLOUD", "").strip().lower()
    if explicit_flag in {"1", "true", "yes", "on"}:
        return True
    return False


def get_runtime_name() -> str:
    return "cloud" if is_streamlit_cloud() else "local"


def get_database_url() -> str:
    if is_streamlit_cloud():
        raw = (
            get_setting("CLOUD_DATABASE_URL")
            or get_setting("DATABASE_URL")
            or DEFAULT_SQLITE_URL
        )
    else:
        raw = get_setting("LOCAL_DATABASE_URL", DEFAULT_SQLITE_URL) or DEFAULT_SQLITE_URL
    # Supabase/Heroku style URL compatibility for SQLAlchemy
    # e.g. postgres://... -> postgresql+psycopg2://...
    if raw.startswith("postgres://"):
        return raw.replace("postgres://", "postgresql+psycopg2://", 1)
    if raw.startswith("postgresql://"):
        return raw.replace("postgresql://", "postgresql+psycopg2://", 1)
    return raw


def get_app_password() -> str:
    raw = get_setting("APP_PASSWORD", "") or ""
    # Normalize accidental wrapping quotes from env/secrets, e.g. '"pass"' or "'pass'"
    if len(raw) >= 2 and ((raw[0] == '"' and raw[-1] == '"') or (raw[0] == "'" and raw[-1] == "'")):
        return raw[1:-1].strip()
    return raw.strip()

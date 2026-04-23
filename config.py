from __future__ import annotations

import os
from pathlib import Path


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


def get_setting(key: str, default: str | None = None) -> str | None:
    value = os.getenv(key)
    if value is not None and str(value).strip() != "":
        return str(value).strip()
    secret_value = _read_streamlit_secret(key)
    if secret_value is not None and secret_value != "":
        return secret_value
    return default


def get_database_url() -> str:
    raw = get_setting("DATABASE_URL", DEFAULT_SQLITE_URL) or DEFAULT_SQLITE_URL
    # Supabase/Heroku style URL compatibility for SQLAlchemy
    # e.g. postgres://... -> postgresql+psycopg://...
    if raw.startswith("postgres://"):
        return raw.replace("postgres://", "postgresql+psycopg://", 1)
    if raw.startswith("postgresql://"):
        return raw.replace("postgresql://", "postgresql+psycopg://", 1)
    return raw


def get_app_password() -> str:
    return get_setting("APP_PASSWORD", "admin123") or "admin123"

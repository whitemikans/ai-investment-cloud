from __future__ import annotations

import json
import os

import requests


try:
    from crewai.tools import tool
except Exception:
    def tool(_name: str):  # type: ignore
        def deco(func):
            return func
        return deco


def _split_message(text: str, max_len: int = 1800) -> list[str]:
    src = str(text or "").strip()
    if not src:
        return [""]
    chunks: list[str] = []
    while len(src) > max_len:
        cut = src.rfind("\n", 0, max_len)
        if cut <= 0:
            cut = max_len
        chunks.append(src[:cut])
        src = src[cut:].lstrip("\n")
    if src:
        chunks.append(src)
    return chunks


@tool("Discord通知")
def send_discord_message(message: str, severity: str = "normal", webhook_url: str | None = None) -> str:
    """Send a Markdown message to Discord via webhook.

    `webhook_url` allows weekly technology reports to use a dedicated Discord channel
    while keeping the existing daily report webhook unchanged.
    """
    webhook = (webhook_url or os.getenv("DISCORD_WEBHOOK_URL") or "").strip()
    if not webhook:
        return "DISCORD_WEBHOOK_URL is not set."

    color_map = {
        "normal": 0x22C55E,
        "warning": 0xFACC15,
        "urgent": 0xEF4444,
    }
    level = str(severity).strip().lower()
    color = color_map.get(level, color_map["normal"])
    sent = 0

    for chunk in _split_message(message):
        payload = {
            "embeds": [
                {
                    "title": "AI投資チーム レポート",
                    "description": chunk,
                    "color": color,
                }
            ]
        }
        try:
            resp = requests.post(webhook, json=payload, timeout=10)
            if resp.status_code >= 300:
                return f"Discord send failed: {resp.status_code} {resp.text[:120]}"
            sent += 1
        except Exception as exc:
            return f"Discord send error: {exc}"
    return json.dumps({"ok": True, "sent_chunks": sent}, ensure_ascii=False)

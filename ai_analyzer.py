from __future__ import annotations

import re
from functools import lru_cache
from urllib.parse import quote

import requests

POSITIVE_WORDS = [
    "beat",
    "growth",
    "surge",
    "record",
    "upside",
    "raise",
    "bullish",
    "upgrade",
    "profit",
    "strong",
    "増配",
    "上方修正",
    "自社株買い",
    "好調",
]
NEGATIVE_WORDS = [
    "miss",
    "decline",
    "drop",
    "downgrade",
    "cut",
    "weak",
    "bearish",
    "risk",
    "lawsuit",
    "down",
    "減配",
    "下方修正",
    "不祥事",
    "悪化",
]


def _contains_japanese(text: str) -> bool:
    return bool(re.search(r"[\u3040-\u30ff\u3400-\u9fff]", text or ""))


def _ascii_ratio(text: str) -> float:
    if not text:
        return 1.0
    ascii_count = sum(1 for c in text if ord(c) < 128)
    return ascii_count / max(1, len(text))


@lru_cache(maxsize=4096)
def _translate_cached(src: str, target_lang: str, timeout_sec: int) -> str:
    if not src:
        return ""
    if target_lang == "ja" and _contains_japanese(src):
        return src

    try:
        url = (
            "https://translate.googleapis.com/translate_a/single"
            f"?client=gtx&sl=auto&tl={target_lang}&dt=t&q={quote(src)}"
        )
        resp = requests.get(url, timeout=timeout_sec)
        resp.raise_for_status()
        data = resp.json()
        translated = "".join(part[0] for part in data[0] if part and part[0])
        return translated.strip() or src
    except Exception:
        return src


def translate_to_japanese(text: str, timeout_sec: int = 4) -> str:
    """Translate text to Japanese with in-process cache."""
    src = (text or "").strip()
    return _translate_cached(src, "ja", int(timeout_sec))


def translate_to_english(text: str, timeout_sec: int = 4) -> str:
    """Translate text to English with in-process cache."""
    src = (text or "").strip()
    return _translate_cached(src, "en", int(timeout_sec))


def summarize_news(title: str, content: str) -> str:
    text = f"{title} {content}".strip()
    if not text:
        return "要約対象テキストがありません。"
    normalized = re.sub(r"\s+", " ", text)
    p1 = normalized[:90]
    p2 = normalized[90:180] if len(normalized) > 90 else ""
    p3 = normalized[180:270] if len(normalized) > 180 else ""
    lines = [f"・{p1}"]
    if p2:
        lines.append(f"・{p2}")
    if p3:
        lines.append(f"・{p3}")
    return "\n".join(lines)


def build_japanese_summary(title: str, content: str) -> str:
    """Build a Japanese summary with translation fallback."""
    raw = summarize_news(title, content)
    ja = translate_to_japanese(raw)
    if _contains_japanese(ja) and _ascii_ratio(ja) < 0.75:
        return ja

    content_snippet = re.sub(r"\s+", " ", (content or "").strip())[:220]
    content_ja = translate_to_japanese(content_snippet) if content_snippet else ""
    if _contains_japanese(content_ja) and _ascii_ratio(content_ja) < 0.75:
        c1 = content_ja[:70]
        c2 = content_ja[70:140] if len(content_ja) > 70 else ""
        c3 = content_ja[140:210] if len(content_ja) > 140 else ""
        lines = [f"・{c1}"]
        if c2:
            lines.append(f"・{c2}")
        if c3:
            lines.append(f"・{c3}")
        return "\n".join(lines)

    return (
        f"・{title} に関するニュースです。\n"
        "・重要ポイントは元記事で事実確認してください。\n"
        "・投資判断は一次情報と合わせて確認してください。"
    )


def analyze_sentiment(text: str) -> tuple[float, str]:
    lower = text.lower()
    pos = sum(1 for w in POSITIVE_WORDS if w in lower)
    neg = sum(1 for w in NEGATIVE_WORDS if w in lower)
    total = pos + neg
    if total == 0:
        return 0.0, "neutral"
    score = (pos - neg) / total
    if score > 0.2:
        return float(score), "positive"
    if score < -0.2:
        return float(score), "negative"
    return float(score), "neutral"


def score_importance(text: str, source: str) -> int:
    score = 2
    lower = text.lower()
    high_impact = ["earnings", "guidance", "fomc", "cpi", "jobs", "merger", "acquisition", "決算", "利上げ", "利下げ"]
    if any(k in lower for k in high_impact):
        score += 2
    if source in {"Reuters", "Bloomberg", "WSJ", "SEC EDGAR"}:
        score += 1
    return max(1, min(score, 5))


def extract_related_stocks(text: str, stock_codes: list[str]) -> list[str]:
    found: list[str] = []
    upper_text = text.upper()
    for code in stock_codes:
        if re.search(rf"\b{re.escape(code.upper())}\b", upper_text):
            found.append(code.upper())
    return sorted(set(found))


def infer_sector(text: str) -> str:
    lower = text.lower()
    mapping = {
        "半導体": ["semiconductor", "chip", "nvidia", "nvda", "tsmc"],
        "テック": ["ai", "cloud", "software", "microsoft", "apple", "google", "amazon"],
        "金融": ["bank", "interest rate", "fed", "bond", "jpmorgan", "goldman"],
        "ヘルスケア": ["health", "pharma", "drug", "biotech"],
        "エネルギー": ["oil", "gas", "energy", "opec"],
    }
    for sector, keywords in mapping.items():
        if any(k in lower for k in keywords):
            return sector
    return "その他"

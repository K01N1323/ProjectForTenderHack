from __future__ import annotations

import re
from typing import Iterable, List, Tuple


TOKEN_RE = re.compile(r"[0-9A-Za-zА-Яа-яЁё]+")
WHITESPACE_RE = re.compile(r"\s+")

STOPWORDS = {
    "и",
    "в",
    "во",
    "на",
    "по",
    "для",
    "с",
    "со",
    "к",
    "ко",
    "из",
    "от",
    "до",
    "под",
    "над",
    "не",
    "или",
    "а",
    "но",
    "о",
    "об",
    "у",
    "the",
    "and",
    "for",
    "of",
    "to",
}

RUSSIAN_ENDINGS = (
    "иями",
    "ями",
    "ами",
    "ией",
    "ией",
    "ого",
    "ему",
    "ому",
    "ыми",
    "ими",
    "его",
    "ого",
    "ая",
    "яя",
    "ое",
    "ее",
    "ые",
    "ие",
    "ий",
    "ый",
    "ой",
    "ам",
    "ям",
    "ах",
    "ях",
    "ом",
    "ем",
    "ов",
    "ев",
    "ей",
    "ую",
    "юю",
    "ия",
    "ья",
    "ие",
    "ье",
    "ию",
    "ью",
    "иям",
    "иях",
    "ию",
    "а",
    "я",
    "ы",
    "и",
    "е",
    "о",
    "у",
    "ю",
)


def clean_text(value: str) -> str:
    if value is None:
        return ""
    value = value.replace("\ufeff", " ").replace("\t", " ").replace("\n", " ").replace("\r", " ")
    value = value.strip().strip('"').strip()
    return WHITESPACE_RE.sub(" ", value)


def normalize_text(value: str) -> str:
    value = clean_text(value).lower().replace("ё", "е")
    return " ".join(TOKEN_RE.findall(value))


def tokenize(value: str) -> List[str]:
    normalized = normalize_text(value)
    if not normalized:
        return []
    return [token for token in normalized.split() if token]


def is_noise_token(token: str) -> bool:
    if token in STOPWORDS:
        return True
    if len(token) == 1 and not token.isdigit():
        return True
    return False


def normalize_tokens(tokens: Iterable[str]) -> List[str]:
    result: List[str] = []
    seen = set()
    for token in tokens:
        token = normalize_text(token)
        if not token or is_noise_token(token):
            continue
        if token in seen:
            continue
        seen.add(token)
        result.append(token)
    return result


def stem_token(token: str) -> str:
    token = normalize_text(token)
    if len(token) <= 4:
        return token
    if token.isdigit():
        return token
    for ending in RUSSIAN_ENDINGS:
        if token.endswith(ending) and len(token) - len(ending) >= 3:
            return token[: -len(ending)]
    return token


def stem_tokens(tokens: Iterable[str]) -> List[str]:
    result: List[str] = []
    seen = set()
    for token in tokens:
        if not token:
            continue
        stem = stem_token(token)
        if not stem or is_noise_token(stem):
            continue
        if stem in seen:
            continue
        seen.add(stem)
        result.append(stem)
    return result


def unique_preserve_order(items: Iterable[str]) -> List[str]:
    result: List[str] = []
    seen = set()
    for item in items:
        if not item:
            continue
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


# ---------------------------------------------------------------------------
# Attribute extraction  (value + unit pairs like "500 мг", "16 гб", "a4")
# ---------------------------------------------------------------------------

# Known measurement units — lowercase, ё→е already applied by normalize_text
_UNITS = {
    # Mass / dosage
    "мкг", "мг", "г", "кг",
    # Volume
    "мл", "л", "дл",
    # Length / area
    "нм", "мкм", "мм", "см", "м", "км", "м2",
    # Digital storage
    "б", "кб", "мб", "гб", "тб", "mb", "gb", "tb", "kb",
    # Electrical
    "ма", "а", "мв", "в", "квт", "вт", "ом", "мом",
    # Frequency
    "гц", "кгц", "мгц", "ггц", "hz", "mhz", "ghz",
    # Capacitance
    "пф", "нф", "мкф",
    # Interface / version markers frequently appearing after a number
    "usb", "rpm", "dpi",
    # Pharma / packaging
    "таб", "капс", "амп", "фл", "шт", "уп",
    # Paper format shorthand (a3, a4, a5 etc.)
    "a3", "a4", "a5", "a6",
}

# Regex: a sequence of digits (possibly with decimal comma/dot), optionally
# followed immediately or after a space by a known unit token.
_ATTR_RE = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(" + "|".join(re.escape(u) for u in sorted(_UNITS, key=len, reverse=True)) + r")\b",
    re.IGNORECASE,
)

# Also catch standalone format tokens like "a4", "usb", "usb3" that contain
# no digit in the value part — these are already matched via the unit list.

def extract_attribute_spans(query: str) -> List[Tuple[str, str]]:
    """Return a list of (value, unit) pairs found in *query*.

    The query should already be normalised (lower-case, ё→е).

    Examples
    --------
    >>> extract_attribute_spans("парацетамол 500 мг таблетки")
    [('500', 'мг')]
    >>> extract_attribute_spans("флешка 16 гб usb 3 0")
    [('16', 'гб')]
    >>> extract_attribute_spans("бумага a4 100 г м2")
    [('100', 'г')]
    """
    normalized = normalize_text(query)
    spans: List[Tuple[str, str]] = []
    seen: set = set()
    for m in _ATTR_RE.finditer(normalized):
        value = m.group(1).replace(",", ".")
        unit = m.group(2).lower()
        key = (value, unit)
        if key not in seen:
            seen.add(key)
            spans.append(key)
    return spans


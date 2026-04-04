from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Dict, Iterable, Optional


DEFAULT_RAW_STE_CATALOG_PATH = Path("СТЕ_20260403.csv")

WHITESPACE_RE = re.compile(r"\s+")
INTEGER_FLOAT_RE = re.compile(r"^-?\d+\.0+$")
TRAILING_ZERO_FLOAT_RE = re.compile(r"(-?\d+\.\d*?[1-9])0+$")

PRIORITY_KEYS = [
    "назначение",
    "общие характеристики",
    "описание",
    "состав",
    "тип",
    "дозировка",
    "лекарственная форма",
    "форма",
    "материал",
    "цвет",
    "объем накопителя",
    "интерфейс подключения",
    "максимальная скорость чтения",
    "скорость записи данных",
    "количество определяемых аллергенов",
    "количество тестов в упаковке",
    "совместимость",
    "страна происхождения",
]


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return WHITESPACE_RE.sub(" ", str(value).replace("\ufeff", " ").strip()).strip()


def _normalize_key(value: str) -> str:
    return _clean_text(value).lower().replace("ё", "е")


def _humanize_value(value: str) -> str:
    value = _clean_text(value)
    if not value:
        return ""
    if INTEGER_FLOAT_RE.fullmatch(value):
        return str(int(float(value)))
    return TRAILING_ZERO_FLOAT_RE.sub(r"\1", value)


def _is_noise_value(value: str) -> bool:
    normalized = _normalize_key(value)
    return normalized in {"", "0", "0.0", "0.00", "0.00000"}


def _truncate(value: str, limit: int = 240) -> str:
    value = _clean_text(value)
    if len(value) <= limit:
        return value
    trimmed = value[: limit - 1].rsplit(" ", 1)[0].strip()
    return f"{trimmed}…"


def _parse_attribute_pairs(raw_attributes: str) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for chunk in _clean_text(raw_attributes).split(";"):
        chunk = _clean_text(chunk)
        if not chunk:
            continue
        if ":" in chunk:
            key, value = chunk.split(":", 1)
        else:
            key, value = chunk, ""
        key = _clean_text(key)
        value = _humanize_value(value)
        if not key or _is_noise_value(value):
            continue
        pairs.append((key, value))
    return pairs


class CatalogDescriptionService:
    def __init__(self, raw_catalog_path: Path | str | None = DEFAULT_RAW_STE_CATALOG_PATH) -> None:
        self.raw_catalog_path = Path(raw_catalog_path) if raw_catalog_path else None
        self._loaded = False
        self._preview_by_ste_id: Dict[str, str] = {}

    def close(self) -> None:
        return None

    def get_previews(
        self,
        ste_ids: Iterable[str],
        *,
        fallback_by_ste_id: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> Dict[str, str]:
        self._ensure_loaded()
        result: Dict[str, str] = {}
        fallback_by_ste_id = fallback_by_ste_id or {}
        for ste_id in [str(value) for value in ste_ids if value]:
            preview = self._preview_by_ste_id.get(ste_id, "")
            if not preview:
                fallback_payload = fallback_by_ste_id.get(ste_id, {})
                preview = self.build_fallback_preview(
                    attribute_keys=str(fallback_payload.get("attribute_keys") or ""),
                )
            if preview:
                result[ste_id] = preview
        return result

    @staticmethod
    def build_fallback_preview(*, attribute_keys: str) -> str:
        keys = [_clean_text(chunk) for chunk in str(attribute_keys or "").split("|")]
        keys = [key for key in keys if key]
        if not keys:
            return ""
        shown = ", ".join(keys[:5])
        suffix = "…" if len(keys) > 5 else ""
        return _truncate(f"Характеристики: {shown}{suffix}")

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        if not self.raw_catalog_path or not self.raw_catalog_path.exists():
            return
        self._preview_by_ste_id = self._load_from_raw_catalog(self.raw_catalog_path)

    def _load_from_raw_catalog(self, path: Path) -> Dict[str, str]:
        previews: Dict[str, str] = {}
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle, delimiter=";")
            for row in reader:
                if len(row) < 4:
                    continue
                ste_id = _clean_text(row[0])
                if not ste_id or ste_id in previews:
                    continue
                preview = self._build_preview_from_raw_attributes(row[3])
                if preview:
                    previews[ste_id] = preview
        return previews

    def _build_preview_from_raw_attributes(self, raw_attributes: str) -> str:
        pairs = _parse_attribute_pairs(raw_attributes)
        if not pairs:
            return ""

        normalized_pairs = [(_normalize_key(key), key, value) for key, value in pairs]
        segments: list[str] = []
        used_keys: set[str] = set()

        for key_norm in ("назначение", "общие характеристики", "описание"):
            for pair_norm, display_key, value in normalized_pairs:
                if pair_norm != key_norm or not value:
                    continue
                if len(value.split()) > 4:
                    segments.append(value[:1].upper() + value[1:])
                else:
                    segments.append(f"{display_key}: {value}")
                used_keys.add(pair_norm)
                break
            if segments:
                break

        for key_norm in PRIORITY_KEYS:
            if key_norm in used_keys:
                continue
            for pair_norm, display_key, value in normalized_pairs:
                if pair_norm != key_norm or not value:
                    continue
                segments.append(f"{display_key}: {value}")
                used_keys.add(pair_norm)
                break
            if len(segments) >= 4:
                break

        if len(segments) < 4:
            for pair_norm, display_key, value in normalized_pairs:
                if pair_norm in used_keys or not value:
                    continue
                segments.append(f"{display_key}: {value}")
                used_keys.add(pair_norm)
                if len(segments) >= 4:
                    break

        return _truncate(" ".join(segments))

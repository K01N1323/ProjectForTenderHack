#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tenderhack.text import STOPWORDS, normalize_text, tokenize, unique_preserve_order


DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "data" / "reference" / "search_synonyms.json"
RAW_CATALOG_GLOBS = (
    "СТЕ*.csv",
    "data/raw/СТЕ*.csv",
    "data/raw/*ste*.csv",
    "data/reference/*ste*.csv",
)

PAREN_ALIAS_RE = re.compile(r"(?P<base>[^()]{3,}?)\s*\((?P<alias>[^()]{2,80})\)")
BAD_ALIAS_TOKENS = {
    "вариант",
    "варианта",
    "комплектации",
    "исполнения",
    "упаковке",
    "упаковка",
    "форма",
    "цвет",
    "размер",
    "тип",
    "модель",
    "серия",
    "код",
    "позиция",
    "набор",
    "комплект",
}

# Доменные группы: пользовательские слова + короткие формы + канонические формы,
# которые реально встречаются в закупочном каталоге.
CONCEPT_GROUPS: list[list[str]] = [
    ["телефон", "мобильный телефон", "сотовый телефон", "телефон сотовой связи", "мобильник", "сотовый", "смартфон"],
    ["ноутбук", "ноут", "лэптоп", "laptop"],
    ["компьютер", "пк", "персональный компьютер", "системный блок", "системник"],
    ["монитор", "дисплей", "экран"],
    ["клавиатура", "клава"],
    ["мышь", "мышка", "манипулятор мышь"],
    ["наушники", "гарнитура", "headset"],
    ["микрофон", "microphone"],
    ["веб камера", "вебкамера", "web camera"],
    ["принтер", "печатающее устройство", "печатающий аппарат"],
    ["мфу", "многофункциональное устройство", "принтер сканер копир", "сканер принтер копир", "сканер копир принтер"],
    ["сканер", "scanner"],
    ["картридж", "расходник", "расходные материалы для принтера", "тонер картридж"],
    ["тонер", "тонер картридж", "картридж"],
    ["флешка", "флеш накопитель", "usb накопитель", "usb флешка", "флеш драйв", "флешдрайв", "накопитель"],
    ["жесткий диск", "hdd", "винчестер"],
    ["ssd", "твердотельный накопитель"],
    ["роутер", "маршрутизатор", "router"],
    ["модем", "modem"],
    ["коммутатор", "switch", "сетевой коммутатор"],
    ["источник бесперебойного питания", "ибп", "бесперебойник"],
    ["проектор", "projector"],
    ["сервер", "server"],
    ["программное обеспечение", "по", "софт", "software"],
    ["лицензия", "license", "подписка"],
    ["антивирус", "защитное по"],
    ["электронная подпись", "эцп", "кэп", "квалифицированная электронная подпись"],
    ["электронный документооборот", "эдо", "доступ к электронному документообороту"],
    ["телефония", "связь", "телефонная связь"],
    ["видеонаблюдение", "cctv", "камера видеонаблюдения", "видеокамера"],
    ["камера", "видеокамера", "камера наблюдения"],
    ["аккумулятор", "акб", "батарея"],
    ["батарейка", "батарейки", "элемент питания", "элементы питания"],
    ["светильник", "лампа", "осветительный прибор"],
    ["светодиодный", "led"],
    ["кабель", "провод", "шнур"],
    ["удлинитель", "сетевой фильтр"],
    ["бумага", "бумага офисная", "офисная бумага"],
    ["ручка", "ручка канцелярская", "канц ручка", "шариковая ручка"],
    ["карандаш", "карандаш простой"],
    ["маркер", "фломастер", "маркер канцелярский"],
    ["папка", "скоросшиватель", "папка пластиковая"],
    ["файл", "мультифора"],
    ["тетрадь", "тетрадка"],
    ["степлер", "скобосшиватель"],
    ["ластик", "стерка", "стирательная резинка"],
    ["клей", "клей канцелярский", "клей карандаш"],
    ["учебник", "учебная литература", "учебное пособие"],
    ["обучение", "курсы", "образовательные услуги"],
    ["повышение квалификации", "повышение профессиональной квалификации", "курсы повышения квалификации"],
    ["уборка", "клининг", "клининговые услуги"],
    ["охрана", "охранные услуги", "security"],
    ["вывоз мусора", "утилизация отходов"],
    ["лекарство", "препарат", "медикамент", "лекарственный препарат"],
    ["обезболивающее", "анальгетик", "анальгетики"],
    ["жаропонижающее", "антипиретик", "антипиретики"],
    ["антибиотик", "антибактериальный препарат"],
    ["антисептик", "дезинфицирующее средство", "дезсредство"],
    ["шприц", "syringe"],
    ["маска", "маска медицинская"],
    ["перчатки", "одноразовые перчатки"],
    ["бахилы", "одноразовые бахилы"],
    ["капельница", "инфузия", "инфузионная система"],
    ["укол", "инъекция"],
    ["таблетка", "таблетки", "табл"],
    ["капсула", "капсулы", "капс"],
    ["ампула", "ампулы", "амп"],
    ["флакон", "флаконы", "фл"],
    ["раствор", "р р", "раствор для инъекций", "раствор для инфузий"],
    ["суспензия", "сусп"],
    ["мазь", "ointment"],
    ["крем", "cream"],
    ["гель", "gel"],
    ["внутривенно", "в в", "вв"],
    ["внутримышечно", "в м", "вм"],
    ["подкожно", "п к", "пк"],
    ["автомобиль", "авто", "машина", "транспортное средство"],
    ["бензин", "топливо"],
    ["дизельное топливо", "дизтопливо", "дт"],
    ["шина", "покрышка"],
    ["кондиционер", "сплит система", "сплитсистема"],
    ["обогреватель", "нагреватель"],
    ["вентилятор", "fan"],
    ["насос", "pump"],
    ["фильтр", "filter"],
    ["смеситель", "кран"],
    ["раковина", "умывальник"],
    ["унитаз", "санфаянс"],
    ["сантехника", "санитарно техническое оборудование"],
    ["дрель", "drill"],
    ["перфоратор", "rotary hammer"],
    ["болгарка", "ушм", "углошлифовальная машина"],
    ["шуруповерт", "винтоверт"],
    ["саморез", "шуруп"],
    ["болт", "bolt"],
    ["гайка", "nut"],
    ["краска", "лакокрасочные материалы", "лкм"],
    ["грунтовка", "primer"],
    ["труба", "трубопровод"],
    ["дверь", "door"],
    ["окно", "window"],
    ["стол", "рабочий стол"],
    ["стул", "кресло"],
    ["шкаф", "шкафчик"],
    ["диван", "sofa"],
    ["мебель", "предметы мебели"],
    ["холодильник", "холодильное оборудование"],
    ["мл", "миллилитр", "миллилитров"],
    ["мг", "миллиграмм", "миллиграммов"],
    ["г", "грамм", "граммов"],
    ["кг", "килограмм", "килограммов"],
    ["мм", "миллиметр", "миллиметров"],
    ["см", "сантиметр", "сантиметров"],
    ["м", "метр", "метров"],
    ["л", "литр", "литров"],
    ["гб", "гигабайт", "гигабайтов"],
    ["мб", "мегабайт", "мегабайтов"],
    ["тб", "терабайт", "терабайтов"],
    ["вт", "ватт", "ваттов"],
]


@dataclass
class CatalogRecord:
    name: str
    category: str
    attributes: str


def _find_default_catalog() -> Path | None:
    for pattern in RAW_CATALOG_GLOBS:
        matches = sorted(PROJECT_ROOT.glob(pattern))
        if matches:
            return matches[0]
    return None


def _resolve_catalog_path(catalog_path: str | None) -> Path:
    if catalog_path:
        path = Path(catalog_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Не найден файл датасета: {path}")
        return path
    detected = _find_default_catalog()
    if detected is None:
        raise FileNotFoundError(
            "Не нашел файл СТЕ для генерации синонимов. "
            "Передай путь через --catalog-path."
        )
    return detected


def _row_has_header(row: list[str]) -> bool:
    normalized = {normalize_text(cell) for cell in row}
    return bool({"ste_id", "clean_name", "category"} & normalized) or any(
        marker in normalized for marker in {"наименование сте", "категория сте"}
    )


def _detect_csv_dialect(catalog_path: Path) -> csv.Dialect:
    sample = catalog_path.read_text(encoding="utf-8-sig", errors="ignore")[:8192]
    try:
        return csv.Sniffer().sniff(sample, delimiters=";,")
    except csv.Error:
        dialect = csv.excel()
        dialect.delimiter = ";"
        return dialect


def iter_catalog_records(catalog_path: Path) -> Iterable[CatalogRecord]:
    dialect = _detect_csv_dialect(catalog_path)
    with catalog_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, dialect)
        first_row = next(reader, None)
        if not first_row:
            return
        if _row_has_header(first_row):
            handle.seek(0)
            dict_reader = csv.DictReader(handle, dialect=dialect)
            for row in dict_reader:
                name = (
                    row.get("clean_name")
                    or row.get("normalized_name")
                    or row.get("Наименование СТЕ")
                    or row.get("name")
                    or ""
                )
                category = (
                    row.get("category")
                    or row.get("normalized_category")
                    or row.get("Категория СТЕ")
                    or ""
                )
                attributes = (
                    row.get("attribute_keys")
                    or row.get("key_tokens")
                    or row.get("Характеристики СТЕ")
                    or ""
                )
                yield CatalogRecord(name=str(name), category=str(category), attributes=str(attributes))
            return

        current_row = first_row
        while current_row:
            name = current_row[1] if len(current_row) > 1 else ""
            category = current_row[2] if len(current_row) > 2 else ""
            attributes = current_row[3] if len(current_row) > 3 else ""
            yield CatalogRecord(name=name, category=category, attributes=attributes)
            current_row = next(reader, None)


def _looks_like_bad_alias(normalized: str) -> bool:
    tokens = tokenize(normalized)
    if not tokens:
        return True
    if len(tokens) > 5:
        return True
    if STOPWORDS & set(tokens):
        return True
    if any(not token.isalpha() for token in tokens):
        return True
    if BAD_ALIAS_TOKENS & set(tokens):
        return True
    if all(len(token) <= 2 for token in tokens):
        return True
    if len("".join(tokens)) > 40:
        return True
    return False


def _extract_parenthetical_alias_pairs(text: str) -> list[tuple[str, str, bool]]:
    pairs: list[tuple[str, str, bool]] = []
    if not text:
        return pairs
    for match in PAREN_ALIAS_RE.finditer(text):
        base_raw = match.group("base").strip(" ,;-")
        alias_raw = match.group("alias").strip(" ,;-")
        base = normalize_text(base_raw)
        alias = normalize_text(alias_raw)
        if not base or not alias or base == alias:
            continue
        alias_tokens = tokenize(alias)
        base_tokens = tokenize(base)
        alias_compact = alias.replace(" ", "")
        base_compact = base.replace(" ", "")
        if len(alias_tokens) > 2 or len(base_tokens) > 6:
            continue
        if len(alias_tokens) == 2 and max(len(token) for token in alias_tokens) > 7:
            continue
        if len(alias_compact) > 18:
            continue
        if len(alias_compact) >= len(base_compact):
            continue
        if _looks_like_bad_alias(base) or _looks_like_bad_alias(alias):
            continue
        alias_is_short = len(alias_tokens) == 1 and 2 <= len(alias_compact) <= 8
        pairs.append((alias, base, alias_is_short))
    return pairs


def _target_tokens_supported(value: str, token_counter: Counter[str]) -> bool:
    tokens = tokenize(value)
    if not tokens:
        return False
    return all(token_counter.get(token, 0) > 0 for token in tokens)


def _score_target(value: str, token_counter: Counter[str]) -> tuple[int, int, str]:
    tokens = tokenize(value)
    token_frequency_sum = sum(token_counter.get(token, 0) for token in tokens)
    return (len(tokens), -token_frequency_sum, value)


def _append_mapping(
    token_synonyms: dict[str, list[str]],
    phrase_synonyms: dict[str, list[str]],
    source: str,
    target: str,
) -> None:
    source_normalized = normalize_text(source)
    target_normalized = normalize_text(target)
    if not source_normalized or not target_normalized or source_normalized == target_normalized:
        return
    if len(tokenize(source_normalized)) == 1:
        token_synonyms.setdefault(source_normalized, []).append(target_normalized)
        return
    phrase_synonyms.setdefault(source_normalized, []).append(target_normalized)


def generate_synonyms_payload(
    catalog_path: Path,
    min_auto_pair_count: int = 2,
    max_targets_per_source: int = 8,
) -> dict[str, object]:
    token_counter: Counter[str] = Counter()
    auto_pairs: Counter[tuple[str, str]] = Counter()
    auto_short_aliases: set[tuple[str, str]] = set()
    row_count = 0

    for record in iter_catalog_records(catalog_path):
        row_count += 1
        combined_text = " ".join(part for part in [record.name, record.category, record.attributes] if part)
        for token in tokenize(combined_text):
            if len(token) <= 1:
                continue
            token_counter[token] += 1
        for source, target, is_short_alias in _extract_parenthetical_alias_pairs(record.category):
            auto_pairs[(source, target)] += 1
            if is_short_alias:
                auto_short_aliases.add((source, target))

    token_synonyms: dict[str, list[str]] = {}
    phrase_synonyms: dict[str, list[str]] = {}

    for group in CONCEPT_GROUPS:
        normalized_group = unique_preserve_order(normalize_text(item) for item in group if normalize_text(item))
        for source in normalized_group:
            for target in normalized_group:
                if source == target:
                    continue
                if not _target_tokens_supported(target, token_counter):
                    continue
                _append_mapping(token_synonyms, phrase_synonyms, source, target)

    for (source, target), count in auto_pairs.items():
        if count < min_auto_pair_count and (source, target) not in auto_short_aliases:
            continue
        if not _target_tokens_supported(target, token_counter):
            continue
        _append_mapping(token_synonyms, phrase_synonyms, source, target)
        _append_mapping(token_synonyms, phrase_synonyms, target, source)

    def finalize(items: dict[str, list[str]]) -> dict[str, list[str]]:
        normalized: dict[str, list[str]] = {}
        for source, targets in sorted(items.items()):
            unique_targets = unique_preserve_order(target for target in targets if target and target != source)
            supported_targets = [target for target in unique_targets if _target_tokens_supported(target, token_counter)]
            supported_targets.sort(key=lambda value: _score_target(value, token_counter))
            if max_targets_per_source > 0:
                supported_targets = supported_targets[:max_targets_per_source]
            if supported_targets:
                normalized[source] = supported_targets
        return normalized

    finalized_token_synonyms = finalize(token_synonyms)
    finalized_phrase_synonyms = finalize(phrase_synonyms)
    return {
        "metadata": {
            "catalog_path": str(catalog_path),
            "rows_scanned": row_count,
            "dataset_token_vocab_size": len(token_counter),
            "manual_concept_group_count": len(CONCEPT_GROUPS),
            "auto_parenthetical_pair_count": sum(1 for count in auto_pairs.values() if count >= min_auto_pair_count),
        },
        "phrase_synonyms": finalized_phrase_synonyms,
        "token_synonyms": finalized_token_synonyms,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a large search synonym dictionary from the STE dataset.")
    parser.add_argument("--catalog-path", help="Path to raw or preprocessed STE catalog CSV.")
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH), help="Where to save the generated JSON.")
    parser.add_argument("--min-auto-pair-count", type=int, default=2, help="How many times an auto alias pair must appear before it is kept.")
    parser.add_argument("--max-targets-per-source", type=int, default=8, help="Hard cap for how many targets to keep per source term.")
    args = parser.parse_args()

    catalog_path = _resolve_catalog_path(args.catalog_path)
    output_path = Path(args.output_path).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = generate_synonyms_payload(
        catalog_path=catalog_path,
        min_auto_pair_count=args.min_auto_pair_count,
        max_targets_per_source=args.max_targets_per_source,
    )
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "output_path": str(output_path),
                "metadata": payload["metadata"],
                "phrase_synonyms": len(payload["phrase_synonyms"]),
                "token_synonyms": len(payload["token_synonyms"]),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()

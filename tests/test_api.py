from __future__ import annotations

import csv
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from backend.main import AppSettings, SearchRequest, TenderHackApiService, create_app
from scripts.build_search_assets import build_search_db
from tenderhack.personalization_runtime import PersonalizationRuntimeService
from tenderhack.search import SearchService
from tenderhack.text import unique_preserve_order


class ApiTests(unittest.TestCase):
    @staticmethod
    def _suggestion_texts(payload: list[dict]) -> list[str]:
        return [str(item.get("text") or "") for item in payload]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.TemporaryDirectory()
        base_path = Path(cls.temp_dir.name)
        cls.catalog_path = base_path / "catalog.csv"
        cls.raw_catalog_path = base_path / "raw_ste.csv"
        cls.search_db_path = base_path / "search.sqlite"
        cls.preprocessed_db_path = base_path / "preprocessed.sqlite"
        cls.synonyms_path = base_path / "synonyms.json"

        with cls.catalog_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "ste_id",
                    "clean_name",
                    "normalized_name",
                    "category",
                    "normalized_category",
                    "attribute_keys",
                    "attribute_count",
                    "key_tokens",
                ]
            )
            writer.writerow(
                [
                    "ste-1",
                    "Ручка канцелярская синяя",
                    "ручка канцелярская синяя",
                    "Ручки канцелярские",
                    "ручки канцелярские",
                    "Цвет | Тип",
                    "2",
                    "ручка канцелярская синяя шариковая",
                ]
            )
            writer.writerow(
                [
                    "ste-2",
                    "Флеш накопитель 16 ГБ USB 3.0",
                    "флеш накопитель 16 гб usb 3 0",
                    "Usb-накопители твердотельные (флеш-драйвы)",
                    "usb накопители твердотельные флеш драйвы",
                    "Объем | Интерфейс",
                    "2",
                    "флеш накопитель 16 гб usb накопитель",
                ]
            )
            writer.writerow(
                [
                    "ste-3",
                    "Парацетамол таблетки 500 мг №10",
                    "парацетамол таблетки 500 мг 10",
                    "Анальгетики и антипиретики (n02bg)",
                    "анальгетики и антипиретики n02bg",
                    "Дозировка | Форма",
                    "2",
                    "парацетамол таблетки 500 мг анальгетики",
                ]
            )
            writer.writerow(
                [
                    "ste-4",
                    "Ручка офисная красная",
                    "ручка офисная красная",
                    "Ручки канцелярские",
                    "ручки канцелярские",
                    "Цвет | Тип",
                    "2",
                    "ручка офисная красная шариковая",
                ]
            )
            writer.writerow(
                [
                    "ste-5",
                    "Труба стальная 20 мм",
                    "труба стальная 20 мм",
                    "Трубы стальные",
                    "трубы стальные",
                    "Диаметр | Материал",
                    "2",
                    "труба стальная 20 мм",
                ]
            )
            writer.writerow(
                [
                    "ste-6",
                    "Стул на металлическом каркасе",
                    "стул на металлическом каркасе",
                    "Стулья офисные",
                    "стулья офисные",
                    "Материал | Каркас",
                    "2",
                    "стул металлический каркас офисный",
                ]
            )
            writer.writerow(
                [
                    "ste-7",
                    "Альбумин человеческий 20 процентов 100 мл",
                    "альбумин человеческий 20 процентов 100 мл",
                    "Плазмозамещающие и перфузионные растворы",
                    "плазмозамещающие и перфузионные растворы",
                    "Концентрация | Объем",
                    "2",
                    "альбумин человеческий 20 процентов 100 мл плазмозамещающие",
                ]
            )
            writer.writerow(
                [
                    "ste-8",
                    "Альбом для рисования А4",
                    "альбом для рисования а4",
                    "Альбомы для рисования",
                    "альбомы для рисования",
                    "Формат | Листы",
                    "2",
                    "альбом для рисования а4 бумага",
                ]
            )
            writer.writerow(
                [
                    "ste-9",
                    "Анализатор мочи полуавтоматический",
                    "анализатор мочи полуавтоматический",
                    "Анализаторы мочи",
                    "анализаторы мочи",
                    "Тип | Производительность",
                    "2",
                    "анализатор мочи анализаторы мочи",
                ]
            )
            writer.writerow(
                [
                    "ste-10",
                    "Услуги по организационно техническому обеспечению закупочной деятельности",
                    "услуги по организационно техническому обеспечению закупочной деятельности",
                    "Услуги по организационно техническому обеспечению закупочной деятельности",
                    "услуги по организационно техническому обеспечению закупочной деятельности",
                    "Вид услуги | Сфера",
                    "2",
                    "услуги организационно техническое обеспечение закупочная деятельность",
                ]
            )
            writer.writerow(
                [
                    "ste-11",
                    "Доска магнитно маркерная настенная",
                    "доска магнитно маркерная настенная",
                    "Доски магнитно маркерные",
                    "доски магнитно маркерные",
                    "Тип | Размещение",
                    "2",
                    "доска магнитно маркерная настенная",
                ]
            )
            writer.writerow(
                [
                    "ste-12",
                    "Молоток",
                    "молоток",
                    "Молотки",
                    "молотки",
                    "Вес | Тип",
                    "2",
                    "молоток слесарный ручной",
                ]
            )
            writer.writerow(
                [
                    "ste-13",
                    "Молоток отбойный электрический",
                    "молоток отбойный электрический",
                    "Молотки отбойные",
                    "молотки отбойные",
                    "Мощность | Тип",
                    "2",
                    "молоток отбойный электрический",
                ]
            )
            writer.writerow(
                [
                    "ste-14",
                    "Молоко",
                    "молоко",
                    "Молоко питьевое",
                    "молоко питьевое",
                    "Жирность | Объем",
                    "2",
                    "молоко питьевое",
                ]
            )
            writer.writerow(
                [
                    "ste-15",
                    "Молоко сгущенное",
                    "молоко сгущенное",
                    "Молоко сгущенное",
                    "молоко сгущенное",
                    "Состав | Объем",
                    "2",
                    "молоко сгущенное с сахаром",
                ]
            )

        with cls.raw_catalog_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, delimiter=";")
            writer.writerow(
                [
                    "ste-1",
                    "Ручка канцелярская синяя",
                    "Ручки канцелярские",
                    "Цвет:синий;Тип:шариковая;Материал:пластик",
                ]
            )
            writer.writerow(
                [
                    "ste-2",
                    "Флеш накопитель 16 ГБ USB 3.0",
                    "Usb-накопители твердотельные (флеш-драйвы)",
                    "Объем накопителя:16.00000;Интерфейс подключения:USB 3.0;Цвет:черный",
                ]
            )
            writer.writerow(
                [
                    "ste-3",
                    "Парацетамол таблетки 500 мг №10",
                    "Анальгетики и антипиретики (n02bg)",
                    "Дозировка:500 мг;Лекарственная форма:таблетки;Количество в упаковке:10",
                ]
            )
            writer.writerow(
                [
                    "ste-4",
                    "Ручка офисная красная",
                    "Ручки канцелярские",
                    "Цвет:красный;Тип:шариковая;Материал:пластик",
                ]
            )
            writer.writerow(
                [
                    "ste-5",
                    "Труба стальная 20 мм",
                    "Трубы стальные",
                    "Диаметр:20 мм;Материал:сталь;Тип:круглая",
                ]
            )
            writer.writerow(
                [
                    "ste-6",
                    "Стул на металлическом каркасе",
                    "Стулья офисные",
                    "Материал:металл;Каркас:металлический;Цвет:черный",
                ]
            )
            writer.writerow(
                [
                    "ste-7",
                    "Альбумин человеческий 20 процентов 100 мл",
                    "Плазмозамещающие и перфузионные растворы",
                    "Концентрация:20 процентов;Объем:100 мл;Форма:раствор",
                ]
            )
            writer.writerow(
                [
                    "ste-8",
                    "Альбом для рисования А4",
                    "Альбомы для рисования",
                    "Формат:А4;Листы:24;Бумага:офсет",
                ]
            )
            writer.writerow(
                [
                    "ste-9",
                    "Анализатор мочи полуавтоматический",
                    "Анализаторы мочи",
                    "Тип:полуавтоматический;Производительность:200 тестов в час",
                ]
            )
            writer.writerow(
                [
                    "ste-10",
                    "Услуги по организационно техническому обеспечению закупочной деятельности",
                    "Услуги по организационно техническому обеспечению закупочной деятельности",
                    "Вид услуги:организационно-техническое обеспечение;Сфера:закупочная деятельность",
                ]
            )
            writer.writerow(
                [
                    "ste-11",
                    "Доска магнитно маркерная настенная",
                    "Доски магнитно маркерные",
                    "Тип:магнитно-маркерная;Размещение:настенная",
                ]
            )
            writer.writerow(
                [
                    "ste-12",
                    "Молоток",
                    "Молотки",
                    "Вес:500 г;Тип:слесарный",
                ]
            )
            writer.writerow(
                [
                    "ste-13",
                    "Молоток отбойный электрический",
                    "Молотки отбойные",
                    "Мощность:1200 вт;Тип:электрический",
                ]
            )
            writer.writerow(
                [
                    "ste-14",
                    "Молоко",
                    "Молоко питьевое",
                    "Жирность:3.2 процента;Объем:1 л",
                ]
            )
            writer.writerow(
                [
                    "ste-15",
                    "Молоко сгущенное",
                    "Молоко сгущенное",
                    "Состав:с сахаром;Объем:380 г",
                ]
            )

        cls.synonyms_path.write_text(
            json.dumps(
                {
                    "phrase_synonyms": {
                        "флешка": ["флеш накопитель", "usb накопитель"],
                    },
                    "token_synonyms": {
                        "флешка": ["накопитель", "usb"],
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        build_search_db(
            cls.catalog_path,
            cls.search_db_path,
            semantic_min_frequency=1,
            semantic_neighbors_per_token=4,
        )

        conn = sqlite3.connect(cls.preprocessed_db_path)
        try:
            conn.executescript(
                """
                CREATE TABLE category_lookup (
                    category_id INTEGER PRIMARY KEY,
                    category TEXT NOT NULL,
                    normalized_category TEXT NOT NULL
                );

                CREATE TABLE customer_category_stats (
                    customer_inn TEXT NOT NULL,
                    category_id INTEGER NOT NULL,
                    purchase_count INTEGER NOT NULL,
                    total_amount REAL NOT NULL,
                    first_purchase_dt TEXT,
                    last_purchase_dt TEXT,
                    PRIMARY KEY (customer_inn, category_id)
                );

                CREATE TABLE customer_ste_stats (
                    customer_inn TEXT NOT NULL,
                    ste_id TEXT NOT NULL,
                    category_id INTEGER NOT NULL,
                    purchase_count INTEGER NOT NULL,
                    total_amount REAL NOT NULL,
                    first_purchase_dt TEXT,
                    last_purchase_dt TEXT,
                    PRIMARY KEY (customer_inn, ste_id)
                );

                CREATE TABLE region_category_stats (
                    customer_region TEXT NOT NULL,
                    category_id INTEGER NOT NULL,
                    purchase_count INTEGER NOT NULL,
                    total_amount REAL NOT NULL,
                    first_purchase_dt TEXT,
                    last_purchase_dt TEXT,
                    PRIMARY KEY (customer_region, category_id)
                );

                CREATE TABLE customer_region_lookup (
                    customer_inn TEXT PRIMARY KEY,
                    customer_region TEXT NOT NULL,
                    frequency INTEGER NOT NULL
                );

                CREATE TABLE ste_offer_lookup (
                    ste_id TEXT PRIMARY KEY,
                    supplier_inn TEXT NOT NULL,
                    supplier_region TEXT,
                    offer_count INTEGER NOT NULL,
                    avg_price REAL NOT NULL,
                    min_price REAL NOT NULL,
                    last_contract_dt TEXT
                );
                """
            )

            conn.executemany(
                "INSERT INTO category_lookup (category_id, category, normalized_category) VALUES (?, ?, ?)",
                [
                    (1, "Ручки канцелярские", "ручки канцелярские"),
                    (2, "Usb-накопители твердотельные (флеш-драйвы)", "usb накопители твердотельные флеш драйвы"),
                    (3, "Анальгетики и антипиретики (n02bg)", "анальгетики и антипиретики n02bg"),
                    (4, "Трубы стальные", "трубы стальные"),
                    (5, "Плазмозамещающие и перфузионные растворы", "плазмозамещающие и перфузионные растворы"),
                    (6, "Альбомы для рисования", "альбомы для рисования"),
                    (7, "Анализаторы мочи", "анализаторы мочи"),
                ],
            )
            conn.executemany(
                """
                INSERT INTO customer_category_stats (
                    customer_inn, category_id, purchase_count, total_amount, first_purchase_dt, last_purchase_dt
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    ("7701234567", 1, 5, 1500.0, "2024-01-01", "2025-01-10"),
                    ("7702155262", 1, 24, 7200.0, "2024-01-01", "2025-01-10"),
                    ("7702155262", 2, 1, 550.0, "2024-01-01", "2024-03-10"),
                    ("7702155262", 4, 1, 790.0, "2024-01-01", "2024-04-10"),
                    ("7702155262", 6, 1, 120.0, "2024-01-01", "2024-05-10"),
                    ("7707654321", 3, 12, 4200.0, "2024-01-01", "2025-01-10"),
                    ("7707654322", 5, 18, 18000.0, "2024-01-01", "2025-01-10"),
                    ("7707654322", 3, 6, 2400.0, "2024-01-01", "2025-01-10"),
                    ("7707654322", 6, 1, 200.0, "2024-01-01", "2025-01-10"),
                ],
            )
            conn.executemany(
                """
                INSERT INTO customer_ste_stats (
                    customer_inn, ste_id, category_id, purchase_count, total_amount, first_purchase_dt, last_purchase_dt
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("7701234567", "ste-1", 1, 4, 900.0, "2024-01-01", "2025-01-10"),
                    ("7701234567", "ste-3", 3, 1, 110.0, "2024-06-10", "2024-06-10"),
                    ("7702155262", "ste-1", 1, 20, 4800.0, "2024-01-01", "2025-01-10"),
                    ("7702155262", "ste-4", 1, 1, 240.0, "2024-02-15", "2024-02-15"),
                    ("7702155262", "ste-2", 2, 1, 550.0, "2024-03-10", "2024-03-10"),
                    ("7702155262", "ste-5", 4, 1, 790.0, "2024-04-10", "2024-04-10"),
                    ("7702155262", "ste-8", 6, 1, 120.0, "2024-05-10", "2024-05-10"),
                    ("7707654321", "ste-3", 3, 7, 980.0, "2024-01-01", "2025-01-10"),
                    ("7707654322", "ste-7", 5, 14, 16500.0, "2024-01-01", "2025-01-10"),
                    ("7707654322", "ste-3", 3, 4, 540.0, "2024-01-01", "2025-01-10"),
                    ("7707654322", "ste-8", 6, 1, 200.0, "2024-01-01", "2025-01-10"),
                ],
            )
            conn.executemany(
                """
                INSERT INTO region_category_stats (
                    customer_region, category_id, purchase_count, total_amount, first_purchase_dt, last_purchase_dt
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    ("Москва", 1, 11, 3300.0, "2024-01-01", "2025-01-10"),
                    ("Москва", 3, 8, 980.0, "2024-01-01", "2025-01-10"),
                    ("Москва", 5, 19, 21000.0, "2024-01-01", "2025-01-10"),
                ],
            )
            conn.executemany(
                "INSERT INTO customer_region_lookup (customer_inn, customer_region, frequency) VALUES (?, ?, ?)",
                [
                    ("7701234567", "Москва", 6),
                    ("7702155262", "Москва", 8),
                    ("7707654321", "Москва", 5),
                    ("7707654322", "Москва", 5),
                ],
            )
            conn.executemany(
                """
                INSERT INTO ste_offer_lookup (
                    ste_id, supplier_inn, supplier_region, offer_count, avg_price, min_price, last_contract_dt
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("ste-1", "1234567890", "Москва", 4, 225.0, 199.99, "2025-01-10"),
                    ("ste-2", "5555555555", "Москва", 2, 599.0, 549.0, "2025-01-11"),
                    ("ste-3", "7777777777", "Москва", 3, 120.0, 99.0, "2025-01-12"),
                    ("ste-4", "8888888888", "Москва", 2, 240.0, 219.0, "2025-01-15"),
                    ("ste-5", "9999999999", "Москва", 5, 820.0, 790.0, "2025-01-20"),
                    ("ste-7", "1111111111", "Москва", 4, 1450.0, 1390.0, "2025-01-22"),
                    ("ste-8", "2222222222", "Москва", 1, 120.0, 110.0, "2025-01-22"),
                    ("ste-9", "3333333333", "Москва", 2, 35000.0, 32990.0, "2025-01-25"),
                    ("ste-10", "4444444444", "Москва", 1, 150000.0, 150000.0, "2025-01-26"),
                    ("ste-11", "5555555556", "Москва", 3, 6500.0, 5990.0, "2025-01-26"),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        settings = AppSettings(
            search_db_path=cls.search_db_path,
            preprocessed_db_path=cls.preprocessed_db_path,
            synonyms_path=cls.synonyms_path,
            semantic_backend="sqlite",
            raw_ste_catalog_path=cls.raw_catalog_path,
            redis_url="memory://",
            search_rerank_enabled=False,
        )
        cls.client_cm = TestClient(create_app(settings=settings))
        cls.client = cls.client_cm.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client_cm.__exit__(None, None, None)
        cls.temp_dir.cleanup()

    def test_login_returns_region_and_seeded_categories(self) -> None:
        response = self.client.post("/api/auth/login", json={"inn": "7701234567"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["inn"], "7701234567")
        self.assertEqual(payload["region"], "Москва")
        self.assertTrue(payload["viewedCategories"])
        self.assertTrue(payload["topCategories"])
        self.assertTrue(payload["frequentProducts"])
        self.assertEqual(payload["frequentProducts"][0]["steId"], "ste-1")

        service = self.client.app.state.service
        cache_key = service.cache_service.build_key(
            "login",
            data={"inn": "7701234567", "version": service.LOGIN_CACHE_VERSION},
        )
        cached_payload = service.cache_service.get_json(cache_key)
        self.assertEqual(service.cache_service.backend_name, "memory")
        self.assertIsInstance(cached_payload, dict)
        self.assertEqual(cached_payload["inn"], "7701234567")

    def test_runtime_personalization_predictor_returns_reason_codes(self) -> None:
        search_service = SearchService(
            search_db_path=self.search_db_path,
            synonyms_path=self.synonyms_path,
            semantic_backend="sqlite",
        )
        runtime_service = PersonalizationRuntimeService(db_path=self.preprocessed_db_path)
        try:
            search_payload = search_service.search(query="канцелярские ручки", top_k=5)
            reranked = runtime_service.rerank_candidates(
                query=str(search_payload["query"]["corrected_query"] or search_payload["query"]["normalized_query"]),
                candidates=list(search_payload["results"]),
                user_id="user-7701234567",
                customer_inn="7701234567",
                customer_region="Москва",
                session_categories=["Ручки канцелярские"],
            )
            self.assertTrue(reranked)
            self.assertEqual(reranked[0]["ste_id"], "ste-1")
            self.assertGreater(reranked[0]["personalization_score"], 0.0)
            self.assertIn("USER_CATEGORY_AFFINITY", reranked[0]["top_reason_codes"])
        finally:
            search_service.close()
            runtime_service.close()

    def test_runtime_does_not_promote_history_above_stronger_query_match(self) -> None:
        runtime_service = PersonalizationRuntimeService(db_path=self.preprocessed_db_path)
        try:
            reranked = runtime_service.rerank_candidates(
                query="флеш накопитель",
                candidates=[
                    {
                        "ste_id": "ste-1",
                        "clean_name": "Ручка канцелярская синяя",
                        "normalized_name": "ручка канцелярская синяя",
                        "category": "Ручки канцелярские",
                        "normalized_category": "ручки канцелярские",
                        "attribute_keys": "Цвет | Тип",
                        "attribute_count": 2,
                        "key_tokens": "ручка канцелярская синяя шариковая",
                        "search_score": 7.0,
                        "search_features": {
                            "exact_phrase": 0.0,
                            "full_name_cover": 0.0,
                            "full_category_cover": 0.0,
                            "corrected_token_overlap": 0.0,
                            "name_stem_overlap": 0.0,
                            "category_stem_overlap": 0.0,
                            "semantic_name_overlap": 0.05,
                            "semantic_category_overlap": 0.0,
                            "semantic_vector_similarity": 0.08,
                        },
                    },
                    {
                        "ste_id": "ste-2",
                        "clean_name": "Флеш накопитель 16 ГБ USB 3.0",
                        "normalized_name": "флеш накопитель 16 гб usb 3 0",
                        "category": "Usb-накопители твердотельные (флеш-драйвы)",
                        "normalized_category": "usb накопители твердотельные флеш драйвы",
                        "attribute_keys": "Объем | Интерфейс",
                        "attribute_count": 2,
                        "key_tokens": "флеш накопитель 16 гб usb накопитель",
                        "search_score": 14.0,
                        "search_features": {
                            "exact_phrase": 1.0,
                            "full_name_cover": 1.0,
                            "full_category_cover": 0.0,
                            "corrected_token_overlap": 1.0,
                            "name_stem_overlap": 1.0,
                            "category_stem_overlap": 0.4,
                            "semantic_name_overlap": 0.4,
                            "semantic_category_overlap": 0.2,
                            "semantic_vector_similarity": 0.72,
                        },
                    },
                ],
                user_id="user-7701234567",
                customer_inn="7701234567",
                customer_region="Москва",
                session_categories=[],
            )
            self.assertEqual(reranked[0]["ste_id"], "ste-2")
            self.assertEqual(reranked[0]["query_match_quality"], 1.0)
            self.assertEqual(reranked[1]["history_priority"], 0.0)
        finally:
            runtime_service.close()

    def test_runtime_prioritizes_purchase_history_over_plain_search_score(self) -> None:
        runtime_service = PersonalizationRuntimeService(db_path=self.preprocessed_db_path)
        try:
            reranked = runtime_service.rerank_candidates(
                query="ручка",
                candidates=[
                    {
                        "ste_id": "ste-4",
                        "clean_name": "Ручка офисная красная",
                        "normalized_name": "ручка офисная красная",
                        "category": "Ручки канцелярские",
                        "normalized_category": "ручки канцелярские",
                        "search_score": 12.0,
                        "search_features": {
                            "exact_phrase": 0.0,
                            "full_name_cover": 0.0,
                            "full_category_cover": 1.0,
                            "corrected_token_overlap": 1.0,
                            "name_stem_overlap": 1.0,
                            "category_stem_overlap": 1.0,
                            "semantic_name_overlap": 0.2,
                            "semantic_category_overlap": 0.2,
                            "semantic_vector_similarity": 0.5,
                        },
                    },
                    {
                        "ste_id": "ste-1",
                        "clean_name": "Ручка канцелярская синяя",
                        "normalized_name": "ручка канцелярская синяя",
                        "category": "Ручки канцелярские",
                        "normalized_category": "ручки канцелярские",
                        "search_score": 10.0,
                        "search_features": {
                            "exact_phrase": 0.0,
                            "full_name_cover": 0.0,
                            "full_category_cover": 1.0,
                            "corrected_token_overlap": 1.0,
                            "name_stem_overlap": 1.0,
                            "category_stem_overlap": 1.0,
                            "semantic_name_overlap": 0.3,
                            "semantic_category_overlap": 0.2,
                            "semantic_vector_similarity": 0.55,
                        },
                    },
                ],
                user_id="user-7701234567",
                customer_inn="7701234567",
                customer_region="Москва",
                session_categories=["Ручки канцелярские"],
            )
            self.assertEqual(reranked[0]["ste_id"], "ste-1")
            self.assertGreater(reranked[0]["history_priority"], 0.0)
            self.assertGreater(reranked[0]["history_priority"], reranked[1]["history_priority"])
        finally:
            runtime_service.close()

    def test_runtime_drops_exact_item_boost_for_one_off_purchase(self) -> None:
        runtime_service = PersonalizationRuntimeService(db_path=self.preprocessed_db_path)
        try:
            reranked = runtime_service.rerank_candidates(
                query="ручка",
                candidates=[
                    {
                        "ste_id": "ste-4",
                        "clean_name": "Ручка офисная красная",
                        "normalized_name": "ручка офисная красная",
                        "category": "Ручки канцелярские",
                        "normalized_category": "ручки канцелярские",
                        "search_score": 12.0,
                        "search_features": {
                            "exact_phrase": 0.0,
                            "full_name_cover": 0.0,
                            "full_category_cover": 1.0,
                            "corrected_token_overlap": 1.0,
                            "name_stem_overlap": 1.0,
                            "category_stem_overlap": 1.0,
                            "semantic_name_overlap": 0.2,
                            "semantic_category_overlap": 0.2,
                            "semantic_vector_similarity": 0.5,
                        },
                    },
                    {
                        "ste_id": "ste-1",
                        "clean_name": "Ручка канцелярская синяя",
                        "normalized_name": "ручка канцелярская синяя",
                        "category": "Ручки канцелярские",
                        "normalized_category": "ручки канцелярские",
                        "search_score": 10.0,
                        "search_features": {
                            "exact_phrase": 0.0,
                            "full_name_cover": 0.0,
                            "full_category_cover": 1.0,
                            "corrected_token_overlap": 1.0,
                            "name_stem_overlap": 1.0,
                            "category_stem_overlap": 1.0,
                            "semantic_name_overlap": 0.3,
                            "semantic_category_overlap": 0.2,
                            "semantic_vector_similarity": 0.55,
                        },
                    },
                ],
                user_id="user-7702155262",
                customer_inn="7702155262",
                customer_region="Москва",
                session_categories=["Ручки канцелярские"],
            )
            self.assertEqual(reranked[0]["ste_id"], "ste-1")
            by_id = {item["ste_id"]: item for item in reranked}
            self.assertGreater(by_id["ste-1"]["history_priority"], by_id["ste-4"]["history_priority"])
            self.assertLess(by_id["ste-4"]["history_priority"], 60.0)
        finally:
            runtime_service.close()

    def test_search_returns_personalized_product_shape(self) -> None:
        request_payload = {
            "query": "канцелярские ручки",
            "userContext": {
                "id": "user-7701234567",
                "inn": "7701234567",
                "region": "Москва",
                "viewedCategories": ["Ручки канцелярские"],
            },
            "viewedCategories": ["Ручки канцелярские"],
            "bouncedCategories": [],
            "topK": 5,
        }
        response = self.client.post(
            "/api/search",
            json=request_payload,
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertGreaterEqual(payload["totalCount"], 1)
        self.assertEqual(payload["total_found"], payload["totalCount"])
        self.assertIs(payload["has_more"], False)
        self.assertEqual(payload["items"][0]["id"], "ste-1")
        self.assertEqual(payload["items"][0]["supplierInn"], "1234567890")
        self.assertEqual(payload["items"][0]["offerCount"], 4)
        self.assertAlmostEqual(payload["items"][0]["price"], 199.99)
        self.assertIn("Цвет", payload["items"][0]["descriptionPreview"])
        self.assertEqual(payload["items"][0]["reasonToShow"], "На основе ваших закупок")

        service = self.client.app.state.service
        server_session = service.online_state_service.get_session_state(
            user_id="user-7701234567",
            customer_inn="7701234567",
            customer_region="Москва",
        )
        merged_session = {
            "recent_categories": unique_preserve_order(
                list(server_session.get("recent_categories", []))
                + list(request_payload["viewedCategories"])
                + list(request_payload["userContext"]["viewedCategories"])
            ),
            "clicked_ste_ids": list(server_session.get("clicked_ste_ids", [])),
            "cart_ste_ids": list(server_session.get("cart_ste_ids", [])),
            "bounced_categories": list(server_session.get("bounced_categories", [])),
            "version": int(server_session.get("version", 0) or 0),
        }
        cache_key = service.cache_service.build_key(
            "search",
            data=service._search_cache_data(
                SearchRequest(**request_payload),
                server_session=merged_session,
            ),
        )
        cached_payload = service.cache_service.get_json(cache_key)
        self.assertIsInstance(cached_payload, dict)
        self.assertEqual(cached_payload["items"][0]["id"], "ste-1")

    def test_event_updates_session_and_dynamic_search_uses_it(self) -> None:
        dynamic_user_id = "user-7701234567-dyn"
        search_payload = {
            "query": "ручка канцелярская",
            "userContext": {
                "id": dynamic_user_id,
                "inn": "7701234567",
                "region": "Москва",
                "viewedCategories": [],
            },
            "viewedCategories": [],
            "bouncedCategories": [],
            "topK": 5,
        }
        baseline = self.client.post("/api/search", json=search_payload)
        self.assertEqual(baseline.status_code, 200)
        baseline_items = baseline.json()["items"]
        self.assertGreaterEqual(len(baseline_items), 2)
        self.assertEqual(baseline_items[0]["id"], "ste-1")

        event_response = self.client.post(
            "/api/event",
            json={
                "userId": dynamic_user_id,
                "inn": "7701234567",
                "region": "Москва",
                "eventType": "cart_add",
                "steId": "ste-4",
                "category": "Ручки канцелярские",
            },
        )
        self.assertEqual(event_response.status_code, 200)
        event_payload = event_response.json()
        self.assertGreaterEqual(event_payload["sessionVersion"], 1)
        self.assertIn("ste-4", event_payload["cartSteIds"])

        reranked = self.client.post("/api/search", json=search_payload)
        self.assertEqual(reranked.status_code, 200)
        reranked_items = reranked.json()["items"]
        self.assertGreaterEqual(len(reranked_items), 2)
        self.assertEqual(reranked_items[0]["id"], "ste-4")
        self.assertEqual(reranked_items[0]["reasonToShow"], "Продолжить подбор в этой категории")

    def test_search_returns_corrected_query(self) -> None:
        response = self.client.post(
            "/api/search",
            json={
                "query": "парацетомол 500 мг",
                "userContext": None,
                "viewedCategories": [],
                "bouncedCategories": [],
                "topK": 5,
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["correctedQuery"], "парацетамол 500 мг")
        self.assertIn("total_found", payload)
        self.assertIn("has_more", payload)

    def test_search_supports_limit_offset_and_has_more(self) -> None:
        request_payload = {
            "query": "ручка",
            "userContext": None,
            "viewedCategories": [],
            "bouncedCategories": [],
            "limit": 1,
            "offset": 0,
            "min_score": 0.0,
        }
        first_page = self.client.post("/api/search", json=request_payload)
        self.assertEqual(first_page.status_code, 200)
        first_payload = first_page.json()
        self.assertEqual(len(first_payload["items"]), 1)
        self.assertGreaterEqual(first_payload["total_found"], 2)
        self.assertTrue(first_payload["has_more"])

        second_page = self.client.post(
            "/api/search",
            json={**request_payload, "offset": 1},
        )
        self.assertEqual(second_page.status_code, 200)
        second_payload = second_page.json()
        self.assertEqual(second_payload["total_found"], first_payload["total_found"])
        self.assertEqual(len(second_payload["items"]), 1)
        self.assertNotEqual(first_payload["items"][0]["id"], second_payload["items"][0]["id"])

    def test_search_short_prefix_can_surface_same_type_professional_item(self) -> None:
        response = self.client.post(
            "/api/search",
            json={
                "query": "аль",
                "userContext": {
                    "id": "user-7707654321",
                    "inn": "7707654321",
                    "region": "Москва",
                    "viewedCategories": [],
                },
                "viewedCategories": [],
                "bouncedCategories": [],
                "topK": 5,
                "min_score": 0.0,
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        item_ids = [item["id"] for item in payload["items"]]
        self.assertIn("ste-7", item_ids)
        self.assertLess(item_ids.index("ste-7"), item_ids.index("ste-8"))

    def test_suggestions_return_correction_and_abstract_phrases(self) -> None:
        response = self.client.get("/api/search/suggestions", params={"q": "флешка"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload)
        suggestion_texts = self._suggestion_texts(payload)
        self.assertIn("флеш накопитель", suggestion_texts)
        self.assertNotIn("Флеш накопитель 16 ГБ USB 3.0", suggestion_texts)
        self.assertEqual(payload[0]["type"], "query")

    def test_suggestions_prioritize_purchase_categories_for_user(self) -> None:
        response = self.client.get(
            "/api/search/suggestions",
            params={
                "q": "ручка",
                "inn": "7701234567",
                "top_categories": "Ручки канцелярские|Бумага офисная",
                "viewed_categories": "Ручки канцелярские",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload)
        suggestion_texts = self._suggestion_texts(payload)
        self.assertEqual(payload[0]["text"], "Ручка канцелярская синяя")
        self.assertEqual(payload[0]["type"], "product")
        self.assertEqual(payload[0]["reason"], "Часто закупалось")
        self.assertEqual(payload[1]["text"], "ручки канцелярские")
        self.assertEqual(payload[1]["type"], "category")
        self.assertIn("ручки канцелярские", suggestion_texts)

    def test_personalized_product_suggestions_drop_one_off_items_inside_core_category(self) -> None:
        suggestions = TenderHackApiService._build_personalized_product_suggestions(
            query="руч",
            products=[
                {
                    "steId": "ste-1",
                    "name": "Ручка канцелярская синяя",
                    "category": "Ручки канцелярские",
                    "purchaseCount": 20,
                    "reason": "Часто закупалось учреждением",
                    "recommendationScore": 6.0,
                },
                {
                    "steId": "ste-4",
                    "name": "Ручка офисная красная",
                    "category": "Ручки канцелярские",
                    "purchaseCount": 1,
                    "reason": "Часто закупалось учреждением",
                    "recommendationScore": 5.0,
                },
            ],
            user_weights={"ручки канцелярские": 2.0},
            user_item_weights={"ste-1": 1.5, "ste-4": 0.0},
            require_positive_boost=True,
        )

        suggestion_texts = [item.text for item in suggestions]
        self.assertIn("Ручка канцелярская синяя", suggestion_texts)
        self.assertNotIn("Ручка офисная красная", suggestion_texts)

    def test_history_reason_cap_keeps_room_for_non_history_suggestions(self) -> None:
        suggestions = [
            TenderHackApiService._build_suggestion(
                text="бумага для офисной техники",
                suggestion_type="category",
                reason="Категория из истории",
                score=210.0,
            ),
            TenderHackApiService._build_suggestion(
                text="зажимы для бумаг",
                suggestion_type="category",
                reason="Категория из истории",
                score=180.0,
            ),
            TenderHackApiService._build_suggestion(
                text="марка почтовая",
                suggestion_type="product",
                reason="Часто закупалось",
                score=170.0,
            ),
            TenderHackApiService._build_suggestion(
                text="бумага",
                suggestion_type="query",
                reason="Продолжение запроса",
                score=160.0,
            ),
            TenderHackApiService._build_suggestion(
                text="бумажные блоки",
                suggestion_type="query",
                reason="Популярное продолжение",
                score=150.0,
            ),
        ]

        kept, overflow = TenderHackApiService._partition_suggestions_by_history_limit(suggestions, top_k=5)
        reasons = [item.reason for item in kept]

        self.assertEqual(reasons.count("Категория из истории"), 1)
        self.assertIn("Продолжение запроса", reasons)
        self.assertIn("Популярное продолжение", reasons)
        self.assertEqual(len(overflow), 1)

    def test_institution_type_reason_cap_keeps_room_for_query_suggestions(self) -> None:
        suggestions = [
            TenderHackApiService._build_suggestion(
                text="Ручка гелевая черная",
                suggestion_type="product",
                reason="По типу учреждения",
                score=300.0,
            ),
            TenderHackApiService._build_suggestion(
                text="Ручка шариковая",
                suggestion_type="product",
                reason="По типу учреждения",
                score=290.0,
            ),
            TenderHackApiService._build_suggestion(
                text="Ручка шариковая черная",
                suggestion_type="product",
                reason="По типу учреждения",
                score=280.0,
            ),
            TenderHackApiService._build_suggestion(
                text="Ручка шариковая business",
                suggestion_type="product",
                reason="По типу учреждения",
                score=270.0,
            ),
            TenderHackApiService._build_suggestion(
                text="ручка офисная красная",
                suggestion_type="query",
                reason="Продолжение запроса",
                score=180.0,
            ),
            TenderHackApiService._build_suggestion(
                text="ручка канцелярская синяя",
                suggestion_type="query",
                reason="Продолжение запроса",
                score=175.0,
            ),
        ]

        kept, overflow = TenderHackApiService._partition_suggestions_by_history_limit(suggestions, top_k=8)
        reasons = [item.reason for item in kept]

        self.assertEqual(reasons.count("По типу учреждения"), 3)
        self.assertIn("Продолжение запроса", reasons)
        self.assertEqual(len(overflow), 1)

    def test_short_same_type_prefix_reserves_slots_for_query_completions(self) -> None:
        service = self.client.app.state.service
        with (
            patch.object(
                service,
                "_resolve_same_type_prefix_products",
                return_value=[
                    {
                        "steId": "ste-x1",
                        "name": "Ручка гелевая черная",
                        "category": "Ручки канцелярские",
                        "purchaseCount": 9,
                        "reason": "Популярно у учреждений того же типа",
                        "recommendationScore": 9.0,
                    },
                    {
                        "steId": "ste-x2",
                        "name": "Ручка шариковая",
                        "category": "Ручки канцелярские",
                        "purchaseCount": 8,
                        "reason": "Популярно у учреждений того же типа",
                        "recommendationScore": 8.0,
                    },
                    {
                        "steId": "ste-x3",
                        "name": "Ручка шариковая черная",
                        "category": "Ручки канцелярские",
                        "purchaseCount": 7,
                        "reason": "Популярно у учреждений того же типа",
                        "recommendationScore": 7.0,
                    },
                    {
                        "steId": "ste-x4",
                        "name": "Ручка шариковая business",
                        "category": "Ручки канцелярские",
                        "purchaseCount": 6,
                        "reason": "Популярно у учреждений того же типа",
                        "recommendationScore": 6.0,
                    },
                ],
            ),
            patch.object(service, "_resolve_suggestion_products", return_value=[]),
            patch.object(service, "_resolve_suggestion_categories", return_value=[]),
            patch.object(service, "_resolve_suggestion_user_weights", return_value={}),
            patch.object(service, "_resolve_suggestion_user_ste_weights", return_value={}),
        ):
            payload = service.suggestions(query="руч", user_inn="7700000000", top_k=6)

        reasons = [item.reason for item in payload]
        suggestion_types = [item.type for item in payload]
        self.assertEqual(reasons.count("По типу учреждения"), 3)
        self.assertGreaterEqual(suggestion_types.count("query"), 2)
        self.assertGreaterEqual(sum(1 for reason in reasons if reason != "По типу учреждения"), 3)
        self.assertTrue(all(reason == "По типу учреждения" for reason in reasons[:3]))

    def test_suggestions_do_not_personalize_one_off_products_by_prefix(self) -> None:
        response = self.client.get(
            "/api/search/suggestions",
            params={
                "q": "пар",
                "inn": "7701234567",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload)
        self.assertEqual(payload[0]["text"], "парацетамол таблетки")
        self.assertEqual(payload[0]["type"], "query")
        self.assertIn("Парацетамол", self._suggestion_texts(payload))

    def test_suggestions_correct_transposed_typo_to_truba(self) -> None:
        response = self.client.get(
            "/api/search/suggestions",
            params={
                "q": "турба",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload)
        self.assertEqual(payload[0]["text"], "труба")
        self.assertEqual(payload[0]["type"], "correction")
        self.assertEqual(payload[0]["reason"], "Исправление запроса")

    def test_suggestions_prioritize_same_type_professional_prefix_products(self) -> None:
        response = self.client.get(
            "/api/search/suggestions",
            params={
                "q": "аль",
                "inn": "7707654321",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload)
        suggestion_texts = self._suggestion_texts(payload)
        self.assertIn("Альбумин человеческий", suggestion_texts)
        self.assertNotIn("Альбом для рисования", suggestion_texts)
        albumin_item = next(item for item in payload if item["text"] == "Альбумин человеческий")
        self.assertEqual(albumin_item["reason"], "По типу учреждения")

    def test_suggestions_drop_one_off_history_items_for_single_letter_prefix(self) -> None:
        response = self.client.get(
            "/api/search/suggestions",
            params={
                "q": "а",
                "inn": "7707654322",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload)
        suggestion_texts = self._suggestion_texts(payload)
        self.assertIn("Альбумин человеческий", suggestion_texts)
        self.assertNotIn("Альбом для рисования", suggestion_texts)
        self.assertNotIn("альбомы для рисования", [text.lower() for text in suggestion_texts])

    def test_suggestions_trim_trailing_prepositions_from_product_phrases(self) -> None:
        response = self.client.get(
            "/api/search/suggestions",
            params={
                "q": "сту",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload)
        suggestion_texts = self._suggestion_texts(payload)
        self.assertNotIn("стул на", [text.lower() for text in suggestion_texts])
        self.assertTrue(any(text.lower().startswith("стул") for text in suggestion_texts))

    def test_suggestions_dedupe_morphological_variants_of_same_phrase(self) -> None:
        response = self.client.get(
            "/api/search/suggestions",
            params={
                "q": "анализ",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload)
        suggestion_texts = [text.lower() for text in self._suggestion_texts(payload)]
        self.assertIn("анализатор мочи полуавтоматический", suggestion_texts)
        self.assertIn("анализаторы мочи", suggestion_texts)
        self.assertNotIn("анализатор мочи", suggestion_texts)

    def test_suggestions_keep_full_long_category_phrase(self) -> None:
        response = self.client.get(
            "/api/search/suggestions",
            params={
                "q": "услуги",
                "viewed_categories": "Услуги по организационно техническому обеспечению закупочной деятельности",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload)
        suggestion_texts = [text.lower() for text in self._suggestion_texts(payload)]
        self.assertIn(
            "услуги по организационно техническому обеспечению закупочной деятельности",
            suggestion_texts,
        )
        self.assertNotIn("услуги по организационно техническому обеспечению", suggestion_texts)

    def test_suggestions_keep_full_meaningful_product_phrase(self) -> None:
        response = self.client.get(
            "/api/search/suggestions",
            params={
                "q": "доска",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload)
        suggestion_texts = [text.lower() for text in self._suggestion_texts(payload)]
        self.assertIn("доска магнитно маркерная настенная", suggestion_texts)
        self.assertNotIn("доска магнитно", suggestion_texts)

    def test_product_suggestion_phrase_trims_verbose_specific_tail(self) -> None:
        service = self.client.app.state.service

        phrase = service._product_suggestion_phrase(
            "Песок для строительных работ, Рядовой/Карьерный, фракция 2.5 мм",
        )

        self.assertEqual(phrase, "Песок для строительных работ")

    def test_abstract_name_phrase_trims_verbose_clause_after_head(self) -> None:
        service = self.client.app.state.service

        phrase = service._abstract_name_phrase(
            "Строительный контроль за выполнением работ по капитальному ремонту АПС",
            "стро",
        )

        self.assertEqual(phrase, "строительный контроль")

    def test_login_returns_extended_frequent_products_batch_for_profile_panel(self) -> None:
        service = self.client.app.state.service
        synthetic_profile = {
            "customer_region": "Москва",
            "recommended_categories": [
                {
                    "category": "Ручки канцелярские",
                    "purchase_count": 5,
                    "total_amount": 1500.0,
                    "reason": "Часто закупалось учреждением",
                    "recommendation_score": 12.0,
                }
            ],
            "recommended_ste": [
                {
                    "ste_id": f"ste-{index}",
                    "category": "Ручки канцелярские" if index in {1, 4, 11} else "Тестовая категория",
                    "purchase_count": 10 - index,
                    "total_amount": 1000.0 + index,
                    "reason": "Часто закупалось учреждением",
                    "recommendation_score": 20.0 - index,
                }
                for index in range(1, 9)
            ],
        }

        with patch.object(service.personalization_service, "build_customer_profile", return_value=synthetic_profile):
            payload = service.login("7700000000")

        self.assertEqual(len(payload.frequentProducts), 8)
        self.assertEqual(payload.frequentProducts[0]["steId"], "ste-1")

    def test_dedupe_suggestions_collapses_reordered_query_phrase(self) -> None:
        service = self.client.app.state.service
        suggestions = [
            service._build_suggestion(
                text="колбаса сервелат варено копченая",
                suggestion_type="query",
                reason="Продолжение запроса",
                score=120.0,
            ),
            service._build_suggestion(
                text="колбаса варено копченая сервелат",
                suggestion_type="query",
                reason="Продолжение запроса",
                score=120.0,
            ),
        ]

        deduped = service._dedupe_suggestions(
            suggestions,
            query="колбаса сервелат варено копченая",
        )

        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0].text.lower(), "колбаса сервелат варено копченая")

    def test_completion_suggestions_surface_multiple_prefix_families(self) -> None:
        service = self.client.app.state.service

        suggestions = service._build_completion_suggestions(
            query="моло",
            query_payload={"completion_expansions": ["молоко", "молоток", "молотки"]},
        )

        suggestion_texts = [item.text.lower() for item in suggestions]
        self.assertIn("молоко", suggestion_texts)
        self.assertIn("молоток", suggestion_texts)
        self.assertIn("молоко питьевое", suggestion_texts)

    def test_diversify_suggestions_by_family_interleaves_different_heads(self) -> None:
        service = self.client.app.state.service
        suggestions = [
            service._build_suggestion(
                text="молоток",
                suggestion_type="query",
                reason="Продолжение запроса",
                score=180.0,
            ),
            service._build_suggestion(
                text="молоток отбойный",
                suggestion_type="query",
                reason="Продолжение запроса",
                score=179.0,
            ),
            service._build_suggestion(
                text="молоко",
                suggestion_type="query",
                reason="Продолжение запроса",
                score=178.0,
            ),
        ]

        diversified = service._diversify_suggestions_by_family(suggestions)

        self.assertEqual(diversified[0].text.lower(), "молоток")
        self.assertEqual(diversified[1].text.lower(), "молоко")

    def test_suggestions_default_limit_can_expand_up_to_eight(self) -> None:
        response = self.client.get("/api/search/suggestions", params={"q": "моло"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload), 7)


if __name__ == "__main__":
    unittest.main()

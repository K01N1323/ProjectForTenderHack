from __future__ import annotations

import csv
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from backend.main import AppSettings, create_app
from scripts.build_search_assets import build_search_db
from tenderhack.personalization_runtime import PersonalizationRuntimeService
from tenderhack.search import SearchService


class ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.TemporaryDirectory()
        base_path = Path(cls.temp_dir.name)
        cls.catalog_path = base_path / "catalog.csv"
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
                ],
            )
            conn.execute(
                "INSERT INTO customer_region_lookup (customer_inn, customer_region, frequency) VALUES (?, ?, ?)",
                ("7701234567", "Москва", 6),
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

    def test_search_returns_personalized_product_shape(self) -> None:
        response = self.client.post(
            "/api/search",
            json={
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
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertGreaterEqual(payload["totalCount"], 1)
        self.assertEqual(payload["items"][0]["id"], "ste-1")
        self.assertEqual(payload["items"][0]["supplierInn"], "1234567890")
        self.assertAlmostEqual(payload["items"][0]["price"], 199.99)
        self.assertEqual(payload["items"][0]["reasonToShow"], "На основе ваших закупок")

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

    def test_suggestions_return_correction_and_product_name(self) -> None:
        response = self.client.get("/api/search/suggestions", params={"q": "флешка"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload)
        self.assertIn("Флеш накопитель 16 ГБ USB 3.0", payload)


if __name__ == "__main__":
    unittest.main()

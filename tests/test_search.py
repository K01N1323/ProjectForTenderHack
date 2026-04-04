from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.build_search_assets import build_search_db
from tenderhack.search import SearchService


class SearchServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.TemporaryDirectory()
        base_path = Path(cls.temp_dir.name)
        cls.catalog_path = base_path / "catalog.csv"
        cls.search_db_path = base_path / "search.sqlite"
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
                    "Многофункциональное устройство (МФУ) лазерное",
                    "многофункциональное устройство мфу лазерное",
                    "Печатающее оборудование",
                    "печатающее оборудование",
                    "Тип печати | Форм-фактор",
                    "2",
                    "мфу многофункциональное устройство",
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

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp_dir.cleanup()

    def setUp(self) -> None:
        self.service = SearchService(
            search_db_path=self.search_db_path,
            synonyms_path=self.synonyms_path,
            semantic_backend="sqlite",
        )

    def tearDown(self) -> None:
        self.service.close()

    def test_exact_search_returns_relevant_ste(self) -> None:
        results = self.service.search_ste("парацетамол 500 мг", top_k=3)
        self.assertTrue(results)
        self.assertEqual(results[0]["ste_id"], "ste-3")

    def test_typo_correction_updates_query_and_result(self) -> None:
        payload = self.service.search("парацетомол 500 мг", top_k=3)
        self.assertEqual(payload["query"]["corrected_query"], "парацетамол 500 мг")
        self.assertEqual(payload["query"]["applied_corrections"][0]["target"], "парацетамол")
        self.assertEqual(payload["results"][0]["ste_id"], "ste-3")

    def test_synonym_query_finds_flash_drive(self) -> None:
        payload = self.service.search("флешка 16 гб", top_k=3)
        self.assertTrue(payload["query"]["applied_synonyms"])
        self.assertEqual(payload["results"][0]["ste_id"], "ste-2")

    def test_wordform_query_matches_stationery_pen_category(self) -> None:
        payload = self.service.search("канцелярские ручки", top_k=3)
        self.assertEqual(payload["results"][0]["ste_id"], "ste-1")
        self.assertEqual(payload["results"][0]["category"], "Ручки канцелярские")

    def test_semantic_neighbors_are_applied_for_acronym_query(self) -> None:
        payload = self.service.search("мфу", top_k=3)
        self.assertEqual(payload["results"][0]["ste_id"], "ste-4")
        self.assertTrue(payload["query"]["applied_semantic_neighbors"])
        semantic_targets = {
            target
            for item in payload["query"]["applied_semantic_neighbors"]
            if item["source"] == "мфу"
            for target in item["targets"]
        }
        self.assertTrue({"многофункциональное", "устройство"} & semantic_targets)
        self.assertGreater(payload["results"][0]["search_features"]["semantic_name_overlap"], 0.0)

    def test_incomplete_word_query_uses_completions_without_bad_short_correction(self) -> None:
        payload = self.service.search("мног", top_k=3)
        self.assertEqual(payload["results"][0]["ste_id"], "ste-4")
        self.assertEqual(payload["query"]["corrected_query"], "мног")
        self.assertFalse(payload["query"]["applied_corrections"])
        completion_targets = {
            target
            for item in payload["query"]["applied_completions"]
            if item["source"] == "мног"
            for target in item["targets"]
        }
        self.assertIn("многофункциональное", completion_targets)

    def test_incomplete_category_prefix_adds_completion_variants(self) -> None:
        payload = self.service.search("канц", top_k=3)
        self.assertEqual(payload["results"][0]["ste_id"], "ste-1")
        completion_targets = {
            target
            for item in payload["query"]["applied_completions"]
            if item["source"] == "канц"
            for target in item["targets"]
        }
        self.assertTrue({"канцелярская", "канцелярские"} & completion_targets)


    def test_phrase_synonym_does_not_match_inside_larger_token(self) -> None:
        payload = self.service.search("\u0441\u0443\u043f\u0435\u0440\u0444\u043b\u0435\u0448\u043a\u0430", top_k=3)
        self.assertFalse(payload["query"]["applied_synonyms"])

    def test_typo_query_does_not_keep_original_misspelled_token_in_expansions(self) -> None:
        payload = self.service.search("\u043f\u0430\u0440\u0430\u0446\u0435\u0442\u043e\u043c\u043e\u043b 500 \u043c\u0433", top_k=3)
        self.assertIn("\u043f\u0430\u0440\u0430\u0446\u0435\u0442\u0430\u043c\u043e\u043b", payload["query"]["expanded_tokens"])
        self.assertNotIn("\u043f\u0430\u0440\u0430\u0446\u0435\u0442\u043e\u043c\u043e\u043b", payload["query"]["expanded_tokens"])

    def test_sqlite_semantic_backend_exposes_sentence_similarity_signal(self) -> None:
        similarity = self.service.semantic_expander.sentence_similarity(
            "\u043c\u0444\u0443 \u043b\u0430\u0437\u0435\u0440\u043d\u043e\u0435",
            "\u043c\u043d\u043e\u0433\u043e\u0444\u0443\u043d\u043a\u0446\u0438\u043e\u043d\u0430\u043b\u044c\u043d\u043e\u0435 \u0443\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e \u043b\u0430\u0437\u0435\u0440\u043d\u043e\u0435",
        )
        self.assertGreater(similarity, 0.0)


if __name__ == "__main__":
    unittest.main()

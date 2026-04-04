import unittest
from src.tenderhack.penalization import InMemorySkipStorage, InteractionTracker, RankingModifier

class TestPenalizationService(unittest.TestCase):
    def setUp(self):
        self.storage = InMemorySkipStorage()
        self.tracker = InteractionTracker(self.storage)
        self.modifier = RankingModifier(self.storage)
        self.user_id = "test_user_123"

    def test_multiplier_math(self):
        # 1. 0 скипов = множитель 1.0
        self.assertAlmostEqual(self.modifier.calculate_multiplier(0), 1.0)
        
        # 2. 3 скипа = множитель 0.6
        # log2(4) = 2.0 -> 1.0 - (0.2 * 2.0) = 0.6
        self.assertAlmostEqual(self.modifier.calculate_multiplier(3), 0.6)
        
        # 3. 10 скипов = множитель 0.4 (работает ограничитель)
        # log2(11) ≈ 3.45 -> 1.0 - 0.69 = 0.31 -> max вернет 0.4
        self.assertAlmostEqual(self.modifier.calculate_multiplier(10), 0.4)

    def test_category_ranking_penalty(self):
        base_recs = [
            {"item_id": 1, "category_id": "Laptops", "base_score": 100.0},
            {"item_id": 2, "category_id": "Smartphones", "base_score": 80.0},
            {"item_id": 3, "category_id": "Accessories", "base_score": 50.0},
        ]
        
        # Пользователь случайно открыл и сразу закрыл 3 ноутбука подряд (< 2000ms ms)
        for _ in range(3):
            self.tracker.register_view(self.user_id, "Laptops", 1500) 
            
        # Пользователь долго смотрел смартфон
        self.tracker.register_view(self.user_id, "Smartphones", 45000) 
        
        final_recs = self.modifier.apply_penalties(base_recs, self.user_id)
        
        # Ожидания:
        # Laptops: 100.0 * 0.6 = 60.0
        # Smartphones: 80.0 * 1.0 = 80.0
        # Accessories: 50.0 * 1.0 = 50.0
        
        # Элементы должны поменяться местами
        self.assertEqual(final_recs[0]["category_id"], "Smartphones") # Вылез на 1 место
        self.assertEqual(final_recs[1]["category_id"], "Laptops")     # Упал на 2 место
        self.assertEqual(final_recs[2]["category_id"], "Accessories") # Остался на 3 месте
        
        self.assertAlmostEqual(final_recs[1]["final_score"], 60.0)

if __name__ == '__main__':
    unittest.main()

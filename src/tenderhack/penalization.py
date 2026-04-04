import math
import time
from typing import List, Dict, Protocol, Tuple

class SkipStorage(Protocol):
    def increment_skip(self, user_id: str, category_id: str) -> int:
        pass
    
    def get_skips(self, user_id: str, category_id: str) -> int:
        pass

class InMemorySkipStorage(SkipStorage):
    def __init__(self, ttl_seconds: int = 4 * 3600):
        # Dictionary structure: { "user:{id}:cat:{id}:skips": (count, expiration_timestamp) }
        self._store: Dict[str, Tuple[int, float]] = {}
        self._ttl_seconds = ttl_seconds

    def _make_key(self, user_id: str, category_id: str) -> str:
        return f"user:{user_id}:cat:{category_id}:skips"

    def increment_skip(self, user_id: str, category_id: str) -> int:
        current_time = time.time()
        key = self._make_key(user_id, category_id)
        count, exp = self._store.get(key, (0, 0.0))
        
        # Если счетчик протух (TTL истек), начинаем с нуля
        if 0 < exp < current_time:
            count = 0
            
        new_count = count + 1
        # Обновляем счетчик и продлеваем TTL на 4 часа после последнего скипа
        self._store[key] = (new_count, current_time + self._ttl_seconds)
        return new_count

    def get_skips(self, user_id: str, category_id: str) -> int:
        current_time = time.time()
        key = self._make_key(user_id, category_id)
        count, exp = self._store.get(key, (0, 0.0))
        
        # Игнорируем протухшие значения
        if 0 < exp < current_time:
            return 0
        return count


class InteractionTracker:
    # Порог времени до которого закрытие считается скипом
    SKIP_THRESHOLD_MS = 2000 
    
    def __init__(self, storage: SkipStorage):
        self.storage = storage

    def register_view(self, user_id: str, category_id: str, dwell_time_ms: int) -> None:
        if dwell_time_ms < self.SKIP_THRESHOLD_MS:
            self.storage.increment_skip(user_id, category_id)


class RankingModifier:
    def __init__(self, storage: SkipStorage):
        self.storage = storage

    def calculate_multiplier(self, skip_count: int) -> float:
        if skip_count == 0:
            return 1.0
        
        penalty = 0.2 * math.log2(1 + skip_count)
        return max(0.4, 1.0 - penalty)

    def apply_penalties(self, recommendations: List[Dict], user_id: str) -> List[Dict]:
        result = []
        for rec in recommendations:
            cat_id = rec.get("category_id") or rec.get("category")
            if not cat_id:
                # Fallback, just append as is if no category
                result.append(rec)
                continue

            base_score = float(rec.get("base_score", rec.get("search_score", 0.0)))
            skip_count = self.storage.get_skips(user_id, str(cat_id))
            multiplier = self.calculate_multiplier(skip_count)
            
            updated_rec = dict(rec)
            # Если уже есть final_score, умножаем его, иначе base_score
            current_score = float(rec.get("final_score", base_score))
            updated_rec["final_score"] = current_score * multiplier
            
            # Для отладки и UI сохраняем множитель
            updated_rec["penalty_multiplier"] = multiplier
            if multiplier < 1.0:
                updated_rec["reason_to_hide"] = f"Пессимизация категории. Множитель: {multiplier:.2f} (Скипов: {skip_count})"

            result.append(updated_rec)
            
        result.sort(key=lambda x: x.get("final_score", 0.0), reverse=True)
        return result

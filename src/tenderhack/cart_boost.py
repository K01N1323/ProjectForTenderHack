"""
cart_boost.py
=============
Soft-boosting для товаров из категорий, которые пользователь добавил в корзину.

Бизнес-логика
-------------
В B2B корзина — шорт-лист, а не гарантия покупки. Поэтому бонус за
добавление в корзину имеет асимптотический потолок: при любом числе добавлений
множитель стремится к 1+M, но никогда его не достигает.

Формула
-------
    multiplier = 1.0 + M * (1.0 - exp(-k * cart_adds))

Параметры по умолчанию:
    M = 0.15   # максимальный бонус 15 %
    k = 1.0    # скорость насыщения (при 1 добавлении уже ~9.5 % буста)

Комбинирование с RankingModifier (пессимизация)
------------------------------------------------
Порядок применения:
    1. CartBoostModifier.apply_boost(results, user_id)   — буст за корзину
    2. RankingModifier.apply_penalties(results, user_id) — штраф за скипы

Это гарантирует, что штраф за скипы можно применить поверх уже бустированного
final_score.
"""
from __future__ import annotations

import math
import time
import threading
from typing import Dict, List, Protocol, Tuple


# ---------------------------------------------------------------------------
# Storage protocol — поддерживает in-memory и Redis-совместимые бэкенды
# ---------------------------------------------------------------------------

class CartStorage(Protocol):
    def increment_cart(self, user_id: str, category_id: str) -> int:
        """Инкрементировать счётчик добавлений в корзину. Вернуть новое значение."""
        ...

    def decrement_cart(self, user_id: str, category_id: str) -> int:
        """Декрементировать счётчик (не ниже 0). Вернуть новое значение."""
        ...

    def get_cart_adds(self, user_id: str, category_id: str) -> int:
        """Получить текущий счётчик добавлений."""
        ...

    def get_bulk_cart_adds(self, user_id: str, category_ids: List[str]) -> Dict[str, int]:
        """Получить счётчики для нескольких категорий за один вызов."""
        ...


# ---------------------------------------------------------------------------
# In-memory backend (prod-ready: thread-safe, TTL 7 дней)
# ---------------------------------------------------------------------------

_CART_TTL_SECONDS: int = 7 * 24 * 3600  # 7 days


class InMemoryCartStorage:
    """Thread-safe хранилище счётчиков корзины в памяти процесса.

    Структура: { key -> (count: int, expires_at: float) }
    Ключ: ``user:{user_id}:cat:{category_id}:cart``
    """

    def __init__(self, ttl_seconds: int = _CART_TTL_SECONDS) -> None:
        self._store: Dict[str, Tuple[int, float]] = {}
        self._lock = threading.Lock()
        self._ttl_seconds = ttl_seconds

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_key(user_id: str, category_id: str) -> str:
        return f"user:{user_id}:cat:{category_id}:cart"

    def _get_live_count(self, key: str, now: float) -> int:
        """Вернуть живое значение счётчика (0 если просрочен)."""
        count, exp = self._store.get(key, (0, 0.0))
        if exp > 0 and exp < now:
            # TTL истёк — удаляем lazy
            self._store.pop(key, None)
            return 0
        return count

    def _set(self, key: str, count: int, now: float) -> None:
        """Записать счётчик с обновлённым TTL."""
        self._store[key] = (count, now + self._ttl_seconds)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def increment_cart(self, user_id: str, category_id: str) -> int:
        now = time.time()
        key = self._make_key(user_id, category_id)
        with self._lock:
            count = self._get_live_count(key, now) + 1
            self._set(key, count, now)
        return count

    def decrement_cart(self, user_id: str, category_id: str) -> int:
        now = time.time()
        key = self._make_key(user_id, category_id)
        with self._lock:
            count = max(0, self._get_live_count(key, now) - 1)
            if count == 0:
                self._store.pop(key, None)
            else:
                self._set(key, count, now)
        return count

    def get_cart_adds(self, user_id: str, category_id: str) -> int:
        now = time.time()
        key = self._make_key(user_id, category_id)
        with self._lock:
            return self._get_live_count(key, now)

    def get_bulk_cart_adds(self, user_id: str, category_ids: List[str]) -> Dict[str, int]:
        now = time.time()
        with self._lock:
            return {
                cat_id: self._get_live_count(self._make_key(user_id, cat_id), now)
                for cat_id in category_ids
            }


# ---------------------------------------------------------------------------
# Redis-backed storage (опционально — требует redis-py ≥ 4)
# ---------------------------------------------------------------------------

class RedisCartStorage:
    """Redis-бэкенд для хранилища счётчиков корзины.

    Использует атомарные команды INCR / DECR + EXPIRE.
    Совместим с любым redis-py клиентом (sync).
    """

    def __init__(self, redis_client: object, ttl_seconds: int = _CART_TTL_SECONDS) -> None:
        self._r = redis_client
        self._ttl = ttl_seconds

    @staticmethod
    def _make_key(user_id: str, category_id: str) -> str:
        return f"user:{user_id}:cat:{category_id}:cart"

    def increment_cart(self, user_id: str, category_id: str) -> int:
        key = self._make_key(user_id, category_id)
        count = int(self._r.incr(key))  # type: ignore[attr-defined]
        self._r.expire(key, self._ttl)  # type: ignore[attr-defined]
        return count

    def decrement_cart(self, user_id: str, category_id: str) -> int:
        key = self._make_key(user_id, category_id)
        current = self._r.get(key)  # type: ignore[attr-defined]
        if current is None or int(current) <= 0:
            return 0
        count = int(self._r.decr(key))  # type: ignore[attr-defined]
        if count < 0:
            # На случай гонки — фиксируем в 0
            self._r.set(key, 0, ex=self._ttl)  # type: ignore[attr-defined]
            return 0
        self._r.expire(key, self._ttl)  # type: ignore[attr-defined]
        return count

    def get_cart_adds(self, user_id: str, category_id: str) -> int:
        key = self._make_key(user_id, category_id)
        raw = self._r.get(key)  # type: ignore[attr-defined]
        return int(raw) if raw is not None else 0

    def get_bulk_cart_adds(self, user_id: str, category_ids: List[str]) -> Dict[str, int]:
        if not category_ids:
            return {}
        keys = [self._make_key(user_id, cat_id) for cat_id in category_ids]
        # Redis MGET — один round-trip
        values = self._r.mget(*keys)  # type: ignore[attr-defined]
        return {
            cat_id: (int(v) if v is not None else 0)
            for cat_id, v in zip(category_ids, values)
        }


# ---------------------------------------------------------------------------
# CartBoostModifier — применяет асимптотический буст к final_score
# ---------------------------------------------------------------------------

class CartBoostModifier:
    """Повышающий модификатор ранжирования на основе добавлений в корзину.

    Формула
    -------
        multiplier = 1.0 + M * (1.0 - exp(-k * cart_adds))

    Свойства:
    - cart_adds == 0  →  multiplier == 1.0       (нет буста)
    - cart_adds → ∞   →  multiplier → 1.0 + M    (асимптота)

    Параметры
    ----------
    storage : CartStorage
        Хранилище счётчиков (InMemoryCartStorage или RedisCartStorage).
    max_boost : float
        M — максимальный бонус. По умолчанию 0.15 (15 %).
    saturation_rate : float
        k — скорость насыщения. По умолчанию 1.0.
    """

    DEFAULT_MAX_BOOST: float = 0.15      # M
    DEFAULT_SATURATION_RATE: float = 1.0  # k

    def __init__(
        self,
        storage: CartStorage,
        max_boost: float = DEFAULT_MAX_BOOST,
        saturation_rate: float = DEFAULT_SATURATION_RATE,
    ) -> None:
        self.storage = storage
        self.max_boost = max_boost
        self.saturation_rate = saturation_rate

    def calculate_multiplier(self, cart_adds: int) -> float:
        """Чистая функция — вычислить множитель по числу добавлений.

        Можно тестировать изолированно без зависимости от хранилища.

            multiplier = 1.0 + M * (1.0 - exp(-k * n))
        """
        if cart_adds <= 0:
            return 1.0
        return 1.0 + self.max_boost * (1.0 - math.exp(-self.saturation_rate * cart_adds))

    def apply_boost(
        self,
        recommendations: List[Dict],
        user_id: str,
    ) -> List[Dict]:
        """Применить буст корзины к списку результатов поиска.

        Читает счётчики для всех уникальных категорий одним bulk-запросом.
        Модифицирует ``final_score`` in-place (создаёт копию записи).

        Args:
            recommendations: Список результатов поиска.
                             Каждый элемент — dict с ключами:
                             ``category`` / ``category_id``, ``final_score``
                             (или ``search_score`` как fallback).
            user_id: Идентификатор пользователя.

        Returns:
            Тот же список (мутированные копии записей), отсортированный
            по ``final_score`` по убыванию.
        """
        # Собираем уникальные категории
        category_ids: List[str] = list({
            str(rec.get("category_id") or rec.get("category") or "")
            for rec in recommendations
            if rec.get("category_id") or rec.get("category")
        })

        if not category_ids:
            return recommendations

        # Один bulk-запрос в хранилище
        cart_counts = self.storage.get_bulk_cart_adds(user_id, category_ids)

        result: List[Dict] = []
        for rec in recommendations:
            cat_id = str(rec.get("category_id") or rec.get("category") or "")
            adds = cart_counts.get(cat_id, 0)

            if adds == 0:
                result.append(rec)
                continue

            multiplier = self.calculate_multiplier(adds)
            updated = dict(rec)

            # Применяем к final_score; если его нет — берём search_score
            base = float(rec.get("final_score", rec.get("search_score", 0.0)))
            updated["final_score"] = round(base * multiplier, 6)
            updated["cart_boost_multiplier"] = round(multiplier, 6)
            updated["cart_adds"] = adds

            # Проставляем reason-code — его подхватит _map_reason_to_show
            # и покажет метку "Продолжить подбор в этой категории"
            existing_codes: list = list(rec.get("top_reason_codes") or [])
            if "SESSION_CART_BOOST" not in existing_codes:
                existing_codes = ["SESSION_CART_BOOST"] + existing_codes
            updated["top_reason_codes"] = existing_codes

            result.append(updated)

        # Сортируем по итоговому final_score
        result.sort(key=lambda x: float(x.get("final_score", 0.0)), reverse=True)
        return result

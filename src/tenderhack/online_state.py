from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from .cache import CacheService
from .text import normalize_text, unique_preserve_order


SESSION_KEEP_LIMIT = 20


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _dedupe_trim(values: List[str], limit: int = SESSION_KEEP_LIMIT) -> List[str]:
    return unique_preserve_order([value for value in values if value])[:limit]


@dataclass
class OnlineStateService:
    cache_service: CacheService
    session_ttl_seconds: int = 86400

    def get_session_state(
        self,
        *,
        user_id: Optional[str],
        customer_inn: Optional[str] = None,
        customer_region: Optional[str] = None,
    ) -> Dict[str, object]:
        state = self._load_session_state(
            user_id=user_id,
            customer_inn=customer_inn,
            customer_region=customer_region,
        )
        return state

    def record_event(
        self,
        *,
        user_id: Optional[str],
        customer_inn: Optional[str],
        customer_region: Optional[str],
        event_type: str,
        ste_id: Optional[str] = None,
        category: Optional[str] = None,
        duration_ms: Optional[int] = None,
    ) -> Dict[str, object]:
        state = self._load_session_state(
            user_id=user_id,
            customer_inn=customer_inn,
            customer_region=customer_region,
        )
        event_type = str(event_type or "").strip().lower()
        normalized_category = normalize_text(category or "")
        ste_id = str(ste_id or "").strip()
        duration_ms = int(duration_ms or 0)

        event_counts = {str(key): int(value) for key, value in dict(state.get("event_counts", {})).items()}
        event_counts[event_type] = event_counts.get(event_type, 0) + 1
        state["event_counts"] = event_counts
        state["last_event_type"] = event_type
        state["last_event_at"] = _utc_now_iso()
        state["version"] = int(state.get("version", 0) or 0) + 1

        recent_categories = [normalize_text(value) for value in state.get("recent_categories", [])]
        clicked_ste_ids = [str(value) for value in state.get("clicked_ste_ids", [])]
        cart_ste_ids = [str(value) for value in state.get("cart_ste_ids", [])]
        bounced_categories = [normalize_text(value) for value in state.get("bounced_categories", [])]

        if event_type in {"search_result_click", "item_open", "item_click"}:
            if ste_id:
                clicked_ste_ids = _dedupe_trim([ste_id, *clicked_ste_ids])
            if normalized_category:
                recent_categories = _dedupe_trim([normalized_category, *recent_categories])

        if event_type == "cart_add":
            if ste_id:
                cart_ste_ids = _dedupe_trim([ste_id, *cart_ste_ids])
            if normalized_category:
                recent_categories = _dedupe_trim([normalized_category, *recent_categories])

        if event_type == "cart_remove" and ste_id:
            cart_ste_ids = [value for value in cart_ste_ids if value != ste_id]

        is_bounce = event_type == "bounce" or (event_type == "item_close" and 0 < duration_ms < 3000)
        if is_bounce and normalized_category:
            bounced_categories = _dedupe_trim([normalized_category, *bounced_categories])

        if event_type == "purchase" and normalized_category:
            recent_categories = _dedupe_trim([normalized_category, *recent_categories])
            bounced_categories = [value for value in bounced_categories if value != normalized_category]

        state["recent_categories"] = recent_categories
        state["clicked_ste_ids"] = clicked_ste_ids
        state["cart_ste_ids"] = cart_ste_ids
        state["bounced_categories"] = bounced_categories

        self._store_session_state(user_id=user_id, state=state)
        return state

    def _load_session_state(
        self,
        *,
        user_id: Optional[str],
        customer_inn: Optional[str],
        customer_region: Optional[str],
    ) -> Dict[str, object]:
        state = self._empty_state(
            user_id=user_id,
            customer_inn=customer_inn,
            customer_region=customer_region,
        )
        if not user_id or not self.cache_service.enabled:
            return state

        cache_key = self.cache_service.build_key("session", suffix=str(user_id))
        cached_payload = self.cache_service.get_json(cache_key)
        if not isinstance(cached_payload, dict):
            return state

        state.update(cached_payload)
        if customer_inn:
            state["customer_inn"] = customer_inn
        if customer_region:
            state["customer_region"] = customer_region
        state["recent_categories"] = _dedupe_trim([normalize_text(value) for value in state.get("recent_categories", [])])
        state["clicked_ste_ids"] = _dedupe_trim([str(value) for value in state.get("clicked_ste_ids", [])])
        state["cart_ste_ids"] = _dedupe_trim([str(value) for value in state.get("cart_ste_ids", [])])
        state["bounced_categories"] = _dedupe_trim([normalize_text(value) for value in state.get("bounced_categories", [])])
        state["event_counts"] = {str(key): int(value) for key, value in dict(state.get("event_counts", {})).items()}
        state["version"] = int(state.get("version", 0) or 0)
        return state

    def _store_session_state(self, *, user_id: Optional[str], state: Dict[str, object]) -> None:
        if not user_id or not self.cache_service.enabled:
            return
        cache_key = self.cache_service.build_key("session", suffix=str(user_id))
        self.cache_service.set_json(cache_key, state, ttl_seconds=self.session_ttl_seconds)

    @staticmethod
    def _empty_state(
        *,
        user_id: Optional[str],
        customer_inn: Optional[str],
        customer_region: Optional[str],
    ) -> Dict[str, object]:
        return {
            "user_id": user_id or "anonymous",
            "customer_inn": customer_inn or "",
            "customer_region": customer_region or "",
            "recent_categories": [],
            "clicked_ste_ids": [],
            "cart_ste_ids": [],
            "bounced_categories": [],
            "event_counts": {},
            "last_event_type": None,
            "last_event_at": None,
            "version": 0,
        }

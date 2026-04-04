from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Optional

try:
    import redis as redis_module
except Exception:  # pragma: no cover - optional dependency
    redis_module = None


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


class _MemoryBackend:
    def __init__(self) -> None:
        self._store: dict[str, tuple[Optional[float], str]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[str]:
        now = time.time()
        with self._lock:
            payload = self._store.get(key)
            if payload is None:
                return None
            expires_at, value = payload
            if expires_at is not None and expires_at <= now:
                self._store.pop(key, None)
                return None
            return value

    def setex(self, key: str, ttl_seconds: int, value: str) -> None:
        expires_at = time.time() + max(1, int(ttl_seconds))
        with self._lock:
            self._store[key] = (expires_at, value)

    def delete(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def close(self) -> None:
        return None


@dataclass
class CacheService:
    url: Optional[str] = None
    prefix: str = "tenderhack"

    def __post_init__(self) -> None:
        self.backend_name = "none"
        self.enabled = False
        self._client: Any = None

        if self.url == "memory://":
            self._client = _MemoryBackend()
            self.backend_name = "memory"
            self.enabled = True
            return

        if not self.url or redis_module is None:
            return

        try:
            client = redis_module.Redis.from_url(self.url, decode_responses=True)
            client.ping()
        except Exception:
            return

        self._client = client
        self.backend_name = "redis"
        self.enabled = True

    def close(self) -> None:
        if self._client is None:
            return
        close = getattr(self._client, "close", None)
        if callable(close):
            close()

    def build_key(self, namespace: str, *, data: Optional[Any] = None, suffix: Optional[str] = None) -> str:
        parts = [self.prefix, namespace]
        if suffix:
            parts.append(suffix)
        if data is not None:
            digest = sha256(_stable_json(data).encode("utf-8")).hexdigest()
            parts.append(digest)
        return ":".join(parts)

    def get_json(self, key: str) -> Optional[Any]:
        if not self.enabled or self._client is None:
            return None
        try:
            payload = self._client.get(key)
        except Exception:
            return None
        if not payload:
            return None
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return None

    def set_json(self, key: str, value: Any, ttl_seconds: int) -> None:
        if not self.enabled or self._client is None:
            return
        try:
            self._client.setex(key, max(1, int(ttl_seconds)), _stable_json(value))
        except Exception:
            return

    def delete(self, key: str) -> None:
        if not self.enabled or self._client is None:
            return
        try:
            self._client.delete(key)
        except Exception:
            return

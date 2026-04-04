from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import List, Literal, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from tenderhack.cache import CacheService
from tenderhack.descriptions import CatalogDescriptionService
from tenderhack.online_state import OnlineStateService
from tenderhack.offers import OfferLookupService
from tenderhack.personalization import PersonalizationService
from tenderhack.personalization_runtime import PersonalizationRuntimeService
from tenderhack.search import SearchService
from tenderhack.text import normalize_text, tokenize, unique_preserve_order


@dataclass
class AppSettings:
    search_db_path: Path = PROJECT_ROOT / "data" / "processed" / "tenderhack_search.sqlite"
    preprocessed_db_path: Path = PROJECT_ROOT / "data" / "processed" / "tenderhack_preprocessed.sqlite"
    synonyms_path: Path = PROJECT_ROOT / "data" / "reference" / "search_synonyms.json"
    fasttext_model_path: Path = PROJECT_ROOT / "data" / "processed" / "tenderhack_fasttext.bin"
    personalization_model_path: Path = PROJECT_ROOT / "artifacts" / "personalization_model.cbm"
    raw_ste_catalog_path: Path = PROJECT_ROOT / "СТЕ_20260403.csv"
    redis_url: Optional[str] = "memory://"
    semantic_backend: str = "auto"
    login_cache_ttl_seconds: int = 1800
    search_cache_ttl_seconds: int = 120
    suggestions_cache_ttl_seconds: int = 300
    user_profile_cache_ttl_seconds: int = 1800
    offer_lookup_cache_ttl_seconds: int = 1800
    session_state_ttl_seconds: int = 86400

    @classmethod
    def from_env(cls) -> "AppSettings":
        return cls(
            search_db_path=Path(os.getenv("TENDERHACK_SEARCH_DB", cls.search_db_path)),
            preprocessed_db_path=Path(os.getenv("TENDERHACK_PREPROCESSED_DB", cls.preprocessed_db_path)),
            synonyms_path=Path(os.getenv("TENDERHACK_SYNONYMS_PATH", cls.synonyms_path)),
            fasttext_model_path=Path(os.getenv("TENDERHACK_FASTTEXT_MODEL_PATH", cls.fasttext_model_path)),
            personalization_model_path=Path(
                os.getenv("TENDERHACK_PERSONALIZATION_MODEL_PATH", cls.personalization_model_path)
            ),
            raw_ste_catalog_path=Path(os.getenv("TENDERHACK_RAW_STE_CATALOG_PATH", cls.raw_ste_catalog_path)),
            redis_url=os.getenv("TENDERHACK_REDIS_URL") or cls.redis_url,
            semantic_backend=os.getenv("TENDERHACK_SEMANTIC_BACKEND", cls.semantic_backend),
            login_cache_ttl_seconds=int(os.getenv("TENDERHACK_LOGIN_CACHE_TTL_SECONDS", cls.login_cache_ttl_seconds)),
            search_cache_ttl_seconds=int(
                os.getenv("TENDERHACK_SEARCH_CACHE_TTL_SECONDS", cls.search_cache_ttl_seconds)
            ),
            suggestions_cache_ttl_seconds=int(
                os.getenv("TENDERHACK_SUGGESTIONS_CACHE_TTL_SECONDS", cls.suggestions_cache_ttl_seconds)
            ),
            user_profile_cache_ttl_seconds=int(
                os.getenv("TENDERHACK_USER_PROFILE_CACHE_TTL_SECONDS", cls.user_profile_cache_ttl_seconds)
            ),
            offer_lookup_cache_ttl_seconds=int(
                os.getenv("TENDERHACK_OFFER_LOOKUP_CACHE_TTL_SECONDS", cls.offer_lookup_cache_ttl_seconds)
            ),
            session_state_ttl_seconds=int(
                os.getenv("TENDERHACK_SESSION_STATE_TTL_SECONDS", cls.session_state_ttl_seconds)
            ),
        )


class LoginRequest(BaseModel):
    inn: str = Field(min_length=1)


class UserPayload(BaseModel):
    id: str
    inn: str
    region: str
    viewedCategories: List[str] = Field(default_factory=list)


class SearchUserContext(BaseModel):
    id: Optional[str] = None
    inn: Optional[str] = None
    region: Optional[str] = None
    viewedCategories: List[str] = Field(default_factory=list)


class SearchRequest(BaseModel):
    query: str = Field(min_length=1)
    userContext: Optional[SearchUserContext] = None
    viewedCategories: List[str] = Field(default_factory=list)
    bouncedCategories: List[str] = Field(default_factory=list)
    topK: int = Field(default=20, ge=1, le=50)


class ProductPayload(BaseModel):
    id: str
    name: str
    category: str
    price: float
    supplierInn: str
    descriptionPreview: Optional[str] = None
    reasonToShow: Optional[str] = None


class SearchResponsePayload(BaseModel):
    items: List[ProductPayload]
    totalCount: int
    correctedQuery: Optional[str] = None


class EventRequest(BaseModel):
    userId: Optional[str] = None
    inn: Optional[str] = None
    region: Optional[str] = None
    eventType: Literal[
        "search_result_click",
        "item_open",
        "item_close",
        "bounce",
        "cart_add",
        "cart_remove",
        "purchase",
        "item_click",
    ]
    steId: Optional[str] = None
    category: Optional[str] = None
    durationMs: Optional[int] = Field(default=None, ge=0)


class EventResponsePayload(BaseModel):
    status: str
    userId: str
    sessionVersion: int
    recentCategories: List[str] = Field(default_factory=list)
    clickedSteIds: List[str] = Field(default_factory=list)
    cartSteIds: List[str] = Field(default_factory=list)
    bouncedCategories: List[str] = Field(default_factory=list)


class TenderHackApiService:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self._validate_required_paths()
        self.cache_service = CacheService(url=settings.redis_url, prefix="tenderhack")
        self.description_service = CatalogDescriptionService(raw_catalog_path=settings.raw_ste_catalog_path)
        self.online_state_service = OnlineStateService(
            cache_service=self.cache_service,
            session_ttl_seconds=settings.session_state_ttl_seconds,
        )
        self.search_service = SearchService(
            search_db_path=settings.search_db_path,
            synonyms_path=settings.synonyms_path,
            semantic_backend=settings.semantic_backend,
            fasttext_model_path=settings.fasttext_model_path,
        )
        self.personalization_service = PersonalizationService(db_path=settings.preprocessed_db_path)
        self.personalization_runtime_service = PersonalizationRuntimeService(
            db_path=settings.preprocessed_db_path,
            model_path=settings.personalization_model_path,
            cache_service=self.cache_service,
            base_profile_ttl_seconds=settings.user_profile_cache_ttl_seconds,
        )
        self.offer_lookup_service = OfferLookupService(
            db_path=settings.preprocessed_db_path,
            cache_service=self.cache_service,
            lookup_ttl_seconds=settings.offer_lookup_cache_ttl_seconds,
        )

    def close(self) -> None:
        self.search_service.close()
        self.personalization_service.close()
        self.personalization_runtime_service.close()
        self.offer_lookup_service.close()
        self.description_service.close()
        self.cache_service.close()

    def _validate_required_paths(self) -> None:
        missing = [
            str(path)
            for path in [self.settings.search_db_path, self.settings.preprocessed_db_path, self.settings.synonyms_path]
            if not Path(path).exists()
        ]
        if missing:
            raise FileNotFoundError(
                "Missing required search assets:\n" + "\n".join(f"- {path}" for path in missing)
            )

    def login(self, inn: str) -> UserPayload:
        cache_key = self.cache_service.build_key("login", data={"inn": inn})
        cached_payload = self.cache_service.get_json(cache_key)
        if isinstance(cached_payload, dict):
            return UserPayload(**cached_payload)

        profile = self.personalization_service.build_customer_profile(customer_inn=inn)
        viewed_categories = [item["category"] for item in profile.get("top_categories", [])[:5]]
        region = str(profile.get("customer_region") or "")
        payload = UserPayload(
            id=f"user-{inn}",
            inn=inn,
            region=region,
            viewedCategories=viewed_categories,
        )
        self.cache_service.set_json(cache_key, self._model_dump(payload), ttl_seconds=self.settings.login_cache_ttl_seconds)
        return payload

    def search(self, payload: SearchRequest) -> SearchResponsePayload:
        user_context = payload.userContext or SearchUserContext()
        server_session = self.online_state_service.get_session_state(
            user_id=user_context.id or (f"user-{user_context.inn}" if user_context.inn else None),
            customer_inn=user_context.inn,
            customer_region=user_context.region,
        )
        session_categories = unique_preserve_order(
            list(server_session.get("recent_categories", [])) + payload.viewedCategories + user_context.viewedCategories
        )
        bounced_categories = {
            normalize_text(value)
            for value in [*server_session.get("bounced_categories", []), *payload.bouncedCategories]
            if value
        }
        merged_session_state = {
            "recent_categories": session_categories,
            "clicked_ste_ids": list(server_session.get("clicked_ste_ids", [])),
            "cart_ste_ids": list(server_session.get("cart_ste_ids", [])),
            "bounced_categories": list(bounced_categories),
            "version": int(server_session.get("version", 0) or 0),
        }

        cache_key = self.cache_service.build_key(
            "search",
            data=self._search_cache_data(payload, server_session=merged_session_state),
        )
        cached_payload = self.cache_service.get_json(cache_key)
        if isinstance(cached_payload, dict):
            return SearchResponsePayload(**cached_payload)

        raw_payload = self.search_service.search(query=payload.query, top_k=max(payload.topK * 5, 60))
        results = list(raw_payload["results"])

        if user_context.inn or user_context.region or session_categories:
            personalization_query = (
                raw_payload["query"].get("corrected_query")
                or raw_payload["query"].get("normalized_query")
                or payload.query
            )
            results = self.personalization_runtime_service.rerank_candidates(
                query=str(personalization_query),
                candidates=results,
                user_id=user_context.id or (f"user-{user_context.inn}" if user_context.inn else "anonymous"),
                customer_inn=user_context.inn,
                customer_region=user_context.region,
                session_categories=session_categories,
                session_state=merged_session_state,
            )
        else:
            for item in results:
                item["final_score"] = item.get("search_score", 0.0)
                item["top_reason_codes"] = []
                item["reasons"] = ["оставлено выше за счёт базовой текстовой релевантности"]

        for item in results:
            category_norm = normalize_text(str(item.get("category", "")))
            if category_norm and category_norm in bounced_categories:
                item["final_score"] = round(float(item.get("final_score", item.get("search_score", 0.0))) - 100.0, 4)
                item["reason_to_hide"] = "Категория пессимизирована после быстрого отказа"

        results.sort(
            key=lambda item: (
                float(item.get("final_score", item.get("search_score", 0.0))),
                float(item.get("search_score", 0.0)),
            ),
            reverse=True,
        )

        ste_ids = [str(item["ste_id"]) for item in results[: payload.topK * 2]]
        offer_lookup = self.offer_lookup_service.get_offer_lookup(ste_ids)
        description_lookup = self.description_service.get_previews(
            ste_ids,
            fallback_by_ste_id={
                str(item["ste_id"]): {
                    "attribute_keys": str(item.get("attribute_keys") or ""),
                }
                for item in results[: payload.topK * 2]
            },
        )

        products: List[ProductPayload] = []
        for item in results:
            if len(products) >= payload.topK:
                break
            ste_id = str(item["ste_id"])
            offer = offer_lookup.get(ste_id, {})
            reason_to_show = self._map_reason_to_show(
                reason_codes=item.get("top_reason_codes", []),
                category=str(item.get("category") or ""),
                session_categories=session_categories,
                is_bounced=bool(item.get("reason_to_hide")),
            )
            products.append(
                ProductPayload(
                    id=ste_id,
                    name=str(item.get("clean_name") or item.get("normalized_name") or ste_id),
                    category=str(item.get("category") or ""),
                    price=round(float(offer.get("price", 0.0)), 2),
                    supplierInn=str(offer.get("supplier_inn") or "не указан"),
                    descriptionPreview=description_lookup.get(ste_id),
                    reasonToShow=reason_to_show,
                )
            )

        corrected_query = raw_payload["query"].get("corrected_query") or None
        normalized_query = raw_payload["query"].get("normalized_query") or None
        if corrected_query == normalized_query:
            corrected_query = None

        response_payload = SearchResponsePayload(
            items=products,
            totalCount=len(results),
            correctedQuery=corrected_query,
        )
        self.cache_service.set_json(
            cache_key,
            self._model_dump(response_payload),
            ttl_seconds=self.settings.search_cache_ttl_seconds,
        )
        return response_payload

    def record_event(self, payload: EventRequest) -> EventResponsePayload:
        resolved_user_id = payload.userId or (f"user-{payload.inn}" if payload.inn else "anonymous")
        session_state = self.online_state_service.record_event(
            user_id=resolved_user_id,
            customer_inn=payload.inn,
            customer_region=payload.region,
            event_type=payload.eventType,
            ste_id=payload.steId,
            category=payload.category,
            duration_ms=payload.durationMs,
        )
        return EventResponsePayload(
            status="ok",
            userId=resolved_user_id,
            sessionVersion=int(session_state.get("version", 0) or 0),
            recentCategories=[str(value) for value in session_state.get("recent_categories", [])],
            clickedSteIds=[str(value) for value in session_state.get("clicked_ste_ids", [])],
            cartSteIds=[str(value) for value in session_state.get("cart_ste_ids", [])],
            bouncedCategories=[str(value) for value in session_state.get("bounced_categories", [])],
        )

    def suggestions(self, query: str, top_k: int = 5) -> List[str]:
        cache_key = self.cache_service.build_key("suggestions", data={"query": query, "top_k": top_k})
        cached_payload = self.cache_service.get_json(cache_key)
        if isinstance(cached_payload, list):
            return [str(item) for item in cached_payload]

        payload = self.search_service.search(query=query, top_k=max(top_k * 3, 12))
        suggestions: List[str] = []
        query_payload = payload["query"]
        corrected_query = query_payload.get("corrected_query")
        normalized_query = query_payload.get("normalized_query")
        if corrected_query and corrected_query != normalized_query:
            suggestions.append(corrected_query)
        suggestions.extend(self._build_abstract_suggestions(query=query, query_payload=query_payload, results=payload["results"]))
        result = unique_preserve_order(suggestions)[:top_k]
        self.cache_service.set_json(
            cache_key,
            result,
            ttl_seconds=self.settings.suggestions_cache_ttl_seconds,
        )
        return result

    @staticmethod
    def _model_dump(model: BaseModel) -> dict:
        if hasattr(model, "model_dump"):
            return model.model_dump()
        return model.dict()

    @staticmethod
    def _abstract_name_phrase(name: str, query: str) -> str:
        query_tokens = tokenize(query)
        name_tokens = tokenize(name)
        if not name_tokens:
            return ""

        limit = 2 if len(query_tokens) <= 1 else 3
        phrase_tokens: List[str] = []
        for token in name_tokens:
            if token.isdigit() or any(char.isdigit() for char in token):
                break
            phrase_tokens.append(token)
            if len(phrase_tokens) >= limit:
                break
        if len(phrase_tokens) < 2:
            return ""
        return " ".join(phrase_tokens)

    @staticmethod
    def _compact_category_phrase(category: str) -> str:
        category_tokens = [token for token in tokenize(category) if not token.isdigit()]
        if not category_tokens:
            return ""
        return " ".join(category_tokens[:5])

    @classmethod
    def _build_abstract_suggestions(cls, *, query: str, query_payload: dict, results: List[dict]) -> List[str]:
        query_norm = normalize_text(query)
        expanded_tokens = [str(token) for token in query_payload.get("expanded_tokens", []) if token]
        corrected_query = str(query_payload.get("corrected_query") or "")
        query_tokens = unique_preserve_order(tokenize(query) + tokenize(corrected_query) + expanded_tokens)
        suggestions: List[str] = []

        for synonym_rule in query_payload.get("applied_synonyms", []):
            for target in synonym_rule.get("targets", []):
                candidate = normalize_text(str(target))
                if not candidate or candidate == query_norm:
                    continue
                suggestions.append(candidate)

        for item in results:
            name_phrase = cls._abstract_name_phrase(str(item.get("clean_name") or ""), query)
            category_phrase = cls._compact_category_phrase(str(item.get("category") or ""))

            for candidate in [name_phrase, category_phrase]:
                candidate_norm = normalize_text(candidate)
                if not candidate_norm or candidate_norm == query_norm:
                    continue
                if query_tokens and not any(token in candidate_norm.split() for token in query_tokens):
                    continue
                suggestions.append(candidate)

        return unique_preserve_order(suggestions)

    @staticmethod
    def _search_cache_data(payload: SearchRequest, server_session: Optional[dict] = None) -> dict:
        user_context = payload.userContext or SearchUserContext()
        return {
            "query": payload.query,
            "user_id": user_context.id,
            "user_inn": user_context.inn,
            "user_region": user_context.region,
            "user_viewed_categories": unique_preserve_order([str(value) for value in user_context.viewedCategories if value]),
            "viewed_categories": unique_preserve_order([str(value) for value in payload.viewedCategories if value]),
            "bounced_categories": unique_preserve_order(
                [normalize_text(str(value)) for value in payload.bouncedCategories if value]
            ),
            "top_k": int(payload.topK),
            "server_session_version": int((server_session or {}).get("version", 0) or 0),
            "server_recent_categories": unique_preserve_order(
                [str(value) for value in (server_session or {}).get("recent_categories", []) if value]
            ),
            "server_clicked_ste_ids": unique_preserve_order(
                [str(value) for value in (server_session or {}).get("clicked_ste_ids", []) if value]
            ),
            "server_cart_ste_ids": unique_preserve_order(
                [str(value) for value in (server_session or {}).get("cart_ste_ids", []) if value]
            ),
            "server_bounced_categories": unique_preserve_order(
                [normalize_text(str(value)) for value in (server_session or {}).get("bounced_categories", []) if value]
            ),
        }

    @staticmethod
    def _map_reason_to_show(
        reason_codes: List[str],
        category: str,
        session_categories: List[str],
        is_bounced: bool,
    ) -> Optional[str]:
        if is_bounced:
            return None
        codes = {str(code) for code in reason_codes}
        session_category_set = {normalize_text(value) for value in session_categories if value}
        category_norm = normalize_text(category)

        if "SESSION_CART_BOOST" in codes or "SESSION_CLICK_BOOST" in codes:
            return "Продолжить подбор в этой категории"
        if codes & {"USER_CATEGORY_AFFINITY", "USER_REPEAT_BUY", "RECENT_SIMILAR_PURCHASE", "SUPPLIER_AFFINITY"}:
            return "На основе ваших закупок"
        if "REGIONAL_POPULARITY" in codes:
            return "Популярно в вашем регионе"
        if "SIMILAR_CUSTOMER_POPULARITY" in codes:
            return "Популярно у похожих заказчиков"
        if category_norm and category_norm in session_category_set:
            return "Продолжить подбор в этой категории"
        return None


def create_app(settings: Optional[AppSettings] = None) -> FastAPI:
    active_settings = settings or AppSettings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service = TenderHackApiService(active_settings)
        app.state.service = service
        yield
        service.close()

    app = FastAPI(
        title="TenderHack Search API",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/auth/login", response_model=UserPayload)
    async def login(payload: LoginRequest, request: Request) -> UserPayload:
        return request.app.state.service.login(payload.inn)

    @app.post("/api/search", response_model=SearchResponsePayload)
    async def search(payload: SearchRequest, request: Request) -> SearchResponsePayload:
        return request.app.state.service.search(payload)

    @app.post("/api/event", response_model=EventResponsePayload)
    async def event(payload: EventRequest, request: Request) -> EventResponsePayload:
        return request.app.state.service.record_event(payload)

    @app.get("/api/search/suggestions", response_model=List[str])
    async def suggestions(
        request: Request,
        q: str = Query(min_length=1),
        top_k: int = Query(default=5, ge=1, le=10),
    ) -> List[str]:
        return request.app.state.service.suggestions(query=q, top_k=top_k)

    @app.exception_handler(FileNotFoundError)
    async def file_not_found_handler(_: Request, exc: FileNotFoundError):
        return JSONResponse(status_code=503, content={"detail": str(exc)})

    return app


app = create_app()

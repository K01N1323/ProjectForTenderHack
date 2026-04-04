from __future__ import annotations

import math
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import date
from statistics import median
from typing import Iterable, Optional

from data.personalization_data import ContractRecord, STERecord
from tenderhack.text import normalize_text, stem_tokens, tokenize


MISSING_RECENCY_DAYS = 3650.0
RECENT_WINDOW_DAYS = 365
ROLLING_AMOUNT_WINDOW = 200


FEATURE_SPEC = [
    {"name": "query_name_token_overlap", "scope": "query_candidate", "default": 0.0, "description": "Token overlap between query and STE name."},
    {"name": "query_category_token_overlap", "scope": "query_candidate", "default": 0.0, "description": "Token overlap between query and category."},
    {"name": "query_attribute_token_overlap", "scope": "query_candidate", "default": 0.0, "description": "Token overlap between query and attributes."},
    {"name": "query_token_coverage", "scope": "query_candidate", "default": 0.0, "description": "Coverage of query tokens by candidate fields."},
    {"name": "query_exact_name_match", "scope": "query_candidate", "default": 0.0, "description": "Exact normalized query phrase appears in STE name."},
    {"name": "query_length_tokens", "scope": "query", "default": 0.0, "description": "Number of query tokens."},
    {"name": "candidate_name_length_tokens", "scope": "candidate", "default": 0.0, "description": "Length of candidate STE name in tokens."},
    {"name": "candidate_attribute_count", "scope": "candidate", "default": 0.0, "description": "Number of known candidate attributes."},
    {"name": "query_attribute_match_count", "scope": "query_candidate", "default": 0.0, "description": "Count of query tokens matched in attribute keys."},
    {"name": "user_total_purchase_count", "scope": "user_profile", "default": 0.0, "description": "Total historical purchases of the customer."},
    {"name": "user_category_purchase_count", "scope": "user_profile", "default": 0.0, "description": "Historical purchase count in candidate category."},
    {"name": "user_ste_purchase_count", "scope": "user_profile", "default": 0.0, "description": "Historical purchase count of the candidate STE."},
    {"name": "user_supplier_purchase_count", "scope": "user_profile", "default": 0.0, "description": "Historical purchase count from candidate dominant supplier."},
    {"name": "user_category_purchase_share", "scope": "user_profile", "default": 0.0, "description": "Share of purchases in candidate category."},
    {"name": "user_repeat_buy_signal", "scope": "user_profile", "default": 0.0, "description": "Candidate STE already exists in purchase history."},
    {"name": "user_novelty_signal", "scope": "user_profile", "default": 1.0, "description": "Candidate STE is new for the customer."},
    {"name": "user_last_category_recency_days", "scope": "user_profile", "default": MISSING_RECENCY_DAYS, "description": "Days since last purchase in candidate category."},
    {"name": "user_last_ste_recency_days", "scope": "user_profile", "default": MISSING_RECENCY_DAYS, "description": "Days since last purchase of candidate STE."},
    {"name": "user_last_supplier_recency_days", "scope": "user_profile", "default": MISSING_RECENCY_DAYS, "description": "Days since last purchase from candidate dominant supplier."},
    {"name": "user_recent_30d_purchase_count", "scope": "user_profile", "default": 0.0, "description": "Historical purchases in last 30 days."},
    {"name": "user_recent_90d_purchase_count", "scope": "user_profile", "default": 0.0, "description": "Historical purchases in last 90 days."},
    {"name": "user_recent_30d_category_count", "scope": "user_profile", "default": 0.0, "description": "Historical purchases in category in last 30 days."},
    {"name": "user_recent_90d_category_count", "scope": "user_profile", "default": 0.0, "description": "Historical purchases in category in last 90 days."},
    {"name": "user_avg_amount", "scope": "user_profile", "default": 0.0, "description": "Average contract amount of the user history."},
    {"name": "user_median_amount", "scope": "user_profile", "default": 0.0, "description": "Median contract amount of the user history."},
    {"name": "category_price_p25", "scope": "candidate_prior", "default": 0.0, "description": "25th percentile of historical category price proxy."},
    {"name": "category_price_p75", "scope": "candidate_prior", "default": 0.0, "description": "75th percentile of historical category price proxy."},
    {"name": "candidate_price_proxy", "scope": "candidate_prior", "default": 0.0, "description": "Candidate price proxy from historical contracts."},
    {"name": "candidate_price_vs_user_avg_ratio", "scope": "derived", "default": 0.0, "description": "Candidate price proxy divided by user average amount."},
    {"name": "candidate_price_distance_to_user_median", "scope": "derived", "default": 0.0, "description": "Absolute distance between candidate price proxy and user median amount."},
    {"name": "candidate_price_in_user_range", "scope": "derived", "default": 0.0, "description": "Candidate price is inside category/user habitual range."},
    {"name": "global_ste_popularity", "scope": "candidate_prior", "default": 0.0, "description": "Historical global popularity of candidate STE."},
    {"name": "global_category_popularity", "scope": "candidate_prior", "default": 0.0, "description": "Historical global popularity of candidate category."},
    {"name": "regional_ste_popularity", "scope": "candidate_prior", "default": 0.0, "description": "Historical popularity of candidate STE in the user region."},
    {"name": "regional_category_popularity", "scope": "candidate_prior", "default": 0.0, "description": "Historical popularity of candidate category in the user region."},
    {"name": "similar_customer_ste_popularity", "scope": "candidate_prior", "default": 0.0, "description": "Historical popularity among a lightweight similar-customer segment."},
    {"name": "seasonal_category_popularity", "scope": "candidate_prior", "default": 0.0, "description": "Historical category popularity in the same calendar month."},
    {"name": "days_since_last_similar_purchase", "scope": "user_profile", "default": MISSING_RECENCY_DAYS, "description": "Days since last purchase in the same category."},
    {"name": "candidate_ste_recent_30d_popularity", "scope": "candidate_prior", "default": 0.0, "description": "Recent 30d popularity of candidate STE."},
    {"name": "candidate_category_recent_90d_popularity", "scope": "candidate_prior", "default": 0.0, "description": "Recent 90d popularity of candidate category."},
    {"name": "candidate_primary_supplier_share", "scope": "candidate_prior", "default": 0.0, "description": "Share of the dominant supplier in historical candidate purchases."},
    {"name": "candidate_supplier_affinity", "scope": "derived", "default": 0.0, "description": "Affinity of the user to the candidate dominant supplier."},
    {"name": "candidate_supplier_region_match", "scope": "derived", "default": 0.0, "description": "Dominant supplier region matches the user region."},
    {"name": "candidate_item_kind_affinity", "scope": "derived", "default": 0.0, "description": "Affinity of the user to the candidate item kind."},
]

FEATURE_DEFAULTS = {item["name"]: item["default"] for item in FEATURE_SPEC}

EXPLAIN_RULES = [
    {
        "code": "QUERY_NAME_MATCH",
        "template": "Запрос хорошо совпадает с названием СТЕ",
        "feature": "query_name_token_overlap",
        "threshold": 0.6,
    },
    {
        "code": "USER_CATEGORY_AFFINITY",
        "template": "Заказчик часто закупает эту категорию",
        "feature": "user_category_purchase_share",
        "threshold": 0.2,
    },
    {
        "code": "USER_REPEAT_BUY",
        "template": "Похожий СТЕ уже закупался ранее",
        "feature": "user_repeat_buy_signal",
        "threshold": 0.5,
    },
    {
        "code": "RECENT_SIMILAR_PURCHASE",
        "template": "Похожий СТЕ уже закупался недавно",
        "feature": "user_last_category_recency_days",
        "threshold": 45.0,
        "direction": "lte",
    },
    {
        "code": "SUPPLIER_AFFINITY",
        "template": "Этот поставщик часто встречается в истории закупок",
        "feature": "candidate_supplier_affinity",
        "threshold": 0.1,
    },
    {
        "code": "PRICE_IN_RANGE",
        "template": "Цена близка к типичному диапазону закупок заказчика",
        "feature": "candidate_price_in_user_range",
        "threshold": 0.5,
    },
    {
        "code": "REGIONAL_POPULARITY",
        "template": "СТЕ популярно у заказчиков этого региона",
        "feature": "regional_ste_popularity",
        "threshold": 1.0,
    },
    {
        "code": "SIMILAR_CUSTOMER_POPULARITY",
        "template": "Позиция популярна у похожих заказчиков",
        "feature": "similar_customer_ste_popularity",
        "threshold": 1.0,
    },
]


def _sorted_values(values: Iterable[float]) -> list[float]:
    return sorted(float(value) for value in values if value is not None)


def _quantile(values: Iterable[float], q: float) -> float:
    ordered = _sorted_values(values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return float(ordered[0])
    position = q * (len(ordered) - 1)
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    fraction = position - lower_index
    lower = ordered[lower_index]
    upper = ordered[upper_index]
    return float(lower + (upper - lower) * fraction)


def _overlap_ratio(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / max(1, len(left))


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _recency_days(last_date: Optional[date], current_date: date) -> float:
    if last_date is None:
        return MISSING_RECENCY_DAYS
    return float(max(0, (current_date - last_date).days))


def _count_recent(history: deque[date], current_date: date, window_days: int) -> int:
    return sum(1 for item in history if 0 <= (current_date - item).days <= window_days)


def build_query_context(query: str) -> dict[str, object]:
    normalized_query = normalize_text(query)
    tokens = tokenize(normalized_query)
    stems = stem_tokens(tokens)
    return {
        "query": query,
        "normalized_query": normalized_query,
        "tokens": tokens,
        "token_set": set(tokens),
        "stems": stems,
        "stem_set": set(stems),
        "length_tokens": len(tokens),
    }


def derive_item_kind(name: str, category: str) -> str:
    text = normalize_text(f"{name} {category}")
    service_markers = ("услуг", "обслужив", "страхован", "консультац", "обучен", "аренд", "перевозк")
    work_markers = ("работ", "ремонт", "монтаж", "строител", "установк", "реконструкц", "проектир")
    if any(marker in text for marker in work_markers):
        return "works"
    if any(marker in text for marker in service_markers):
        return "services"
    return "goods"


def generate_pseudo_queries(contract: ContractRecord, ste: STERecord) -> dict[str, str]:
    contract_name = normalize_text(contract.contract_item_name)
    ste_name = normalize_text(ste.clean_name)
    category = normalize_text(ste.category)
    attributes = normalize_text(ste.attribute_keys)
    return {
        "contract_item_name": contract_name or ste_name,
        "ste_name": ste_name or contract_name,
        "contract_plus_category": " ".join(part for part in [contract_name, category] if part).strip(),
        "ste_name_plus_category": " ".join(part for part in [ste_name, category] if part).strip(),
        "ste_name_category_attributes": " ".join(part for part in [ste_name, category, attributes] if part).strip(),
    }


@dataclass
class RollingDistribution:
    maxlen: int = ROLLING_AMOUNT_WINDOW
    values: deque[float] = field(default_factory=lambda: deque(maxlen=ROLLING_AMOUNT_WINDOW))
    total_count: int = 0
    total_sum: float = 0.0

    def add(self, value: float) -> None:
        value = float(value)
        self.total_count += 1
        self.total_sum += value
        self.values.append(value)

    @property
    def mean(self) -> float:
        return self.total_sum / self.total_count if self.total_count else 0.0

    @property
    def median(self) -> float:
        return float(median(self.values)) if self.values else 0.0

    @property
    def p25(self) -> float:
        return _quantile(self.values, 0.25)

    @property
    def p75(self) -> float:
        return _quantile(self.values, 0.75)

    def to_list(self) -> list[float]:
        return [float(value) for value in self.values]

    @classmethod
    def from_values(cls, values: Iterable[float], maxlen: int = ROLLING_AMOUNT_WINDOW) -> "RollingDistribution":
        instance = cls(maxlen=maxlen, values=deque(maxlen=maxlen))
        for value in values:
            instance.add(float(value))
        return instance


@dataclass
class UserHistoryState:
    user_id: str
    customer_region: str = "UNKNOWN"
    total_purchases: int = 0
    total_amount: float = 0.0
    amounts: RollingDistribution = field(default_factory=RollingDistribution)
    category_counts: Counter[str] = field(default_factory=Counter)
    ste_counts: Counter[str] = field(default_factory=Counter)
    supplier_counts: Counter[str] = field(default_factory=Counter)
    item_kind_counts: Counter[str] = field(default_factory=Counter)
    last_purchase_dt: Optional[date] = None
    last_category_purchase_dt: dict[str, date] = field(default_factory=dict)
    last_ste_purchase_dt: dict[str, date] = field(default_factory=dict)
    last_supplier_purchase_dt: dict[str, date] = field(default_factory=dict)
    last_item_kind_purchase_dt: dict[str, date] = field(default_factory=dict)
    recent_purchase_dates: deque[date] = field(default_factory=lambda: deque(maxlen=RECENT_WINDOW_DAYS))
    recent_category_dates: dict[str, deque[date]] = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=RECENT_WINDOW_DAYS)))

    def dominant_category(self) -> str:
        return self.category_counts.most_common(1)[0][0] if self.category_counts else "UNKNOWN"

    def segment_key(self) -> str:
        dominant_category = self.dominant_category()
        dominant_item_kind = self.item_kind_counts.most_common(1)[0][0] if self.item_kind_counts else "unknown"
        return f"{self.customer_region}|{dominant_category}|{dominant_item_kind}"

    def update(self, contract: ContractRecord, ste: STERecord) -> None:
        category = ste.category or "UNKNOWN"
        supplier = contract.supplier_inn or "UNKNOWN"
        item_kind = derive_item_kind(ste.clean_name, ste.category)
        self.customer_region = contract.customer_region or self.customer_region or "UNKNOWN"
        self.total_purchases += 1
        self.total_amount += float(contract.contract_amount)
        self.amounts.add(float(contract.contract_amount))
        self.category_counts[category] += 1
        self.ste_counts[ste.ste_id] += 1
        self.supplier_counts[supplier] += 1
        self.item_kind_counts[item_kind] += 1
        self.last_purchase_dt = contract.contract_date
        self.last_category_purchase_dt[category] = contract.contract_date
        self.last_ste_purchase_dt[ste.ste_id] = contract.contract_date
        self.last_supplier_purchase_dt[supplier] = contract.contract_date
        self.last_item_kind_purchase_dt[item_kind] = contract.contract_date
        self.recent_purchase_dates.append(contract.contract_date)
        self.recent_category_dates[category].append(contract.contract_date)

    def to_profile(self) -> dict[str, object]:
        return {
            "user_id": self.user_id,
            "customer_region": self.customer_region,
            "total_purchases": self.total_purchases,
            "total_amount": round(self.total_amount, 4),
            "recent_amounts": self.amounts.to_list(),
            "category_counts": dict(self.category_counts),
            "ste_counts": dict(self.ste_counts),
            "supplier_counts": dict(self.supplier_counts),
            "item_kind_counts": dict(self.item_kind_counts),
            "last_purchase_dt": self.last_purchase_dt.isoformat() if self.last_purchase_dt else None,
            "last_category_purchase_dt": {key: value.isoformat() for key, value in self.last_category_purchase_dt.items()},
            "last_ste_purchase_dt": {key: value.isoformat() for key, value in self.last_ste_purchase_dt.items()},
            "last_supplier_purchase_dt": {key: value.isoformat() for key, value in self.last_supplier_purchase_dt.items()},
            "last_item_kind_purchase_dt": {key: value.isoformat() for key, value in self.last_item_kind_purchase_dt.items()},
            "recent_purchase_dates": [value.isoformat() for value in self.recent_purchase_dates],
            "recent_category_dates": {
                key: [item.isoformat() for item in values]
                for key, values in self.recent_category_dates.items()
            },
        }

    @classmethod
    def from_profile(cls, payload: Optional[dict[str, object]]) -> "UserHistoryState":
        payload = payload or {}
        instance = cls(
            user_id=str(payload.get("user_id", "UNKNOWN")),
            customer_region=str(payload.get("customer_region", "UNKNOWN") or "UNKNOWN"),
            total_purchases=int(payload.get("total_purchases", 0) or 0),
            total_amount=float(payload.get("total_amount", 0.0) or 0.0),
        )
        instance.amounts = RollingDistribution.from_values(payload.get("recent_amounts", []))
        if instance.total_purchases > 0:
            instance.amounts.total_count = instance.total_purchases
        if instance.total_amount > 0:
            instance.amounts.total_sum = instance.total_amount
        instance.category_counts.update({str(key): int(value) for key, value in dict(payload.get("category_counts", {})).items()})
        instance.ste_counts.update({str(key): int(value) for key, value in dict(payload.get("ste_counts", {})).items()})
        instance.supplier_counts.update({str(key): int(value) for key, value in dict(payload.get("supplier_counts", {})).items()})
        instance.item_kind_counts.update({str(key): int(value) for key, value in dict(payload.get("item_kind_counts", {})).items()})
        instance.last_purchase_dt = _parse_profile_date(payload.get("last_purchase_dt"))
        instance.last_category_purchase_dt = {str(key): _parse_profile_date(value) for key, value in dict(payload.get("last_category_purchase_dt", {})).items() if _parse_profile_date(value)}
        instance.last_ste_purchase_dt = {str(key): _parse_profile_date(value) for key, value in dict(payload.get("last_ste_purchase_dt", {})).items() if _parse_profile_date(value)}
        instance.last_supplier_purchase_dt = {str(key): _parse_profile_date(value) for key, value in dict(payload.get("last_supplier_purchase_dt", {})).items() if _parse_profile_date(value)}
        instance.last_item_kind_purchase_dt = {str(key): _parse_profile_date(value) for key, value in dict(payload.get("last_item_kind_purchase_dt", {})).items() if _parse_profile_date(value)}
        instance.recent_purchase_dates.extend(
            parsed
            for parsed in (_parse_profile_date(value) for value in payload.get("recent_purchase_dates", []))
            if parsed is not None
        )
        for key, values in dict(payload.get("recent_category_dates", {})).items():
            instance.recent_category_dates[str(key)].extend(
                parsed
                for parsed in (_parse_profile_date(value) for value in values)
                if parsed is not None
            )
        return instance


def _parse_profile_date(value: object) -> Optional[date]:
    if not value:
        return None
    cleaned = str(value)
    try:
        return date.fromisoformat(cleaned)
    except ValueError:
        return None


@dataclass
class GlobalHistoryState:
    ste_counts: Counter[str] = field(default_factory=Counter)
    category_counts: Counter[str] = field(default_factory=Counter)
    region_ste_counts: Counter[tuple[str, str]] = field(default_factory=Counter)
    region_category_counts: Counter[tuple[str, str]] = field(default_factory=Counter)
    segment_ste_counts: Counter[tuple[str, str]] = field(default_factory=Counter)
    month_category_counts: Counter[tuple[str, int]] = field(default_factory=Counter)
    recent_ste_dates: dict[str, deque[date]] = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=RECENT_WINDOW_DAYS)))
    recent_category_dates: dict[str, deque[date]] = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=RECENT_WINDOW_DAYS)))
    ste_price_distributions: dict[str, RollingDistribution] = field(default_factory=lambda: defaultdict(RollingDistribution))
    category_price_distributions: dict[str, RollingDistribution] = field(default_factory=lambda: defaultdict(RollingDistribution))
    ste_supplier_counts: dict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))
    ste_supplier_region_counts: dict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))

    def update(self, contract: ContractRecord, ste: STERecord, segment_key: str) -> None:
        category = ste.category or "UNKNOWN"
        region = contract.customer_region or "UNKNOWN"
        supplier = contract.supplier_inn or "UNKNOWN"
        supplier_region = contract.supplier_region or "UNKNOWN"
        self.ste_counts[ste.ste_id] += 1
        self.category_counts[category] += 1
        self.region_ste_counts[(region, ste.ste_id)] += 1
        self.region_category_counts[(region, category)] += 1
        self.segment_ste_counts[(segment_key, ste.ste_id)] += 1
        self.month_category_counts[(category, contract.contract_date.month)] += 1
        self.recent_ste_dates[ste.ste_id].append(contract.contract_date)
        self.recent_category_dates[category].append(contract.contract_date)
        self.ste_price_distributions[ste.ste_id].add(contract.contract_amount)
        self.category_price_distributions[category].add(contract.contract_amount)
        self.ste_supplier_counts[ste.ste_id][supplier] += 1
        self.ste_supplier_region_counts[ste.ste_id][supplier_region] += 1

    def build_candidate_priors(self, ste: STERecord, user_state: UserHistoryState, current_date: date, customer_region: str) -> dict[str, object]:
        category = ste.category or "UNKNOWN"
        segment_key = user_state.segment_key()
        ste_dist = self.ste_price_distributions.get(ste.ste_id)
        category_dist = self.category_price_distributions.get(category)
        candidate_price_proxy = ste_dist.median if ste_dist and ste_dist.total_count else (category_dist.median if category_dist and category_dist.total_count else 0.0)
        category_price_p25 = category_dist.p25 if category_dist else 0.0
        category_price_p75 = category_dist.p75 if category_dist else 0.0
        supplier_inn, supplier_share = _dominant_counter_item(self.ste_supplier_counts.get(ste.ste_id))
        supplier_region, _ = _dominant_counter_item(self.ste_supplier_region_counts.get(ste.ste_id))
        return {
            "global_ste_popularity": float(self.ste_counts.get(ste.ste_id, 0)),
            "global_category_popularity": float(self.category_counts.get(category, 0)),
            "regional_ste_popularity": float(self.region_ste_counts.get((customer_region, ste.ste_id), 0)),
            "regional_category_popularity": float(self.region_category_counts.get((customer_region, category), 0)),
            "similar_customer_ste_popularity": float(self.segment_ste_counts.get((segment_key, ste.ste_id), 0)),
            "seasonal_category_popularity": float(self.month_category_counts.get((category, current_date.month), 0)),
            "candidate_ste_recent_30d_popularity": float(_count_recent(self.recent_ste_dates[ste.ste_id], current_date, 30)),
            "candidate_category_recent_90d_popularity": float(_count_recent(self.recent_category_dates[category], current_date, 90)),
            "candidate_primary_supplier_inn": supplier_inn,
            "candidate_primary_supplier_share": float(supplier_share),
            "candidate_primary_supplier_region": supplier_region,
            "candidate_price_proxy": float(candidate_price_proxy),
            "category_price_p25": float(category_price_p25),
            "category_price_p75": float(category_price_p75),
        }


def _dominant_counter_item(counter: Optional[Counter[str]]) -> tuple[str, float]:
    if not counter:
        return "", 0.0
    key, count = counter.most_common(1)[0]
    total = sum(counter.values())
    return key, _safe_ratio(float(count), float(total))


def _candidate_record_from_payload(candidate: dict[str, object]) -> STERecord:
    clean_name = str(candidate.get("clean_name") or candidate.get("name") or "")
    category = str(candidate.get("category") or "")
    attribute_keys = str(candidate.get("attribute_keys") or candidate.get("attributes") or "")
    attribute_count = int(candidate.get("attribute_count", len(tokenize(attribute_keys))) or 0)
    return STERecord(
        ste_id=str(candidate.get("ste_id") or candidate.get("candidate_id") or ""),
        clean_name=clean_name,
        normalized_name=str(candidate.get("normalized_name") or normalize_text(clean_name)),
        category=category,
        normalized_category=str(candidate.get("normalized_category") or normalize_text(category)),
        attribute_keys=attribute_keys,
        attribute_count=attribute_count,
        key_tokens=str(candidate.get("key_tokens") or ""),
    )


def build_feature_vector(
    *,
    query: str,
    candidate: STERecord,
    user_state: UserHistoryState,
    current_date: date,
    customer_region: str,
    global_state: Optional[GlobalHistoryState] = None,
    candidate_priors: Optional[dict[str, object]] = None,
) -> dict[str, float]:
    query_ctx = build_query_context(query)
    query_tokens = set(query_ctx["tokens"])
    query_stems = set(query_ctx["stems"])
    candidate_name_tokens = set(candidate.name_tokens)
    candidate_category_tokens = set(candidate.category_tokens)
    candidate_attribute_tokens = set(candidate.attribute_tokens)
    candidate_all_tokens = candidate_name_tokens | candidate_category_tokens | candidate_attribute_tokens
    candidate_item_kind = derive_item_kind(candidate.clean_name, candidate.category)

    priors = dict(FEATURE_DEFAULTS)
    if candidate_priors:
        priors.update(candidate_priors)
    elif global_state is not None:
        priors.update(global_state.build_candidate_priors(candidate, user_state, current_date, customer_region))

    candidate_supplier = str(priors.get("candidate_primary_supplier_inn") or "")
    supplier_region = str(priors.get("candidate_primary_supplier_region") or "")
    user_total_purchase_count = float(user_state.total_purchases)
    user_category_purchase_count = float(user_state.category_counts.get(candidate.category, 0))
    user_ste_purchase_count = float(user_state.ste_counts.get(candidate.ste_id, 0))
    user_supplier_purchase_count = float(user_state.supplier_counts.get(candidate_supplier, 0))
    user_category_purchase_share = _safe_ratio(user_category_purchase_count, user_total_purchase_count)
    user_repeat_buy_signal = 1.0 if user_ste_purchase_count > 0 else 0.0
    user_novelty_signal = 0.0 if user_repeat_buy_signal else 1.0
    user_last_category_recency = _recency_days(user_state.last_category_purchase_dt.get(candidate.category), current_date)
    user_last_ste_recency = _recency_days(user_state.last_ste_purchase_dt.get(candidate.ste_id), current_date)
    user_last_supplier_recency = _recency_days(user_state.last_supplier_purchase_dt.get(candidate_supplier), current_date)
    user_recent_30d_purchase_count = float(_count_recent(user_state.recent_purchase_dates, current_date, 30))
    user_recent_90d_purchase_count = float(_count_recent(user_state.recent_purchase_dates, current_date, 90))
    user_recent_30d_category_count = float(_count_recent(user_state.recent_category_dates[candidate.category], current_date, 30))
    user_recent_90d_category_count = float(_count_recent(user_state.recent_category_dates[candidate.category], current_date, 90))
    user_avg_amount = float(user_state.amounts.mean)
    user_median_amount = float(user_state.amounts.median)
    candidate_price_proxy = float(priors.get("candidate_price_proxy", 0.0) or 0.0)
    category_price_p25 = float(priors.get("category_price_p25", 0.0) or 0.0)
    category_price_p75 = float(priors.get("category_price_p75", 0.0) or 0.0)
    candidate_price_vs_user_avg_ratio = _safe_ratio(candidate_price_proxy, user_avg_amount) if user_avg_amount > 0 else 0.0
    candidate_price_distance_to_user_median = abs(candidate_price_proxy - user_median_amount) if user_median_amount > 0 else candidate_price_proxy
    candidate_price_in_user_range = 1.0 if candidate_price_proxy > 0 and category_price_p25 <= candidate_price_proxy <= max(category_price_p75, category_price_p25) else 0.0
    candidate_supplier_affinity = _safe_ratio(user_supplier_purchase_count, user_total_purchase_count)
    candidate_supplier_region_match = 1.0 if customer_region and supplier_region and customer_region == supplier_region else 0.0
    candidate_item_kind_affinity = _safe_ratio(float(user_state.item_kind_counts.get(candidate_item_kind, 0)), user_total_purchase_count)

    features = {
        "query_name_token_overlap": _overlap_ratio(query_tokens, candidate_name_tokens),
        "query_category_token_overlap": _overlap_ratio(query_tokens, candidate_category_tokens),
        "query_attribute_token_overlap": _overlap_ratio(query_tokens, candidate_attribute_tokens),
        "query_token_coverage": _overlap_ratio(query_tokens, candidate_all_tokens),
        "query_exact_name_match": 1.0 if str(query_ctx["normalized_query"]) and str(query_ctx["normalized_query"]) in candidate.normalized_name else 0.0,
        "query_length_tokens": float(query_ctx["length_tokens"]),
        "candidate_name_length_tokens": float(len(candidate_name_tokens)),
        "candidate_attribute_count": float(candidate.attribute_count),
        "query_attribute_match_count": float(len(query_tokens & candidate_attribute_tokens)),
        "user_total_purchase_count": user_total_purchase_count,
        "user_category_purchase_count": user_category_purchase_count,
        "user_ste_purchase_count": user_ste_purchase_count,
        "user_supplier_purchase_count": user_supplier_purchase_count,
        "user_category_purchase_share": user_category_purchase_share,
        "user_repeat_buy_signal": user_repeat_buy_signal,
        "user_novelty_signal": user_novelty_signal,
        "user_last_category_recency_days": user_last_category_recency,
        "user_last_ste_recency_days": user_last_ste_recency,
        "user_last_supplier_recency_days": user_last_supplier_recency,
        "user_recent_30d_purchase_count": user_recent_30d_purchase_count,
        "user_recent_90d_purchase_count": user_recent_90d_purchase_count,
        "user_recent_30d_category_count": user_recent_30d_category_count,
        "user_recent_90d_category_count": user_recent_90d_category_count,
        "user_avg_amount": user_avg_amount,
        "user_median_amount": user_median_amount,
        "category_price_p25": category_price_p25,
        "category_price_p75": category_price_p75,
        "candidate_price_proxy": candidate_price_proxy,
        "candidate_price_vs_user_avg_ratio": candidate_price_vs_user_avg_ratio,
        "candidate_price_distance_to_user_median": candidate_price_distance_to_user_median,
        "candidate_price_in_user_range": candidate_price_in_user_range,
        "global_ste_popularity": float(priors.get("global_ste_popularity", 0.0) or 0.0),
        "global_category_popularity": float(priors.get("global_category_popularity", 0.0) or 0.0),
        "regional_ste_popularity": float(priors.get("regional_ste_popularity", 0.0) or 0.0),
        "regional_category_popularity": float(priors.get("regional_category_popularity", 0.0) or 0.0),
        "similar_customer_ste_popularity": float(priors.get("similar_customer_ste_popularity", 0.0) or 0.0),
        "seasonal_category_popularity": float(priors.get("seasonal_category_popularity", 0.0) or 0.0),
        "days_since_last_similar_purchase": user_last_category_recency,
        "candidate_ste_recent_30d_popularity": float(priors.get("candidate_ste_recent_30d_popularity", 0.0) or 0.0),
        "candidate_category_recent_90d_popularity": float(priors.get("candidate_category_recent_90d_popularity", 0.0) or 0.0),
        "candidate_primary_supplier_share": float(priors.get("candidate_primary_supplier_share", 0.0) or 0.0),
        "candidate_supplier_affinity": candidate_supplier_affinity,
        "candidate_supplier_region_match": candidate_supplier_region_match,
        "candidate_item_kind_affinity": candidate_item_kind_affinity,
    }
    for feature_name, default_value in FEATURE_DEFAULTS.items():
        features.setdefault(feature_name, float(default_value))
    return {name: float(value) for name, value in features.items()}


def build_inference_feature_vector(
    *,
    query: str,
    candidate_payload: dict[str, object],
    user_profile: Optional[dict[str, object]],
    reference_date: Optional[date] = None,
) -> dict[str, float]:
    current_date = reference_date or date.today()
    candidate = _candidate_record_from_payload(candidate_payload)
    user_state = UserHistoryState.from_profile(user_profile)
    customer_region = str((user_profile or {}).get("customer_region") or candidate_payload.get("customer_region") or "UNKNOWN")
    candidate_priors = {key: candidate_payload.get(key, default) for key, default in FEATURE_DEFAULTS.items()}
    candidate_priors["candidate_primary_supplier_inn"] = candidate_payload.get("candidate_primary_supplier_inn", "")
    candidate_priors["candidate_primary_supplier_region"] = candidate_payload.get("candidate_primary_supplier_region", "")
    return build_feature_vector(
        query=query,
        candidate=candidate,
        user_state=user_state,
        current_date=current_date,
        customer_region=customer_region,
        candidate_priors=candidate_priors,
    )


def build_reason_trace(
    features: dict[str, float],
    contributions: Optional[dict[str, float]] = None,
    max_reasons: int = 3,
) -> tuple[list[str], list[str]]:
    matched: list[tuple[float, str, str]] = []
    for rule in EXPLAIN_RULES:
        feature_name = rule["feature"]
        value = float(features.get(feature_name, FEATURE_DEFAULTS.get(feature_name, 0.0)))
        threshold = float(rule["threshold"])
        direction = rule.get("direction", "gte")
        is_match = value <= threshold if direction == "lte" else value >= threshold
        if not is_match:
            continue
        score = float(contributions.get(feature_name, value) if contributions else value)
        matched.append((score, rule["code"], rule["template"]))
    matched.sort(key=lambda item: item[0], reverse=True)
    top = matched[:max_reasons]
    reason_codes = [item[1] for item in top]
    reason_text = [item[2] for item in top]
    return reason_codes, reason_text

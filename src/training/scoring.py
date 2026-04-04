from __future__ import annotations

import math


NON_PERSONALIZED_WEIGHTS = {
    "query_name_token_overlap": 4.0,
    "query_category_token_overlap": 1.5,
    "query_attribute_token_overlap": 1.25,
    "query_token_coverage": 1.0,
    "query_exact_name_match": 0.75,
    "query_attribute_match_count": 0.2,
}


def _recency_boost(days: float, horizon: float = 120.0) -> float:
    if days >= 3650:
        return 0.0
    return max(0.0, 1.0 - (days / horizon))


def score_non_personalized_baseline(features: dict[str, float]) -> float:
    score = 0.0
    for feature_name, weight in NON_PERSONALIZED_WEIGHTS.items():
        score += weight * float(features.get(feature_name, 0.0))
    return float(score)


def score_rule_based_baseline(features: dict[str, float]) -> float:
    lexical = score_non_personalized_baseline(features)
    affinity = (
        1.8 * math.log1p(float(features.get("user_category_purchase_count", 0.0)))
        + 2.2 * float(features.get("user_category_purchase_share", 0.0))
        + 2.8 * float(features.get("user_repeat_buy_signal", 0.0))
        + 1.4 * _recency_boost(float(features.get("user_last_category_recency_days", 3650.0)), horizon=90.0)
        + 1.2 * _recency_boost(float(features.get("user_last_ste_recency_days", 3650.0)), horizon=180.0)
    )
    price = (
        0.9 * float(features.get("candidate_price_in_user_range", 0.0))
        + 0.4 * max(0.0, 1.0 - min(float(features.get("candidate_price_vs_user_avg_ratio", 0.0)), 3.0) / 3.0)
    )
    popularity = (
        0.35 * math.log1p(float(features.get("global_ste_popularity", 0.0)))
        + 0.25 * math.log1p(float(features.get("regional_ste_popularity", 0.0)))
        + 0.35 * math.log1p(float(features.get("similar_customer_ste_popularity", 0.0)))
        + 0.15 * math.log1p(float(features.get("candidate_ste_recent_30d_popularity", 0.0)))
    )
    structural = (
        0.8 * float(features.get("candidate_supplier_affinity", 0.0))
        + 0.35 * float(features.get("candidate_supplier_region_match", 0.0))
        + 0.45 * float(features.get("candidate_item_kind_affinity", 0.0))
    )
    return float(lexical + affinity + price + popularity + structural)


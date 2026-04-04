from __future__ import annotations

import json
import math
import random
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

from data.personalization_data import DatasetPaths, load_and_validate_datasets, write_data_contract_report
from eval.ranking_metrics import evaluate_grouped_rows
from features.personalization_features import (
    EXPLAIN_RULES,
    FEATURE_DEFAULTS,
    FEATURE_SPEC,
    GlobalHistoryState,
    UserHistoryState,
    build_feature_vector,
    generate_pseudo_queries,
)
from training.scoring import score_non_personalized_baseline, score_rule_based_baseline

try:
    from catboost import CatBoostRanker, Pool

    CATBOOST_AVAILABLE = True
    CATBOOST_IMPORT_ERROR = ""
except Exception as exc:  # pragma: no cover - optional dependency
    CatBoostRanker = None  # type: ignore[assignment]
    Pool = None  # type: ignore[assignment]
    CATBOOST_AVAILABLE = False
    CATBOOST_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"


DEFAULT_TRAIN_CONFIG = {
    "seed": 42,
    "paths": {
        "artifacts_dir": "artifacts",
        "reports_dir": "reports",
        "ste_catalog_path": None,
        "contracts_path": None,
    },
    "ranking_dataset": {
        "query_variants": [
            "contract_item_name",
            "ste_name",
            "contract_plus_category",
            "ste_name_plus_category",
            "ste_name_category_attributes",
        ],
        "random_negatives": 2,
        "same_category_negatives": 2,
        "similar_text_negatives": 2,
        "max_positive_events_total": 60000,
        "max_positive_events_per_user": 250,
        "deduplicate_contract_keys": True,
    },
    "time_split": {
        "strategy": "ratio",
        "train_ratio": 0.7,
        "val_ratio": 0.15,
    },
    "evaluation": {
        "active_user_threshold": 5,
    },
    "catboost": {
        "enabled": True,
        "pairwise_benchmark": True,
        "loss_function": "YetiRank",
        "eval_metric": "NDCG:top=10",
        "iterations": 500,
        "learning_rate": 0.05,
        "depth": 6,
        "l2_leaf_reg": 6.0,
        "random_strength": 0.5,
        "subsample": 0.8,
        "random_seed": 42,
        "early_stopping_rounds": 50,
        "verbose": 100,
        "use_best_model": True,
    },
}


@dataclass
class RankingBuildResult:
    rows: list[dict[str, object]]
    stats: dict[str, object]
    split_boundaries: dict[str, str]
    category_buckets: dict[str, set[str]]


class CatalogIndex:
    def __init__(self, catalog_by_id: dict[str, object]) -> None:
        self.catalog_by_id = catalog_by_id
        self.all_ids = list(catalog_by_id)
        self.by_category: dict[str, list[str]] = defaultdict(list)
        self.token_index: dict[str, list[str]] = defaultdict(list)
        for ste_id, ste in catalog_by_id.items():
            self.by_category[ste.category].append(ste_id)
            token_pool = set(ste.name_tokens) | set(ste.category_tokens) | set(ste.attribute_tokens)
            for token in token_pool:
                bucket = self.token_index[token]
                if len(bucket) < 500:
                    bucket.append(ste_id)

    def sample_negative_ids(
        self,
        *,
        positive_ste_id: str,
        positive_category: str,
        query: str,
        random_negatives: int,
        same_category_negatives: int,
        similar_text_negatives: int,
        rng: random.Random,
    ) -> list[str]:
        selected: list[str] = []
        used = {positive_ste_id}

        category_candidates = [candidate_id for candidate_id in self.by_category.get(positive_category, []) if candidate_id not in used]
        if category_candidates:
            if len(category_candidates) <= same_category_negatives:
                sampled = category_candidates
            else:
                sampled = rng.sample(category_candidates, same_category_negatives)
            selected.extend(sampled)
            used.update(sampled)

        if similar_text_negatives > 0:
            token_counter: Counter[str] = Counter()
            query_tokens = set(query.split()) | set(self.catalog_by_id[positive_ste_id].name_tokens)
            for token in query_tokens:
                for candidate_id in self.token_index.get(token, []):
                    if candidate_id in used:
                        continue
                    token_counter[candidate_id] += 1
            for candidate_id, _count in token_counter.most_common(similar_text_negatives):
                if candidate_id in used:
                    continue
                selected.append(candidate_id)
                used.add(candidate_id)

        attempts = 0
        while len([item for item in selected if item != positive_ste_id]) < (same_category_negatives + similar_text_negatives + random_negatives):
            if not self.all_ids:
                break
            candidate_id = rng.choice(self.all_ids)
            attempts += 1
            if candidate_id in used:
                if attempts > 1000:
                    break
                continue
            selected.append(candidate_id)
            used.add(candidate_id)
            if attempts > 1000:
                break
        return selected


def _deep_merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config(config_path: Optional[Path | str] = None) -> dict:
    config = json.loads(json.dumps(DEFAULT_TRAIN_CONFIG))
    if not config_path:
        return config
    payload = json.loads(Path(config_path).read_text(encoding="utf-8"))
    return _deep_merge(config, payload)


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_json_yaml(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _dataset_paths_from_config(project_root: Path, config: dict) -> Optional[DatasetPaths]:
    ste_catalog_path = config["paths"].get("ste_catalog_path")
    contracts_path = config["paths"].get("contracts_path")
    if not ste_catalog_path and not contracts_path:
        return None
    return DatasetPaths(
        ste_catalog_path=(project_root / str(ste_catalog_path)) if ste_catalog_path else None,
        contracts_path=(project_root / str(contracts_path)) if contracts_path else None,
    )


def _deduplicate_contracts(contracts: list[object]) -> list[object]:
    deduped: list[object] = []
    seen = set()
    for record in contracts:
        if record.key in seen:
            continue
        seen.add(record.key)
        deduped.append(record)
    return deduped


def _determine_time_split_boundaries(contracts: list[object], config: dict) -> dict[str, date]:
    dates = sorted({record.contract_date for record in contracts})
    if not dates:
        today = date.today()
        return {"train_end": today, "val_end": today}
    if "train_end" in config["time_split"] and "val_end" in config["time_split"]:
        return {
            "train_end": date.fromisoformat(config["time_split"]["train_end"]),
            "val_end": date.fromisoformat(config["time_split"]["val_end"]),
        }

    train_ratio = float(config["time_split"].get("train_ratio", 0.7))
    val_ratio = float(config["time_split"].get("val_ratio", 0.15))
    train_index = max(0, min(len(dates) - 1, int(len(dates) * train_ratio) - 1))
    val_index = max(train_index, min(len(dates) - 1, int(len(dates) * (train_ratio + val_ratio)) - 1))
    return {
        "train_end": dates[train_index],
        "val_end": dates[val_index],
    }


def _assign_split(event_date: date, split_boundaries: dict[str, date]) -> str:
    if event_date <= split_boundaries["train_end"]:
        return "train"
    if event_date <= split_boundaries["val_end"]:
        return "val"
    return "test"


def _build_category_buckets(contracts: list[object], catalog_by_id: dict[str, object]) -> dict[str, set[str]]:
    category_counts = Counter()
    for contract in contracts:
        ste = catalog_by_id.get(contract.ste_id)
        if ste:
            category_counts[ste.category] += 1
    if not category_counts:
        return {"frequent": set(), "rare": set()}
    ordered = category_counts.most_common()
    boundary = max(1, len(ordered) // 4)
    frequent = {category for category, _count in ordered[:boundary]}
    rare = {category for category, _count in ordered[-boundary:]}
    return {"frequent": frequent, "rare": rare}


def _group_counts(rows: list[dict[str, object]]) -> dict[str, int]:
    counter = Counter()
    for row in rows:
        counter[str(row["split"])] += 1
    return dict(counter)


def _build_ranking_rows(
    catalog_by_id: dict[str, object],
    contracts: list[object],
    config: dict,
) -> RankingBuildResult:
    ranking_config = config["ranking_dataset"]
    rng = random.Random(int(config["seed"]))
    split_boundaries = _determine_time_split_boundaries(contracts, config)
    category_buckets = _build_category_buckets(contracts, catalog_by_id)
    catalog_index = CatalogIndex(catalog_by_id)
    global_state = GlobalHistoryState()
    user_states: dict[str, UserHistoryState] = {}
    per_user_emitted = Counter()
    rows: list[dict[str, object]] = []
    stats = {
        "processed_positive_events": 0,
        "emitted_positive_events": 0,
        "skipped_missing_catalog_match": 0,
        "skipped_empty_query": 0,
        "rows_total": 0,
        "groups_total": 0,
        "rows_by_split": {},
    }

    max_positive_events_total = int(ranking_config.get("max_positive_events_total", 60000))
    max_positive_events_per_user = int(ranking_config.get("max_positive_events_per_user", 250))
    query_variants = list(ranking_config["query_variants"])
    group_counter = 0

    for contract in contracts:
        stats["processed_positive_events"] += 1
        ste = catalog_by_id.get(contract.ste_id)
        if ste is None:
            stats["skipped_missing_catalog_match"] += 1
            continue

        user_state = user_states.setdefault(contract.customer_inn, UserHistoryState(user_id=contract.customer_inn, customer_region=contract.customer_region))
        allow_emit = (
            stats["emitted_positive_events"] < max_positive_events_total
            and per_user_emitted[contract.customer_inn] < max_positive_events_per_user
        )
        event_split = _assign_split(contract.contract_date, split_boundaries)
        category_bucket = "frequent" if ste.category in category_buckets["frequent"] else "rare" if ste.category in category_buckets["rare"] else "middle"

        if allow_emit:
            query_candidates = generate_pseudo_queries(contract, ste)
            for query_variant in query_variants:
                query = str(query_candidates.get(query_variant, "")).strip()
                if not query:
                    stats["skipped_empty_query"] += 1
                    continue
                negative_ids = catalog_index.sample_negative_ids(
                    positive_ste_id=ste.ste_id,
                    positive_category=ste.category,
                    query=query,
                    random_negatives=int(ranking_config["random_negatives"]),
                    same_category_negatives=int(ranking_config["same_category_negatives"]),
                    similar_text_negatives=int(ranking_config["similar_text_negatives"]),
                    rng=rng,
                )
                group_counter += 1
                group_id = f"{contract.contract_id}:{contract.customer_inn}:{contract.ste_id}:{query_variant}:{group_counter}"
                candidate_ids = [ste.ste_id] + [candidate_id for candidate_id in negative_ids if candidate_id != ste.ste_id]
                for candidate_id in candidate_ids:
                    candidate = catalog_by_id[candidate_id]
                    label = 1 if candidate_id == ste.ste_id else 0
                    features = build_feature_vector(
                        query=query,
                        candidate=candidate,
                        user_state=user_state,
                        current_date=contract.contract_date,
                        customer_region=contract.customer_region,
                        global_state=global_state,
                    )
                    rows.append(
                        {
                            "group_id": group_id,
                            "split": event_split,
                            "query_variant": query_variant,
                            "query": query,
                            "user_id": contract.customer_inn,
                            "candidate_id": candidate_id,
                            "label": label,
                            "contract_date": contract.contract_date.isoformat(),
                            "user_history_count": user_state.total_purchases,
                            "is_new_user": 1 if user_state.total_purchases == 0 else 0,
                            "category_bucket": category_bucket,
                            "positive_category": ste.category,
                            "features": features,
                        }
                    )
                stats["groups_total"] += 1
            stats["emitted_positive_events"] += 1
            per_user_emitted[contract.customer_inn] += 1

        user_state.update(contract, ste)
        global_state.update(contract, ste, user_state.segment_key())

    stats["rows_total"] = len(rows)
    stats["rows_by_split"] = _group_counts(rows)
    return RankingBuildResult(
        rows=rows,
        stats=stats,
        split_boundaries={key: value.isoformat() for key, value in split_boundaries.items()},
        category_buckets=category_buckets,
    )


def _score_rows(rows: list[dict[str, object]], scorer) -> list[dict[str, object]]:
    scored_rows = []
    for row in rows:
        scored_rows.append(
            {
                "group_id": row["group_id"],
                "split": row["split"],
                "query_variant": row["query_variant"],
                "label": row["label"],
                "score": scorer(row["features"]),
                "user_history_count": row["user_history_count"],
                "is_new_user": row["is_new_user"],
                "category_bucket": row["category_bucket"],
                "candidate_id": row["candidate_id"],
                "features": row["features"],
            }
        )
    return scored_rows


def _evaluate_scored_rows(scored_rows: list[dict[str, object]], active_user_threshold: int) -> dict[str, dict[str, float]]:
    test_rows = [row for row in scored_rows if row["split"] == "test"]
    return {
        "overall": evaluate_grouped_rows(test_rows),
        "new_users": evaluate_grouped_rows([row for row in test_rows if int(row["is_new_user"]) == 1]),
        "active_users": evaluate_grouped_rows([row for row in test_rows if int(row["user_history_count"]) >= active_user_threshold]),
        "frequent_categories": evaluate_grouped_rows([row for row in test_rows if row["category_bucket"] == "frequent"]),
        "rare_categories": evaluate_grouped_rows([row for row in test_rows if row["category_bucket"] == "rare"]),
    }


def _select_best_query_variant(rows: list[dict[str, object]], active_user_threshold: int) -> tuple[str, dict[str, dict[str, float]]]:
    variants = sorted({str(row["query_variant"]) for row in rows})
    benchmark = {}
    best_variant = variants[0] if variants else "contract_item_name"
    best_score = -1.0
    for variant in variants:
        variant_rows = [row for row in rows if row["query_variant"] == variant and row["split"] == "val"]
        metrics = evaluate_grouped_rows(_score_rows(variant_rows, score_non_personalized_baseline))
        benchmark[variant] = metrics
        if metrics["ndcg@10"] > best_score:
            best_score = metrics["ndcg@10"]
            best_variant = variant
    return best_variant, benchmark


def _encode_group_ids(rows: list[dict[str, object]]) -> tuple[list[list[float]], list[float], list[int], list[dict[str, object]]]:
    ordered_rows = sorted(rows, key=lambda item: (str(item["group_id"]), -int(item["label"]), str(item["candidate_id"])))
    feature_order = [item["name"] for item in FEATURE_SPEC]
    matrices = []
    labels = []
    group_ids = []
    group_lookup: dict[str, int] = {}
    for row in ordered_rows:
        group_id = str(row["group_id"])
        if group_id not in group_lookup:
            group_lookup[group_id] = len(group_lookup) + 1
        matrices.append([float(row["features"].get(name, FEATURE_DEFAULTS[name])) for name in feature_order])
        labels.append(float(row["label"]))
        group_ids.append(group_lookup[group_id])
    return matrices, labels, group_ids, ordered_rows


def _build_pool(rows: list[dict[str, object]]):
    matrices, labels, group_ids, ordered_rows = _encode_group_ids(rows)
    pool = Pool(data=matrices, label=labels, group_id=group_ids)
    return pool, ordered_rows


def _collect_global_feature_importance(model) -> list[dict[str, object]]:
    importance = model.get_feature_importance(type="PredictionValuesChange")
    payload = []
    for feature, value in zip([item["name"] for item in FEATURE_SPEC], importance):
        payload.append({"feature": feature, "importance": round(float(value), 6)})
    payload.sort(key=lambda item: item["importance"], reverse=True)
    return payload[:20]


def _collect_per_object_contributions(model, rows: list[dict[str, object]]) -> list[dict[str, object]]:
    if not rows:
        return []
    sample_rows = rows[: min(len(rows), 25)]
    pool, ordered_rows = _build_pool(sample_rows)
    shap_values = model.get_feature_importance(pool, type="ShapValues")
    results = []
    feature_order = [item["name"] for item in FEATURE_SPEC]
    for index, row in enumerate(ordered_rows):
        contributions = shap_values[index]
        ranked = []
        for feature_index, feature_name in enumerate(feature_order):
            value = float(contributions[feature_index])
            if value <= 0:
                continue
            ranked.append((value, feature_name))
        ranked.sort(reverse=True)
        results.append(
            {
                "group_id": row["group_id"],
                "candidate_id": row["candidate_id"],
                "top_positive_factors": [
                    {"feature": feature_name, "contribution": round(value, 6)}
                    for value, feature_name in ranked[:5]
                ],
            }
        )
    return results


def _train_catboost_model(train_rows: list[dict[str, object]], val_rows: list[dict[str, object]], config: dict, loss_function: str):
    params = {
        "loss_function": loss_function,
        "eval_metric": config["catboost"]["eval_metric"],
        "iterations": int(config["catboost"]["iterations"]),
        "learning_rate": float(config["catboost"]["learning_rate"]),
        "depth": int(config["catboost"]["depth"]),
        "l2_leaf_reg": float(config["catboost"]["l2_leaf_reg"]),
        "random_strength": float(config["catboost"]["random_strength"]),
        "subsample": float(config["catboost"]["subsample"]),
        "random_seed": int(config["seed"]),
    }
    train_pool, _train_ordered = _build_pool(train_rows)
    val_pool, _val_ordered = _build_pool(val_rows)
    model = CatBoostRanker(**params)
    model.fit(
        train_pool,
        eval_set=val_pool,
        use_best_model=bool(config["catboost"].get("use_best_model", True)),
        early_stopping_rounds=int(config["catboost"].get("early_stopping_rounds", 50)),
        verbose=int(config["catboost"].get("verbose", 100)),
    )
    return model


def _predict_model(model, rows: list[dict[str, object]]) -> list[dict[str, object]]:
    pool, ordered_rows = _build_pool(rows)
    predictions = model.predict(pool)
    scored = []
    for row, prediction in zip(ordered_rows, predictions):
        scored.append(
            {
                "group_id": row["group_id"],
                "split": row["split"],
                "query_variant": row["query_variant"],
                "label": row["label"],
                "score": float(prediction),
                "user_history_count": row["user_history_count"],
                "is_new_user": row["is_new_user"],
                "category_bucket": row["category_bucket"],
                "candidate_id": row["candidate_id"],
                "features": row["features"],
            }
        )
    return scored


def _write_static_artifacts(artifacts_dir: Path, config: dict) -> None:
    feature_spec_payload = {
        "model_feature_order": [item["name"] for item in FEATURE_SPEC],
        "features": FEATURE_SPEC,
        "integration_contract": {
            "input": {
                "user_id": "str",
                "query_features": {"query": "str", "reference_date": "ISO-8601 date optional"},
                "user_profile": {
                    "customer_region": "str",
                    "total_purchases": "int",
                    "recent_amounts": "list[float]",
                    "category_counts": "dict[str,int]",
                    "ste_counts": "dict[str,int]",
                    "supplier_counts": "dict[str,int]",
                    "item_kind_counts": "dict[str,int]",
                    "last_category_purchase_dt": "dict[str,ISO-date]",
                    "last_ste_purchase_dt": "dict[str,ISO-date]",
                    "last_supplier_purchase_dt": "dict[str,ISO-date]",
                },
                "candidates": [
                    {
                        "candidate_id": "str",
                        "clean_name": "str",
                        "category": "str",
                        "attribute_keys": "str",
                        "attribute_count": "int",
                        "candidate_price_proxy": "float",
                        "global_ste_popularity": "float",
                        "global_category_popularity": "float",
                        "regional_ste_popularity": "float",
                        "regional_category_popularity": "float",
                        "similar_customer_ste_popularity": "float",
                        "seasonal_category_popularity": "float",
                        "candidate_ste_recent_30d_popularity": "float",
                        "candidate_category_recent_90d_popularity": "float",
                        "candidate_primary_supplier_inn": "str",
                        "candidate_primary_supplier_region": "str",
                        "candidate_primary_supplier_share": "float",
                    }
                ],
            },
            "output": {
                "candidate_id": "str",
                "personalization_score": "float",
                "top_reason_codes": "list[str]",
                "reasons": "list[str]",
            },
            "function_signature": "predict_personalization(candidates, user_profile, query_features) -> rescored_candidates",
        },
    }
    feature_defaults_payload = {
        "feature_defaults": FEATURE_DEFAULTS,
        "candidate_payload_defaults": {
            "candidate_primary_supplier_inn": "",
            "candidate_primary_supplier_region": "UNKNOWN",
            "clean_name": "",
            "category": "",
            "attribute_keys": "",
            "attribute_count": 0,
        },
        "user_profile_defaults": {
            "customer_region": "UNKNOWN",
            "total_purchases": 0,
            "recent_amounts": [],
            "category_counts": {},
            "ste_counts": {},
            "supplier_counts": {},
            "item_kind_counts": {},
            "last_category_purchase_dt": {},
            "last_ste_purchase_dt": {},
            "last_supplier_purchase_dt": {},
        },
    }
    _write_json(artifacts_dir / "feature_spec.json", feature_spec_payload)
    _write_json(artifacts_dir / "feature_defaults.json", feature_defaults_payload)
    _write_json(artifacts_dir / "explain_rules.json", {"rules": EXPLAIN_RULES})
    _write_json_yaml(artifacts_dir / "train_config.yaml", config)


def _write_offline_eval_report(report_path: Path, payload: dict) -> None:
    lines = [
        "# Offline Evaluation",
        "",
        f"- Status: `{payload.get('status', 'unknown')}`",
        f"- Selected pseudo-query variant: `{payload.get('selected_query_variant')}`",
        "",
        "## Ranking Dataset",
        "",
    ]
    dataset_stats = payload.get("ranking_dataset", {})
    for key, value in dataset_stats.items():
        lines.append(f"- {key}: `{value}`")

    lines.extend(["", "## Pseudo-Query Benchmark", ""])
    for variant, metrics in payload.get("pseudo_query_benchmark", {}).items():
        lines.append(f"- `{variant}`: NDCG@10={metrics.get('ndcg@10', 0.0)}, NDCG@5={metrics.get('ndcg@5', 0.0)}")

    lines.extend(["", "## Model Comparison", ""])
    for model_name, model_metrics in payload.get("model_comparison", {}).items():
        lines.append(f"### {model_name}")
        lines.append("")
        for slice_name, metrics in model_metrics.items():
            lines.append(
                f"- {slice_name}: NDCG@5={metrics.get('ndcg@5', 0.0)}, NDCG@10={metrics.get('ndcg@10', 0.0)}, "
                f"MRR@10={metrics.get('mrr@10', 0.0)}, Recall@10={metrics.get('recall@10', 0.0)}, HitRate@10={metrics.get('hitrate@10', 0.0)}"
            )
        lines.append("")

    lines.extend(["## Global Feature Importance", ""])
    for item in payload.get("global_feature_importance", []):
        lines.append(f"- `{item['feature']}`: {item['importance']}")

    lines.extend(
        [
            "",
            "## Integration Contract",
            "",
            "- Input: `user_id`, `query_features`, `candidates`, `user_profile`.",
            "- Output: `candidate_id`, `personalization_score`, `top_reason_codes`, `reasons`.",
            "- Stable inference entrypoint: `predict_personalization(candidates, user_profile, query_features)`.",
        ]
    )

    if payload.get("notes"):
        lines.extend(["", "## Notes", ""])
        for note in payload["notes"]:
            lines.append(f"- {note}")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_pipeline(project_root: Path | str = ".", config_path: Optional[Path | str] = None) -> dict:
    project_root = Path(project_root)
    config = load_config(config_path)
    artifacts_dir = project_root / config["paths"]["artifacts_dir"]
    reports_dir = project_root / config["paths"]["reports_dir"]
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    _write_static_artifacts(artifacts_dir, config)
    loaded = load_and_validate_datasets(
        project_root=project_root,
        dataset_paths=_dataset_paths_from_config(project_root, config),
        strict=False,
    )
    write_data_contract_report(loaded.validation_summary, reports_dir / "data_contract.md")

    if loaded.validation_summary["status"] != "ready" or not loaded.catalog_by_id or not loaded.contracts:
        payload = {
            "status": "missing_input",
            "selected_query_variant": None,
            "ranking_dataset": {},
            "pseudo_query_benchmark": {},
            "model_comparison": {},
            "global_feature_importance": [],
            "per_object_contributions": [],
            "notes": [
                "В репозитории отсутствуют оба обязательных входных датасета либо один из них.",
                "Pipeline сгенерировал только статические артефакты и data contract.",
                "Для реального обучения требуется положить каталог СТЕ и контракты в ожидаемые пути.",
            ],
        }
        _write_json(artifacts_dir / "offline_metrics.json", payload)
        _write_offline_eval_report(reports_dir / "offline_eval.md", payload)
        return payload

    contracts = loaded.contracts
    if config["ranking_dataset"].get("deduplicate_contract_keys", True):
        contracts = _deduplicate_contracts(contracts)

    ranking_result = _build_ranking_rows(loaded.catalog_by_id, contracts, config)
    active_user_threshold = int(config["evaluation"].get("active_user_threshold", 5))
    selected_query_variant, query_variant_benchmark = _select_best_query_variant(ranking_result.rows, active_user_threshold)
    selected_rows = [row for row in ranking_result.rows if row["query_variant"] == selected_query_variant]

    model_comparison = {
        "baseline_non_personalized": _evaluate_scored_rows(_score_rows(selected_rows, score_non_personalized_baseline), active_user_threshold),
        "baseline_rule_based": _evaluate_scored_rows(_score_rows(selected_rows, score_rule_based_baseline), active_user_threshold),
    }
    global_feature_importance = []
    per_object_contributions = []
    notes = []

    train_rows = [row for row in selected_rows if row["split"] == "train"]
    val_rows = [row for row in selected_rows if row["split"] == "val"]
    test_rows = [row for row in selected_rows if row["split"] == "test"]

    if not CATBOOST_AVAILABLE:
        notes.append(f"CatBoost недоступен в текущем окружении: {CATBOOST_IMPORT_ERROR}")
    elif not config["catboost"].get("enabled", True):
        notes.append("Обучение CatBoost отключено в конфиге.")
    elif not train_rows or not val_rows or not test_rows:
        notes.append("Недостаточно групп для train/val/test после time split.")
    else:
        try:
            model = _train_catboost_model(train_rows, val_rows, config, loss_function=str(config["catboost"]["loss_function"]))
            model.save_model(str(artifacts_dir / "personalization_model.cbm"))
            model_comparison["catboost_yetirank"] = _evaluate_scored_rows(_predict_model(model, selected_rows), active_user_threshold)
            global_feature_importance = _collect_global_feature_importance(model)
            per_object_contributions = _collect_per_object_contributions(model, test_rows)
        except Exception as exc:  # pragma: no cover - optional runtime dependency
            notes.append(f"CatBoost training failed: {type(exc).__name__}: {exc}")

        if CATBOOST_AVAILABLE and config["catboost"].get("pairwise_benchmark", True) and train_rows and val_rows and test_rows:
            try:
                pairwise_model = _train_catboost_model(train_rows, val_rows, config, loss_function="YetiRankPairwise")
                model_comparison["catboost_yetirank_pairwise"] = _evaluate_scored_rows(_predict_model(pairwise_model, selected_rows), active_user_threshold)
            except Exception as exc:  # pragma: no cover - optional runtime dependency
                notes.append(f"CatBoost YetiRankPairwise benchmark failed: {type(exc).__name__}: {exc}")

    payload = {
        "status": "ready" if "catboost_yetirank" in model_comparison else "partial",
        "selected_query_variant": selected_query_variant,
        "time_split": ranking_result.split_boundaries,
        "ranking_dataset": ranking_result.stats,
        "pseudo_query_benchmark": query_variant_benchmark,
        "model_comparison": model_comparison,
        "global_feature_importance": global_feature_importance,
        "per_object_contributions": per_object_contributions,
        "notes": notes,
    }
    _write_json(artifacts_dir / "offline_metrics.json", payload)
    _write_offline_eval_report(reports_dir / "offline_eval.md", payload)
    return payload

from __future__ import annotations

import math
from collections import defaultdict
from typing import Iterable


def _dcg(labels: list[float], k: int) -> float:
    value = 0.0
    for rank, label in enumerate(labels[:k], start=1):
        gain = (2**label) - 1
        value += gain / math.log2(rank + 1)
    return value


def _ndcg(labels: list[float], k: int) -> float:
    ideal = sorted(labels, reverse=True)
    ideal_dcg = _dcg(ideal, k)
    if ideal_dcg <= 0:
        return 0.0
    return _dcg(labels, k) / ideal_dcg


def _mrr(labels: list[float], k: int) -> float:
    for rank, label in enumerate(labels[:k], start=1):
        if label > 0:
            return 1.0 / rank
    return 0.0


def _recall(labels: list[float], k: int) -> float:
    positives = sum(1 for label in labels if label > 0)
    if positives == 0:
        return 0.0
    return sum(1 for label in labels[:k] if label > 0) / positives


def _hit_rate(labels: list[float], k: int) -> float:
    return 1.0 if any(label > 0 for label in labels[:k]) else 0.0


def evaluate_grouped_rows(rows: Iterable[dict[str, object]]) -> dict[str, float]:
    groups: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        groups[str(row["group_id"])].append(dict(row))

    if not groups:
        return {
            "groups": 0,
            "ndcg@5": 0.0,
            "ndcg@10": 0.0,
            "mrr@10": 0.0,
            "recall@10": 0.0,
            "hitrate@10": 0.0,
        }

    ndcg_5 = 0.0
    ndcg_10 = 0.0
    mrr_10 = 0.0
    recall_10 = 0.0
    hitrate_10 = 0.0
    for group_rows in groups.values():
        ranked = sorted(group_rows, key=lambda row: (float(row["score"]), float(row["label"])), reverse=True)
        labels = [float(row["label"]) for row in ranked]
        ndcg_5 += _ndcg(labels, 5)
        ndcg_10 += _ndcg(labels, 10)
        mrr_10 += _mrr(labels, 10)
        recall_10 += _recall(labels, 10)
        hitrate_10 += _hit_rate(labels, 10)

    group_count = float(len(groups))
    return {
        "groups": int(group_count),
        "ndcg@5": round(ndcg_5 / group_count, 6),
        "ndcg@10": round(ndcg_10 / group_count, 6),
        "mrr@10": round(mrr_10 / group_count, 6),
        "recall@10": round(recall_10 / group_count, 6),
        "hitrate@10": round(hitrate_10 / group_count, 6),
    }

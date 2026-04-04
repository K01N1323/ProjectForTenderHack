from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .rerank_dataset import build_rerank_row

try:
    from catboost import CatBoostRanker

    CATBOOST_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    CatBoostRanker = None  # type: ignore[assignment]
    CATBOOST_AVAILABLE = False

try:
    import lightgbm as lgb

    LIGHTGBM_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    lgb = None  # type: ignore[assignment]
    LIGHTGBM_AVAILABLE = False


DEFAULT_SEARCH_RERANK_ARTIFACT_CANDIDATES: List[Tuple[Path, Path]] = [
    (
        Path("data/processed/tenderhack_yeti_ranker_merged_5k.cbm"),
        Path("data/processed/tenderhack_yeti_ranker_merged_5k.json"),
    ),
    (
        Path("data/processed/tenderhack_lightgbm_ranker_merged_5k.txt"),
        Path("data/processed/tenderhack_lightgbm_ranker_merged_5k.json"),
    ),
    (
        Path("data/processed/tenderhack_yeti_ranker_current_pairwise.cbm"),
        Path("data/processed/tenderhack_yeti_ranker_current_pairwise.json"),
    ),
    (
        Path("data/processed/tenderhack_yeti_ranker_current.cbm"),
        Path("data/processed/tenderhack_yeti_ranker_current.json"),
    ),
    (
        Path("data/processed/tenderhack_yeti_ranker_large_pairwise.cbm"),
        Path("data/processed/tenderhack_yeti_ranker_large_pairwise.json"),
    ),
    (
        Path("data/processed/tenderhack_yeti_ranker_large.cbm"),
        Path("data/processed/tenderhack_yeti_ranker_large.json"),
    ),
    (
        Path("data/processed/tenderhack_yeti_ranker_small_pairwise.cbm"),
        Path("data/processed/tenderhack_yeti_ranker_small_pairwise.json"),
    ),
    (
        Path("data/processed/tenderhack_yeti_ranker_small.cbm"),
        Path("data/processed/tenderhack_yeti_ranker_small.json"),
    ),
    (
        Path("data/processed/tenderhack_yeti_ranker.cbm"),
        Path("data/processed/tenderhack_yeti_ranker.json"),
    ),
    (
        Path("data/processed/tenderhack_lightgbm_ranker_current.txt"),
        Path("data/processed/tenderhack_lightgbm_ranker_current.json"),
    ),
    (
        Path("data/processed/tenderhack_lightgbm_ranker_large.txt"),
        Path("data/processed/tenderhack_lightgbm_ranker_large.json"),
    ),
]
DEFAULT_SEARCH_RERANK_MODEL_PATH = DEFAULT_SEARCH_RERANK_ARTIFACT_CANDIDATES[0][0]
DEFAULT_SEARCH_RERANK_METADATA_PATH = DEFAULT_SEARCH_RERANK_ARTIFACT_CANDIDATES[0][1]


def resolve_search_rerank_artifacts(
    model_path: Path | str | None = None,
    metadata_path: Path | str | None = None,
) -> Tuple[Path, Path]:
    if model_path is not None or metadata_path is not None:
        resolved_model_path = Path(model_path) if model_path is not None else DEFAULT_SEARCH_RERANK_MODEL_PATH
        resolved_metadata_path = (
            Path(metadata_path) if metadata_path is not None else resolved_model_path.with_suffix(".json")
        )
        return resolved_model_path, resolved_metadata_path

    for candidate_model_path, candidate_metadata_path in DEFAULT_SEARCH_RERANK_ARTIFACT_CANDIDATES:
        if candidate_model_path.exists() and candidate_metadata_path.exists():
            return candidate_model_path, candidate_metadata_path

    return DEFAULT_SEARCH_RERANK_MODEL_PATH, DEFAULT_SEARCH_RERANK_METADATA_PATH


class SearchRerankPredictor:
    def __init__(
        self,
        model_path: Path | str | None = None,
        metadata_path: Path | str | None = None,
    ) -> None:
        self.model_path, self.metadata_path = resolve_search_rerank_artifacts(model_path, metadata_path)
        self.model = None
        self.feature_order: List[str] = []
        self.model_type = self._infer_model_type(self.model_path)

        if self.metadata_path.exists():
            metadata = json.loads(self.metadata_path.read_text(encoding="utf-8"))
            self.feature_order = [str(name) for name in metadata.get("feature_names", []) if str(name)]
            self.model_type = str(metadata.get("model_type") or self.model_type or "catboost").lower()

        if self.model_type == "lightgbm" and LIGHTGBM_AVAILABLE and self.model_path.exists() and self.feature_order:
            self.model = lgb.Booster(model_file=str(self.model_path))
        elif CATBOOST_AVAILABLE and self.model_path.exists() and self.feature_order:
            self.model = CatBoostRanker()
            self.model.load_model(str(self.model_path))

    @staticmethod
    def _infer_model_type(model_path: Path) -> str:
        suffix = model_path.suffix.lower()
        if suffix in {".txt", ".lgb", ".lightgbm"}:
            return "lightgbm"
        return "catboost"

    @property
    def enabled(self) -> bool:
        return self.model is not None and bool(self.feature_order)

    def rerank_candidates(
        self,
        *,
        query: str,
        query_meta: Dict[str, object],
        candidates: List[Dict[str, object]],
    ) -> List[Dict[str, object]]:
        if not candidates or not self.enabled:
            return list(candidates)

        feature_matrix: List[List[float]] = []
        for rank, candidate in enumerate(candidates, start=1):
            row = build_rerank_row(
                group_id="inference",
                query=query,
                query_meta=query_meta,
                contract_id="",
                customer_inn="",
                customer_region="",
                positive_ste_id="",
                candidate=candidate,
                candidate_rank=rank,
            )
            feature_matrix.append([float(row.get(name, 0.0) or 0.0) for name in self.feature_order])

        predictions = self.model.predict(feature_matrix)
        if not isinstance(predictions, list):
            predictions = list(predictions)

        reranked: List[Dict[str, object]] = []
        for candidate, prediction in zip(candidates, predictions):
            enriched = dict(candidate)
            original_search_score = float(enriched.get("search_score", 0.0) or 0.0)
            ml_score = float(prediction)
            enriched["retrieval_score"] = round(original_search_score, 6)
            enriched["ml_rerank_score"] = round(ml_score, 6)
            enriched["search_score"] = round(ml_score, 6)
            reranked.append(enriched)

        reranked.sort(
            key=lambda item: (
                float(item.get("ml_rerank_score", item.get("search_score", 0.0))),
                float(item.get("retrieval_score", 0.0)),
            ),
            reverse=True,
        )
        return reranked


def rerank_search_candidates(
    *,
    query: str,
    query_meta: Dict[str, object],
    candidates: List[Dict[str, object]],
    model_path: Path | str | None = None,
    metadata_path: Path | str | None = None,
) -> List[Dict[str, object]]:
    predictor = SearchRerankPredictor(model_path=model_path, metadata_path=metadata_path)
    return predictor.rerank_candidates(query=query, query_meta=query_meta, candidates=candidates)

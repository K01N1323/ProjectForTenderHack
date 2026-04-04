from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

from features.personalization_features import FEATURE_SPEC, build_inference_feature_vector, build_reason_trace
from training.scoring import score_rule_based_baseline

try:
    from catboost import CatBoostRanker

    CATBOOST_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    CatBoostRanker = None  # type: ignore[assignment]
    CATBOOST_AVAILABLE = False


DEFAULT_MODEL_PATH = Path("artifacts/personalization_model.cbm")


class PersonalizationPredictor:
    def __init__(self, model_path: Path | str = DEFAULT_MODEL_PATH) -> None:
        self.model_path = Path(model_path)
        self.model = None
        self.feature_order = [item["name"] for item in FEATURE_SPEC]
        if CATBOOST_AVAILABLE and self.model_path.exists():
            self.model = CatBoostRanker()
            self.model.load_model(str(self.model_path))

    def _score(self, features: dict[str, float]) -> float:
        if self.model is None:
            return score_rule_based_baseline(features)
        matrix = [[float(features.get(feature_name, 0.0)) for feature_name in self.feature_order]]
        prediction = self.model.predict(matrix)
        if isinstance(prediction, list):
            return float(prediction[0])
        return float(prediction)

    def predict_personalization(
        self,
        candidates: list[dict[str, object]],
        user_profile: Optional[dict[str, object]],
        query_features: dict[str, object],
    ) -> list[dict[str, object]]:
        query = str(query_features.get("query") or query_features.get("normalized_query") or "")
        reference_date_value = query_features.get("reference_date")
        reference_date = date.fromisoformat(str(reference_date_value)) if reference_date_value else date.today()
        rescored = []
        for candidate in candidates:
            features = build_inference_feature_vector(
                query=query,
                candidate_payload=candidate,
                user_profile=user_profile,
                reference_date=reference_date,
            )
            score = self._score(features)
            reason_codes, reasons = build_reason_trace(features)
            rescored.append(
                {
                    "candidate_id": str(candidate.get("candidate_id") or candidate.get("ste_id") or ""),
                    "personalization_score": round(float(score), 6),
                    "top_reason_codes": reason_codes,
                    "reasons": reasons,
                }
            )
        rescored.sort(key=lambda item: item["personalization_score"], reverse=True)
        return rescored


def predict_personalization(
    candidates: list[dict[str, object]],
    user_profile: Optional[dict[str, object]],
    query_features: dict[str, object],
    model_path: Path | str = DEFAULT_MODEL_PATH,
) -> list[dict[str, object]]:
    predictor = PersonalizationPredictor(model_path=model_path)
    return predictor.predict_personalization(candidates=candidates, user_profile=user_profile, query_features=query_features)


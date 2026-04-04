# Offline Evaluation

- Status: `missing_input`
- Selected pseudo-query variant: `None`

## Ranking Dataset


## Pseudo-Query Benchmark


## Model Comparison

## Global Feature Importance


## Integration Contract

- Input: `user_id`, `query_features`, `candidates`, `user_profile`.
- Output: `candidate_id`, `personalization_score`, `top_reason_codes`, `reasons`.
- Stable inference entrypoint: `predict_personalization(candidates, user_profile, query_features)`.

## Notes

- В репозитории отсутствуют оба обязательных входных датасета либо один из них.
- Pipeline сгенерировал только статические артефакты и data contract.
- Для реального обучения требуется положить каталог СТЕ и контракты в ожидаемые пути.

# Offline Evaluation

- Status: `ready`
- Selected pseudo-query variant: `contract_item_name`

## Ranking Dataset

- processed_positive_events: `1006`
- emitted_positive_events: `1000`
- skipped_missing_catalog_match: `6`
- skipped_empty_query: `0`
- rows_total: `35000`
- groups_total: `5000`
- rows_by_split: `{'train': 35000}`

## Pseudo-Query Benchmark

- `contract_item_name`: NDCG@10=0.0, NDCG@5=0.0
- `contract_plus_category`: NDCG@10=0.0, NDCG@5=0.0
- `ste_name`: NDCG@10=0.0, NDCG@5=0.0
- `ste_name_category_attributes`: NDCG@10=0.0, NDCG@5=0.0
- `ste_name_plus_category`: NDCG@10=0.0, NDCG@5=0.0

## Model Comparison

### baseline_non_personalized

- overall: NDCG@5=0.0, NDCG@10=0.0, MRR@10=0.0, Recall@10=0.0, HitRate@10=0.0
- new_users: NDCG@5=0.0, NDCG@10=0.0, MRR@10=0.0, Recall@10=0.0, HitRate@10=0.0
- active_users: NDCG@5=0.0, NDCG@10=0.0, MRR@10=0.0, Recall@10=0.0, HitRate@10=0.0
- frequent_categories: NDCG@5=0.0, NDCG@10=0.0, MRR@10=0.0, Recall@10=0.0, HitRate@10=0.0
- rare_categories: NDCG@5=0.0, NDCG@10=0.0, MRR@10=0.0, Recall@10=0.0, HitRate@10=0.0

### baseline_rule_based

- overall: NDCG@5=0.0, NDCG@10=0.0, MRR@10=0.0, Recall@10=0.0, HitRate@10=0.0
- new_users: NDCG@5=0.0, NDCG@10=0.0, MRR@10=0.0, Recall@10=0.0, HitRate@10=0.0
- active_users: NDCG@5=0.0, NDCG@10=0.0, MRR@10=0.0, Recall@10=0.0, HitRate@10=0.0
- frequent_categories: NDCG@5=0.0, NDCG@10=0.0, MRR@10=0.0, Recall@10=0.0, HitRate@10=0.0
- rare_categories: NDCG@5=0.0, NDCG@10=0.0, MRR@10=0.0, Recall@10=0.0, HitRate@10=0.0

### catboost_yetirank

- overall: NDCG@5=0.0, NDCG@10=0.0, MRR@10=0.0, Recall@10=0.0, HitRate@10=0.0
- new_users: NDCG@5=0.0, NDCG@10=0.0, MRR@10=0.0, Recall@10=0.0, HitRate@10=0.0
- active_users: NDCG@5=0.0, NDCG@10=0.0, MRR@10=0.0, Recall@10=0.0, HitRate@10=0.0
- frequent_categories: NDCG@5=0.0, NDCG@10=0.0, MRR@10=0.0, Recall@10=0.0, HitRate@10=0.0
- rare_categories: NDCG@5=0.0, NDCG@10=0.0, MRR@10=0.0, Recall@10=0.0, HitRate@10=0.0

## Global Feature Importance

- `query_token_coverage`: 26.340946
- `user_last_category_recency_days`: 18.318905
- `days_since_last_similar_purchase`: 16.367241
- `query_name_token_overlap`: 9.984048
- `user_recent_30d_category_count`: 6.555544
- `query_attribute_match_count`: 4.712612
- `candidate_attribute_count`: 2.606012
- `query_category_token_overlap`: 2.273113
- `regional_ste_popularity`: 2.137138
- `candidate_ste_recent_30d_popularity`: 1.920676
- `candidate_name_length_tokens`: 1.771618
- `user_last_ste_recency_days`: 1.629775
- `query_length_tokens`: 1.522262
- `category_price_p75`: 1.063617
- `query_exact_name_match`: 0.592708
- `candidate_item_kind_affinity`: 0.528966
- `user_novelty_signal`: 0.436988
- `candidate_price_proxy`: 0.351538
- `user_supplier_purchase_count`: 0.312673
- `global_ste_popularity`: 0.278144

## Integration Contract

- Input: `user_id`, `query_features`, `candidates`, `user_profile`.
- Output: `candidate_id`, `personalization_score`, `top_reason_codes`, `reasons`.
- Stable inference entrypoint: `predict_personalization(candidates, user_profile, query_features)`.

## Notes

- Time split не дал val/test; применен fallback group split для обучения runtime-модели.

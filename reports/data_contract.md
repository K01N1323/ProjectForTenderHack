# Personalization Data Contract

## Expected Inputs

- STE catalog: `data/processed/ste_catalog_clean.csv` or raw `СТЕ_*.csv`.
- Contracts: `data/processed/contracts_clean.csv` or raw `Контракты_*.csv`.
- Primary join key between contracts and STE catalog: `ste_id`.

## Required STE Columns

- `ste_id`
- `clean_name`
- `normalized_name`
- `category`
- `normalized_category`
- `attribute_keys`
- `attribute_count`
- `key_tokens`

## Required Contract Columns

- `contract_item_name`
- `contract_id`
- `ste_id`
- `contract_datetime`
- `contract_amount`
- `customer_inn`
- `customer_name`
- `customer_region`
- `supplier_inn`
- `supplier_name`
- `supplier_region`

## Validation Status

- Status: `missing_input`
- STE path: `None`
- Contracts path: `None`

## Observed STE Dataset

- Rows: 0
- Unique `ste_id`: 0
- Duplicate `ste_id`: 0
- Missing `ste_id`: 0
- Missing `clean_name`: 0
- Missing `normalized_name`: 0
- Missing `category`: 0
- Missing `normalized_category`: 0
- Missing `attribute_keys`: 0
- Missing `attribute_count`: 0
- Missing `key_tokens`: 0

## Observed Contracts Dataset

- Rows: 0
- Unique compound keys (`contract_id`, `ste_id`, `customer_inn`): 0
- Duplicate compound keys: 0
- Missing `contract_item_name`: 0
- Missing `contract_id`: 0
- Missing `ste_id`: 0
- Missing `contract_datetime`: 0
- Missing `contract_amount`: 0
- Missing `customer_inn`: 0
- Missing `customer_name`: 0
- Missing `customer_region`: 0
- Missing `supplier_inn`: 0
- Missing `supplier_name`: 0
- Missing `supplier_region`: 0

## Join Checks

- Join key: `ste_id`
- Contract rows without STE catalog match: None
- Catalog STE without contract history: None

## Missing Data Notes

- Empty `customer_inn` is replaced with `UNKNOWN` during loading.
- Empty `customer_region` and `supplier_region` are replaced with `UNKNOWN` during loading.
- Invalid `contract_datetime` rows are skipped because ranking splits are time-based.
- Invalid or empty `contract_amount` is retained as `0.0` and flagged in the contract summary.

## Samples

### STE


### Contracts


## Pending Inputs

- Реальный запуск pipeline заблокирован до появления обоих входных файлов.
- Второй участник команды должен положить очищенный контрактный датасет в один из ожидаемых путей либо в корень репозитория по маске `Контракты_*.csv`.

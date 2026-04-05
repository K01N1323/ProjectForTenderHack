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

- Status: `ready`
- STE path: `/Users/eugenytokmakov/Desktop/programming/tenderhack/ProjectForTenderHack/data/processed/ste_catalog_personalization_quick.csv`
- Contracts path: `/Users/eugenytokmakov/Desktop/programming/tenderhack/ProjectForTenderHack/data/processed/contracts_personalization_quick.csv`

## Observed STE Dataset

- Rows: 12,575
- Unique `ste_id`: 12,575
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

- Rows: 20,000
- Unique compound keys (`contract_id`, `ste_id`, `customer_inn`): 19,985
- Duplicate compound keys: 15
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
- Contract rows without STE catalog match: 34
- Catalog STE without contract history: 0

## Missing Data Notes

- Empty `customer_inn` is replaced with `UNKNOWN` during loading.
- Empty `customer_region` and `supplier_region` are replaced with `UNKNOWN` during loading.
- Invalid `contract_datetime` rows are skipped because ranking splits are time-based.
- Invalid or empty `contract_amount` is retained as `0.0` and flagged in the contract summary.

## Samples

### STE

- `{'ste_id': '1159353', 'clean_name': 'Karcher SV 7 1.439-410.0 паропылесос', 'category': 'Пылесосы', 'attribute_count': 16}`
- `{'ste_id': '1159612', 'clean_name': 'Bosch BCH6ATH18 пылесос', 'category': 'Пылесосы', 'attribute_count': 14}`
- `{'ste_id': '1159727', 'clean_name': 'Иглы для бытовых швейных машин "Schmetz", комбинированные, 9 шт', 'category': 'Принадлежности для шитья и рукоделия металлические', 'attribute_count': 20}`
- `{'ste_id': '1160180', 'clean_name': 'Утюг Vitek VT-1263(B)', 'category': 'Утюги электрические бытовые', 'attribute_count': 10}`
- `{'ste_id': '1160204', 'clean_name': 'Утюг Maxwell MW-3042(VT)', 'category': 'Утюги электрические бытовые', 'attribute_count': 21}`

### Contracts

- `{'contract_id': '203255114', 'ste_id': '36047183', 'customer_inn': '7417005438', 'contract_date': '2024-06-05', 'contract_amount': 1200.0}`
- `{'contract_id': '194452173', 'ste_id': '35778463', 'customer_inn': '5911021660', 'contract_date': '2022-05-26', 'contract_amount': 16800.0}`
- `{'contract_id': '214274949', 'ste_id': '46358579', 'customer_inn': '5944020101', 'contract_date': '2026-02-11', 'contract_amount': 6825.0}`
- `{'contract_id': '199009522', 'ste_id': '24120140', 'customer_inn': '8907002558', 'contract_date': '2023-06-17', 'contract_amount': 205000.0}`
- `{'contract_id': '193681593', 'ste_id': '35754688', 'customer_inn': '8901007006', 'contract_date': '2022-04-01', 'contract_amount': 73130.0}`

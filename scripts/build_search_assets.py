#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path


PROGRESS_EVERY = 100_000


def build_search_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA temp_store=MEMORY;

        DROP TABLE IF EXISTS ste_catalog;
        DROP TABLE IF EXISTS token_frequency;
        DROP TABLE IF EXISTS search_metadata;
        DROP TABLE IF EXISTS ste_catalog_fts;

        CREATE TABLE ste_catalog (
            ste_id TEXT PRIMARY KEY,
            clean_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            category TEXT NOT NULL,
            normalized_category TEXT NOT NULL,
            attribute_keys TEXT,
            attribute_count INTEGER NOT NULL,
            key_tokens TEXT NOT NULL
        );

        CREATE VIRTUAL TABLE ste_catalog_fts USING fts5(
            clean_name,
            normalized_name,
            category,
            normalized_category,
            key_tokens,
            content='ste_catalog',
            content_rowid='rowid',
            tokenize='unicode61 remove_diacritics 2'
        );

        CREATE TABLE token_frequency (
            token TEXT PRIMARY KEY,
            first_char TEXT NOT NULL,
            token_length INTEGER NOT NULL,
            frequency INTEGER NOT NULL
        );

        CREATE INDEX token_frequency_lookup_idx
        ON token_frequency(first_char, token_length, frequency DESC);

        CREATE TABLE search_metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )
    conn.commit()


def tokenize(value: str) -> list[str]:
    return [token for token in value.split() if token]


def build_search_db(catalog_path: Path, search_db_path: Path) -> None:
    if search_db_path.exists():
        search_db_path.unlink()
    conn = sqlite3.connect(search_db_path)
    try:
        build_search_schema(conn)
        rows_seen = 0
        with catalog_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            batch = []
            for row in reader:
                rows_seen += 1
                batch.append(
                    (
                        row["ste_id"],
                        row["clean_name"],
                        row["normalized_name"],
                        row["category"],
                        row["normalized_category"],
                        row["attribute_keys"],
                        int(row["attribute_count"] or 0),
                        row["key_tokens"],
                    )
                )
                if len(batch) >= 10_000:
                    conn.executemany(
                        """
                        INSERT INTO ste_catalog (
                            ste_id, clean_name, normalized_name, category, normalized_category,
                            attribute_keys, attribute_count, key_tokens
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(ste_id) DO UPDATE SET
                            clean_name = CASE
                                WHEN length(excluded.clean_name) > length(ste_catalog.clean_name) THEN excluded.clean_name
                                ELSE ste_catalog.clean_name
                            END,
                            normalized_name = CASE
                                WHEN length(excluded.normalized_name) > length(ste_catalog.normalized_name) THEN excluded.normalized_name
                                ELSE ste_catalog.normalized_name
                            END,
                            category = CASE
                                WHEN length(excluded.category) > length(ste_catalog.category) THEN excluded.category
                                ELSE ste_catalog.category
                            END,
                            normalized_category = CASE
                                WHEN length(excluded.normalized_category) > length(ste_catalog.normalized_category) THEN excluded.normalized_category
                                ELSE ste_catalog.normalized_category
                            END,
                            attribute_keys = CASE
                                WHEN length(excluded.attribute_keys) > length(COALESCE(ste_catalog.attribute_keys, '')) THEN excluded.attribute_keys
                                ELSE ste_catalog.attribute_keys
                            END,
                            attribute_count = MAX(ste_catalog.attribute_count, excluded.attribute_count),
                            key_tokens = CASE
                                WHEN length(excluded.key_tokens) > length(ste_catalog.key_tokens) THEN excluded.key_tokens
                                ELSE ste_catalog.key_tokens
                            END
                        """,
                        batch,
                    )
                    conn.commit()
                    batch.clear()
                if rows_seen % PROGRESS_EVERY == 0:
                    print(f"[Search DB] processed {rows_seen:,} source rows", flush=True)
            if batch:
                conn.executemany(
                    """
                    INSERT INTO ste_catalog (
                        ste_id, clean_name, normalized_name, category, normalized_category,
                        attribute_keys, attribute_count, key_tokens
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(ste_id) DO UPDATE SET
                        clean_name = CASE
                            WHEN length(excluded.clean_name) > length(ste_catalog.clean_name) THEN excluded.clean_name
                            ELSE ste_catalog.clean_name
                        END,
                        normalized_name = CASE
                            WHEN length(excluded.normalized_name) > length(ste_catalog.normalized_name) THEN excluded.normalized_name
                            ELSE ste_catalog.normalized_name
                        END,
                        category = CASE
                            WHEN length(excluded.category) > length(ste_catalog.category) THEN excluded.category
                            ELSE ste_catalog.category
                        END,
                        normalized_category = CASE
                            WHEN length(excluded.normalized_category) > length(ste_catalog.normalized_category) THEN excluded.normalized_category
                            ELSE ste_catalog.normalized_category
                        END,
                        attribute_keys = CASE
                            WHEN length(excluded.attribute_keys) > length(COALESCE(ste_catalog.attribute_keys, '')) THEN excluded.attribute_keys
                            ELSE ste_catalog.attribute_keys
                        END,
                        attribute_count = MAX(ste_catalog.attribute_count, excluded.attribute_count),
                        key_tokens = CASE
                            WHEN length(excluded.key_tokens) > length(ste_catalog.key_tokens) THEN excluded.key_tokens
                            ELSE ste_catalog.key_tokens
                        END
                    """,
                    batch,
                )
                conn.commit()

        conn.execute("INSERT INTO ste_catalog_fts(ste_catalog_fts) VALUES ('rebuild')")
        conn.commit()

        token_counter: Counter[str] = Counter()
        row_count = 0
        for normalized_name, normalized_category, key_tokens in conn.execute(
            "SELECT normalized_name, normalized_category, key_tokens FROM ste_catalog"
        ):
            row_count += 1
            token_counter.update(tokenize(normalized_name))
            token_counter.update(tokenize(normalized_category))
            token_counter.update(tokenize(key_tokens))
            if row_count % PROGRESS_EVERY == 0:
                print(f"[Search DB] tokenized {row_count:,} deduped rows", flush=True)

        token_rows = []
        for token, frequency in token_counter.items():
            if len(token) <= 1:
                continue
            token_rows.append((token, token[0], len(token), frequency))
        conn.executemany(
            "INSERT INTO token_frequency (token, first_char, token_length, frequency) VALUES (?, ?, ?, ?)",
            token_rows,
        )
        conn.executemany(
            "INSERT INTO search_metadata (key, value) VALUES (?, ?)",
            [
                ("source_rows", str(rows_seen)),
                ("deduped_rows", str(conn.execute("SELECT COUNT(*) FROM ste_catalog").fetchone()[0])),
                ("token_count", str(conn.execute("SELECT COUNT(*) FROM token_frequency").fetchone()[0])),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def build_customer_region_lookup(contracts_path: Path, preprocessed_db_path: Path, output_csv_path: Path) -> None:
    region_counter: dict[str, Counter[str]] = defaultdict(Counter)
    rows_seen = 0
    with contracts_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=";", quotechar='"')
        for row in reader:
            rows_seen += 1
            if len(row) != 11:
                continue
            customer_inn = row[5].strip()
            customer_region = row[7].strip()
            if customer_inn and customer_region:
                region_counter[customer_inn][customer_region] += 1
            if rows_seen % 250_000 == 0:
                print(f"[Customer Region] processed {rows_seen:,} contract rows", flush=True)

    records = []
    for customer_inn, counter in region_counter.items():
        customer_region, frequency = counter.most_common(1)[0]
        records.append((customer_inn, customer_region, frequency))
    records.sort(key=lambda item: (item[1], item[0]))

    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with output_csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["customer_inn", "customer_region", "frequency"])
        writer.writerows(records)

    conn = sqlite3.connect(preprocessed_db_path)
    try:
        conn.executescript(
            """
            DROP TABLE IF EXISTS customer_region_lookup;
            CREATE TABLE customer_region_lookup (
                customer_inn TEXT PRIMARY KEY,
                customer_region TEXT NOT NULL,
                frequency INTEGER NOT NULL
            );
            CREATE INDEX customer_region_lookup_region_idx
            ON customer_region_lookup(customer_region);
            """
        )
        conn.executemany(
            "INSERT INTO customer_region_lookup (customer_inn, customer_region, frequency) VALUES (?, ?, ?)",
            records,
        )
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build search and personalization assets.")
    parser.add_argument("--catalog-path", default="data/processed/ste_catalog_search_ready.csv")
    parser.add_argument("--contracts-path", default="Контракты_20260403.csv")
    parser.add_argument("--search-db-path", default="data/processed/tenderhack_search.sqlite")
    parser.add_argument("--preprocessed-db-path", default="data/processed/tenderhack_preprocessed.sqlite")
    parser.add_argument("--customer-region-output", default="data/processed/customer_region_lookup.csv")
    args = parser.parse_args()

    build_search_db(Path(args.catalog_path), Path(args.search_db_path))
    build_customer_region_lookup(
        contracts_path=Path(args.contracts_path),
        preprocessed_db_path=Path(args.preprocessed_db_path),
        output_csv_path=Path(args.customer_region_output),
    )


if __name__ == "__main__":
    main()

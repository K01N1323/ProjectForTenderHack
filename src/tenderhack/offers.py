from __future__ import annotations

import csv
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List


DEFAULT_PREPROCESSED_DB = Path("data/processed/tenderhack_preprocessed.sqlite")
DEFAULT_CONTRACTS_PATH = Path("Контракты_20260403.csv")


def _chunked(values: List[str], size: int = 800) -> Iterable[List[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


class OfferLookupService:
    def __init__(self, db_path: Path | str = DEFAULT_PREPROCESSED_DB) -> None:
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def close(self) -> None:
        self.conn.close()

    def has_offer_lookup(self) -> bool:
        row = self.conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = 'ste_offer_lookup'
            LIMIT 1
            """
        ).fetchone()
        return row is not None

    def get_offer_lookup(self, ste_ids: Iterable[str]) -> Dict[str, Dict[str, object]]:
        normalized_ids = [str(value) for value in ste_ids if value]
        if not normalized_ids:
            return {}
        if self.has_offer_lookup():
            return self._load_offer_lookup(normalized_ids)
        return self._load_estimated_lookup(normalized_ids)

    def _load_offer_lookup(self, ste_ids: List[str]) -> Dict[str, Dict[str, object]]:
        result: Dict[str, Dict[str, object]] = {}
        for chunk in _chunked(ste_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""
                SELECT
                    ste_id,
                    supplier_inn,
                    supplier_region,
                    offer_count,
                    avg_price,
                    min_price,
                    last_contract_dt
                FROM ste_offer_lookup
                WHERE ste_id IN ({placeholders})
                """,
                chunk,
            ).fetchall()
            for row in rows:
                result[row["ste_id"]] = {
                    "supplier_inn": row["supplier_inn"] or "не указан",
                    "supplier_region": row["supplier_region"] or "",
                    "offer_count": int(row["offer_count"] or 0),
                    "avg_price": round(float(row["avg_price"] or 0.0), 2),
                    "min_price": round(float(row["min_price"] or 0.0), 2),
                    "price": round(float(row["min_price"] or row["avg_price"] or 0.0), 2),
                    "last_contract_dt": row["last_contract_dt"],
                    "price_source": "contracts_lookup",
                }
        return result

    def _load_estimated_lookup(self, ste_ids: List[str]) -> Dict[str, Dict[str, object]]:
        result: Dict[str, Dict[str, object]] = {}
        for chunk in _chunked(ste_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""
                SELECT
                    ste_id,
                    SUM(purchase_count) AS purchase_count,
                    SUM(total_amount) AS total_amount
                FROM customer_ste_stats
                WHERE ste_id IN ({placeholders})
                GROUP BY ste_id
                """,
                chunk,
            ).fetchall()
            for row in rows:
                purchase_count = int(row["purchase_count"] or 0)
                total_amount = float(row["total_amount"] or 0.0)
                avg_price = total_amount / purchase_count if purchase_count else 0.0
                result[row["ste_id"]] = {
                    "supplier_inn": "не указан",
                    "supplier_region": "",
                    "offer_count": purchase_count,
                    "avg_price": round(avg_price, 2),
                    "min_price": round(avg_price, 2),
                    "price": round(avg_price, 2),
                    "last_contract_dt": None,
                    "price_source": "estimated_from_history",
                }
        return result


def build_offer_lookup_table(
    contracts_path: Path | str = DEFAULT_CONTRACTS_PATH,
    db_path: Path | str = DEFAULT_PREPROCESSED_DB,
) -> int:
    contracts_path = Path(contracts_path)
    db_path = Path(db_path)

    aggregates: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "offer_count": 0,
            "total_amount": 0.0,
            "min_price": None,
            "supplier_inn": "не указан",
            "supplier_region": "",
            "last_contract_dt": None,
        }
    )

    with contracts_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=";", quotechar='"')
        for row in reader:
            if len(row) != 11:
                continue
            ste_id = row[2].strip()
            if not ste_id:
                continue
            contract_dt = row[3].strip()[:10] if row[3].strip() else None
            try:
                amount = float(row[4].strip() or 0.0)
            except ValueError:
                amount = 0.0
            supplier_inn = row[8].strip() or "не указан"
            supplier_region = row[10].strip()

            payload = aggregates[ste_id]
            payload["offer_count"] = int(payload["offer_count"]) + 1
            payload["total_amount"] = float(payload["total_amount"]) + amount

            min_price = payload["min_price"]
            if min_price is None or amount < float(min_price):
                payload["min_price"] = amount
                payload["supplier_inn"] = supplier_inn
                payload["supplier_region"] = supplier_region

            last_contract_dt = payload["last_contract_dt"]
            if contract_dt and (last_contract_dt is None or contract_dt > str(last_contract_dt)):
                payload["last_contract_dt"] = contract_dt

    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            DROP TABLE IF EXISTS ste_offer_lookup;
            CREATE TABLE ste_offer_lookup (
                ste_id TEXT PRIMARY KEY,
                supplier_inn TEXT NOT NULL,
                supplier_region TEXT,
                offer_count INTEGER NOT NULL,
                avg_price REAL NOT NULL,
                min_price REAL NOT NULL,
                last_contract_dt TEXT
            );
            CREATE INDEX ste_offer_lookup_supplier_idx
            ON ste_offer_lookup(supplier_inn);
            """
        )

        rows = []
        for ste_id, payload in aggregates.items():
            offer_count = int(payload["offer_count"])
            total_amount = float(payload["total_amount"])
            avg_price = total_amount / offer_count if offer_count else 0.0
            rows.append(
                (
                    ste_id,
                    str(payload["supplier_inn"] or "не указан"),
                    str(payload["supplier_region"] or ""),
                    offer_count,
                    round(avg_price, 2),
                    round(float(payload["min_price"] or avg_price or 0.0), 2),
                    payload["last_contract_dt"],
                )
            )
        conn.executemany(
            """
            INSERT INTO ste_offer_lookup (
                ste_id, supplier_inn, supplier_region, offer_count, avg_price, min_price, last_contract_dt
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    return len(aggregates)

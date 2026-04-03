from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .text import normalize_text, normalize_tokens, stem_token, stem_tokens, tokenize, unique_preserve_order


DEFAULT_SEARCH_DB = Path("data/processed/tenderhack_search.sqlite")
DEFAULT_SYNONYMS_PATH = Path("data/reference/search_synonyms.json")


def _edit_distance(left: str, right: str, max_distance: int = 2) -> int:
    if left == right:
        return 0
    if abs(len(left) - len(right)) > max_distance:
        return max_distance + 1
    previous = list(range(len(right) + 1))
    for i, left_char in enumerate(left, start=1):
        current = [i]
        best = current[0]
        for j, right_char in enumerate(right, start=1):
            cost = 0 if left_char == right_char else 1
            value = min(
                previous[j] + 1,
                current[j - 1] + 1,
                previous[j - 1] + cost,
            )
            current.append(value)
            best = min(best, value)
        if best > max_distance:
            return max_distance + 1
        previous = current
    return previous[-1]


@dataclass
class QueryAnalysis:
    original_query: str
    normalized_query: str
    corrected_query: str
    original_tokens: List[str]
    corrected_tokens: List[str]
    stemmed_tokens: List[str]
    expanded_tokens: List[str]
    expanded_stems: List[str]
    applied_corrections: List[Dict[str, str]]
    applied_synonyms: List[Dict[str, List[str]]]


class TypoCorrector:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def _candidate_tokens(self, token: str) -> List[sqlite3.Row]:
        first_char = token[0]
        length = len(token)
        candidates: Dict[str, sqlite3.Row] = {}

        prefix = token[: min(4, len(token))]
        for row in self.conn.execute(
            """
            SELECT token, frequency
            FROM token_frequency
            WHERE token LIKE ?
              AND token_length BETWEEN ? AND ?
            ORDER BY frequency DESC
            LIMIT 150
            """,
            (f"{prefix}%", max(1, length - 2), length + 2),
        ).fetchall():
            candidates[row["token"]] = row

        for row in self.conn.execute(
            """
            SELECT token, frequency
            FROM token_frequency
            WHERE first_char = ?
              AND token_length BETWEEN ? AND ?
            ORDER BY frequency DESC
            LIMIT 250
            """,
            (first_char, max(1, length - 2), length + 2),
        ).fetchall():
            candidates.setdefault(row["token"], row)

        if candidates:
            return sorted(candidates.values(), key=lambda row: row["frequency"], reverse=True)

        return self.conn.execute(
            """
            SELECT token, frequency
            FROM token_frequency
            WHERE token_length BETWEEN ? AND ?
            ORDER BY frequency DESC
            LIMIT 250
            """,
            (max(1, length - 1), length + 1),
        ).fetchall()

    def correct_tokens(self, tokens: Iterable[str]) -> tuple[List[str], List[Dict[str, str]]]:
        corrected: List[str] = []
        applied: List[Dict[str, str]] = []
        for token in tokens:
            if len(token) <= 2 or token.isdigit():
                corrected.append(token)
                continue
            exists = self.conn.execute(
                "SELECT 1 FROM token_frequency WHERE token = ? LIMIT 1",
                (token,),
            ).fetchone()
            if exists:
                corrected.append(token)
                continue
            best_token = token
            best_distance = 99
            best_frequency = -1
            for row in self._candidate_tokens(token):
                candidate = row["token"]
                distance = _edit_distance(token, candidate, max_distance=2)
                if distance > 2:
                    continue
                if distance < best_distance or (distance == best_distance and row["frequency"] > best_frequency):
                    best_token = candidate
                    best_distance = distance
                    best_frequency = row["frequency"]
            corrected.append(best_token)
            if best_token != token:
                applied.append({"source": token, "target": best_token})
        return corrected, applied


class SearchService:
    def __init__(
        self,
        search_db_path: Path | str = DEFAULT_SEARCH_DB,
        synonyms_path: Path | str = DEFAULT_SYNONYMS_PATH,
    ) -> None:
        self.search_db_path = Path(search_db_path)
        self.synonyms_path = Path(synonyms_path)
        self.conn = sqlite3.connect(self.search_db_path)
        self.conn.row_factory = sqlite3.Row
        self.corrector = TypoCorrector(self.conn)
        self.synonyms = self._load_synonyms()

    def close(self) -> None:
        self.conn.close()

    def _load_synonyms(self) -> Dict[str, Dict[str, List[str]]]:
        payload = json.loads(self.synonyms_path.read_text(encoding="utf-8"))
        phrase_synonyms = {normalize_text(key): [normalize_text(value) for value in values] for key, values in payload.get("phrase_synonyms", {}).items()}
        token_synonyms = {normalize_text(key): [normalize_text(value) for value in values] for key, values in payload.get("token_synonyms", {}).items()}
        return {
            "phrase_synonyms": phrase_synonyms,
            "token_synonyms": token_synonyms,
        }

    def _apply_synonyms(self, normalized_query: str, corrected_tokens: List[str]) -> tuple[List[str], List[Dict[str, List[str]]]]:
        expanded: List[str] = list(corrected_tokens)
        applied: List[Dict[str, List[str]]] = []
        phrase_synonyms = self.synonyms["phrase_synonyms"]
        token_synonyms = self.synonyms["token_synonyms"]

        for phrase, replacements in phrase_synonyms.items():
            if phrase and phrase in normalized_query:
                expanded.extend(replacements)
                applied.append({"source": phrase, "targets": replacements})

        for token in corrected_tokens:
            replacements = token_synonyms.get(token)
            if replacements:
                expanded.extend(replacements)
                applied.append({"source": token, "targets": replacements})

        expanded_tokens: List[str] = []
        for item in expanded:
            expanded_tokens.extend(tokenize(item))
        return unique_preserve_order(expanded_tokens), applied

    def analyze_query(self, query: str) -> QueryAnalysis:
        normalized_query = normalize_text(query)
        original_tokens = normalize_tokens(tokenize(normalized_query))
        corrected_tokens, corrections = self.corrector.correct_tokens(original_tokens)
        corrected_tokens = normalize_tokens(corrected_tokens)
        corrected_query = " ".join(corrected_tokens)
        synonym_expansions, applied_synonyms = self._apply_synonyms(corrected_query or normalized_query, corrected_tokens)
        merged_tokens = unique_preserve_order(corrected_tokens + synonym_expansions)
        stemmed_tokens = stem_tokens(corrected_tokens)
        expanded_stems = stem_tokens(merged_tokens)
        return QueryAnalysis(
            original_query=query,
            normalized_query=normalized_query,
            corrected_query=corrected_query,
            original_tokens=original_tokens,
            corrected_tokens=corrected_tokens,
            stemmed_tokens=stemmed_tokens,
            expanded_tokens=merged_tokens,
            expanded_stems=expanded_stems,
            applied_corrections=corrections,
            applied_synonyms=applied_synonyms,
        )

    def _build_match_query(self, analysis: QueryAnalysis) -> str:
        terms: List[str] = []
        for token in analysis.corrected_tokens:
            if token:
                terms.append(f"{token}*")
        for stem in analysis.expanded_stems:
            if len(stem) >= 3:
                terms.append(f"{stem}*")
        terms = unique_preserve_order(terms)
        if not terms:
            return ""
        return " OR ".join(terms)

    def _fetch_candidates(self, analysis: QueryAnalysis, candidate_limit: int = 250) -> List[sqlite3.Row]:
        match_query = self._build_match_query(analysis)
        if not match_query:
            return []
        rows = self.conn.execute(
            """
            SELECT
                ste_catalog.rowid AS row_id,
                ste_catalog.ste_id,
                ste_catalog.clean_name,
                ste_catalog.normalized_name,
                ste_catalog.category,
                ste_catalog.normalized_category,
                ste_catalog.attribute_keys,
                ste_catalog.attribute_count,
                ste_catalog.key_tokens,
                bm25(ste_catalog_fts, 1.6, 2.1, 0.9, 1.2, 0.7) AS bm25_score
            FROM ste_catalog_fts
            JOIN ste_catalog ON ste_catalog_fts.rowid = ste_catalog.rowid
            WHERE ste_catalog_fts MATCH ?
            ORDER BY bm25_score
            LIMIT ?
            """,
            (match_query, candidate_limit),
        ).fetchall()
        return rows

    def _score_candidate(self, row: sqlite3.Row, analysis: QueryAnalysis) -> tuple[float, Dict[str, float]]:
        name_tokens = set(tokenize(row["normalized_name"]))
        category_tokens = set(tokenize(row["normalized_category"]))
        key_tokens = set(tokenize(row["key_tokens"]))

        name_stems = set(stem_tokens(name_tokens))
        category_stems = set(stem_tokens(category_tokens))
        key_stems = set(stem_tokens(key_tokens))

        corrected_set = set(analysis.corrected_tokens)
        stem_set = set(analysis.stemmed_tokens)
        expanded_stem_set = set(analysis.expanded_stems)

        query_target = analysis.corrected_query or analysis.normalized_query
        exact_phrase = 1.0 if query_target and query_target in row["normalized_name"] else 0.0
        corrected_hits = len(corrected_set & name_tokens)
        stem_hits_name = len(stem_set & name_stems)
        stem_hits_category = len(expanded_stem_set & category_stems)
        stem_hits_key = len(expanded_stem_set & key_stems)
        full_name_cover = 1.0 if stem_set and stem_set.issubset(name_stems) else 0.0
        full_category_cover = 1.0 if stem_set and stem_set.issubset(category_stems) else 0.0
        synonym_bonus = 1.0 if analysis.applied_synonyms and stem_hits_name + stem_hits_category > 0 else 0.0

        coverage_denominator = max(1, len(corrected_set))
        stem_denominator = max(1, len(stem_set))
        expanded_denominator = max(1, len(expanded_stem_set))

        bm25_score = row["bm25_score"] if row["bm25_score"] is not None else 0.0
        bm25_component = 1.0 / (1.0 + max(0.0, bm25_score))

        score = 0.0
        score += 12.0 * exact_phrase
        score += 4.0 * full_name_cover
        score += 6.0 * full_category_cover
        score += 6.0 * (corrected_hits / coverage_denominator)
        score += 4.0 * (stem_hits_name / stem_denominator)
        score += 3.0 * (stem_hits_category / expanded_denominator)
        score += 2.0 * (stem_hits_key / expanded_denominator)
        score += 1.5 * synonym_bonus
        score += 2.0 * bm25_component

        features = {
            "exact_phrase": round(exact_phrase, 4),
            "full_name_cover": round(full_name_cover, 4),
            "full_category_cover": round(full_category_cover, 4),
            "corrected_token_overlap": round(corrected_hits / coverage_denominator, 4),
            "name_stem_overlap": round(stem_hits_name / stem_denominator, 4),
            "category_stem_overlap": round(stem_hits_category / expanded_denominator, 4),
            "key_token_overlap": round(stem_hits_key / expanded_denominator, 4),
            "synonym_bonus": round(synonym_bonus, 4),
            "bm25_component": round(bm25_component, 4),
        }
        return score, features

    def search(self, query: str, top_k: int = 20, candidate_limit: int = 250) -> Dict[str, object]:
        analysis = self.analyze_query(query)
        candidates = self._fetch_candidates(analysis, candidate_limit=candidate_limit)
        scored_results: List[Dict[str, object]] = []
        for row in candidates:
            lexical_score, features = self._score_candidate(row, analysis)
            scored_results.append(
                {
                    "ste_id": row["ste_id"],
                    "clean_name": row["clean_name"],
                    "normalized_name": row["normalized_name"],
                    "category": row["category"],
                    "normalized_category": row["normalized_category"],
                    "attribute_keys": row["attribute_keys"],
                    "attribute_count": int(row["attribute_count"] or 0),
                    "key_tokens": row["key_tokens"],
                    "search_score": round(lexical_score, 4),
                    "search_features": features,
                }
            )

        scored_results.sort(
            key=lambda item: (
                item["search_score"],
                item["search_features"]["exact_phrase"],
                item["search_features"]["corrected_token_overlap"],
                item["search_features"]["category_stem_overlap"],
            ),
            reverse=True,
        )
        return {
            "query": {
                "original_query": analysis.original_query,
                "normalized_query": analysis.normalized_query,
                "corrected_query": analysis.corrected_query,
                "applied_corrections": analysis.applied_corrections,
                "applied_synonyms": analysis.applied_synonyms,
                "expanded_tokens": analysis.expanded_tokens,
            },
            "results": scored_results[:top_k],
        }

    def search_ste(self, query: str, top_k: int = 20) -> List[Dict[str, object]]:
        return self.search(query, top_k=top_k)["results"]


def search_ste(
    query: str,
    top_k: int = 20,
    search_db_path: Path | str = DEFAULT_SEARCH_DB,
    synonyms_path: Path | str = DEFAULT_SYNONYMS_PATH,
) -> List[Dict[str, object]]:
    service = SearchService(search_db_path=search_db_path, synonyms_path=synonyms_path)
    try:
        return service.search_ste(query=query, top_k=top_k)
    finally:
        service.close()

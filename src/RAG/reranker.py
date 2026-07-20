import logging

from common.models import SearchResult

log = logging.getLogger(__name__)


def rerank(query: str, results: list[SearchResult]) -> list[SearchResult]:
    if not results:
        return results
    scored = []
    query_lower = query.lower()
    query_terms = set(query_lower.split())
    for r in results:
        boost = _title_match_boost(r, query_terms) + _recency_boost(r)
        scored.append((r.score + boost, r))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [r for _, r in scored]


def _title_match_boost(r: SearchResult, query_terms: set[str]) -> float:
    if not query_terms:
        return 0.0
    title_lower = r.title.lower()
    matches = sum(1 for t in query_terms if t in title_lower)
    if matches == len(query_terms):
        return 0.3
    if matches > 0:
        return 0.15
    return 0.0


def _recency_boost(r: SearchResult) -> float:
    return 0.0

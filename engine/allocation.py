import logging
from config import TOP_TIER_PCT, TOP_TIER_MIN, TOP_TIER_ALLOC, REST_ALLOC, INVESTABLE_PCT
from db.client import get_all_units_with_scoring, upsert_scoring, update_unit, get_portfolio_state

log = logging.getLogger(__name__)

def _rank_ascending(units, key):
    sorted_units = sorted(units, key=lambda u: u[key] or 0, reverse=True)
    return {u["unit_id"]: i + 1 for i, u in enumerate(sorted_units)}

def _compute_ranks(units):
    score_ranks = _rank_ascending(units, "score")
    return_ranks = _rank_ascending(units, "max_return")
    green_ranks = _rank_ascending(units, "green_count")

    result = {}
    for u in units:
        uid = u["unit_id"]
        sr = score_ranks[uid]
        rr = return_ranks[uid]
        gr = green_ranks[uid]
        result[uid] = {
            "score_rank": sr,
            "return_rank": rr,
            "green_count_rank": gr,
            "composite_rank": sr + rr,
        }
    return result

def _top_tier_count(n):
    return min(n, max(TOP_TIER_MIN, round(n * TOP_TIER_PCT)))

def _proportional_alloc(units_in_tier, key, budget):
    total_value = sum(u[key] or 0 for u in units_in_tier)
    if total_value == 0:
        equal = budget / len(units_in_tier)
        return {u["unit_id"]: equal for u in units_in_tier}

    return {u["unit_id"]: (u[key] or 0) / total_value * budget for u in units_in_tier}

def _criterion_allocation(units, ranks, rank_key, value_key, y):
    n = len(units)
    top_n = _top_tier_count(n)
    sorted_units = sorted(units, key=lambda u: ranks[u["unit_id"]][rank_key])

    top_tier = sorted_units[:top_n]
    rest = sorted_units[top_n:]
    allocs = {}

    allocs.update(_proportional_alloc(top_tier, value_key, y * TOP_TIER_ALLOC))
    if rest:
        allocs.update(_proportional_alloc(rest, value_key, y * REST_ALLOC))
    return allocs

def run_allocation():
    rows = get_all_units_with_scoring()
    if not rows:
        return

    units = []
    for row in rows:
        scoring = row.get("scoring") or {}
        flat = {**row}
        flat["score"] = max(0.0, float(scoring.get("score") or 0.0))
        flat["green_count"] = max(0.0, float(scoring.get("green_count") or 0.0))
        flat["max_return"] = max(0.0, float(scoring.get("max_return") or 0.0))
        units.append(flat)

    ranks = _compute_ranks(units)
    for uid, rank_data in ranks.items():
        upsert_scoring(uid, rank_data)

    state = get_portfolio_state()
    if not state:
        log.error("No portfolio_state row found")
        return

    investable = float(state["total_capital"]) * INVESTABLE_PCT
    y = investable / 3

    alloc_score = _criterion_allocation(units, ranks, "score_rank", "score", y)
    alloc_green = _criterion_allocation(
        units, ranks, "green_count_rank", "green_count", y)
    alloc_return = _criterion_allocation(
        units, ranks, "return_rank", "max_return", y)

    for u in units:
        uid = u["unit_id"]
        a_score = alloc_score.get(uid, 0)
        a_green = alloc_green.get(uid, 0)
        a_return = alloc_return.get(uid, 0)

        update_unit(uid, {
            "allocation_score": a_score,
            "allocation_green_count": a_green,
            "allocation_max_return": a_return,
        })

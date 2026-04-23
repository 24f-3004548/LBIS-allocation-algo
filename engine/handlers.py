import logging
from datetime import date

from config import BOTTOM_FISH_PCT, BOTTOM_FISH_MAX, SELL_LADDER_STEPS
from db.client import (
    get_unit,
    update_unit,
    write_ledger,
    upsert_portfolio_state,
    get_portfolio_state,
    delete_scoring,
    insert_directive,
)

log = logging.getLogger(__name__)

# ── Shared helpers ────────────────────────────────────────────────────────────

def _current_date():
    return date.today().isoformat()

def _update_portfolio_invested(delta):
    state = get_portfolio_state()
    if not state:
        log.warning(
            "[portfolio] No portfolio_state row — cannot update invested.")
        return
    new_invested = float(state["total_invested"]) + delta
    upsert_portfolio_state({"total_invested": new_invested})

def _shares_for_amount(amount, price):
    if price <= 0:
        raise ValueError(f"Invalid price: {price}")
    return int(amount // price)

def _get_l1_l4_proceeds(unit_id):
    from db.client import db
    res = (
        db.table("ledger")
        .select("delta_investment, stock_directives(directive)")
        .eq("unit_id", unit_id)
        .execute()
    )
    total = 0.0
    for row in res.data:
        directive = (row.get("stock_directives") or {}).get("directive", "")
        if directive in ("SELL L1", "SELL L2", "SELL L3", "SELL L4"):
            total += abs(float(row["delta_investment"]))
    return total

# ── BUY / BUY-IN-BUY ─────────────────────────────────────────────────────────

def handle_buy(directive):
    unit_id = directive["unit_id"]
    directive_id = directive["directive_id"]
    price = float(directive["current_price"])
    unit = get_unit(unit_id)

    # Determine allocation amount for this unit
    # At BUY time, use allocation fields already written by scoring/allocation
    alloc_score = float(unit.get("allocation_score") or 0)
    alloc_green = float(unit.get("allocation_green_count") or 0)
    alloc_return = float(unit.get("allocation_max_return") or 0)
    total_alloc = alloc_score + alloc_green + alloc_return

    if total_alloc <= 0:
        log.warning(
            f"[BUY] Unit {unit_id} has no allocation yet — "
            "scoring must run first. Skipping ledger entry."
        )
        return

    shares = _shares_for_amount(total_alloc, price)
    actual_investment = shares * price

    # Update positions
    update_unit(unit_id, {
        "num_shares":       shares,
        "total_investment": actual_investment,
        "status":           "active",
    })

    # Ledger entry (positive = buy)
    write_ledger(unit_id, directive_id, shares, actual_investment)

    # Portfolio state
    _update_portfolio_invested(actual_investment)

    log.info(
        f"[BUY] unit={unit_id} shares={shares} @ {price} "
        f"investment={actual_investment:.2f}"
    )

# ── PARTIAL SELL ──────────────────────────────────────────────────────────────

def handle_partial_sell(directive):
    unit_id = directive["unit_id"]
    directive_id = directive["directive_id"]
    price = float(directive["current_price"])
    unit = get_unit(unit_id)

    if unit["partial_sell_done"]:
        log.warning(
            f"[PARTIAL SELL] Unit {unit_id} already has partial_sell_done=True. Skipping.")
        return

    total_investment = float(unit["total_investment"])
    shares_to_sell = _shares_for_amount(total_investment, price)
    shares_to_sell = min(shares_to_sell, unit["num_shares"])  # safety cap
    proceeds = shares_to_sell * price

    new_shares = unit["num_shares"] - shares_to_sell

    update_unit(unit_id, {
        "num_shares":       new_shares,
        "total_investment": float(unit["total_investment"]) - proceeds,
        "status":           "free",
        "partial_sell_done": True,
    })

    write_ledger(unit_id, directive_id, -shares_to_sell, -proceeds)
    _update_portfolio_invested(-proceeds)

    log.info(
        f"[PARTIAL SELL] unit={unit_id} sold={shares_to_sell} shares "
        f"@ {price} proceeds={proceeds:.2f} → status=free"
    )

# ── SELL L1 – L4 ─────────────────────────────────────────────────────────────

# Map directive name → target cumulative pct after execution
_SELL_LEVEL_TARGET = {
    "SELL L1": 25,
    "SELL L2": 50,
    "SELL L3": 75,
    "SELL L4": 100,
}

def handle_sell_ladder(directive):
    unit_id = directive["unit_id"]
    directive_id = directive["directive_id"]
    price = float(directive["current_price"])
    level_name = directive["directive"]
    unit = get_unit(unit_id)

    target_pct = _SELL_LEVEL_TARGET[level_name]
    current_pct = unit["sell_ladder_pct"]

    if current_pct >= target_pct:
        log.warning(
            f"[{level_name}] Unit {unit_id} already at {current_pct}% sold. "
            f"Target {target_pct}%. Skipping."
        )
        return

    pct_to_sell = target_pct - current_pct

    # Original holding = what was bought at BUY time
    # We reconstruct from ledger or use num_shares / (1 - current_pct/100)
    original_shares = round(unit["num_shares"] / (1 - current_pct / 100)
                            ) if current_pct < 100 else unit["num_shares"]
    shares_to_sell = round(original_shares * pct_to_sell / 100)
    shares_to_sell = min(shares_to_sell, unit["num_shares"])
    proceeds = shares_to_sell * price

    new_shares = unit["num_shares"] - shares_to_sell
    new_pct = target_pct

    new_status = "sold" if new_pct == 100 else unit["status"]

    update_unit(unit_id, {
        "num_shares":      new_shares,
        "total_investment": max(0, float(unit["total_investment"]) - proceeds),
        "sell_ladder_pct": new_pct,
        "status":          new_status,
    })

    write_ledger(unit_id, directive_id, -shares_to_sell, -proceeds)
    _update_portfolio_invested(-proceeds)

    if new_status == "sold":
        # Clean up scoring row — will be recreated on re-entry
        delete_scoring(unit_id)
        log.info(f"[{level_name}] Unit {unit_id} fully sold → scoring deleted.")

    log.info(
        f"[{level_name}] unit={unit_id} sold={shares_to_sell} shares "
        f"@ {price} ladder={current_pct}%→{new_pct}% status={new_status}"
    )

# ── BOTTOM FISHING BUY ────────────────────────────────────────────────────────

def handle_bottom_fishing(directive):
    unit_id = directive["unit_id"]
    directive_id = directive["directive_id"]
    price = float(directive["current_price"])
    unit = get_unit(unit_id)

    if unit["sell_ladder_pct"] < 100:
        raise ValueError(
            f"[BFB] Unit {unit_id} sell_ladder_pct={unit['sell_ladder_pct']} — not fully sold."
        )
    if unit["bottom_fish_count"] >= BOTTOM_FISH_MAX:
        raise ValueError(
            f"[BFB] Unit {unit_id} already at max bottom fish count ({BOTTOM_FISH_MAX})."
        )

    total_proceeds = _get_l1_l4_proceeds(unit_id)
    per_fish_amount = total_proceeds * BOTTOM_FISH_PCT
    shares = _shares_for_amount(per_fish_amount, price)
    actual_cost = shares * price

    new_fish_count = unit["bottom_fish_count"] + 1

    update_unit(unit_id, {
        "num_shares":        unit["num_shares"] + shares,
        "total_investment":  float(unit["total_investment"]) + actual_cost,
        "bottom_fish_count": new_fish_count,
        "status":            "active",
        # Re-entry: reset ladder and partial sell flag
        "sell_ladder_pct":   0,
        "partial_sell_done": False,
    })

    write_ledger(unit_id, directive_id, shares, actual_cost)
    _update_portfolio_invested(actual_cost)

    log.info(
        f"[BFB] unit={unit_id} fish_count={new_fish_count} "
        f"shares={shares} @ {price} cost={actual_cost:.2f}"
    )

# ── STOPLOSS BUY / OLD BUY ────────────────────────────────────────────────────

def handle_stoploss_or_old_buy(directive):
    unit_id = directive["unit_id"]
    directive_id = directive["directive_id"]
    price = float(directive["current_price"])
    unit = get_unit(unit_id)

    total_proceeds = _get_l1_l4_proceeds(unit_id)
    n = unit["bottom_fish_count"]
    per_fish_amount = total_proceeds * BOTTOM_FISH_PCT
    deploy_amount = total_proceeds - (n * per_fish_amount)

    if deploy_amount <= 0:
        log.warning(
            f"[{directive['directive']}] Unit {unit_id} has no remaining proceeds. Skipping.")
        return

    shares = _shares_for_amount(deploy_amount, price)
    actual_cost = shares * price

    update_unit(unit_id, {
        "num_shares":        unit["num_shares"] + shares,
        "total_investment":  float(unit["total_investment"]) + actual_cost,
        "status":            "active",
        # Full re-entry reset
        "sell_ladder_pct":   0,
        "bottom_fish_count": 0,
        "partial_sell_done": False,
    })

    write_ledger(unit_id, directive_id, shares, actual_cost)
    _update_portfolio_invested(actual_cost)

    log.info(
        f"[{directive['directive']}] unit={unit_id} deployed={deploy_amount:.2f} "
        f"shares={shares} @ {price} actual_cost={actual_cost:.2f}"
    )

# ── ADJ BUY / ADJ SELL ───────────────────────────────────────────────────────

def handle_adj_buy(directive):
    unit_id = directive["unit_id"]
    directive_id = directive["directive_id"]
    price = float(directive["current_price"])
    unit = get_unit(unit_id)

    # adj_amount is stored temporarily in a context dict passed by rebalancer.
    # We read it from the directive's implicit context via the amount field.
    # For now, calculate from current allocation vs invested.
    alloc_total = (
        float(unit.get("allocation_score") or 0)
        + float(unit.get("allocation_green_count") or 0)
        + float(unit.get("allocation_max_return") or 0)
    )
    current_invested = float(unit["total_investment"])
    adj_amount = alloc_total - current_invested

    if adj_amount <= 0:
        log.debug(f"[ADJ BUY] Unit {unit_id} no increase needed. Skipping.")
        return

    shares = _shares_for_amount(adj_amount, price)
    actual_cost = shares * price

    update_unit(unit_id, {
        "num_shares":       unit["num_shares"] + shares,
        "total_investment": current_invested + actual_cost,
    })

    write_ledger(unit_id, directive_id, shares, actual_cost)
    _update_portfolio_invested(actual_cost)

    log.info(
        f"[ADJ BUY] unit={unit_id} +{shares} shares @ {price} cost={actual_cost:.2f}")

def handle_adj_sell(directive):
    unit_id = directive["unit_id"]
    directive_id = directive["directive_id"]
    price = float(directive["current_price"])
    unit = get_unit(unit_id)

    alloc_total = (
        float(unit.get("allocation_score") or 0)
        + float(unit.get("allocation_green_count") or 0)
        + float(unit.get("allocation_max_return") or 0)
    )
    current_invested = float(unit["total_investment"])
    adj_amount = current_invested - alloc_total

    if adj_amount <= 0:
        log.debug(f"[ADJ SELL] Unit {unit_id} no reduction needed. Skipping.")
        return

    shares_to_sell = _shares_for_amount(adj_amount, price)
    shares_to_sell = min(shares_to_sell, unit["num_shares"])
    proceeds = shares_to_sell * price

    update_unit(unit_id, {
        "num_shares":       unit["num_shares"] - shares_to_sell,
        "total_investment": current_invested - proceeds,
    })

    write_ledger(unit_id, directive_id, -shares_to_sell, -proceeds)
    _update_portfolio_invested(-proceeds)

    log.info(
        f"[ADJ SELL] unit={unit_id} -{shares_to_sell} shares @ {price} proceeds={proceeds:.2f}")

# ── Dispatch table ────────────────────────────────────────────────────────────

HANDLER_MAP = {
    "BUY":                handle_buy,
    "BUY-IN-BUY":         handle_buy,
    "PARTIAL SELL":       handle_partial_sell,
    "SELL L1":            handle_sell_ladder,
    "SELL L2":            handle_sell_ladder,
    "SELL L3":            handle_sell_ladder,
    "SELL L4":            handle_sell_ladder,
    "BOTTOM FISHING BUY": handle_bottom_fishing,
    "STOPLOSS BUY":       handle_stoploss_or_old_buy,
    "OLD BUY":            handle_stoploss_or_old_buy,
    "ADJ BUY":            handle_adj_buy,
    "ADJ SELL":           handle_adj_sell,
}

import logging
from datetime import datetime, timezone, date
from db.client import (
    get_all_units_with_scoring,
    get_portfolio_state,
    upsert_portfolio_state,
    insert_directive,
)
from engine.processor import process_unprocessed_directives
from engine.scoring import run_scoring
from engine.allocation import run_allocation
from engine.handlers import HANDLER_MAP

log = logging.getLogger(__name__)

def _get_latest_price(unit):
    from db.client import db
    res = (
        db.table("stock_directives")
        .select("current_price")
        .eq("unit_id", unit["unit_id"])
        .order("date", desc=True)
        .order("directive_id", desc=True)
        .limit(1)
        .execute()
    )
    return float(res.data[0]["current_price"]) if res.data else 0.0

def run_rebalance():
    log.info("Rebalance cycle started")
    now = datetime.now(timezone.utc)
    today = date.today().isoformat()

    process_unprocessed_directives()

    # units = get_all_units_with_scoring()
    # for row in units:
    #     uid = row["unit_id"]
    #     isin = row["isin"]
    #     name = row["name"]
    #     price = _get_latest_price(row)

    #     if price <= 0:
    #         continue

    #     try:
    #         run_scoring(uid, isin, name, today, price)
    #     except Exception as exc:
    #         log.exception(f"Score failed unit {uid}: {exc}")

    run_allocation()

    units = get_all_units_with_scoring()
    state = get_portfolio_state()

    if state:
        buffer_available = float(state.get("buffer_available") or 0)
        for row in units:
            uid = row["unit_id"]
            isin = row["isin"]
            name = row["name"]
            price = _get_latest_price(row)

            if price <= 0:
                continue

            alloc_total = (
                float(row.get("allocation_score") or 0) +
                float(row.get("allocation_green_count") or 0) +
                float(row.get("allocation_max_return") or 0)
            )
            current_invested = float(row["total_investment"])
            diff = alloc_total - current_invested

            if abs(diff) < 1.0:
                continue

            if diff > 0:
                if buffer_available >= diff:
                    directive_type = "ADJ BUY"
                else:
                    directive_type = "ADJ SELL"
                    diff = -abs(diff)
            else:
                directive_type = "ADJ SELL"

            try:
                adj_directive = insert_directive(
                    uid, isin, name, directive_type, price, today)
                HANDLER_MAP[directive_type](adj_directive)
                log.info(f"{directive_type} unit {uid} diff={diff:+.2f}")
            except Exception as exc:
                log.exception(f"ADJ failed unit {uid}: {exc}")

    upsert_portfolio_state({"last_rebalance_date": now.isoformat()})
    log.info("Rebalance cycle complete")

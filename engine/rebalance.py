import logging
from datetime import datetime, timezone, date
from db.client import (
    get_all_units_with_scoring,
    get_portfolio_state,
    upsert_portfolio_state,
    insert_directive,
)
from engine.processor import run_all_scoring, process_all_pending_directives
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

    # 1. Finish scoring all new stocks
    run_all_scoring()

    # 2. Run allocation
    run_allocation()

    # 3. Prepare ADJ directives based on new allocation
    units = get_all_units_with_scoring()
    state = get_portfolio_state()

    if state:
        total_capital = float(state.get("total_capital") or 0)
        total_invested = sum(float(u["total_investment"]) for u in units)
        actual_cash = total_capital - total_invested
        
        # 98% Rule: Only allow spending if it leaves 2% of total_capital as buffer
        reserve_limit = total_capital * 0.02
        spendable_buffer = actual_cash - reserve_limit
        
        log.info(f"Rebalance Stats — Capital: {total_capital:.2f}, Invested: {total_invested:.2f}, Cash: {actual_cash:.2f}, Spendable: {spendable_buffer:.2f}")

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
                # Only ADJ BUY if we have spendable buffer (98% rule)
                if spendable_buffer >= diff:
                    directive_type = "ADJ BUY"
                    spendable_buffer -= diff # Update local spendable for next unit
                else:
                    directive_type = "ADJ SELL"
                    diff = -abs(diff)
            else:
                directive_type = "ADJ SELL"

            try:
                insert_directive(uid, isin, name, directive_type, price, today)
                log.info(f"Queued {directive_type} for unit {uid} diff={diff:+.2f}")
            except Exception as exc:
                log.error(f"Failed to queue ADJ directive for unit {uid}: {exc}")

        # Update the state in DB
        upsert_portfolio_state({
            "total_invested": total_invested
        })

    # 4. Run all pending directives (including the ones just queued) and make ledger entries
    process_all_pending_directives()

    upsert_portfolio_state({"last_rebalance_date": now.isoformat()})
    log.info("Rebalance cycle complete")

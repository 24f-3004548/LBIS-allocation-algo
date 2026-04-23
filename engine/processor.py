import logging
from datetime import date
from db.client import (
    get_unprocessed_directives,
    get_pending_buy_directives,
    mark_directive_processed,
    create_unit,
)
from db.errors import record_error
from engine.handlers import HANDLER_MAP
from engine.scoring import run_scoring
from engine.allocation import run_allocation

log = logging.getLogger(__name__)

QUEUED_DIRECTIVES = {"BUY", "BUY-IN-BUY"}
IMMEDIATE_DIRECTIVES = {
    "PARTIAL SELL", "SELL L1", "SELL L2", "SELL L3", "SELL L4",
    "BOTTOM FISHING BUY", "STOPLOSS BUY", "OLD BUY", "ADJ BUY", "ADJ SELL",
}

def process_immediate_directives():
    directives = get_unprocessed_directives()
    immediate = [d for d in directives if d["directive"]
                 in IMMEDIATE_DIRECTIVES]
    if not immediate:
        return 0

    processed = 0
    for d in immediate:
        directive_id = d["directive_id"]
        directive = d["directive"]
        unit_id = d["unit_id"]
        try:
            handler = HANDLER_MAP.get(directive)
            if not handler:
                continue
            handler(d)
            mark_directive_processed(directive_id)
            processed += 1
        except Exception as exc:
            log.exception(f"Error {directive_id}: {exc}")
            record_error(str(exc), directive_id, unit_id, directive, exc)
    return processed

def process_unprocessed_directives():
    directives = get_unprocessed_directives()
    if not directives:
        return 0

    log.info(f"Processing {len(directives)} directives")
    processed = 0

    for d in directives:
        directive_id = d["directive_id"]
        isin = d["isin"]
        name = d["name"]
        price = float(d["current_price"])
        directive = d["directive"]
        today = date.today().isoformat()

        try:
            if directive in QUEUED_DIRECTIVES:
                unit_id = d["unit_id"]
                run_scoring(unit_id, isin, name, today, price)

            handler = HANDLER_MAP.get(directive)
            if not handler:
                continue

            handler(d)
            mark_directive_processed(directive_id)
            processed += 1
        except Exception as exc:
            log.exception(f"Error {directive_id}: {exc}")
            record_error(str(exc), directive_id,
                         d.get("unit_id"), directive, exc)
    return processed

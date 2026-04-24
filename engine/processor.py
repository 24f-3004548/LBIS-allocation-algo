import logging
from datetime import date
from db.client import (
    get_unprocessed_directives,
    mark_directive_processed,
)
from db.errors import record_error
from engine.handlers import HANDLER_MAP
from engine.scoring import run_scoring

log = logging.getLogger(__name__)

QUEUED_DIRECTIVES = {"BUY", "BUY-IN-BUY"}
IMMEDIATE_DIRECTIVES = {
    "PARTIAL SELL", "SELL L1", "SELL L2", "SELL L3", "SELL L4",
    "BOTTOM FISHING BUY", "STOPLOSS BUY", "OLD BUY", "ADJ BUY", "ADJ SELL",
}

def run_all_scoring():
    directives = get_unprocessed_directives()
    scored = 0
    today = date.today().isoformat()
    for d in directives:
        if d["directive"] in QUEUED_DIRECTIVES:
            try:
                run_scoring(d["unit_id"], d["name"], today, float(d["current_price"]))
                scored += 1
            except Exception as exc:
                log.error(f"Scoring failed for directive {d['directive_id']}: {exc}")
    return scored

def process_all_pending_directives():
    directives = get_unprocessed_directives()
    return _process_directive_list(directives, "pending")

def process_immediate_directives():
    directives = get_unprocessed_directives()
    immediate = [d for d in directives if d["directive"] in IMMEDIATE_DIRECTIVES]
    return _process_directive_list(immediate, "immediate")

def _process_directive_list(directives, label):
    if not directives:
        return 0

    log.info(f"Processing {len(directives)} {label} directives")
    processed = 0

    for d in directives:
        directive_id = d["directive_id"]
        directive_type = d["directive"]
        unit_id = d.get("unit_id")
        
        try:
            handler = HANDLER_MAP.get(directive_type)
            if not handler:
                log.warning(f"No handler found for {directive_type}")
                continue

            # Execute the trade/logic
            handler(d)
            
            # Change processed_at ONLY after successful execution (including ledger entry inside handler)
            mark_directive_processed(directive_id)
            processed += 1
            
        except Exception as exc:
            log.exception(f"Error processing directive {directive_id}: {exc}")
            record_error(str(exc), directive_id, unit_id, directive_type, exc)
            
    return processed

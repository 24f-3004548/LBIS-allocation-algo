import logging
import traceback as tb

from db.client import db

log = logging.getLogger(__name__)

def record_error(
    error_message,
    directive_id=None,
    unit_id=None,
    directive_type=None,
    exc=None,
    attempt=1,
):
    try:
        payload = {
            "error_message": str(error_message),
            "attempt":       attempt,
            "resolved":      False,
        }
        if directive_id:
            payload["directive_id"] = directive_id
        if unit_id:
            payload["unit_id"] = unit_id
        if directive_type:
            payload["directive_type"] = directive_type
        if exc:
            payload["traceback"] = tb.format_exc()

        db.table("engine_errors").insert(payload).execute()

    except Exception as inner:
        log.error(f"[errors] Failed to write error to DB: {inner}")



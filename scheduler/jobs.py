import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import POLL_INTERVAL_SECONDS, REBALANCE_INTERVAL_DAYS
from engine.processor import process_immediate_directives
from engine.rebalance import run_rebalance

log = logging.getLogger(__name__)

_scheduler = None
_is_rebalancing = False

def _poll_job():
    global _is_rebalancing
    if _is_rebalancing:
        return

    try:
        count = process_immediate_directives()
        if count:
            log.info(f"[scheduler/poll] Processed {count} directive(s).")
    except Exception as exc:
        log.exception(f"[scheduler/poll] Error during poll: {exc}")

def _rebalance_job():
    global _is_rebalancing
    if _is_rebalancing:
        return

    _is_rebalancing = True
    try:
        run_rebalance()
    except Exception as exc:
        log.exception(f"[scheduler/rebalance] Error during rebalance: {exc}")
    finally:
        _is_rebalancing = False

def trigger_rebalance():
    log.info("[scheduler] Manual rebalance triggered.")
    _rebalance_job()

def start_scheduler():
    global _scheduler

    _scheduler = BackgroundScheduler(timezone="Asia/Kolkata")

    _scheduler.add_job(
        _poll_job,
        trigger=IntervalTrigger(seconds=POLL_INTERVAL_SECONDS),
        id="poll_directives",
        name="Poll immediate directives",
        replace_existing=True,
    )

    from datetime import datetime

    _scheduler.add_job(
        _rebalance_job,
        trigger=IntervalTrigger(
            days=REBALANCE_INTERVAL_DAYS, timezone="Asia/Kolkata"),
        next_run_time=datetime.now(),
        id="rebalance",
        name="15-day rebalance",
        replace_existing=True,
    )

    _scheduler.start()
    log.info(
        f"[scheduler] Started — poll every {POLL_INTERVAL_SECONDS}s, "
        f"rebalance immediately, then every {REBALANCE_INTERVAL_DAYS} days."
    )
    return _scheduler

def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        log.info("[scheduler] Stopped.")

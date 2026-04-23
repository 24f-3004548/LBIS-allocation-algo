from scheduler.jobs import start_scheduler, stop_scheduler, trigger_rebalance
import argparse
import logging
import signal
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)

log = logging.getLogger("main")

def _handle_signal(signum, frame):
    log.info("Shutting down...")
    stop_scheduler()
    sys.exit(0)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rebalance", action="store_true")
    args = parser.parse_args()

    if args.rebalance:
        trigger_rebalance()
        return

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    log.info("Engine starting...")
    start_scheduler()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        stop_scheduler()
        log.info("Engine stopped.")

if __name__ == "__main__":
    main()

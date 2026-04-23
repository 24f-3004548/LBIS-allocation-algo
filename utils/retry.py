import logging
import time
import functools

log = logging.getLogger(__name__)

def with_retry(
    max_attempts=3,
    backoff_base=2.0,
    exceptions=(Exception,),
    label="",
):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            tag = label or fn.__name__
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    if attempt == max_attempts:
                        log.error(
                            f"[retry/{tag}] All {max_attempts} attempts failed. "
                            f"Last error: {exc}"
                        )
                        raise
                    wait = backoff_base ** (attempt - 1)
                    log.warning(
                        f"[retry/{tag}] Attempt {attempt}/{max_attempts} failed: {exc}. "
                        f"Retrying in {wait:.0f}s..."
                    )
                    time.sleep(wait)
        return wrapper
    return decorator

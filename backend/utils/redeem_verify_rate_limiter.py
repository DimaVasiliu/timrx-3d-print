"""
In-process sliding-window rate limiter for /auth/restore/redeem
and /auth/email/verify endpoints.

Limits each IP to a configurable number of requests per window,
preventing brute-force enumeration beyond the per-code attempt cap.
"""

import time
import threading

# Configuration
REDEEM_VERIFY_WINDOW_SECS = 60
REDEEM_VERIFY_MAX_REQUESTS = 10

_lock = threading.Lock()
_ip_log: dict[str, list[float]] = {}
_last_cleanup = 0.0


def check_redeem_verify_rate(ip: str) -> bool:
    """
    Return True if the IP is within rate limits, False if over.
    Thread-safe; piggyback-cleans stale entries periodically.
    """
    global _last_cleanup

    now = time.time()
    cutoff = now - REDEEM_VERIFY_WINDOW_SECS

    with _lock:
        # Periodic cleanup
        if now - _last_cleanup > REDEEM_VERIFY_WINDOW_SECS:
            stale = [k for k, v in _ip_log.items() if not v or v[-1] < cutoff]
            for k in stale:
                del _ip_log[k]
            _last_cleanup = now

        timestamps = _ip_log.get(ip)
        if timestamps is None:
            timestamps = []
            _ip_log[ip] = timestamps

        # Trim expired
        while timestamps and timestamps[0] < cutoff:
            timestamps.pop(0)

        if len(timestamps) >= REDEEM_VERIFY_MAX_REQUESTS:
            return False

        timestamps.append(now)
        return True

"""
Shared in-memory status cache for generation job status endpoints.

Cuts DB + provider API calls from status polling by returning cached
responses for the same job_id within a short TTL window.

Usage in a status handler:
    from backend.services.status_cache import get_cached_status, cache_status

    # At top of handler, before any DB or provider work:
    cached = get_cached_status(job_id)
    if cached is not None:
        return jsonify(cached)

    # ... do DB + provider work ...

    # Before returning:
    cache_status(job_id, response_data, is_terminal=(status in ('done', 'failed')))
    return jsonify(response_data)
"""

import time

_status_cache: dict = {}  # job_id -> (monotonic_ts, response_dict, is_terminal)

# In-progress jobs: cache for 3s (status can change)
# Terminal jobs (done/failed): cache for 5 minutes (status will never change)
_ACTIVE_TTL = 3       # seconds
_TERMINAL_TTL = 300   # 5 minutes
_MAX_ENTRIES = 2000


def get_cached_status(job_id: str) -> dict | None:
    """Return cached status response or None if expired/missing."""
    cached = _status_cache.get(job_id)
    if not cached:
        return None
    ts, data, is_terminal = cached
    ttl = _TERMINAL_TTL if is_terminal else _ACTIVE_TTL
    if (time.monotonic() - ts) < ttl:
        return data
    # Expired — remove and return None
    del _status_cache[job_id]
    return None


def cache_status(job_id: str, data: dict, is_terminal: bool = False):
    """Cache a status response. Terminal states get a longer TTL."""
    _status_cache[job_id] = (time.monotonic(), data, is_terminal)
    # Periodic eviction to prevent unbounded growth
    if len(_status_cache) > _MAX_ENTRIES:
        _evict_expired()


def invalidate_status(job_id: str):
    """Remove a specific job from the cache (e.g., after a state change)."""
    _status_cache.pop(job_id, None)


def _evict_expired():
    """Remove expired entries."""
    now = time.monotonic()
    expired = [
        k for k, (ts, _, is_terminal) in _status_cache.items()
        if (now - ts) > (_TERMINAL_TTL if is_terminal else _ACTIVE_TTL)
    ]
    for k in expired:
        del _status_cache[k]

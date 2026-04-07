"""
Shared auth rate limiting backed by Postgres.

This replaces per-process in-memory counters for security-sensitive endpoints
that must behave consistently across multiple Render instances.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from backend.db import Tables, fetch_one, hash_string, transaction


class AuthRateLimitService:
    @staticmethod
    def _bucket_start(now: datetime, window_seconds: int) -> datetime:
        bucket_epoch = int(now.timestamp()) // window_seconds * window_seconds
        return datetime.fromtimestamp(bucket_epoch, tz=timezone.utc)

    @staticmethod
    def _key_hash(parts: Iterable[str]) -> str:
        normalized = [str(part).strip().lower() for part in parts if str(part).strip()]
        return hash_string("|".join(normalized))

    @staticmethod
    def hit(scope: str, key_parts: Iterable[str], *, limit: int, window_seconds: int) -> dict:
        """
        Record one auth attempt in the current window and return the result.

        Returns:
          {
            "ok": bool,
            "count": int,
            "limit": int,
            "remaining": int,
            "retry_after": int,
          }
        """
        now = datetime.now(timezone.utc)
        bucket_start = AuthRateLimitService._bucket_start(now, window_seconds)
        retry_after = max(1, window_seconds - int(now.timestamp()) % window_seconds)
        key_hash = AuthRateLimitService._key_hash(key_parts)

        with transaction("auth_rate_limit_hit") as cur:
            cur.execute(
                f"""
                INSERT INTO {Tables.AUTH_RATE_LIMITS}
                    (scope, key_hash, window_seconds, window_start, request_count, created_at, last_seen_at)
                VALUES
                    (%s, %s, %s, %s, 1, NOW(), NOW())
                ON CONFLICT (scope, key_hash, window_seconds, window_start)
                DO UPDATE SET
                    request_count = {Tables.AUTH_RATE_LIMITS}.request_count + 1,
                    last_seen_at = NOW()
                RETURNING request_count
                """,
                (scope, key_hash, window_seconds, bucket_start),
            )
            row = fetch_one(cur)

        count = int(row["request_count"]) if row else 1
        remaining = max(0, limit - count)
        return {
            "ok": count <= limit,
            "count": count,
            "limit": limit,
            "remaining": remaining,
            "retry_after": retry_after,
        }

"""
IDENT-5: Orphaned S3-linked asset detection and cleanup.

Scans models, images, videos for rows that are orphaned — i.e. no longer
reachable by any valid identity through normal user flows.

Orphan categories:
  1. OWNERLESS    — identity_id IS NULL (ON DELETE SET NULL fired)
  2. MERGED_AWAY  — identity_id points to a merged-source identity
                    (pre-merge-engine legacy; merge now migrates rows)
  3. SOFT_DELETED — deleted_at set, older than retention threshold
  4. UNLINKED     — asset row exists but no history_item references it
  5. FAILED       — terminal failed status, no active job, past threshold

Safety rules:
  - Never touches assets younger than age_threshold_days
  - Never touches assets with active/in-flight jobs
  - Quarantine (set deleted_at) rather than hard-delete
  - S3 key collection for optional cleanup is separate and dry-run capable
"""

from typing import Dict, List, Any, Optional
from backend.db import query, query_one, execute, transaction, Tables, fetch_one


# ─────────────────────────────────────────────────────────
#  Constants
# ─────────────────────────────────────────────────────────

_ASSET_TABLES = {
    "models": {
        "table": Tables.MODELS,
        "s3_keys": ["glb_s3_key", "thumbnail_s3_key"],
        "history_fk": "model_id",
        "active_job_fk": "related_model_id",
    },
    "images": {
        "table": Tables.IMAGES,
        "s3_keys": ["image_s3_key", "thumbnail_s3_key", "source_s3_key"],
        "history_fk": "image_id",
        "active_job_fk": "related_image_id",
    },
    "videos": {
        "table": Tables.VIDEOS,
        "s3_keys": ["video_s3_key", "thumbnail_s3_key"],
        "history_fk": "video_id",
        "active_job_fk": None,  # active_jobs has no video FK
    },
}

# In-flight statuses that protect an asset from cleanup
_INFLIGHT_STATUSES = (
    "queued", "pending", "processing", "dispatched",
    "provider_pending", "provider_processing", "stalled",
)


class OrphanAuditService:
    """Detect and report orphaned S3-linked assets."""

    # ─────────────────────────────────────────────────
    #  Summary (quick counts)
    # ─────────────────────────────────────────────────

    @staticmethod
    def summary(age_threshold_days: int = 30) -> Dict[str, Any]:
        """
        Quick count of orphan candidates per asset type and category.
        No row-level detail — just counts for dashboards.
        """
        results = {}
        for asset_type in _ASSET_TABLES:
            counts = OrphanAuditService._count_orphans(asset_type, age_threshold_days)
            results[asset_type] = counts
        results["age_threshold_days"] = age_threshold_days
        return results

    @staticmethod
    def _count_orphans(asset_type: str, age_days: int) -> Dict[str, int]:
        cfg = _ASSET_TABLES[asset_type]
        table = cfg["table"]
        history_fk = cfg["history_fk"]

        counts = {}

        # 1. Ownerless
        row = query_one(
            f"""
            SELECT COUNT(*) as cnt FROM {table}
            WHERE identity_id IS NULL
              AND created_at < NOW() - INTERVAL '%s days'
            """,
            (age_days,),
        )
        counts["ownerless"] = row["cnt"] if row else 0

        # 2. Merged-away owner
        row = query_one(
            f"""
            SELECT COUNT(*) as cnt
            FROM {table} a
            JOIN {Tables.IDENTITIES} i ON a.identity_id = i.id
            WHERE i.merged_into_id IS NOT NULL
              AND a.created_at < NOW() - INTERVAL '%s days'
            """,
            (age_days,),
        )
        counts["merged_away"] = row["cnt"] if row else 0

        # 3. Soft-deleted past threshold
        row = query_one(
            f"""
            SELECT COUNT(*) as cnt FROM {table}
            WHERE deleted_at IS NOT NULL
              AND deleted_at < NOW() - INTERVAL '%s days'
            """,
            (age_days,),
        )
        counts["soft_deleted"] = row["cnt"] if row else 0

        # 4. Unlinked (no history_item references it, not already deleted)
        row = query_one(
            f"""
            SELECT COUNT(*) as cnt
            FROM {table} a
            LEFT JOIN {Tables.HISTORY_ITEMS} h ON h.{history_fk} = a.id
            WHERE h.id IS NULL
              AND a.deleted_at IS NULL
              AND a.created_at < NOW() - INTERVAL '%s days'
            """,
            (age_days,),
        )
        counts["unlinked"] = row["cnt"] if row else 0

        # 5. Failed terminal
        row = query_one(
            f"""
            SELECT COUNT(*) as cnt FROM {table}
            WHERE status = 'failed'
              AND deleted_at IS NULL
              AND created_at < NOW() - INTERVAL '%s days'
            """,
            (age_days,),
        )
        counts["failed"] = row["cnt"] if row else 0

        total = (
            counts["ownerless"]
            + counts["merged_away"]
            + counts["soft_deleted"]
            + counts["unlinked"]
            + counts["failed"]
        )
        counts["total"] = total
        return counts

    # ─────────────────────────────────────────────────
    #  Detailed audit (row-level)
    # ─────────────────────────────────────────────────

    @staticmethod
    def audit(
        asset_type: str,
        category: str,
        age_threshold_days: int = 30,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Return row-level orphan candidates for a given asset_type + category.

        asset_type: "models" | "images" | "videos"
        category:   "ownerless" | "merged_away" | "soft_deleted" | "unlinked" | "failed"
        """
        if asset_type not in _ASSET_TABLES:
            return []

        cfg = _ASSET_TABLES[asset_type]
        table = cfg["table"]
        s3_key_cols = cfg["s3_keys"]
        history_fk = cfg["history_fk"]

        # Build SELECT columns
        s3_select = ", ".join(f"a.{col}" for col in s3_key_cols)
        base_select = f"a.id, a.identity_id, a.s3_bucket, {s3_select}, a.status, a.created_at, a.deleted_at"

        if category == "ownerless":
            rows = query(
                f"""
                SELECT {base_select}
                FROM {table} a
                WHERE a.identity_id IS NULL
                  AND a.created_at < NOW() - INTERVAL '%s days'
                ORDER BY a.created_at ASC
                LIMIT %s
                """,
                (age_threshold_days, limit),
            )
        elif category == "merged_away":
            rows = query(
                f"""
                SELECT {base_select}, i.merged_into_id
                FROM {table} a
                JOIN {Tables.IDENTITIES} i ON a.identity_id = i.id
                WHERE i.merged_into_id IS NOT NULL
                  AND a.created_at < NOW() - INTERVAL '%s days'
                ORDER BY a.created_at ASC
                LIMIT %s
                """,
                (age_threshold_days, limit),
            )
        elif category == "soft_deleted":
            rows = query(
                f"""
                SELECT {base_select}
                FROM {table} a
                WHERE a.deleted_at IS NOT NULL
                  AND a.deleted_at < NOW() - INTERVAL '%s days'
                ORDER BY a.deleted_at ASC
                LIMIT %s
                """,
                (age_threshold_days, limit),
            )
        elif category == "unlinked":
            rows = query(
                f"""
                SELECT {base_select}
                FROM {table} a
                LEFT JOIN {Tables.HISTORY_ITEMS} h ON h.{history_fk} = a.id
                WHERE h.id IS NULL
                  AND a.deleted_at IS NULL
                  AND a.created_at < NOW() - INTERVAL '%s days'
                ORDER BY a.created_at ASC
                LIMIT %s
                """,
                (age_threshold_days, limit),
            )
        elif category == "failed":
            rows = query(
                f"""
                SELECT {base_select}
                FROM {table} a
                WHERE a.status = 'failed'
                  AND a.deleted_at IS NULL
                  AND a.created_at < NOW() - INTERVAL '%s days'
                ORDER BY a.created_at ASC
                LIMIT %s
                """,
                (age_threshold_days, limit),
            )
        else:
            return []

        if not rows:
            return []

        results = []
        for row in rows:
            item = {
                "id": str(row["id"]),
                "asset_type": asset_type,
                "category": category,
                "identity_id": str(row["identity_id"]) if row["identity_id"] else None,
                "s3_bucket": row.get("s3_bucket"),
                "s3_keys": {},
                "status": row.get("status"),
                "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
                "deleted_at": row["deleted_at"].isoformat() if row.get("deleted_at") else None,
            }
            for col in s3_key_cols:
                item["s3_keys"][col] = row.get(col)

            if category == "merged_away" and row.get("merged_into_id"):
                item["merged_into_id"] = str(row["merged_into_id"])

            # Check if protected by active job
            item["has_active_job"] = _has_active_job(asset_type, str(row["id"]))
            item["safe_to_quarantine"] = (
                not item["has_active_job"]
                and category != "unlinked"  # unlinked alone is not sufficient
            )

            results.append(item)

        return results

    # ─────────────────────────────────────────────────
    #  Quarantine (soft-delete)
    # ─────────────────────────────────────────────────

    @staticmethod
    def quarantine_orphans(
        asset_type: str,
        category: str,
        age_threshold_days: int = 30,
        limit: int = 100,
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        """
        Quarantine (set deleted_at) orphan candidates that are safe to clean up.

        Only quarantines rows that:
        - Match the given category
        - Are older than age_threshold_days
        - Have no active/in-flight jobs
        - Are not already soft-deleted (unless category is 'soft_deleted')

        Returns count of affected rows and their IDs.
        """
        if asset_type not in _ASSET_TABLES:
            return {"error": f"Unknown asset type: {asset_type}"}

        # Merged-away assets should be re-migrated, not quarantined
        if category == "merged_away":
            return {
                "error": "merged_away assets should be re-migrated to canonical owner, not quarantined. "
                         "Use the admin merge tool instead.",
                "quarantined": 0,
            }

        # Unlinked alone is not safe — asset may be intentionally retained
        if category == "unlinked":
            return {
                "error": "unlinked assets are not safe to quarantine automatically. "
                         "An asset without a history link may still be user-owned. "
                         "Review manually or combine with another category.",
                "quarantined": 0,
            }

        cfg = _ASSET_TABLES[asset_type]
        table = cfg["table"]
        active_job_fk = cfg["active_job_fk"]

        # Build the WHERE clause per category
        if category == "ownerless":
            where = "a.identity_id IS NULL AND a.deleted_at IS NULL"
        elif category == "soft_deleted":
            # Already soft-deleted — this is a no-op for quarantine.
            # Use collect_s3_keys_for_cleanup() instead.
            return {
                "error": "soft_deleted assets are already quarantined. "
                         "Use the S3 cleanup path to reclaim storage.",
                "quarantined": 0,
            }
        elif category == "failed":
            where = "a.status = 'failed' AND a.deleted_at IS NULL"
        else:
            return {"error": f"Cannot auto-quarantine category: {category}"}

        # Exclude rows with active jobs
        active_job_exclude = ""
        if active_job_fk:
            active_job_exclude = f"""
                AND NOT EXISTS (
                    SELECT 1 FROM {Tables.ACTIVE_JOBS} aj
                    WHERE aj.{active_job_fk} = a.id
                      AND aj.status IN {_sql_tuple(_INFLIGHT_STATUSES)}
                )
            """

        age_filter = f"AND a.created_at < NOW() - INTERVAL '{int(age_threshold_days)} days'"

        if dry_run:
            rows = query(
                f"""
                SELECT a.id FROM {table} a
                WHERE {where} {age_filter} {active_job_exclude}
                ORDER BY a.created_at ASC
                LIMIT %s
                """,
                (limit,),
            )
            ids = [str(r["id"]) for r in (rows or [])]
            return {
                "dry_run": True,
                "asset_type": asset_type,
                "category": category,
                "would_quarantine": len(ids),
                "ids": ids[:20],  # Show first 20
                "age_threshold_days": age_threshold_days,
            }

        # Execute quarantine
        with transaction() as cur:
            cur.execute(
                f"""
                UPDATE {table} a
                SET deleted_at = NOW()
                WHERE {where} {age_filter} {active_job_exclude}
                  AND a.id = ANY(
                    SELECT a2.id FROM {table} a2
                    WHERE {where.replace('a.', 'a2.')} {age_filter.replace('a.', 'a2.')}
                    {active_job_exclude.replace('a.', 'a2.').replace('aj.', 'aj2.').replace('aj2', 'aj')}
                    LIMIT %s
                  )
                RETURNING a.id
                """,
                (limit,),
            )
            affected = cur.fetchall() or []

        ids = [str(r["id"]) for r in affected]
        print(
            f"[ORPHAN] Quarantined {len(ids)} {asset_type} "
            f"(category={category}, age>{age_threshold_days}d)"
        )

        return {
            "dry_run": False,
            "asset_type": asset_type,
            "category": category,
            "quarantined": len(ids),
            "ids": ids[:20],
            "age_threshold_days": age_threshold_days,
        }

    # ──��──────────────────────────────────────────────
    #  S3 key collection (for optional storage cleanup)
    # ─────────────────────────────────────────────────

    @staticmethod
    def collect_s3_keys_for_cleanup(
        asset_type: str,
        age_threshold_days: int = 90,
        limit: int = 200,
    ) -> Dict[str, Any]:
        """
        Collect S3 keys from soft-deleted assets older than threshold.
        Does NOT delete anything — returns keys for review or batch deletion.

        Uses a longer default threshold (90 days) for extra safety.
        """
        if asset_type not in _ASSET_TABLES:
            return {"error": f"Unknown asset type: {asset_type}"}

        cfg = _ASSET_TABLES[asset_type]
        table = cfg["table"]
        s3_key_cols = cfg["s3_keys"]

        s3_select = ", ".join(f"a.{col}" for col in s3_key_cols)

        rows = query(
            f"""
            SELECT a.id, a.s3_bucket, {s3_select}
            FROM {table} a
            WHERE a.deleted_at IS NOT NULL
              AND a.deleted_at < NOW() - INTERVAL '%s days'
            ORDER BY a.deleted_at ASC
            LIMIT %s
            """,
            (age_threshold_days, limit),
        )

        keys = []
        asset_ids = []
        for row in (rows or []):
            bucket = row.get("s3_bucket")
            if not bucket:
                continue
            asset_ids.append(str(row["id"]))
            for col in s3_key_cols:
                key = row.get(col)
                if key:
                    keys.append({"bucket": bucket, "key": key, "asset_id": str(row["id"])})

        return {
            "asset_type": asset_type,
            "age_threshold_days": age_threshold_days,
            "asset_count": len(asset_ids),
            "s3_key_count": len(keys),
            "s3_keys": keys,
        }

    # ─────────────────────────────────────────────────
    #  Merged-away asset re-migration
    # ─────────────────────────────────────────────────

    @staticmethod
    def fix_merged_away(
        asset_type: str,
        age_threshold_days: int = 0,
        limit: int = 500,
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        """
        Fix assets that still point to a merged-away source identity
        by updating their identity_id to the canonical target.

        These are rows that should have been migrated during merge but
        were missed (pre-merge-engine legacy data).
        """
        if asset_type not in _ASSET_TABLES:
            return {"error": f"Unknown asset type: {asset_type}"}

        cfg = _ASSET_TABLES[asset_type]
        table = cfg["table"]

        if dry_run:
            rows = query(
                f"""
                SELECT a.id, a.identity_id, i.merged_into_id
                FROM {table} a
                JOIN {Tables.IDENTITIES} i ON a.identity_id = i.id
                WHERE i.merged_into_id IS NOT NULL
                  AND a.created_at < NOW() - INTERVAL '%s days'
                ORDER BY a.created_at ASC
                LIMIT %s
                """,
                (age_threshold_days, limit),
            )
            items = []
            for r in (rows or []):
                items.append({
                    "asset_id": str(r["id"]),
                    "current_owner": str(r["identity_id"]),
                    "canonical_owner": str(r["merged_into_id"]),
                })
            return {
                "dry_run": True,
                "asset_type": asset_type,
                "would_fix": len(items),
                "items": items[:20],
            }

        # Execute re-migration: update identity_id to merged_into_id
        with transaction() as cur:
            cur.execute(
                f"""
                UPDATE {table} a
                SET identity_id = i.merged_into_id
                FROM {Tables.IDENTITIES} i
                WHERE a.identity_id = i.id
                  AND i.merged_into_id IS NOT NULL
                  AND a.created_at < NOW() - INTERVAL '%s days'
                  AND a.id = ANY(
                    SELECT a2.id FROM {table} a2
                    JOIN {Tables.IDENTITIES} i2 ON a2.identity_id = i2.id
                    WHERE i2.merged_into_id IS NOT NULL
                      AND a2.created_at < NOW() - INTERVAL '%s days'
                    LIMIT %s
                  )
                RETURNING a.id, a.identity_id
                """,
                (age_threshold_days, age_threshold_days, limit),
            )
            affected = cur.fetchall() or []

        print(
            f"[ORPHAN] Re-migrated {len(affected)} {asset_type} "
            f"from merged-away identities to canonical owners"
        )

        return {
            "dry_run": False,
            "asset_type": asset_type,
            "fixed": len(affected),
            "ids": [str(r["id"]) for r in affected[:20]],
        }


# ─────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────

def _has_active_job(asset_type: str, asset_id: str) -> bool:
    """Check if an asset is referenced by an in-flight active_job."""
    cfg = _ASSET_TABLES.get(asset_type)
    if not cfg or not cfg["active_job_fk"]:
        return False

    fk_col = cfg["active_job_fk"]
    row = query_one(
        f"""
        SELECT 1 FROM {Tables.ACTIVE_JOBS}
        WHERE {fk_col} = %s
          AND status IN {_sql_tuple(_INFLIGHT_STATUSES)}
        LIMIT 1
        """,
        (asset_id,),
    )
    return row is not None


def _sql_tuple(values):
    """Format a Python tuple as a SQL IN clause value."""
    return "(" + ", ".join(f"'{v}'" for v in values) + ")"

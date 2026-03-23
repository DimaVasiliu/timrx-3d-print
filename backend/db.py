"""
Database utilities for TimrX Backend.
Provides connection management and common query helpers.

All functions raise meaningful exceptions on failure - no silent failures.

Usage:
    from backend.db import get_conn, transaction, fetch_one, fetch_all, now_utc

    # Simple query
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            user = fetch_one(cur)

    # Transaction with automatic commit/rollback
    with transaction() as cur:
        cur.execute("INSERT INTO users (name) VALUES (%s) RETURNING *", ("John",))
        user = fetch_one(cur)
        cur.execute("INSERT INTO wallets (user_id) VALUES (%s)", (user["id"],))
"""

import hashlib
import os
import re
import threading
from contextlib import contextmanager
from typing import Optional, Any, Dict, List, Union
from datetime import datetime, timezone

try:
    import psycopg
    from psycopg.rows import dict_row
    PSYCOPG_AVAILABLE = True
except ImportError:
    psycopg = None
    dict_row = None
    PSYCOPG_AVAILABLE = False

try:
    from psycopg_pool import ConnectionPool as _ConnectionPool
    _POOL_AVAILABLE = True
except ImportError:
    _ConnectionPool = None
    _POOL_AVAILABLE = False

# NOTE: Do NOT import config at module level - causes circular imports!
# Use _get_config() for lazy access inside functions.

def _get_config():
    """Lazy import of config to avoid circular imports."""
    from backend.config import config
    return config


# Module-level constants using os.getenv() directly to avoid circular imports
_DATABASE_URL = os.getenv("DATABASE_URL", "")
_HAS_DATABASE = bool(_DATABASE_URL)
_DB_CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "10"))
_DB_POOL_ENABLED = os.getenv("DB_POOL_ENABLED", "false").lower() in ("true", "1", "yes")
_DB_POOL_MIN_SIZE = int(os.getenv("DB_POOL_MIN_SIZE", "2"))
_DB_POOL_MAX_SIZE = int(os.getenv("DB_POOL_MAX_SIZE", "10"))
_DB_POOL_TIMEOUT = float(os.getenv("DB_POOL_TIMEOUT", "30"))  # seconds to wait for a connection
_DB_POOL_MAX_LIFETIME = float(os.getenv("DB_POOL_MAX_LIFETIME", "300"))  # seconds before recycling a connection
_DB_POOL_MAX_IDLE = float(os.getenv("DB_POOL_MAX_IDLE", "120"))  # seconds idle before closing excess connections
_DB_POOL_CHECK = os.getenv("DB_POOL_CHECK", "false").lower() in ("true", "1", "yes")  # per-borrow SELECT 1 (DISABLED by default — causes pool churn)
_APP_SCHEMA = os.getenv("APP_SCHEMA", "timrx_app")
_BILLING_SCHEMA = os.getenv("BILLING_SCHEMA", "timrx_billing")

# Fail-closed validation: reject malformed schema names at import time
_SCHEMA_RE = re.compile(r'^[a-z][a-z0-9_]{0,62}$')
if not _SCHEMA_RE.match(_APP_SCHEMA):
    raise ValueError(f"Invalid APP_SCHEMA: {_APP_SCHEMA!r} — must match ^[a-z][a-z0-9_]{{0,62}}$")
if not _SCHEMA_RE.match(_BILLING_SCHEMA):
    raise ValueError(f"Invalid BILLING_SCHEMA: {_BILLING_SCHEMA!r} — must match ^[a-z][a-z0-9_]{{0,62}}$")


# ─────────────────────────────────────────────────────────────
# Connection Pool (opt-in, disabled by default)
# Set DB_POOL_ENABLED=true to activate.
# _create_connection() is NEVER pooled — used by job_worker.py
# for the advisory lock, which requires a dedicated long-lived connection.
# ─────────────────────────────────────────────────────────────
_pool = None
_pool_init_attempted = False
_pool_lock = threading.Lock()

# ─── Pool diagnostics ─────────────────────────────────────────
# Env vars:
#   DB_POOL_TRACE=true          — enable tagged checkout/return logging
#   DB_POOL_TRACE_SLOW_MS=2000  — warn threshold for long-held connections
#   DB_POOL_STATS_INTERVAL=30   — seconds between periodic pool stats log
# ──────────────────────────────────────────────────────────────
import time as _time

_POOL_TRACE = os.getenv("DB_POOL_TRACE", "false").lower() in ("true", "1", "yes")
_POOL_TRACE_SLOW_MS = int(os.getenv("DB_POOL_TRACE_SLOW_MS", "2000"))
_POOL_STATS_INTERVAL = int(os.getenv("DB_POOL_STATS_INTERVAL", "30"))

# Counters — borrows is incremented INSIDE pool.connection() (after the
# connection is actually acquired), not before, so leaked count is accurate.
_pool_borrows = 0
_pool_returns = 0
_pool_last_stats_at = 0.0

# Active borrows tracker: maps thread-name -> (source, t0) for currently
# held connections. Used to snapshot who's holding what on timeout.
_pool_active: Dict[str, tuple] = {}
_pool_active_lock = threading.Lock()


def _pool_trace_checkout(source: str):
    """Log tagged pool checkout. Only logs if DB_POOL_TRACE=true and source is set."""
    global _pool_borrows
    _pool_borrows += 1
    tname = threading.current_thread().name
    with _pool_active_lock:
        _pool_active[tname] = (source or "untagged", _time.monotonic())
    if _POOL_TRACE:
        s = _quick_pool_avail()
        print(
            f"[DB][POOL] CHECKOUT source={source or 'untagged'} pid={os.getpid()} "
            f"thread={tname} avail={s} borrows={_pool_borrows}"
        )


def _pool_trace_return(source: str, t0: float):
    """Log tagged pool return + warn if held too long."""
    global _pool_returns
    _pool_returns += 1
    tname = threading.current_thread().name
    with _pool_active_lock:
        _pool_active.pop(tname, None)
    held_ms = int((_time.monotonic() - t0) * 1000)
    if held_ms >= _POOL_TRACE_SLOW_MS:
        print(
            f"[DB][POOL][WARN] source={source or 'untagged'} pid={os.getpid()} "
            f"thread={tname} held_ms={held_ms}"
        )
    elif _POOL_TRACE:
        print(
            f"[DB][POOL] RETURN  source={source or 'untagged'} pid={os.getpid()} "
            f"thread={tname} held_ms={held_ms}"
        )


def _pool_trace_timeout(source: str):
    """Log full diagnostics on PoolTimeout including who's holding connections."""
    stats = pool_stats()
    with _pool_active_lock:
        now = _time.monotonic()
        holders = {
            t: (src, int((now - t0) * 1000))
            for t, (src, t0) in _pool_active.items()
        }
    print(
        f"[DB][POOL][TIMEOUT] source={source or 'untagged'} pid={os.getpid()} "
        f"thread={threading.current_thread().name} "
        f"borrows={_pool_borrows} returns={_pool_returns} "
        f"leaked={_pool_borrows - _pool_returns} stats={stats}"
    )
    if holders:
        print(f"[DB][POOL][TIMEOUT] active_holders={holders}")
    else:
        print(f"[DB][POOL][TIMEOUT] active_holders=NONE (all returned but pool still empty)")


def _quick_pool_avail() -> str:
    """Quick pool_available string for trace lines (no exception risk)."""
    try:
        return str(_pool.get_stats().get("pool_available", "?")) if _pool else "no_pool"
    except Exception:
        return "?"


def _maybe_log_pool_stats():
    """Log pool stats periodically (every DB_POOL_STATS_INTERVAL seconds)."""
    global _pool_last_stats_at
    if not _pool or not _POOL_TRACE:
        return
    now = _time.monotonic()
    if now - _pool_last_stats_at < _POOL_STATS_INTERVAL:
        return
    _pool_last_stats_at = now
    stats = pool_stats()
    with _pool_active_lock:
        now2 = _time.monotonic()
        holders = {
            t: (src, int((now2 - t0) * 1000))
            for t, (src, t0) in _pool_active.items()
        }
    print(
        f"[DB][POOL] pid={os.getpid()} thread={threading.current_thread().name} "
        f"pool_size={stats.get('pool_size', '?')} "
        f"pool_available={stats.get('pool_available', '?')} "
        f"requests_waiting={stats.get('requests_waiting', '?')} "
        f"borrows={_pool_borrows} returns={_pool_returns} "
        f"leaked={_pool_borrows - _pool_returns} "
        f"holders={holders if holders else 'none'}"
    )


def _configure_pooled_conn(conn):
    """Configure session settings for a new pooled connection.
    Runs in autocommit so SETs are immediate and survive rollbacks."""
    conn.autocommit = True
    conn.execute(f"SET search_path TO {_APP_SCHEMA}, {_BILLING_SCHEMA}, public")
    conn.execute("SET statement_timeout = '30000'")
    conn.execute("SET idle_in_transaction_session_timeout = '60000'")
    conn.execute("SET lock_timeout = '10000'")
    conn.autocommit = False


def _get_pool():
    """Get or create the connection pool. Returns None if pooling is disabled."""
    global _pool, _pool_init_attempted
    if not _DB_POOL_ENABLED:
        return None
    if _pool is not None:
        return _pool
    if _pool_init_attempted:
        return None
    if not _POOL_AVAILABLE or not _DATABASE_URL or not PSYCOPG_AVAILABLE:
        return None

    with _pool_lock:
        if _pool is not None:
            return _pool
        if _pool_init_attempted:
            return None
        _pool_init_attempted = True
        try:
            # check=check_connection does SELECT 1 on EVERY borrow.
            # With autocommit=False this starts a transaction, and when the
            # check finds a dead connection it triggers discard + async
            # replace + backoff sleep — creating a death-spiral under load.
            # Disabled by default; set DB_POOL_CHECK=true to re-enable.
            _check_cb = _ConnectionPool.check_connection if _DB_POOL_CHECK else None

            _pool = _ConnectionPool(
                conninfo=_DATABASE_URL,
                min_size=_DB_POOL_MIN_SIZE,
                max_size=_DB_POOL_MAX_SIZE,
                timeout=_DB_POOL_TIMEOUT,
                max_lifetime=_DB_POOL_MAX_LIFETIME,
                max_idle=_DB_POOL_MAX_IDLE,
                kwargs={
                    "connect_timeout": _DB_CONNECT_TIMEOUT,
                    "row_factory": dict_row,
                },
                configure=_configure_pooled_conn,
                check=_check_cb,
            )
            print(
                f"[DB] Connection pool ACTIVE: min={_DB_POOL_MIN_SIZE} "
                f"max={_DB_POOL_MAX_SIZE} timeout={_DB_POOL_TIMEOUT}s "
                f"max_lifetime={_DB_POOL_MAX_LIFETIME}s "
                f"max_idle={_DB_POOL_MAX_IDLE}s "
                f"check={'SELECT1' if _DB_POOL_CHECK else 'DISABLED'} "
                f"pid={os.getpid()}"
            )
            return _pool
        except Exception as e:
            print(f"[DB] Pool init failed: {e} — using direct connections")
            return None


def pool_stats() -> dict:
    """Return current pool statistics for diagnostics. Safe to call anytime."""
    if _pool is None:
        return {"pooling": False}
    try:
        raw = _pool.get_stats()
        return {
            "pooling": True,
            "pool_min": _pool.min_size,
            "pool_max": _pool.max_size,
            "pool_size": raw.get("pool_size", -1),
            "pool_available": raw.get("pool_available", -1),
            "requests_waiting": raw.get("requests_waiting", -1),
            "connections_lost": raw.get("connections_lost", 0),
            "returns_bad": raw.get("returns_bad", 0),
            "borrows": _pool_borrows,
            "returns": _pool_returns,
            "leaked": _pool_borrows - _pool_returns,
            "pid": os.getpid(),
        }
    except Exception:
        return {"pooling": True, "error": "stats_unavailable"}


def close_pool():
    """Close the connection pool (for clean shutdown)."""
    global _pool
    if _pool is not None:
        try:
            _pool.close()
        except Exception:
            pass
        _pool = None


# ─────────────────────────────────────────────────────────────
# Custom Exceptions
# ─────────────────────────────────────────────────────────────
class DatabaseError(Exception):
    """Base exception for database errors."""
    pass


class DatabaseNotConfiguredError(DatabaseError):
    """Raised when database is not configured but an operation requires it."""
    def __init__(self, message: str = "Database is not configured"):
        super().__init__(message)


class DatabaseConnectionError(DatabaseError):
    """Raised when unable to connect to the database."""
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(message)
        self.original_error = original_error


class DatabaseQueryError(DatabaseError):
    """Raised when a query fails."""
    def __init__(self, message: str, query: str = None, original_error: Exception = None):
        super().__init__(message)
        self.query = query
        self.original_error = original_error


class DatabaseIntegrityError(DatabaseError):
    """Raised on constraint violations (unique, foreign key, etc.)."""
    def __init__(self, message: str, constraint: str = None, original_error: Exception = None):
        super().__init__(message)
        self.constraint = constraint
        self.original_error = original_error


# ─────────────────────────────────────────────────────────────
# Connection State
# ─────────────────────────────────────────────────────────────
USE_DB = bool(_DATABASE_URL and PSYCOPG_AVAILABLE)

_DB_STARTUP_CHECKED = False
_DB_STARTUP_READY = False
_DB_STARTUP_REASON = ""

_DEGRADED_MODE_LIMITATIONS = [
    {
        "id": "durable_jobs_history",
        "summary": (
            "Jobs and history fall back to per-process local storage only where implemented; "
            "persistence across restarts or multiple workers is disabled."
        ),
    },
    {
        "id": "background_recovery",
        "summary": (
            "Stale-job recovery, durable worker leadership, pricing seeding, and "
            "operations/rescue loops do not run without a database."
        ),
    },
    {
        "id": "identity_wallet_billing",
        "summary": (
            "Session-backed identity, wallet, purchases, subscriptions, magic-code email "
            "restore, and payment/webhook reconciliation require the database."
        ),
    },
    {
        "id": "community_assets_admin_diagnostics",
        "summary": (
            "Community routes, DB-backed asset ownership/proxy checks, and admin/auth "
            "database diagnostics are unavailable."
        ),
    },
]

if PSYCOPG_AVAILABLE:
    print(f"[DB] psycopg3 available, DATABASE_URL configured: {_HAS_DATABASE}, USE_DB: {USE_DB}")
else:
    print("[DB] psycopg3 not available - database features disabled")


# ─────────────────────────────────────────────────────────────
# Time Helpers
# ─────────────────────────────────────────────────────────────
def now_utc() -> datetime:
    """Get current UTC datetime (timezone-aware)."""
    return datetime.now(timezone.utc)


def now_utc_iso() -> str:
    """Get current UTC datetime as ISO string."""
    return now_utc().isoformat()


# ─────────────────────────────────────────────────────────────
# Connection Management
# ─────────────────────────────────────────────────────────────
def _safe_rollback(conn):
    """Attempt rollback; silently ignore if the connection is broken.
    Prevents secondary exceptions when rolling back a dead SSL connection."""
    try:
        conn.rollback()
    except Exception:
        pass


def _create_connection():
    """
    Create a new database connection.
    Internal function - raises exceptions on failure.
    """
    if not PSYCOPG_AVAILABLE:
        raise DatabaseNotConfiguredError("psycopg3 is not installed")

    if not _DATABASE_URL:
        raise DatabaseNotConfiguredError("DATABASE_URL is not set")

    try:
        conn = psycopg.connect(
            _DATABASE_URL,
            connect_timeout=_DB_CONNECT_TIMEOUT,
            row_factory=dict_row,  # Default to dict rows
        )
        # Set search path and session safety limits
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {_APP_SCHEMA}, {_BILLING_SCHEMA}, public;")
            cur.execute("SET statement_timeout = '30000';")
            cur.execute("SET idle_in_transaction_session_timeout = '60000';")
            cur.execute("SET lock_timeout = '10000';")
        return conn
    except psycopg.OperationalError as e:
        raise DatabaseConnectionError(f"Failed to connect to database: {e}", original_error=e)
    except Exception as e:
        raise DatabaseConnectionError(f"Unexpected error connecting to database: {e}", original_error=e)


@contextmanager
def get_conn(source: str = ""):
    """
    Context manager for database connections.
    Connection is NOT auto-committed - caller must commit explicitly or use transaction().

    Args:
        source: Optional tag for pool tracing (e.g. "job_worker_claim").
                Only logged when DB_POOL_TRACE=true. Zero overhead when empty.

    When pool is enabled, uses pool.connection() which returns the connection
    to the pool in a clean state on exit (auto-commit on success, auto-rollback
    on exception via psycopg3's Connection context manager).

    When pool is disabled (default), opens and closes a direct connection.

    Raises:
        DatabaseNotConfiguredError: If database is not configured
        DatabaseConnectionError: If connection fails

    Usage:
        with get_conn("wallet_fetch") as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM users")
                rows = cur.fetchall()
            conn.commit()
    """
    pool = _get_pool()
    if pool is not None:
        _maybe_log_pool_stats()
        try:
            with pool.connection() as conn:
                # Trace AFTER pool.connection() succeeds — the connection is
                # actually held now. This keeps leaked count accurate (no
                # false +1 while waiting in the pool queue).
                t0 = _time.monotonic()
                _pool_trace_checkout(source or "get_conn")
                try:
                    yield conn
                finally:
                    _pool_trace_return(source or "get_conn", t0)
        except Exception as exc:
            if "PoolTimeout" in type(exc).__name__:
                _pool_trace_timeout(source or "get_conn")
            raise
    else:
        conn = _create_connection()
        try:
            yield conn
        finally:
            try:
                conn.close()
            except Exception:
                pass


@contextmanager
def _run_transaction(conn):
    """Internal: execute a transaction with error mapping.
    Handles commit on success, safe rollback + exception wrapping on failure."""
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except psycopg.errors.UniqueViolation as e:
        _safe_rollback(conn)
        constraint = getattr(e.diag, 'constraint_name', None)
        raise DatabaseIntegrityError(
            f"Unique constraint violation: {e}",
            constraint=constraint,
            original_error=e
        )
    except psycopg.errors.ForeignKeyViolation as e:
        _safe_rollback(conn)
        constraint = getattr(e.diag, 'constraint_name', None)
        raise DatabaseIntegrityError(
            f"Foreign key violation: {e}",
            constraint=constraint,
            original_error=e
        )
    except psycopg.errors.CheckViolation as e:
        _safe_rollback(conn)
        constraint = getattr(e.diag, 'constraint_name', None)
        raise DatabaseIntegrityError(
            f"Check constraint violation: {e}",
            constraint=constraint,
            original_error=e
        )
    except psycopg.Error as e:
        _safe_rollback(conn)
        raise DatabaseQueryError(f"Database error: {e}", original_error=e)
    except Exception:
        _safe_rollback(conn)
        raise


@contextmanager
def transaction(source: str = ""):
    """
    Context manager for database transactions.
    Automatically commits on success, rolls back on exception.
    Yields a cursor with dict_row factory.

    Args:
        source: Optional tag for pool tracing (e.g. "identity_validate").

    When pool is enabled, wraps pool.connection() so the connection always
    returns to the pool in a clean IDLE state. When pool is disabled (default),
    opens and closes a direct connection.

    Raises:
        DatabaseNotConfiguredError: If database is not configured
        DatabaseConnectionError: If connection fails
        DatabaseQueryError: If a query fails
        DatabaseIntegrityError: On constraint violations

    Usage:
        with transaction("billing_checkout") as cur:
            cur.execute("INSERT INTO users (name) VALUES (%s) RETURNING *", ("John",))
            user = fetch_one(cur)
        # Auto-committed here if no exception
    """
    pool = _get_pool()
    if pool is not None:
        _maybe_log_pool_stats()
        try:
            with pool.connection() as conn:
                t0 = _time.monotonic()
                _pool_trace_checkout(source or "transaction")
                try:
                    with _run_transaction(conn) as cur:
                        yield cur
                finally:
                    _pool_trace_return(source or "transaction", t0)
        except Exception as exc:
            if "PoolTimeout" in type(exc).__name__:
                _pool_trace_timeout(source or "transaction")
            raise
    else:
        conn = _create_connection()
        try:
            with _run_transaction(conn) as cur:
                yield cur
        finally:
            try:
                conn.close()
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────
# Cursor Helpers (for use within transaction/get_conn blocks)
# ─────────────────────────────────────────────────────────────
def fetch_one(cur) -> Optional[Dict[str, Any]]:
    """
    Fetch one row from cursor as dict.
    Returns None if no rows available.

    Usage:
        with transaction() as cur:
            cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            user = fetch_one(cur)
            if user is None:
                raise ValueError("User not found")
    """
    row = cur.fetchone()
    if row is None:
        return None
    # psycopg3 with dict_row already returns dict
    if isinstance(row, dict):
        return row
    # Fallback for tuple rows (shouldn't happen with our setup)
    if cur.description:
        columns = [desc[0] for desc in cur.description]
        return dict(zip(columns, row))
    return None


def fetch_all(cur) -> List[Dict[str, Any]]:
    """
    Fetch all rows from cursor as list of dicts.
    Returns empty list if no rows.

    Usage:
        with transaction() as cur:
            cur.execute("SELECT * FROM users WHERE active = true")
            users = fetch_all(cur)
    """
    rows = cur.fetchall()
    if not rows:
        return []
    # psycopg3 with dict_row already returns list of dicts
    if rows and isinstance(rows[0], dict):
        return list(rows)
    # Fallback for tuple rows
    if cur.description:
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in rows]
    return []


def fetch_scalar(cur) -> Any:
    """
    Fetch a single scalar value from cursor.
    Returns None if no rows.

    Usage:
        with transaction() as cur:
            cur.execute("SELECT COUNT(*) FROM users")
            count = fetch_scalar(cur)
    """
    row = cur.fetchone()
    if row is None:
        return None
    if isinstance(row, dict):
        # Return first value from dict
        return next(iter(row.values()), None)
    # Tuple row
    return row[0] if row else None


# ─────────────────────────────────────────────────────────────
# Standalone Query Helpers (open their own transaction)
# ─────────────────────────────────────────────────────────────
def query_one(sql: str, params: tuple = None, source: str = "") -> Optional[Dict[str, Any]]:
    """
    Execute a query and return one row as dict.
    Opens its own transaction.

    Raises:
        DatabaseError: On any database error

    Usage:
        user = query_one("SELECT * FROM users WHERE id = %s", (user_id,))
    """
    with transaction(source) as cur:
        cur.execute(sql, params or ())
        return fetch_one(cur)


def query_all(sql: str, params: tuple = None, source: str = "") -> List[Dict[str, Any]]:
    """
    Execute a query and return all rows as list of dicts.
    Opens its own transaction.

    Raises:
        DatabaseError: On any database error

    Usage:
        users = query_all("SELECT * FROM users WHERE active = true")
    """
    with transaction(source) as cur:
        cur.execute(sql, params or ())
        return fetch_all(cur)


def execute(sql: str, params: tuple = None, source: str = "") -> int:
    """
    Execute a statement and return affected row count.
    Opens its own transaction.

    Raises:
        DatabaseError: On any database error

    Usage:
        count = execute("DELETE FROM sessions WHERE expires_at < %s", (now_utc(),))
        print(f"Deleted {count} expired sessions")
    """
    with transaction(source) as cur:
        cur.execute(sql, params or ())
        return cur.rowcount


def execute_returning(sql: str, params: tuple = None, source: str = "") -> Optional[Dict[str, Any]]:
    """
    Execute an INSERT/UPDATE with RETURNING clause.
    Opens its own transaction.

    Raises:
        DatabaseError: On any database error

    Usage:
        user = execute_returning(
            "INSERT INTO users (name) VALUES (%s) RETURNING *",
            ("John",)
        )
    """
    with transaction(source) as cur:
        cur.execute(sql, params or ())
        return fetch_one(cur)


def execute_returning_all(sql: str, params: tuple = None, source: str = "") -> List[Dict[str, Any]]:
    """
    Execute an INSERT/UPDATE with RETURNING clause, return all rows.
    Opens its own transaction.

    Raises:
        DatabaseError: On any database error

    Usage:
        users = execute_returning_all(
            "UPDATE users SET active = false WHERE last_login < %s RETURNING *",
            (cutoff_date,)
        )
    """
    with transaction(source) as cur:
        cur.execute(sql, params or ())
        return fetch_all(cur)


# ─────────────────────────────────────────────────────────────
# Schema-aware Table References
# ─────────────────────────────────────────────────────────────
class Tables:
    """Table name constants with schema prefixes."""
    # Billing schema
    IDENTITIES = f"{_BILLING_SCHEMA}.identities"
    IDENTITY_MERGES = f"{_BILLING_SCHEMA}.identity_merges"
    SESSIONS = f"{_BILLING_SCHEMA}.sessions"
    MAGIC_CODES = f"{_BILLING_SCHEMA}.magic_codes"
    WALLETS = f"{_BILLING_SCHEMA}.wallets"
    LEDGER_ENTRIES = f"{_BILLING_SCHEMA}.ledger_entries"
    ACTION_COSTS = f"{_BILLING_SCHEMA}.action_costs"
    CREDIT_RESERVATIONS = f"{_BILLING_SCHEMA}.credit_reservations"
    PURCHASES = f"{_BILLING_SCHEMA}.purchases"
    PLANS = f"{_BILLING_SCHEMA}.plans"
    JOBS = f"{_BILLING_SCHEMA}.jobs"
    DAILY_LIMITS = f"{_BILLING_SCHEMA}.daily_limits"
    SUBSCRIPTIONS = f"{_BILLING_SCHEMA}.subscriptions"
    SUBSCRIPTION_CYCLES = f"{_BILLING_SCHEMA}.subscription_cycles"
    SUBSCRIPTION_EVENTS = f"{_BILLING_SCHEMA}.subscription_events"
    SUBSCRIPTION_NOTIFICATIONS = f"{_BILLING_SCHEMA}.subscription_notifications"
    MOLLIE_CUSTOMERS = f"{_BILLING_SCHEMA}.mollie_customers"
    CRON_LOCKS = f"{_BILLING_SCHEMA}.cron_locks"
    INVOICES = f"{_BILLING_SCHEMA}.invoices"
    INVOICE_ITEMS = f"{_BILLING_SCHEMA}.invoice_items"
    RECEIPTS = f"{_BILLING_SCHEMA}.receipts"
    EMAIL_OUTBOX = f"{_BILLING_SCHEMA}.email_outbox"
    PROVIDER_ALERTS = f"{_BILLING_SCHEMA}.provider_alerts"
    PROVIDER_LEDGER = f"{_BILLING_SCHEMA}.provider_ledger"
    REFUNDS = f"{_BILLING_SCHEMA}.refunds"
    PAYMENT_DISPUTES = f"{_BILLING_SCHEMA}.payment_disputes"
    PROCESSED_WEBHOOK_PAYMENTS = f"{_BILLING_SCHEMA}.processed_webhook_payments"
    CHECKOUT_IDEMPOTENCY = f"{_BILLING_SCHEMA}.checkout_idempotency"
    VIDEO_DAILY_USAGE = f"{_BILLING_SCHEMA}.video_daily_usage"

    # App schema
    MODELS = f"{_APP_SCHEMA}.models"
    IMAGES = f"{_APP_SCHEMA}.images"
    VIDEOS = f"{_APP_SCHEMA}.videos"
    HISTORY_ITEMS = f"{_APP_SCHEMA}.history_items"
    ACTIVE_JOBS = f"{_APP_SCHEMA}.active_jobs"
    ACTIVITY_LOGS = f"{_APP_SCHEMA}.activity_logs"
    PROVIDER_OPERATIONS = f"{_APP_SCHEMA}.provider_operations"


# ─────────────────────────────────────────────────────────────
# Utility Functions
# ─────────────────────────────────────────────────────────────
def hash_string(value: str) -> str:
    """Hash a string using SHA256 (for IP addresses, user agents, etc.)."""
    return hashlib.sha256(value.encode()).hexdigest()


def is_available() -> bool:
    """Check if database is configured and available."""
    return USE_DB


def get_runtime_report() -> Dict[str, Any]:
    """Return a stable, JSON-friendly view of DB runtime mode."""
    if not _HAS_DATABASE:
        reason = "DATABASE_URL is not set"
    elif not PSYCOPG_AVAILABLE:
        reason = "psycopg3 is not installed"
    elif _DB_STARTUP_CHECKED and not _DB_STARTUP_READY:
        reason = _DB_STARTUP_REASON or "Database startup check failed"
    elif not _DB_STARTUP_CHECKED:
        reason = "Database startup check has not run yet"
    else:
        reason = ""

    degraded = not _DB_STARTUP_READY
    return {
        "configured": _HAS_DATABASE,
        "driver_available": PSYCOPG_AVAILABLE,
        "enabled": USE_DB,
        "startup_checked": _DB_STARTUP_CHECKED,
        "ready": _DB_STARTUP_READY,
        "mode": "degraded" if degraded else "full",
        "reason": reason or None,
        "disabled_capabilities": list(_DEGRADED_MODE_LIMITATIONS) if degraded else [],
        "notes": (
            [
                "Health means the HTTP service is up; DB-backed persistence is not available in degraded mode."
            ]
            if degraded
            else []
        ),
    }


def verify_connection() -> bool:
    """
    Test database connectivity.
    Returns True if connected, False otherwise.
    Does not raise exceptions.
    """
    if not USE_DB:
        return False
    try:
        result = query_one("SELECT 1 AS ok")
        return result is not None and result.get("ok") == 1
    except DatabaseError:
        return False


def require_db():
    """
    Assert that database is available.
    Raises DatabaseNotConfiguredError if not.

    Usage:
        require_db()  # Raises if DB not configured
        # ... proceed with DB operations
    """
    if not USE_DB:
        raise DatabaseNotConfiguredError(
            "This operation requires a database connection. "
            "Please configure DATABASE_URL environment variable."
        )


def init_db() -> bool:
    """
    Initialize database connection and verify connectivity.
    Called at app startup.
    Returns True if database is ready.

    Raises:
        DatabaseConnectionError: If database is configured but connection fails
    """
    global _DB_STARTUP_CHECKED, _DB_STARTUP_READY, _DB_STARTUP_REASON
    _DB_STARTUP_CHECKED = True

    if not _HAS_DATABASE:
        print("[DB] DATABASE_URL not set - running without database")
        _DB_STARTUP_READY = False
        _DB_STARTUP_REASON = "DATABASE_URL is not set"
        return False

    if not PSYCOPG_AVAILABLE:
        print("[DB] psycopg3 not installed - running without database")
        _DB_STARTUP_READY = False
        _DB_STARTUP_REASON = "psycopg3 is not installed"
        return False

    # Attempt connection
    try:
        if verify_connection():
            print("[DB] Database connection verified successfully")
            # Log pool mode — pool itself is created lazily on first use
            if _DB_POOL_ENABLED and _POOL_AVAILABLE:
                print(f"[DB] Pool mode: ENABLED (lazy init, min={_DB_POOL_MIN_SIZE} max={_DB_POOL_MAX_SIZE})")
            else:
                print("[DB] Pool mode: DISABLED — using direct connections")
            # Ensure schema indexes exist for idempotency
            ensure_schema()
            _DB_STARTUP_READY = True
            _DB_STARTUP_REASON = ""
            return True
        else:
            raise DatabaseConnectionError("Connection test query failed")
    except DatabaseError as e:
        _DB_STARTUP_READY = False
        _DB_STARTUP_REASON = str(e)
        print(f"[DB] ERROR: {e}")
        raise


def ensure_schema() -> None:
    """
    Verify critical schema elements exist at startup.

    All objects here were created via migrations and already exist in
    production.  The IF NOT EXISTS clauses make every statement a no-op
    under the normal app role (timrx_admin, which has no CREATE privilege).
    If the DDL unexpectedly fails (e.g. new deploy before migration), the
    exception is caught and logged — the app continues without crashing.

    NOTE: If you add a new table or index the app depends on, create it in
    a numbered migration first, then optionally add an IF NOT EXISTS guard
    here as a startup sanity check.
    """
    try:
        with transaction() as cur:
            # Unique index on (provider, provider_payment_id) for webhook idempotency
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_purchases_provider_payment
                ON timrx_billing.purchases(provider, provider_payment_id)
            """)

            # Unique index on (provider, payment_id) for additional idempotency
            # Partial index: only applies when payment_id is not null
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS purchases_provider_payment_id_ux
                ON timrx_billing.purchases (provider, payment_id)
                WHERE payment_id IS NOT NULL
            """)

        print("[DB] Schema indexes ensured")

        # Ensure prompt-safety strike tracking table
        try:
            from backend.services.prompt_safety_service import ensure_safety_schema
            ensure_safety_schema()
        except Exception as e:
            print(f"[DB] Warning: Could not ensure safety schema: {e}")
    except Exception as e:
        # Log but don't fail startup — under timrx_admin these are no-ops
        # (objects pre-exist).  Only fires if a migration was missed.
        print(f"[DB] Warning: Could not ensure schema indexes: {e}")


# ─────────────────────────────────────────────────────────────
# Batch Operations
# ─────────────────────────────────────────────────────────────
def execute_many(sql: str, params_list: List[tuple], source: str = "") -> int:
    """
    Execute a statement with multiple parameter sets.
    Opens its own transaction.

    Raises:
        DatabaseError: On any database error

    Usage:
        count = execute_many(
            "INSERT INTO users (name, email) VALUES (%s, %s)",
            [("John", "john@x.com"), ("Jane", "jane@x.com")]
        )
    """
    if not params_list:
        return 0

    with transaction(source) as cur:
        cur.executemany(sql, params_list)
        return cur.rowcount


# ─────────────────────────────────────────────────────────────
# Query Building Helpers
# ─────────────────────────────────────────────────────────────
def sql_in_clause(values: List[Any]) -> tuple:
    """
    Build a SQL IN clause with proper placeholders.
    Returns (placeholder_string, values_tuple).

    Usage:
        placeholders, params = sql_in_clause([1, 2, 3])
        cur.execute(f"SELECT * FROM users WHERE id IN ({placeholders})", params)
    """
    if not values:
        return "NULL", ()  # Empty IN clause that matches nothing
    placeholders = ", ".join(["%s"] * len(values))
    return placeholders, tuple(values)


# ─────────────────────────────────────────────────────────────
# Exports for backwards compatibility
# ─────────────────────────────────────────────────────────────
# These mirror the old API for gradual migration
get_connection = get_conn  # Alias
get_db = get_conn  # Alias


# ─────────────────────────────────────────────────────────────
# Module exports (explicit __all__ for clarity)
# ─────────────────────────────────────────────────────────────
__all__ = [
    # Re-export psycopg's dict_row for convenience
    "dict_row",
    # Connection state
    "USE_DB",
    "PSYCOPG_AVAILABLE",
    # Exceptions
    "DatabaseError",
    "DatabaseNotConfiguredError",
    "DatabaseConnectionError",
    "DatabaseQueryError",
    "DatabaseIntegrityError",
    # Connection management
    "get_conn",
    "get_connection",
    "get_db",
    "transaction",
    # Time helpers
    "now_utc",
    "now_utc_iso",
    # Cursor helpers
    "fetch_one",
    "fetch_all",
    "fetch_scalar",
    # Standalone query helpers
    "query_one",
    "query_all",
    "execute",
    "execute_returning",
    "execute_returning_all",
    "execute_many",
    # Schema-aware tables
    "Tables",
    # Utilities
    "hash_string",
    "is_available",
    "get_runtime_report",
    "verify_connection",
    "require_db",
    "init_db",
    "ensure_schema",
    "close_pool",
    "sql_in_clause",
]
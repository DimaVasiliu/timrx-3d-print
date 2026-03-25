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
import time as _time
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
_DB_POOL_TIMEOUT = float(os.getenv("DB_POOL_TIMEOUT", "0.5"))  # 0.5s — fail very fast to direct fallback; pool is bonus, not critical path
_DB_POOL_MAX_LIFETIME = float(os.getenv("DB_POOL_MAX_LIFETIME", "60"))    # 60s — aggressive recycle, Render kills SSL at ~2-5min
_DB_POOL_MAX_IDLE = float(os.getenv("DB_POOL_MAX_IDLE", "20"))            # 20s — close idle connections fast
_DB_POOL_CHECK = os.getenv("DB_POOL_CHECK", "true").lower() in ("true", "1", "yes")  # ON — detects dead SSL connections before handing to caller
_APP_SCHEMA = os.getenv("APP_SCHEMA", "timrx_app")
_BILLING_SCHEMA = os.getenv("BILLING_SCHEMA", "timrx_billing")

# Fail-closed validation: reject malformed schema names at import time
_SCHEMA_RE = re.compile(r'^[a-z][a-z0-9_]{0,62}$')
if not _SCHEMA_RE.match(_APP_SCHEMA):
    raise ValueError(f"Invalid APP_SCHEMA: {_APP_SCHEMA!r} — must match ^[a-z][a-z0-9_]{{0,62}}$")
if not _SCHEMA_RE.match(_BILLING_SCHEMA):
    raise ValueError(f"Invalid BILLING_SCHEMA: {_BILLING_SCHEMA!r} — must match ^[a-z][a-z0-9_]{{0,62}}$")


# ─────────────────────────────────────────────────────────────
# Connection Pool
# ─────────────────────────────────────────────────────────────
_pool = None
_pool_init_attempted = False
_pool_lock = threading.Lock()


# ─── Transient connection error detection ─────────────────────
_TRANSIENT_PATTERNS = (
    "bad record mac",
    "eof detected",
    "consuming input failed",
    "closed connection",
    "connection not open",
    "server closed the connection unexpectedly",
    "ssl syscall error",
    "ssl error",
    "broken pipe",
    "connection reset by peer",
    "connection timed out",
    "the connection is closed",
    "can't send query",
    "no connection to the server",
)


def is_transient_db_error(exc: BaseException) -> bool:
    """Return True if exc is a transport/SSL/connection/pool error (not a SQL logic error).
    These are safe to retry on a fresh direct connection."""
    if not PSYCOPG_AVAILABLE:
        return False
    # PoolTimeout: pool couldn't serve a connection in time
    if _POOL_AVAILABLE and isinstance(exc, _pool_timeout_class()):
        return True
    # psycopg.OperationalError covers most transport errors
    if isinstance(exc, psycopg.OperationalError):
        return True
    # psycopg.InterfaceError covers "connection is closed" etc.
    if isinstance(exc, psycopg.InterfaceError):
        return True
    # Check message text for SSL/transport patterns
    msg = str(exc).lower()
    return any(p in msg for p in _TRANSIENT_PATTERNS)


def _pool_timeout_class():
    """Return PoolTimeout class or a dummy that never matches."""
    try:
        from psycopg_pool import PoolTimeout
        return PoolTimeout
    except ImportError:
        return type(None)  # never matches isinstance


def _configure_pooled_conn(conn):
    """Configure session settings for a new pooled connection.
    Runs in autocommit so SETs are immediate and survive rollbacks.
    Leaves the connection in autocommit mode so that read-only
    get_conn() callers never open an implicit transaction (INTRANS).
    transaction() temporarily switches to non-autocommit when it
    needs real transactional semantics."""
    conn.autocommit = True
    conn.execute(f"SET search_path TO {_APP_SCHEMA}, {_BILLING_SCHEMA}, public")
    conn.execute("SET statement_timeout = '30000'")
    conn.execute("SET idle_in_transaction_session_timeout = '60000'")
    conn.execute("SET lock_timeout = '10000'")
    # Stays autocommit=True — no implicit transactions on idle connections.


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
            _check_cb = _ConnectionPool.check_connection if _DB_POOL_CHECK else None

            # open=False (default): pool background threads start on first
            # .connection() or .open() call — NOT in the master process.
            # We call pool.open() + pool.wait() inside init_db() which runs
            # per-worker inside create_app(), so each Gunicorn worker gets
            # its own live pool with min_size connections ready.
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


def _ensure_idle(conn):
    """Roll back if the connection is still INTRANS/INERROR so it can be
    returned to the pool in a clean state.  Without this, psycopg_pool logs
    'rolling back returned connection' warnings and the connection costs an
    extra round-trip inside putconn()."""
    try:
        # psycopg3: conn.info.transaction_status is a pq.TransactionStatus enum.
        # IDLE (0) = clean, INTRANS (2) = open txn, INERROR (3) = failed txn.
        status = conn.info.transaction_status
        if status != 0:  # anything other than IDLE
            conn.rollback()
    except Exception:
        pass  # broken connection — putconn will discard it


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
    Pool-first, direct-fallback connection context manager.

    Tries to borrow from the pool.  If the pool CHECKOUT fails with a
    transient error (PoolTimeout, SSL on idle connection), opens a fresh
    direct connection instead.  If the pool checkout succeeds, the caller
    gets that pooled connection normally.

    Pooled connections are in autocommit mode: each statement commits
    immediately and the connection stays IDLE (no implicit transaction).
    Callers that need atomicity across multiple statements should use
    transaction() instead.

    Direct-fallback connections use psycopg3's default (autocommit=False),
    so callers doing writes on the fallback path should still commit.

    This covers the main failure mode: pool full or dead SSL connections.
    If a query fails mid-execution on a connection that was healthy at
    checkout, that error propagates to the caller (and the pool discards
    the broken connection automatically).
    """
    pool = _get_pool()
    conn = None
    from_pool = False

    if pool is not None:
        try:
            conn = pool.getconn(timeout=_DB_POOL_TIMEOUT)
            from_pool = True
        except Exception as e:
            if is_transient_db_error(e):
                print(f"[DB][FALLBACK] get_conn pool checkout failed, using direct source={source}: {type(e).__name__}")
                conn = None
            else:
                raise

    if conn is not None and from_pool:
        try:
            yield conn
        finally:
            # Clean up transaction state so the pool doesn't have to.
            _ensure_idle(conn)
            # Best-effort: restore autocommit mode for the next borrower.
            try:
                if not conn.autocommit:
                    conn.autocommit = True
            except Exception:
                pass  # broken conn — putconn will discard it
            # ALWAYS return via putconn so the pool can free the slot.
            # putconn detects broken/closed connections and discards them
            # instead of reusing them.  Skipping putconn leaks the slot
            # permanently, eventually exhausting the pool.
            try:
                pool.putconn(conn)
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
    else:
        # Direct connection (pool failed or disabled)
        conn = _create_connection()
        try:
            yield conn
        finally:
            try:
                conn.close()
            except Exception:
                pass


# get_conn_resilient is now an alias — get_conn has the same fallback
# behaviour.  Kept so existing imports (history, inspire, billing,
# subscription_service) continue to resolve without any code changes.
get_conn_resilient = get_conn


@contextmanager
def get_conn_direct(source: str = ""):
    """
    Context manager that ALWAYS creates a fresh direct connection (never pooled).

    Use for auth-critical paths (bootstrap, restore/redeem) that must not
    depend on pool health. The connection is opened, used, and closed
    within this block — no pool threads, no shared state.
    """
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
    Pool-first with direct-fallback on transient checkout errors.
    Automatically commits on success, rolls back on exception.
    Yields a cursor with dict_row factory.
    """
    pool = _get_pool()
    conn = None
    from_pool = False

    if pool is not None:
        try:
            conn = pool.getconn(timeout=_DB_POOL_TIMEOUT)
            from_pool = True
        except Exception as e:
            if is_transient_db_error(e):
                print(f"[DB][TX_FALLBACK] pool checkout failed, using direct source={source}: {type(e).__name__}")
                conn = None
            else:
                raise

    if conn is not None and from_pool:
        # Pooled connections live in autocommit mode.  Switch to
        # transactional mode for the duration of this block so
        # _run_transaction gets real BEGIN/COMMIT/ROLLBACK semantics.
        conn.autocommit = False
        try:
            with _run_transaction(conn) as cur:
                yield cur
        finally:
            # _run_transaction commits on success and rolls back on error,
            # but defensive _ensure_idle catches edge cases (e.g. commit
            # itself raising after partial flush).
            _ensure_idle(conn)
            # Best-effort: restore autocommit for the next borrower.
            try:
                conn.autocommit = True
            except Exception:
                pass  # broken conn — putconn will discard it
            # ALWAYS return via putconn so the pool can free the slot.
            # putconn detects broken/closed connections and discards them.
            # Skipping putconn leaks the slot permanently.
            try:
                pool.putconn(conn)
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
    else:
        # Direct connection (already autocommit=False per psycopg3 default)
        conn = _create_connection()
        try:
            with _run_transaction(conn) as cur:
                yield cur
        finally:
            try:
                conn.close()
            except Exception:
                pass


@contextmanager
def transaction_direct(source: str = ""):
    """
    Transaction that ALWAYS uses a fresh direct connection (never pooled).

    Use for auth-critical paths (bootstrap, restore/redeem) that must work
    even when the pool is full of dead SSL connections.
    """
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
# Standalone Query Helpers — pool-first, direct-fallback on transient errors
#
# Primary path uses the pool. On transient SSL/transport/pool errors,
# the retry uses a DIRECT connection (bypasses the sick pool entirely).
# This is the key resilience mechanism: user-facing reads survive pool storms.
#
# Non-connection errors (constraint violations, syntax errors, etc.)
# are never retried.
# ─────────────────────────────────────────────────────────────
def query_one(sql: str, params: tuple = None, source: str = "") -> Optional[Dict[str, Any]]:
    """Execute a query and return one row as dict. Falls back to direct connection on transient errors."""
    try:
        with transaction(source) as cur:
            cur.execute(sql, params or ())
            return fetch_one(cur)
    except Exception as e:
        if is_transient_db_error(e):
            print(f"[DB][FALLBACK] query_one pool failed, using direct conn source={source or 'unknown'}: {type(e).__name__}: {e}")
            with transaction_direct(source) as cur:
                cur.execute(sql, params or ())
                return fetch_one(cur)
        raise


def query_all(sql: str, params: tuple = None, source: str = "") -> List[Dict[str, Any]]:
    """Execute a query and return all rows. Falls back to direct connection on transient errors."""
    try:
        with transaction(source) as cur:
            cur.execute(sql, params or ())
            return fetch_all(cur)
    except Exception as e:
        if is_transient_db_error(e):
            print(f"[DB][FALLBACK] query_all pool failed, using direct conn source={source or 'unknown'}: {type(e).__name__}: {e}")
            with transaction_direct(source) as cur:
                cur.execute(sql, params or ())
                return fetch_all(cur)
        raise


def execute(sql: str, params: tuple = None, source: str = "") -> int:
    """Execute a statement and return affected row count. Falls back to direct on transient errors."""
    try:
        with transaction(source) as cur:
            cur.execute(sql, params or ())
            return cur.rowcount
    except Exception as e:
        if is_transient_db_error(e):
            print(f"[DB][FALLBACK] execute pool failed, using direct conn source={source or 'unknown'}: {type(e).__name__}: {e}")
            with transaction_direct(source) as cur:
                cur.execute(sql, params or ())
                return cur.rowcount
        raise


def execute_returning(sql: str, params: tuple = None, source: str = "") -> Optional[Dict[str, Any]]:
    """Execute an INSERT/UPDATE with RETURNING clause. Falls back to direct on transient errors."""
    try:
        with transaction(source) as cur:
            cur.execute(sql, params or ())
            return fetch_one(cur)
    except Exception as e:
        if is_transient_db_error(e):
            print(f"[DB][FALLBACK] execute_returning pool failed, using direct conn source={source or 'unknown'}: {type(e).__name__}: {e}")
            with transaction_direct(source) as cur:
                cur.execute(sql, params or ())
                return fetch_one(cur)
        raise


def execute_returning_all(sql: str, params: tuple = None, source: str = "") -> List[Dict[str, Any]]:
    """Execute an INSERT/UPDATE with RETURNING, return all rows. Falls back to direct on transient errors."""
    try:
        with transaction(source) as cur:
            cur.execute(sql, params or ())
            return fetch_all(cur)
    except Exception as e:
        if is_transient_db_error(e):
            print(f"[DB][FALLBACK] execute_returning_all pool failed, using direct conn source={source or 'unknown'}: {type(e).__name__}: {e}")
            with transaction_direct(source) as cur:
                cur.execute(sql, params or ())
                return fetch_all(cur)
        raise


def execute_many(sql: str, params_list: List[tuple], source: str = "") -> int:
    """Execute a batch of statements. Falls back to direct on transient errors."""
    if not params_list:
        return 0
    try:
        with transaction(source) as cur:
            cur.executemany(sql, params_list)
            return cur.rowcount
    except Exception as e:
        if is_transient_db_error(e):
            print(f"[DB][FALLBACK] execute_many pool failed, using direct conn source={source or 'unknown'}: {type(e).__name__}: {e}")
            with transaction_direct(source) as cur:
                cur.executemany(sql, params_list)
                return cur.rowcount
        raise


def is_available() -> bool:
    """Check if database is configured and available."""
    return USE_DB


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
    MOLLIE_CUSTOMERS = f"{_BILLING_SCHEMA}.mollie_customers"
    INVOICES = f"{_BILLING_SCHEMA}.invoices"
    RECEIPTS = f"{_BILLING_SCHEMA}.receipts"
    REFUNDS = f"{_BILLING_SCHEMA}.refunds"
    PAYMENT_DISPUTES = f"{_BILLING_SCHEMA}.payment_disputes"
    EMAIL_OUTBOX = f"{_BILLING_SCHEMA}.email_outbox"
    PROVIDER_LEDGER = f"{_BILLING_SCHEMA}.provider_ledger"
    PROVIDER_ALERTS = f"{_BILLING_SCHEMA}.provider_alerts"
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
# Utilities
# ─────────────────────────────────────────────────────────────
def hash_string(value: str) -> str:
    """Hash a string using SHA256 (for IP addresses, user agents, etc.)."""
    return hashlib.sha256(value.encode()).hexdigest()


def sql_in_clause(values: List[Any]) -> tuple:
    """Build a SQL IN clause with proper placeholders."""
    if not values:
        return "NULL", ()
    placeholders = ", ".join(["%s"] * len(values))
    return placeholders, tuple(values)


def verify_connection() -> bool:
    """Test database connectivity. Returns True if connected."""
    if not USE_DB:
        return False
    try:
        result = query_one("SELECT 1 AS ok")
        return result is not None and result.get("ok") == 1
    except DatabaseError:
        return False


def require_db():
    """Assert that database is available. Raises DatabaseNotConfiguredError if not."""
    if not USE_DB:
        raise DatabaseNotConfiguredError(
            "This operation requires a database connection. "
            "Please configure DATABASE_URL environment variable."
        )


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


def init_db() -> bool:
    """Initialize database connection and verify connectivity at startup."""
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

    try:
        # Verify connectivity with a DIRECT connection (not pooled).
        # create_app() runs in the Gunicorn master process before fork.
        # If we use the pool here, its background threads start in the
        # master and die on fork, causing "couldn't stop thread" errors.
        # The pool stays dormant until the first request hits the worker.
        try:
            conn = _create_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                row = cur.fetchone()
            conn.close()
            if not row or row.get("ok") != 1:
                raise DatabaseConnectionError("Connection test query failed")
        except DatabaseConnectionError:
            raise
        except Exception as e:
            raise DatabaseConnectionError(f"Startup connection test failed: {e}", original_error=e)

        print("[DB] Database connection verified successfully")
        if _DB_POOL_ENABLED and _POOL_AVAILABLE:
            print(f"[DB] Pool mode: ENABLED (will open on first request, min={_DB_POOL_MIN_SIZE} max={_DB_POOL_MAX_SIZE})")
        else:
            print("[DB] Pool mode: DISABLED — using direct connections")

        # Run schema checks with a direct connection too
        _ensure_schema_direct()

        _DB_STARTUP_READY = True
        _DB_STARTUP_REASON = ""
        return True
    except DatabaseError as e:
        _DB_STARTUP_READY = False
        _DB_STARTUP_REASON = str(e)
        print(f"[DB] ERROR: {e}")
        raise


def ensure_schema() -> None:
    """Verify critical schema elements exist at startup (uses pool)."""
    try:
        with transaction() as cur:
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_purchases_provider_payment
                ON timrx_billing.purchases(provider, provider_payment_id)
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS purchases_provider_payment_id_ux
                ON timrx_billing.purchases (provider, payment_id)
                WHERE payment_id IS NOT NULL
            """)

            # ── History pagination indexes ──
            # Composite index for cursor-based pagination:
            # WHERE identity_id = X ORDER BY created_at DESC, id DESC
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_history_identity_created
                ON {_APP_SCHEMA}.history_items (identity_id, created_at DESC, id DESC)
            """)
            # Per-type pagination (WHERE identity_id = X AND item_type = Y)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_history_identity_type_created
                ON {_APP_SCHEMA}.history_items (identity_id, item_type, created_at DESC, id DESC)
            """)
            # JSONB expression indexes for PATCH/POST item lookup:
            # WHERE identity_id = X AND payload->>'original_id' = Y
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_history_payload_original_id
                ON {_APP_SCHEMA}.history_items (identity_id, (payload->>'original_id'))
                WHERE payload->>'original_id' IS NOT NULL
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_history_payload_job_id
                ON {_APP_SCHEMA}.history_items (identity_id, (payload->>'job_id'))
                WHERE payload->>'job_id' IS NOT NULL
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_history_payload_original_job_id
                ON {_APP_SCHEMA}.history_items (identity_id, (payload->>'original_job_id'))
                WHERE payload->>'original_job_id' IS NOT NULL
            """)
            # Credit reservations: covers get_all_reserved_credits() query
            # WHERE identity_id = X AND status = 'held' AND expires_at > NOW()
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_reservations_identity_held
                ON {_BILLING_SCHEMA}.credit_reservations (identity_id, status, expires_at)
                WHERE status = 'held'
            """)
            # Indexes on FK-join columns used by history LEFT JOINs.
            # These cover the JOIN conditions: m.upstream_job_id, i.upstream_id, v.upstream_id
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_models_upstream_job_id
                ON {_APP_SCHEMA}.models (upstream_job_id)
                WHERE upstream_job_id IS NOT NULL
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_images_upstream_id
                ON {_APP_SCHEMA}.images (upstream_id)
                WHERE upstream_id IS NOT NULL
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_videos_upstream_id
                ON {_APP_SCHEMA}.videos (upstream_id)
                WHERE upstream_id IS NOT NULL
            """)

        print("[DB] Schema indexes ensured")

        try:
            from backend.services.prompt_safety_service import ensure_safety_schema
            ensure_safety_schema()
        except Exception as e:
            print(f"[DB] Warning: Could not ensure safety schema: {e}")
    except Exception as e:
        print(f"[DB] Warning: Could not ensure schema indexes: {e}")


def _ensure_schema_direct() -> None:
    """Verify critical schema elements using a direct connection (no pool).
    Safe to call in the Gunicorn master before fork."""
    try:
        conn = _create_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_purchases_provider_payment
                    ON timrx_billing.purchases(provider, provider_payment_id)
                """)
                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS purchases_provider_payment_id_ux
                    ON timrx_billing.purchases (provider, payment_id)
                    WHERE payment_id IS NOT NULL
                """)
                # History pagination indexes
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_history_identity_created
                    ON {_APP_SCHEMA}.history_items (identity_id, created_at DESC, id DESC)
                """)
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_history_identity_type_created
                    ON {_APP_SCHEMA}.history_items (identity_id, item_type, created_at DESC, id DESC)
                """)
                # JSONB expression indexes for PATCH/POST item lookup
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_history_payload_original_id
                    ON {_APP_SCHEMA}.history_items (identity_id, (payload->>'original_id'))
                    WHERE payload->>'original_id' IS NOT NULL
                """)
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_history_payload_job_id
                    ON {_APP_SCHEMA}.history_items (identity_id, (payload->>'job_id'))
                    WHERE payload->>'job_id' IS NOT NULL
                """)
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_history_payload_original_job_id
                    ON {_APP_SCHEMA}.history_items (identity_id, (payload->>'original_job_id'))
                    WHERE payload->>'original_job_id' IS NOT NULL
                """)
                # Credit reservations: covers get_all_reserved_credits() query
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_reservations_identity_held
                    ON {_BILLING_SCHEMA}.credit_reservations (identity_id, status, expires_at)
                    WHERE status = 'held'
                """)
                # FK-join indexes for history LEFT JOINs
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_models_upstream_job_id
                    ON {_APP_SCHEMA}.models (upstream_job_id)
                    WHERE upstream_job_id IS NOT NULL
                """)
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_images_upstream_id
                    ON {_APP_SCHEMA}.images (upstream_id)
                    WHERE upstream_id IS NOT NULL
                """)
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_videos_upstream_id
                    ON {_APP_SCHEMA}.videos (upstream_id)
                    WHERE upstream_id IS NOT NULL
                """)
            conn.commit()
            print("[DB] Schema indexes ensured")
        finally:
            conn.close()

        try:
            from backend.services.prompt_safety_service import ensure_safety_schema
            ensure_safety_schema()
        except Exception as e:
            print(f"[DB] Warning: Could not ensure safety schema: {e}")
    except Exception as e:
        print(f"[DB] Warning: Could not ensure schema indexes: {e}")


# ─────────────────────────────────────────────────────────────
# Backwards compatibility aliases
# ─────────────────────────────────────────────────────────────
get_connection = get_conn
get_db = get_conn
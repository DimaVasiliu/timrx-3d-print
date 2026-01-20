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

from config import config


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
USE_DB = bool(config.DATABASE_URL and PSYCOPG_AVAILABLE)

if PSYCOPG_AVAILABLE:
    print(f"[DB] psycopg3 available, DATABASE_URL configured: {config.HAS_DATABASE}, USE_DB: {USE_DB}")
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
def _create_connection():
    """
    Create a new database connection.
    Internal function - raises exceptions on failure.
    """
    if not PSYCOPG_AVAILABLE:
        raise DatabaseNotConfiguredError("psycopg3 is not installed")

    if not config.DATABASE_URL:
        raise DatabaseNotConfiguredError("DATABASE_URL is not set")

    try:
        conn = psycopg.connect(
            config.DATABASE_URL,
            connect_timeout=config.DB_CONNECT_TIMEOUT,
            row_factory=dict_row,  # Default to dict rows
        )
        # Set search path for both schemas
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {config.APP_SCHEMA}, {config.BILLING_SCHEMA}, public;")
        return conn
    except psycopg.OperationalError as e:
        raise DatabaseConnectionError(f"Failed to connect to database: {e}", original_error=e)
    except Exception as e:
        raise DatabaseConnectionError(f"Unexpected error connecting to database: {e}", original_error=e)


@contextmanager
def get_conn():
    """
    Context manager for database connections.
    Connection is NOT auto-committed - caller must commit explicitly or use transaction().

    Raises:
        DatabaseNotConfiguredError: If database is not configured
        DatabaseConnectionError: If connection fails

    Usage:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM users")
                rows = cur.fetchall()
            conn.commit()  # Must commit explicitly!
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
def transaction():
    """
    Context manager for database transactions.
    Automatically commits on success, rolls back on exception.
    Yields a cursor with dict_row factory.

    Raises:
        DatabaseNotConfiguredError: If database is not configured
        DatabaseConnectionError: If connection fails
        DatabaseQueryError: If a query fails
        DatabaseIntegrityError: On constraint violations

    Usage:
        with transaction() as cur:
            cur.execute("INSERT INTO users (name) VALUES (%s) RETURNING *", ("John",))
            user = fetch_one(cur)
            cur.execute("INSERT INTO wallets (user_id) VALUES (%s)", (user["id"],))
        # Auto-committed here if no exception
    """
    conn = _create_connection()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except psycopg.errors.UniqueViolation as e:
        conn.rollback()
        constraint = getattr(e.diag, 'constraint_name', None)
        raise DatabaseIntegrityError(
            f"Unique constraint violation: {e}",
            constraint=constraint,
            original_error=e
        )
    except psycopg.errors.ForeignKeyViolation as e:
        conn.rollback()
        constraint = getattr(e.diag, 'constraint_name', None)
        raise DatabaseIntegrityError(
            f"Foreign key violation: {e}",
            constraint=constraint,
            original_error=e
        )
    except psycopg.errors.CheckViolation as e:
        conn.rollback()
        constraint = getattr(e.diag, 'constraint_name', None)
        raise DatabaseIntegrityError(
            f"Check constraint violation: {e}",
            constraint=constraint,
            original_error=e
        )
    except psycopg.Error as e:
        conn.rollback()
        raise DatabaseQueryError(f"Database error: {e}", original_error=e)
    except Exception as e:
        conn.rollback()
        raise
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
def query_one(sql: str, params: tuple = None) -> Optional[Dict[str, Any]]:
    """
    Execute a query and return one row as dict.
    Opens its own transaction.

    Raises:
        DatabaseError: On any database error

    Usage:
        user = query_one("SELECT * FROM users WHERE id = %s", (user_id,))
    """
    with transaction() as cur:
        cur.execute(sql, params or ())
        return fetch_one(cur)


def query_all(sql: str, params: tuple = None) -> List[Dict[str, Any]]:
    """
    Execute a query and return all rows as list of dicts.
    Opens its own transaction.

    Raises:
        DatabaseError: On any database error

    Usage:
        users = query_all("SELECT * FROM users WHERE active = true")
    """
    with transaction() as cur:
        cur.execute(sql, params or ())
        return fetch_all(cur)


def execute(sql: str, params: tuple = None) -> int:
    """
    Execute a statement and return affected row count.
    Opens its own transaction.

    Raises:
        DatabaseError: On any database error

    Usage:
        count = execute("DELETE FROM sessions WHERE expires_at < %s", (now_utc(),))
        print(f"Deleted {count} expired sessions")
    """
    with transaction() as cur:
        cur.execute(sql, params or ())
        return cur.rowcount


def execute_returning(sql: str, params: tuple = None) -> Optional[Dict[str, Any]]:
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
    with transaction() as cur:
        cur.execute(sql, params or ())
        return fetch_one(cur)


def execute_returning_all(sql: str, params: tuple = None) -> List[Dict[str, Any]]:
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
    with transaction() as cur:
        cur.execute(sql, params or ())
        return fetch_all(cur)


# ─────────────────────────────────────────────────────────────
# Schema-aware Table References
# ─────────────────────────────────────────────────────────────
class Tables:
    """Table name constants with schema prefixes."""
    # Billing schema
    IDENTITIES = f"{config.BILLING_SCHEMA}.identities"
    SESSIONS = f"{config.BILLING_SCHEMA}.sessions"
    MAGIC_CODES = f"{config.BILLING_SCHEMA}.magic_codes"
    WALLETS = f"{config.BILLING_SCHEMA}.wallets"
    LEDGER_ENTRIES = f"{config.BILLING_SCHEMA}.ledger_entries"
    ACTION_COSTS = f"{config.BILLING_SCHEMA}.action_costs"
    CREDIT_RESERVATIONS = f"{config.BILLING_SCHEMA}.credit_reservations"
    PURCHASES = f"{config.BILLING_SCHEMA}.purchases"
    PLANS = f"{config.BILLING_SCHEMA}.plans"
    JOBS = f"{config.BILLING_SCHEMA}.jobs"
    DAILY_LIMITS = f"{config.BILLING_SCHEMA}.daily_limits"

    # App schema
    MODELS = f"{config.APP_SCHEMA}.models"
    IMAGES = f"{config.APP_SCHEMA}.images"
    HISTORY_ITEMS = f"{config.APP_SCHEMA}.history_items"
    ACTIVE_JOBS = f"{config.APP_SCHEMA}.active_jobs"
    ACTIVITY_LOGS = f"{config.APP_SCHEMA}.activity_logs"


# ─────────────────────────────────────────────────────────────
# Utility Functions
# ─────────────────────────────────────────────────────────────
def hash_string(value: str) -> str:
    """Hash a string using SHA256 (for IP addresses, user agents, etc.)."""
    return hashlib.sha256(value.encode()).hexdigest()


def is_available() -> bool:
    """Check if database is configured and available."""
    return USE_DB


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
    if not config.HAS_DATABASE:
        print("[DB] DATABASE_URL not set - running without database")
        return False

    if not PSYCOPG_AVAILABLE:
        print("[DB] psycopg3 not installed - running without database")
        return False

    # Attempt connection
    try:
        if verify_connection():
            print("[DB] Database connection verified successfully")
            # Ensure schema indexes exist for idempotency
            ensure_schema()
            return True
        else:
            raise DatabaseConnectionError("Connection test query failed")
    except DatabaseError as e:
        print(f"[DB] ERROR: {e}")
        raise


def ensure_schema() -> None:
    """
    Ensure critical schema elements exist.
    Creates indexes needed for idempotency if they don't exist.
    Called at app startup after connection is verified.
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
    except Exception as e:
        # Log but don't fail startup - indexes may already exist or DB user may lack permissions
        print(f"[DB] Warning: Could not ensure schema indexes: {e}")


# ─────────────────────────────────────────────────────────────
# Batch Operations
# ─────────────────────────────────────────────────────────────
def execute_many(sql: str, params_list: List[tuple]) -> int:
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

    with transaction() as cur:
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

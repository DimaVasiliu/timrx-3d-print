"""
Configuration module for TimrX Backend.
Centralizes all environment variables and settings.

Render Compatibility:
- Handles Render's DATABASE_URL format (postgres:// -> postgresql://)
- Uses Render's PORT env var
- Supports Render's internal service URLs

Usage:
    from backend.config import config

    if config.IS_DEV:
        print("Running in development mode")

    conn_str = config.DATABASE_URL
"""

import os
import re
from pathlib import Path
from typing import Optional, List
from dataclasses import dataclass, field
from dotenv import load_dotenv

# Load .env file (safe - won't override existing env vars)
load_dotenv()


def _get_env(key: str, default: str = "") -> str:
    """Safely get and strip an environment variable."""
    return os.getenv(key, default).strip()


def _get_env_bool(key: str, default: bool = False) -> bool:
    """Get an environment variable as boolean."""
    val = _get_env(key, "").lower()
    if val in ("true", "1", "yes", "on"):
        return True
    if val in ("false", "0", "no", "off"):
        return False
    return default


def _get_env_int(key: str, default: int = 0) -> int:
    """Get an environment variable as integer."""
    try:
        return int(_get_env(key, str(default)))
    except ValueError:
        return default


def _get_env_list(key: str, default: List[str] = None) -> List[str]:
    """Get a comma-separated environment variable as list."""
    val = _get_env(key, "")
    if not val:
        return default or []
    return [item.strip() for item in val.split(",") if item.strip()]


def _fix_render_database_url(url: str) -> str:
    """
    Fix Render's DATABASE_URL format.
    Render uses 'postgres://' but psycopg3 requires 'postgresql://'.
    """
    if url and url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


@dataclass
class Config:
    """
    Application configuration with all settings.
    Loaded from environment variables with sensible defaults.
    """

    # ─────────────────────────────────────────────────────────────
    # Paths
    # ─────────────────────────────────────────────────────────────
    APP_DIR: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent)

    @property
    def ROOT_DIR(self) -> Path:
        """Backend/ directory."""
        return self.APP_DIR.parent

    @property
    def CACHE_DIR(self) -> Path:
        """Cache directory for images."""
        cache = self.APP_DIR / "cache_images"
        cache.mkdir(exist_ok=True)
        return cache

    # ─────────────────────────────────────────────────────────────
    # Environment
    # ─────────────────────────────────────────────────────────────
    FLASK_ENV: str = field(default_factory=lambda: _get_env("FLASK_ENV", "production").lower())

    @property
    def IS_DEV(self) -> bool:
        """True if running in development mode."""
        return self.FLASK_ENV in ("development", "dev", "local")

    @property
    def IS_PROD(self) -> bool:
        """True if running in production mode."""
        return not self.IS_DEV

    @property
    def IS_RENDER(self) -> bool:
        """True if running on Render.com."""
        return bool(_get_env("RENDER"))

    # ─────────────────────────────────────────────────────────────
    # Server
    # ─────────────────────────────────────────────────────────────
    PORT: int = field(default_factory=lambda: _get_env_int("PORT", 5001))
    HOST: str = field(default_factory=lambda: _get_env("HOST", "0.0.0.0"))

    # ─────────────────────────────────────────────────────────────
    # Database
    # ─────────────────────────────────────────────────────────────
    _DATABASE_URL_RAW: str = field(default_factory=lambda: _get_env("DATABASE_URL"))

    @property
    def DATABASE_URL(self) -> str:
        """Database connection URL (fixed for psycopg3 compatibility)."""
        return _fix_render_database_url(self._DATABASE_URL_RAW)

    @property
    def HAS_DATABASE(self) -> bool:
        """True if database URL is configured."""
        return bool(self._DATABASE_URL_RAW)

    # Schema names (constants)
    BILLING_SCHEMA: str = "timrx_billing"
    APP_SCHEMA: str = "timrx_app"

    # Connection settings
    DB_CONNECT_TIMEOUT: int = field(default_factory=lambda: _get_env_int("DB_CONNECT_TIMEOUT", 5))
    DB_POOL_SIZE: int = field(default_factory=lambda: _get_env_int("DB_POOL_SIZE", 5))

    # ─────────────────────────────────────────────────────────────
    # Session & Auth
    # ─────────────────────────────────────────────────────────────
    SESSION_COOKIE_NAME: str = "timrx_sid"
    SESSION_COOKIE_SAMESITE: str = "Lax"
    SESSION_COOKIE_HTTPONLY: bool = True
    SESSION_COOKIE_PATH: str = "/"

    # Single TTL for both DB session expiry AND cookie max-age (must match!)
    SESSION_TTL_DAYS: int = field(default_factory=lambda: _get_env_int("SESSION_TTL_DAYS", 30))

    @property
    def SESSION_COOKIE_SECURE(self) -> bool:
        """Secure cookie: True in production, False in dev."""
        return self.IS_PROD

    @property
    def SESSION_TTL_SECONDS(self) -> int:
        """Session TTL in seconds (used for both DB expiry and cookie max-age)."""
        return self.SESSION_TTL_DAYS * 24 * 60 * 60

    # Legacy alias for backward compatibility
    @property
    def SESSION_MAX_AGE_DAYS(self) -> int:
        """Deprecated: Use SESSION_TTL_DAYS instead."""
        return self.SESSION_TTL_DAYS

    @property
    def SESSION_MAX_AGE_SECONDS(self) -> int:
        """Deprecated: Use SESSION_TTL_SECONDS instead."""
        return self.SESSION_TTL_SECONDS

    # Magic code settings
    MAGIC_CODE_EXPIRY_MINUTES: int = field(default_factory=lambda: _get_env_int("MAGIC_CODE_EXPIRY_MINUTES", 15))
    MAGIC_CODE_MAX_ATTEMPTS: int = field(default_factory=lambda: _get_env_int("MAGIC_CODE_MAX_ATTEMPTS", 5))
    MAGIC_CODE_COOLDOWN_SECONDS: int = field(default_factory=lambda: _get_env_int("MAGIC_CODE_COOLDOWN_SECONDS", 60))

    # ─────────────────────────────────────────────────────────────
    # Email Configuration
    # ─────────────────────────────────────────────────────────────
    # Master switch - if false, emails are logged only
    EMAIL_ENABLED: bool = field(default_factory=lambda: _get_env_bool("EMAIL_ENABLED", True))

    # Provider: "neo", "ses", "sendgrid" (default: neo for Neo SMTP)
    EMAIL_PROVIDER: str = field(default_factory=lambda: _get_env("EMAIL_PROVIDER", "neo").lower())

    # SendGrid API (legacy support)
    SENDGRID_API_KEY: str = field(default_factory=lambda: _get_env("SENDGRID_API_KEY"))

    # SMTP configuration
    SMTP_HOST: str = field(default_factory=lambda: _get_env("SMTP_HOST"))
    SMTP_PORT: int = field(default_factory=lambda: _get_env_int("SMTP_PORT", 587))
    SMTP_USER: str = field(default_factory=lambda: _get_env("SMTP_USER"))
    SMTP_PASSWORD: str = field(default_factory=lambda: _get_env("SMTP_PASSWORD"))
    SMTP_USE_TLS: bool = field(default_factory=lambda: _get_env_bool("SMTP_USE_TLS", True))
    SMTP_TIMEOUT: int = field(default_factory=lambda: _get_env_int("SMTP_TIMEOUT", 10))

    # From address (can be "Name <email>" format via SMTP_FROM, or separate)
    _SMTP_FROM_RAW: str = field(default_factory=lambda: _get_env("SMTP_FROM"))
    EMAIL_FROM_ADDRESS: str = field(default_factory=lambda: _get_env("EMAIL_FROM_ADDRESS", "noreply@timrx.app"))
    EMAIL_FROM_NAME: str = field(default_factory=lambda: _get_env("EMAIL_FROM_NAME", "TimrX"))

    @property
    def SMTP_FROM_PARSED(self) -> tuple:
        """Parse SMTP_FROM into (name, address) tuple."""
        raw = self._SMTP_FROM_RAW
        if raw and "<" in raw and ">" in raw:
            # Format: "Name <email@domain.com>"
            import re
            match = re.match(r"^(.+?)\s*<(.+?)>$", raw.strip())
            if match:
                return (match.group(1).strip(), match.group(2).strip())
        if raw and "@" in raw:
            return (self.EMAIL_FROM_NAME, raw.strip())
        return (self.EMAIL_FROM_NAME, self.EMAIL_FROM_ADDRESS)

    @property
    def EMAIL_CONFIGURED(self) -> bool:
        """True if email sending is properly configured."""
        if not self.EMAIL_ENABLED:
            return False
        return bool(self.SMTP_HOST and self.SMTP_USER and self.SMTP_PASSWORD)

    # ─────────────────────────────────────────────────────────────
    # Admin
    # ─────────────────────────────────────────────────────────────
    ADMIN_EMAIL: str = field(default_factory=lambda: _get_env("ADMIN_EMAIL"))

    # Admin authentication (for /api/admin/* endpoints)
    # Option 1: Token-based (X-Admin-Token header) - for existing admin dashboard
    ADMIN_TOKEN: str = field(default_factory=lambda: _get_env("ADMIN_TOKEN"))

    # Option 2: Email-based (comma-separated list of allowed admin emails)
    ADMIN_EMAILS: List[str] = field(default_factory=lambda: _get_env_list("ADMIN_EMAILS"))

    @property
    def ADMIN_AUTH_CONFIGURED(self) -> bool:
        """True if admin authentication is configured."""
        return bool(self.ADMIN_TOKEN or self.ADMIN_EMAILS)

    def is_admin_email(self, email: str) -> bool:
        """Check if email is in the admin list."""
        if not email or not self.ADMIN_EMAILS:
            return False
        return email.lower().strip() in [e.lower() for e in self.ADMIN_EMAILS]

    # Admin notification toggles
    NOTIFY_ON_NEW_IDENTITY: bool = field(default_factory=lambda: _get_env_bool("NOTIFY_ON_NEW_IDENTITY", False))
    NOTIFY_ON_PURCHASE: bool = field(default_factory=lambda: _get_env_bool("NOTIFY_ON_PURCHASE", True))
    NOTIFY_ON_RESTORE_REQUEST: bool = field(default_factory=lambda: _get_env_bool("NOTIFY_ON_RESTORE_REQUEST", False))

    # ─────────────────────────────────────────────────────────────
    # Stripe
    # ─────────────────────────────────────────────────────────────
    STRIPE_SECRET_KEY: str = field(default_factory=lambda: _get_env("STRIPE_SECRET_KEY"))
    STRIPE_PUBLISHABLE_KEY: str = field(default_factory=lambda: _get_env("STRIPE_PUBLISHABLE_KEY"))
    STRIPE_WEBHOOK_SECRET: str = field(default_factory=lambda: _get_env("STRIPE_WEBHOOK_SECRET"))

    @property
    def STRIPE_CONFIGURED(self) -> bool:
        """True if Stripe is configured."""
        return bool(self.STRIPE_SECRET_KEY)

    @property
    def STRIPE_MODE(self) -> str:
        """Returns 'live' or 'test' based on key prefix."""
        if self.STRIPE_SECRET_KEY.startswith("sk_live_"):
            return "live"
        return "test"

    # ─────────────────────────────────────────────────────────────
    # Mollie
    # ─────────────────────────────────────────────────────────────
    MOLLIE_API_KEY: str = field(default_factory=lambda: _get_env("MOLLIE_API_KEY"))
    MOLLIE_PROFILE_ID: str = field(default_factory=lambda: _get_env("MOLLIE_PROFILE_ID"))
    MOLLIE_ENV: str = field(default_factory=lambda: _get_env("MOLLIE_ENV", "test").lower())
    PUBLIC_BASE_URL: str = field(default_factory=lambda: _get_env("PUBLIC_BASE_URL"))

    @property
    def MOLLIE_CONFIGURED(self) -> bool:
        """True if Mollie is configured."""
        return bool(self.MOLLIE_API_KEY)

    @property
    def MOLLIE_MODE(self) -> str:
        """Returns 'live' or 'test' based on MOLLIE_ENV or key prefix."""
        if self.MOLLIE_ENV == "live":
            return "live"
        if self.MOLLIE_API_KEY.startswith("live_"):
            return "live"
        return "test"

    # ─────────────────────────────────────────────────────────────
    # Credits System
    # ─────────────────────────────────────────────────────────────
    RESERVATION_EXPIRY_MINUTES: int = field(default_factory=lambda: _get_env_int("RESERVATION_EXPIRY_MINUTES", 20))
    FREE_CREDITS_ON_SIGNUP: int = field(default_factory=lambda: _get_env_int("FREE_CREDITS_ON_SIGNUP", 0))

    # ─────────────────────────────────────────────────────────────
    # AWS S3
    # ─────────────────────────────────────────────────────────────
    AWS_REGION: str = field(default_factory=lambda: _get_env("AWS_REGION", "eu-west-2"))
    AWS_BUCKET_MODELS: str = field(default_factory=lambda: _get_env("AWS_BUCKET_MODELS"))
    AWS_ACCESS_KEY_ID: str = field(default_factory=lambda: _get_env("AWS_ACCESS_KEY_ID"))
    AWS_SECRET_ACCESS_KEY: str = field(default_factory=lambda: _get_env("AWS_SECRET_ACCESS_KEY"))

    @property
    def AWS_CONFIGURED(self) -> bool:
        """True if AWS S3 is configured."""
        return bool(self.AWS_BUCKET_MODELS and self.AWS_ACCESS_KEY_ID and self.AWS_SECRET_ACCESS_KEY)

    # ─────────────────────────────────────────────────────────────
    # CORS
    # ─────────────────────────────────────────────────────────────
    _ALLOWED_ORIGINS_RAW: str = field(default_factory=lambda: _get_env("ALLOWED_ORIGINS"))

    @property
    def ALLOWED_ORIGINS(self) -> List[str]:
        """List of allowed CORS origins."""
        if self._ALLOWED_ORIGINS_RAW == "*":
            return ["*"]
        if self._ALLOWED_ORIGINS_RAW:
            return [o.strip() for o in self._ALLOWED_ORIGINS_RAW.split(",") if o.strip()]
        # Dev defaults
        if self.IS_DEV:
            return [
                "http://localhost:3000",
                "http://localhost:3001",
                "http://localhost:5173",
                "http://localhost:8080",
                "http://127.0.0.1:3000",
                "http://127.0.0.1:5173",
            ]
        return []

    @property
    def ALLOW_ALL_ORIGINS(self) -> bool:
        """True if wildcard CORS is enabled."""
        return self._ALLOWED_ORIGINS_RAW == "*"

    # ─────────────────────────────────────────────────────────────
    # External APIs
    # ─────────────────────────────────────────────────────────────
    MESHY_API_KEY: str = field(default_factory=lambda: _get_env("MESHY_API_KEY"))
    OPENAI_API_KEY: str = field(default_factory=lambda: _get_env("OPENAI_API_KEY"))

    # ─────────────────────────────────────────────────────────────
    # Feature Flags
    # ─────────────────────────────────────────────────────────────
    REQUIRE_EMAIL_FOR_GENERATION: bool = field(
        default_factory=lambda: _get_env_bool("REQUIRE_EMAIL_FOR_GENERATION", False)
    )
    REQUIRE_AWS_UPLOADS: bool = field(
        default_factory=lambda: _get_env_bool("REQUIRE_AWS_UPLOADS", False)
    )

    # ─────────────────────────────────────────────────────────────
    # Logging & Debug
    # ─────────────────────────────────────────────────────────────
    def log_summary(self) -> None:
        """Print configuration summary for debugging."""
        print("=" * 60)
        print("[CONFIG] TimrX Backend Configuration")
        print("=" * 60)
        print(f"  Environment: {self.FLASK_ENV} (IS_DEV={self.IS_DEV})")
        print(f"  Running on Render: {self.IS_RENDER}")
        print(f"  Port: {self.PORT}")
        print("-" * 60)
        print(f"  Database configured: {self.HAS_DATABASE}")
        print(f"  Email configured: {self.EMAIL_CONFIGURED}")
        print(f"  Stripe configured: {self.STRIPE_CONFIGURED} ({self.STRIPE_MODE if self.STRIPE_CONFIGURED else 'N/A'})")
        print(f"  Mollie configured: {self.MOLLIE_CONFIGURED} ({self.MOLLIE_MODE if self.MOLLIE_CONFIGURED else 'N/A'})")
        print(f"  AWS S3 configured: {self.AWS_CONFIGURED}")
        print("-" * 60)
        print(f"  Admin email: {self.ADMIN_EMAIL or '(not set)'}")
        print(f"  Notify on purchase: {self.NOTIFY_ON_PURCHASE}")
        print(f"  Free credits on signup: {self.FREE_CREDITS_ON_SIGNUP}")
        print("=" * 60)

    def validate(self) -> List[str]:
        """
        Validate configuration and return list of warnings.
        Returns empty list if all critical config is present.
        """
        warnings = []

        if self.IS_PROD:
            if not self.HAS_DATABASE:
                warnings.append("DATABASE_URL not set - running without persistence!")
            if not self.EMAIL_CONFIGURED:
                warnings.append("Email not configured - magic codes and receipts won't send")
            if not self.STRIPE_CONFIGURED and not self.MOLLIE_CONFIGURED:
                warnings.append("No payment provider configured (Stripe/Mollie) - purchases disabled")
            if not self.ALLOWED_ORIGINS:
                warnings.append("ALLOWED_ORIGINS not set - CORS will block requests")
            if self.ALLOW_ALL_ORIGINS:
                warnings.append("ALLOWED_ORIGINS=* - allowing all origins (not recommended for production)")

        return warnings

    def to_dict(self) -> dict:
        """Export safe configuration as dictionary (no secrets)."""
        return {
            "environment": self.FLASK_ENV,
            "is_dev": self.IS_DEV,
            "is_render": self.IS_RENDER,
            "port": self.PORT,
            "has_database": self.HAS_DATABASE,
            "email_configured": self.EMAIL_CONFIGURED,
            "stripe_configured": self.STRIPE_CONFIGURED,
            "stripe_mode": self.STRIPE_MODE if self.STRIPE_CONFIGURED else None,
            "mollie_configured": self.MOLLIE_CONFIGURED,
            "mollie_mode": self.MOLLIE_MODE if self.MOLLIE_CONFIGURED else None,
            "aws_configured": self.AWS_CONFIGURED,
            "free_credits_on_signup": self.FREE_CREDITS_ON_SIGNUP,
        }


# ─────────────────────────────────────────────────────────────
# Singleton instance
# ─────────────────────────────────────────────────────────────
config = Config()

# ─────────────────────────────────────────────────────────────
# Backward compatibility exports (for existing code)
# ─────────────────────────────────────────────────────────────
APP_DIR = config.APP_DIR
ROOT_DIR = config.ROOT_DIR
FLASK_ENV = config.FLASK_ENV
IS_DEV = config.IS_DEV
IS_PROD = config.IS_PROD
DATABASE_URL = config.DATABASE_URL
BILLING_SCHEMA = config.BILLING_SCHEMA
APP_SCHEMA = config.APP_SCHEMA
SESSION_COOKIE_NAME = config.SESSION_COOKIE_NAME
SESSION_TTL_DAYS = config.SESSION_TTL_DAYS
SESSION_TTL_SECONDS = config.SESSION_TTL_SECONDS
SESSION_MAX_AGE_DAYS = config.SESSION_MAX_AGE_DAYS  # Deprecated alias
MAGIC_CODE_EXPIRY_MINUTES = config.MAGIC_CODE_EXPIRY_MINUTES
MAGIC_CODE_MAX_ATTEMPTS = config.MAGIC_CODE_MAX_ATTEMPTS
SENDGRID_API_KEY = config.SENDGRID_API_KEY
SMTP_HOST = config.SMTP_HOST
SMTP_PORT = config.SMTP_PORT
SMTP_USER = config.SMTP_USER
SMTP_PASSWORD = config.SMTP_PASSWORD
EMAIL_FROM_ADDRESS = config.EMAIL_FROM_ADDRESS
EMAIL_FROM_NAME = config.EMAIL_FROM_NAME
ADMIN_EMAIL = config.ADMIN_EMAIL
ADMIN_TOKEN = config.ADMIN_TOKEN
ADMIN_EMAILS = config.ADMIN_EMAILS
NOTIFY_ON_NEW_IDENTITY = config.NOTIFY_ON_NEW_IDENTITY
NOTIFY_ON_PURCHASE = config.NOTIFY_ON_PURCHASE
NOTIFY_ON_RESTORE_REQUEST = config.NOTIFY_ON_RESTORE_REQUEST
STRIPE_SECRET_KEY = config.STRIPE_SECRET_KEY
STRIPE_PUBLISHABLE_KEY = config.STRIPE_PUBLISHABLE_KEY
STRIPE_WEBHOOK_SECRET = config.STRIPE_WEBHOOK_SECRET
MOLLIE_API_KEY = config.MOLLIE_API_KEY
MOLLIE_PROFILE_ID = config.MOLLIE_PROFILE_ID
MOLLIE_ENV = config.MOLLIE_ENV
PUBLIC_BASE_URL = config.PUBLIC_BASE_URL
RESERVATION_EXPIRY_MINUTES = config.RESERVATION_EXPIRY_MINUTES
FREE_CREDITS_ON_SIGNUP = config.FREE_CREDITS_ON_SIGNUP
AWS_REGION = config.AWS_REGION
AWS_BUCKET_MODELS = config.AWS_BUCKET_MODELS
AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
ALLOWED_ORIGINS = config._ALLOWED_ORIGINS_RAW  # Raw string for existing code


def log_config():
    """Legacy function - calls config.log_summary()."""
    config.log_summary()

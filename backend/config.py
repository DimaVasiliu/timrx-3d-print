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
        """
        True if running in development mode.
        Auto-detects local development when FLASK_ENV is not explicitly set.
        """
        # Explicit env var takes precedence
        if self.FLASK_ENV in ("development", "dev", "local"):
            return True
        # If FLASK_ENV is explicitly set to production, respect it
        if _get_env("FLASK_ENV"):
            return False
        # Auto-detect: if FLASK_ENV not set and not on Render, assume dev mode
        # This allows local testing without setting FLASK_ENV=development
        if not self.IS_RENDER:
            return True
        return False

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
    SESSION_COOKIE_HTTPONLY: bool = True
    SESSION_COOKIE_PATH: str = "/"

    # Cookie domain for cross-subdomain sharing (e.g., ".timrx.live")
    # Set via SESSION_COOKIE_DOMAIN env var in production
    # Leading dot allows subdomains (timrx.live + 3d.timrx.live + www.timrx.live)
    _SESSION_COOKIE_DOMAIN_RAW: str = field(default_factory=lambda: _get_env("SESSION_COOKIE_DOMAIN"))

    @property
    def SESSION_COOKIE_DOMAIN(self) -> Optional[str]:
        """
        Cookie domain for cross-subdomain sharing.
        Returns None in dev (localhost doesn't support domain cookies).
        In production, should be ".timrx.live" to share across subdomains.
        """
        if self._SESSION_COOKIE_DOMAIN_RAW:
            return self._SESSION_COOKIE_DOMAIN_RAW
        # In production without explicit domain, default to .timrx.live
        if self.IS_PROD:
            return ".timrx.live"
        # In dev, return None (let browser default to current host)
        return None

    @property
    def SESSION_COOKIE_SAMESITE(self) -> str:
        """
        SameSite policy for session cookie.
        - "Lax": Default, works for same-site navigation
        - "None": Required for cross-site requests (requires Secure=True)

        We use "None" in production for cross-subdomain requests
        (timrx.live -> 3d.timrx.live) to ensure cookies are sent.
        """
        # Use "None" in production for cross-origin subdomain requests
        # SameSite=None requires Secure=True (HTTPS)
        if self.IS_PROD:
            return "None"
        # In dev, use "Lax" since we're on localhost (no HTTPS)
        return "Lax"

    # Single TTL for both DB session expiry AND cookie max-age (must match!)
    SESSION_TTL_DAYS: int = field(default_factory=lambda: _get_env_int("SESSION_TTL_DAYS", 30))

    @property
    def SESSION_COOKIE_SECURE(self) -> bool:
        """Secure cookie: True in production (HTTPS), False in dev (HTTP)."""
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

    # AWS SES configuration (uses AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY from S3 config)
    SES_FROM_EMAIL: str = field(default_factory=lambda: _get_env("SES_FROM_EMAIL"))

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
        # SES uses AWS credentials
        if self.EMAIL_PROVIDER == "ses":
            return bool(
                self.AWS_ACCESS_KEY_ID and
                self.AWS_SECRET_ACCESS_KEY and
                (self.SES_FROM_EMAIL or self.EMAIL_FROM_ADDRESS)
            )
        # SMTP providers (neo, sendgrid, etc.)
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
    # Payment Provider Selection
    # ─────────────────────────────────────────────────────────────
    # PAYMENTS_PROVIDER controls which payment provider to use
    # Options: "mollie" (default), "stripe", "both"
    # If set to "mollie", Stripe will be completely disabled (no init, no warnings, no endpoints)
    PAYMENTS_PROVIDER: str = field(default_factory=lambda: _get_env("PAYMENTS_PROVIDER", "mollie").lower())

    @property
    def USE_STRIPE(self) -> bool:
        """True if Stripe should be used (provider is 'stripe' or 'both')."""
        return self.PAYMENTS_PROVIDER in ("stripe", "both")

    @property
    def USE_MOLLIE(self) -> bool:
        """True if Mollie should be used (provider is 'mollie' or 'both')."""
        return self.PAYMENTS_PROVIDER in ("mollie", "both")

    # ─────────────────────────────────────────────────────────────
    # Stripe
    # ─────────────────────────────────────────────────────────────
    STRIPE_SECRET_KEY: str = field(default_factory=lambda: _get_env("STRIPE_SECRET_KEY"))
    STRIPE_PUBLISHABLE_KEY: str = field(default_factory=lambda: _get_env("STRIPE_PUBLISHABLE_KEY"))
    STRIPE_WEBHOOK_SECRET: str = field(default_factory=lambda: _get_env("STRIPE_WEBHOOK_SECRET"))

    @property
    def STRIPE_CONFIGURED(self) -> bool:
        """True if Stripe is configured AND enabled via PAYMENTS_PROVIDER."""
        # Only consider Stripe configured if the provider flag allows it
        if not self.USE_STRIPE:
            return False
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

    # PUBLIC_BASE_URL: Backend API URL (for webhooks) - e.g., https://3d.timrx.live
    PUBLIC_BASE_URL: str = field(default_factory=lambda: _get_env("PUBLIC_BASE_URL"))

    # FRONTEND_BASE_URL: Frontend site URL (for redirects) - e.g., https://timrx.live
    # If not set, falls back to PUBLIC_BASE_URL for backward compatibility
    FRONTEND_BASE_URL: str = field(default_factory=lambda: _get_env("FRONTEND_BASE_URL"))

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
        """
        List of allowed CORS origins.
        Parses comma-separated URLs, sanitizes common misconfigurations.
        """
        raw = self._ALLOWED_ORIGINS_RAW

        if not raw:
            # Dev defaults
            if self.IS_DEV:
                return [
                    "http://localhost:3000",
                    "http://localhost:3001",
                    "http://localhost:5173",
                    "http://localhost:5500",
                    "http://localhost:5503",
                    "http://localhost:8080",
                    "http://127.0.0.1:3000",
                    "http://127.0.0.1:5173",
                    "http://127.0.0.1:5500",
                    "http://127.0.0.1:5503",
                ]
            return []

        if raw == "*":
            return ["*"]

        # Parse comma-separated list with sanitization
        origins = []
        for part in raw.split(","):
            origin = part.strip()
            if not origin:
                continue

            # Strip accidental "ALLOWED_ORIGINS=" prefix (common misconfiguration)
            if origin.upper().startswith("ALLOWED_ORIGINS="):
                origin = origin[len("ALLOWED_ORIGINS="):]

            # Only accept valid http/https URLs
            if origin.startswith("http://") or origin.startswith("https://"):
                origins.append(origin)

        return origins

    @property
    def ALLOW_ALL_ORIGINS(self) -> bool:
        """True if wildcard CORS is enabled."""
        return self._ALLOWED_ORIGINS_RAW == "*"

    # ─────────────────────────────────────────────────────────────
    # External APIs
    # ─────────────────────────────────────────────────────────────
    MESHY_API_KEY: str = field(default_factory=lambda: _get_env("MESHY_API_KEY"))
    MESHY_API_BASE: str = field(default_factory=lambda: _get_env("MESHY_API_BASE", "https://api.meshy.ai").rstrip("/"))
    OPENAI_API_KEY: str = field(default_factory=lambda: _get_env("OPENAI_API_KEY"))
    # GEMINI_API_KEY with fallback to GOOGLE_API_KEY for backward compatibility
    GEMINI_API_KEY: str = field(default_factory=lambda: _get_env("GEMINI_API_KEY") or _get_env("GOOGLE_API_KEY"))

    # ─────────────────────────────────────────────────────────────
    # Vertex AI (Veo) Video Generation
    # ─────────────────────────────────────────────────────────────
    # Provider selection: "vertex" (production) or "aistudio" (fallback)
    VIDEO_PROVIDER: str = field(default_factory=lambda: _get_env("VIDEO_PROVIDER", "vertex").lower())

    # Google Cloud project and credentials
    GOOGLE_CLOUD_PROJECT: str = field(default_factory=lambda: _get_env("GOOGLE_CLOUD_PROJECT"))
    GOOGLE_CLOUD_REGION: str = field(default_factory=lambda: _get_env("GOOGLE_CLOUD_REGION", "europe-west2"))

    # Service account credentials JSON (full JSON string for Render deployments)
    GOOGLE_APPLICATION_CREDENTIALS_JSON: str = field(default_factory=lambda: _get_env("GOOGLE_APPLICATION_CREDENTIALS_JSON"))

    # Vertex AI location - MUST be us-central1 for Veo quota
    VERTEX_LOCATION: str = field(default_factory=lambda: _get_env("VERTEX_LOCATION", "us-central1"))

    # Vertex AI Veo models
    VERTEX_MODEL_FAST: str = field(default_factory=lambda: _get_env("VERTEX_MODEL_FAST", "veo-3.1-fast-generate-001"))
    VERTEX_MODEL_HQ: str = field(default_factory=lambda: _get_env("VERTEX_MODEL_HQ", "veo-3.1-generate-001"))

    # Video quality: "fast" or "hq" (determines which Veo model to use)
    VIDEO_QUALITY: str = field(default_factory=lambda: _get_env("VIDEO_QUALITY", "fast").lower())

    @property
    def USE_VERTEX_VIDEO(self) -> bool:
        """True if Vertex AI should be used for video generation."""
        return self.VIDEO_PROVIDER == "vertex"

    @property
    def USE_AISTUDIO_VIDEO(self) -> bool:
        """True if AI Studio should be used for video generation."""
        return self.VIDEO_PROVIDER == "aistudio"

    @property
    def VERTEX_VEO_MODEL(self) -> str:
        """Get the appropriate Veo model based on VIDEO_QUALITY setting."""
        if self.VIDEO_QUALITY == "hq":
            return self.VERTEX_MODEL_HQ
        return self.VERTEX_MODEL_FAST

    @property
    def VERTEX_CONFIGURED(self) -> bool:
        """True if Vertex AI is configured for video generation."""
        return bool(
            self.GOOGLE_CLOUD_PROJECT and
            (self.GOOGLE_APPLICATION_CREDENTIALS_JSON or _get_env("GOOGLE_APPLICATION_CREDENTIALS"))
        )

    # ─────────────────────────────────────────────────────────────
    # Generation Defaults & Action Keys
    # ─────────────────────────────────────────────────────────────
    DEFAULT_MODEL_TITLE: str = "3D Model"
    # Maps route-level action names to CANONICAL action keys
    # CANONICAL KEYS: image_generate, text_to_3d_generate, image_to_3d_generate,
    #                 refine, remesh, retexture,
    #                 video_generate, video_text_generate, video_image_animate
    ACTION_KEYS: dict = field(
        default_factory=lambda: {
            # Routes use these keys to look up what to pass to start_paid_job
            "text-to-3d-preview": "text_to_3d_generate",  # Canonical
            "text-to-3d-refine": "refine",                # Canonical
            "image-to-3d": "image_to_3d_generate",        # Canonical
            "remesh": "remesh",                           # Already canonical
            "retexture": "retexture",                     # Already canonical
            "openai-image": "image_generate",             # Canonical
            # Additional canonical mappings
            "image_generate": "image_generate",
            "refine": "refine",
            "video_generate": "video_generate",
            "video_text_generate": "video_text_generate",
            "video_image_animate": "video_image_animate",
        }
    )

    # ─────────────────────────────────────────────────────────────
    # Proxy Hosts
    # ─────────────────────────────────────────────────────────────
    @property
    def DEFAULT_PROXY_HOSTS(self) -> set[str]:
        hosts = {"assets.meshy.ai"}
        if self.AWS_BUCKET_MODELS:
            hosts.add(f"{self.AWS_BUCKET_MODELS}.s3.{self.AWS_REGION}.amazonaws.com")
        return hosts

    @property
    def PROXY_ALLOWED_HOSTS(self) -> set[str]:
        env_hosts = {
            h.strip().lower()
            for h in os.getenv("PROXY_ALLOWED_HOSTS", "").split(",")
            if h.strip()
        }
        return env_hosts or self.DEFAULT_PROXY_HOSTS

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
        dev_note = " (auto-detected)" if self.IS_DEV and not _get_env("FLASK_ENV") else ""
        print(f"  Environment: {self.FLASK_ENV} (IS_DEV={self.IS_DEV}{dev_note})")
        print(f"  Running on Render: {self.IS_RENDER}")
        print(f"  Port: {self.PORT}")
        print("-" * 60)
        print(f"  Database configured: {self.HAS_DATABASE}")
        print(f"  Email configured: {self.EMAIL_CONFIGURED}")
        print(f"  Payment provider: {self.PAYMENTS_PROVIDER}")
        if self.USE_STRIPE:
            print(f"  Stripe configured: {self.STRIPE_CONFIGURED} ({self.STRIPE_MODE if self.STRIPE_CONFIGURED else 'N/A'})")
        else:
            print(f"  Stripe: disabled (PAYMENTS_PROVIDER={self.PAYMENTS_PROVIDER})")
        if self.USE_MOLLIE:
            print(f"  Mollie configured: {self.MOLLIE_CONFIGURED} ({self.MOLLIE_MODE if self.MOLLIE_CONFIGURED else 'N/A'})")
        else:
            print(f"  Mollie: disabled (PAYMENTS_PROVIDER={self.PAYMENTS_PROVIDER})")
        print(f"  AWS S3 configured: {self.AWS_CONFIGURED}")
        print("-" * 60)
        print("[CONFIG] Session Cookie Settings:")
        print(f"  Cookie name: {self.SESSION_COOKIE_NAME}")
        print(f"  Cookie domain: {self.SESSION_COOKIE_DOMAIN!r}")
        print(f"  Cookie secure: {self.SESSION_COOKIE_SECURE}")
        print(f"  Cookie samesite: {self.SESSION_COOKIE_SAMESITE}")
        print(f"  Cookie httponly: {self.SESSION_COOKIE_HTTPONLY}")
        print(f"  Session TTL: {self.SESSION_TTL_DAYS} days")
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
            # Check payment provider based on PAYMENTS_PROVIDER setting
            if self.USE_STRIPE and not self.STRIPE_CONFIGURED:
                warnings.append("PAYMENTS_PROVIDER includes 'stripe' but Stripe not configured")
            if self.USE_MOLLIE and not self.MOLLIE_CONFIGURED:
                warnings.append("PAYMENTS_PROVIDER includes 'mollie' but Mollie not configured")
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
            "payments_provider": self.PAYMENTS_PROVIDER,
            "stripe_enabled": self.USE_STRIPE,
            "stripe_configured": self.STRIPE_CONFIGURED if self.USE_STRIPE else False,
            "stripe_mode": self.STRIPE_MODE if self.STRIPE_CONFIGURED else None,
            "mollie_enabled": self.USE_MOLLIE,
            "mollie_configured": self.MOLLIE_CONFIGURED if self.USE_MOLLIE else False,
            "mollie_mode": self.MOLLIE_MODE if self.MOLLIE_CONFIGURED else None,
            "aws_configured": self.AWS_CONFIGURED,
            "free_credits_on_signup": self.FREE_CREDITS_ON_SIGNUP,
        }


# ─────────────────────────────────────────────────────────────
# Singleton instance
# ─────────────────────────────────────────────────────────────
try:
    config = Config()
    # Backwards-compat: some older modules expect `config.config` to exist
    # (treating the Config instance like a module). Keep this alias to avoid
    # AttributeError during deployment mismatches.
    setattr(config, "config", config)
    print(f"[CONFIG] Loaded successfully (IS_DEV={config.IS_DEV}, IS_RENDER={config.IS_RENDER})")
except Exception as e:
    print(f"[CONFIG] FATAL: Failed to load config: {repr(e)}")
    raise

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
SESSION_COOKIE_DOMAIN = config.SESSION_COOKIE_DOMAIN
SESSION_COOKIE_SECURE = config.SESSION_COOKIE_SECURE
SESSION_COOKIE_SAMESITE = config.SESSION_COOKIE_SAMESITE
SESSION_COOKIE_HTTPONLY = config.SESSION_COOKIE_HTTPONLY
SESSION_COOKIE_PATH = config.SESSION_COOKIE_PATH
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
FRONTEND_BASE_URL = config.FRONTEND_BASE_URL
RESERVATION_EXPIRY_MINUTES = config.RESERVATION_EXPIRY_MINUTES
FREE_CREDITS_ON_SIGNUP = config.FREE_CREDITS_ON_SIGNUP
AWS_REGION = config.AWS_REGION
AWS_BUCKET_MODELS = config.AWS_BUCKET_MODELS
AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
MESHY_API_KEY = config.MESHY_API_KEY
MESHY_API_BASE = config.MESHY_API_BASE
OPENAI_API_KEY = config.OPENAI_API_KEY
GEMINI_API_KEY = config.GEMINI_API_KEY
DEFAULT_MODEL_TITLE = config.DEFAULT_MODEL_TITLE
ACTION_KEYS = config.ACTION_KEYS
PROXY_ALLOWED_HOSTS = config.PROXY_ALLOWED_HOSTS
ALLOWED_ORIGINS = config._ALLOWED_ORIGINS_RAW  # Raw string for existing code


def log_config():
    """Legacy function - calls config.log_summary()."""
    config.log_summary()

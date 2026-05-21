"""
STL Pack storefront service.

Sells themed packs of ready-to-print STL files. Reuses the existing Mollie
payment integration and the Cloudflare R2 bucket the packs are stored in.

Flow:
  1. create_checkout()  -> a Mollie payment with metadata type="stl_pack".
  2. Mollie webhook (payment.paid) -> MollieService._handle_payment_paid()
     branches on type=="stl_pack" -> handle_payment_paid() -> record_entitlement().
  3. presign_download() -> a short-lived Cloudflare R2 URL, gated by
     has_entitlement() so only paying customers get the file.

The STL_PACKS catalog below is the SERVER-AUTHORITATIVE source for price and
R2 key — the client never sets the price. Keep the slugs in sync with the
frontend catalog in js/stl-packs.js.
"""

from __future__ import annotations

import uuid
from typing import Optional, Dict, Any, List

import requests

from backend.config import config
from backend.db import query_one, query_all, execute_returning, transaction

# boto3 is only needed to mint R2 download links. Import is guarded so a
# missing dependency never blocks app startup — it only disables downloads.
try:
    import boto3
    from botocore.config import Config as _BotoConfig
    _BOTO_AVAILABLE = True
except Exception:  # pragma: no cover
    _BOTO_AVAILABLE = False


_TABLE = f"{config.BILLING_SCHEMA}.stl_pack_entitlements"

ALL_ACCESS_SLUG = "*"

# ─────────────────────────────────────────────────────────────────────────────
# CATALOG — edit this to manage the store.
#   price_gbp : source of truth at checkout (the client cannot tamper with it)
#   r2_key    : the object name in the R2 bucket (config.R2_BUCKET)
# Slugs MUST match the frontend catalog in js/stl-packs.js.
# To add a pack: upload <slug>.zip to R2, then add an entry here.
# ─────────────────────────────────────────────────────────────────────────────
STL_PACKS: Dict[str, Dict[str, Any]] = {
    "airplanes": {
        "title": "Airplanes STL Mega Pack",
        "price_gbp": 19.99,
        "r2_key": "airplanes.zip",
    },
    "decorations": {
        "title": "Decorations STL Pack",
        "price_gbp": 9.99,
        "r2_key": "decorations.zip",
    },
    "animals": {
        "title": "Animals STL Pack",
        "price_gbp": 9.99,
        "r2_key": "animals.zip",
    },
}

# All-Access pass — one payment unlocks every pack (entitlement slug "*").
ALL_ACCESS: Dict[str, Any] = {
    "title": "All-Access Library Pass",
    "price_gbp": 49.0,
    "r2_key": None,
}


def get_pack(slug: str) -> Optional[Dict[str, Any]]:
    """Return the catalog entry for a slug (with 'slug' included), or None.
    The special slug '*' is the All-Access pass."""
    if not slug:
        return None
    if slug == ALL_ACCESS_SLUG:
        return dict(ALL_ACCESS, slug=ALL_ACCESS_SLUG)
    pack = STL_PACKS.get(slug)
    return dict(pack, slug=slug) if pack else None


def ensure_stl_schema() -> None:
    """Create the stl_pack_entitlements table if missing.
    Called from db.ensure_schema() at startup (idempotent)."""
    with transaction("ensure_stl_schema") as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {_TABLE} (
                id                  uuid PRIMARY KEY,
                identity_id         uuid NOT NULL,
                pack_slug           text NOT NULL,
                provider            text NOT NULL DEFAULT 'mollie',
                provider_payment_id text NOT NULL,
                amount              numeric(10,2),
                currency            text NOT NULL DEFAULT 'GBP',
                status              text NOT NULL DEFAULT 'completed',
                created_at          timestamptz NOT NULL DEFAULT now()
            )
        """)
        cur.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_stl_ent_provider_payment
            ON {_TABLE} (provider, provider_payment_id)
        """)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_stl_ent_identity_pack
            ON {_TABLE} (identity_id, pack_slug)
        """)
    print("[STL] stl_pack_entitlements schema ensured")


class StlPackService:
    """Checkout, entitlements and R2 download links for STL packs."""

    # ── Availability checks ──────────────────────────────────────────────
    @staticmethod
    def payments_available() -> bool:
        from backend.services.mollie_service import MollieService
        return MollieService.is_available()

    @staticmethod
    def storage_available() -> bool:
        return _BOTO_AVAILABLE and config.R2_CONFIGURED

    # ── Checkout ─────────────────────────────────────────────────────────
    @staticmethod
    def create_checkout(
        identity_id: str,
        email: Optional[str],
        pack_slug: str,
        success_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a Mollie payment for an STL pack. Returns {checkout_url, payment_id}."""
        pack = get_pack(pack_slug)
        if not pack:
            raise ValueError(f"Unknown pack: {pack_slug}")

        from backend.services.mollie_service import MollieService
        if not MollieService.is_available():
            raise ValueError("Mollie is not configured")

        frontend_url = (config.FRONTEND_BASE_URL or config.PUBLIC_BASE_URL or "").rstrip("/")
        backend_url = (config.PUBLIC_BASE_URL or "").rstrip("/")
        if not success_url:
            success_url = f"{frontend_url}/stl-library?purchased={pack_slug}"
        webhook_url = f"{backend_url}/api/billing/webhook/mollie"

        # Metadata is returned verbatim in the webhook — this is how the
        # webhook knows it is an STL pack (type) and which pack / user.
        metadata = {
            "type": "stl_pack",
            "pack_slug": pack_slug,
            "identity_id": identity_id,
            "email": email or "",
        }
        payment_data = {
            "amount": {"currency": "GBP", "value": f"{float(pack['price_gbp']):.2f}"},
            "description": f"TimrX STL Pack — {pack['title']}",
            "redirectUrl": success_url,
            "webhookUrl": webhook_url,
            "metadata": metadata,
            "locale": "en_GB",
        }

        response = requests.post(
            f"{MollieService.MOLLIE_API_BASE}/payments",
            headers=MollieService._get_headers(),
            json=payment_data,
            timeout=30,
        )
        if response.status_code not in (200, 201):
            try:
                detail = response.json().get("detail", response.text)
            except Exception:
                detail = response.text
            print(f"[STL] Mollie error creating payment: {response.status_code} - {detail}")
            raise ValueError(f"Payment service error: {detail}")

        payment = response.json()
        print(f"[STL] Checkout created: payment={payment.get('id')} "
              f"identity={identity_id} pack={pack_slug}")
        return {
            "checkout_url": payment["_links"]["checkout"]["href"],
            "payment_id": payment["id"],
        }

    # ── Entitlements ─────────────────────────────────────────────────────
    @staticmethod
    def record_entitlement(
        identity_id: str,
        pack_slug: str,
        provider_payment_id: str,
        amount: Optional[float] = None,
        currency: str = "GBP",
        provider: str = "mollie",
    ) -> Optional[Dict[str, Any]]:
        """Insert an entitlement row. Idempotent on (provider, provider_payment_id)."""
        row = execute_returning(
            f"""
            INSERT INTO {_TABLE}
                (id, identity_id, pack_slug, provider, provider_payment_id,
                 amount, currency, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'completed')
            ON CONFLICT (provider, provider_payment_id) DO NOTHING
            RETURNING *
            """,
            (str(uuid.uuid4()), identity_id, pack_slug, provider,
             provider_payment_id, amount, currency),
            source="stl_record_entitlement",
        )
        if row:
            print(f"[STL] Entitlement granted: identity={identity_id} "
                  f"pack={pack_slug} payment={provider_payment_id}")
        else:
            print(f"[STL] Entitlement already recorded (idempotent): "
                  f"payment={provider_payment_id}")
        return row

    @staticmethod
    def has_entitlement(identity_id: str, pack_slug: str) -> bool:
        """True if this identity owns the pack — directly or via All-Access."""
        row = query_one(
            f"""
            SELECT 1 FROM {_TABLE}
            WHERE identity_id = %s AND status = 'completed'
              AND (pack_slug = %s OR pack_slug = %s)
            LIMIT 1
            """,
            (identity_id, pack_slug, ALL_ACCESS_SLUG),
            source="stl_has_entitlement",
        )
        return row is not None

    @staticmethod
    def list_entitlements(identity_id: str) -> List[Dict[str, Any]]:
        """All packs this identity owns, most recent first."""
        return query_all(
            f"""
            SELECT pack_slug, created_at
            FROM {_TABLE}
            WHERE identity_id = %s AND status = 'completed'
            ORDER BY created_at DESC
            """,
            (identity_id,),
            source="stl_list_entitlements",
        )

    # ── Webhook handler (called from MollieService._handle_payment_paid) ──
    @staticmethod
    def handle_payment_paid(payment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a paid STL-pack Mollie payment — grants the entitlement."""
        payment_id = payment.get("id")
        metadata = payment.get("metadata", {}) or {}
        identity_id = metadata.get("identity_id")
        pack_slug = metadata.get("pack_slug")

        amount_data = payment.get("amount", {}) or {}
        currency = amount_data.get("currency", "GBP")
        try:
            amount = float(amount_data.get("value")) if amount_data.get("value") else None
        except (TypeError, ValueError):
            amount = None

        if not identity_id or not pack_slug:
            print(f"[STL] Missing metadata in payment {payment_id}: {metadata}")
            return None

        StlPackService.record_entitlement(
            identity_id=identity_id,
            pack_slug=pack_slug,
            provider_payment_id=payment_id,
            amount=amount,
            currency=currency,
        )
        return {"ok": True, "stl_pack": True, "pack_slug": pack_slug}

    # ── R2 pre-signed download ───────────────────────────────────────────
    @staticmethod
    def _r2_client():
        if not _BOTO_AVAILABLE:
            raise RuntimeError("boto3 is not installed")
        if not config.R2_CONFIGURED:
            raise RuntimeError("Cloudflare R2 is not configured (set R2_* env vars)")
        return boto3.client(
            "s3",
            endpoint_url=config.R2_ENDPOINT,
            aws_access_key_id=config.R2_ACCESS_KEY_ID,
            aws_secret_access_key=config.R2_SECRET_ACCESS_KEY,
            config=_BotoConfig(signature_version="s3v4"),
            region_name="auto",
        )

    @staticmethod
    def presign_download(pack_slug: str, ttl_seconds: int = 600) -> str:
        """Return a short-lived R2 URL that downloads the pack ZIP directly.
        Does NOT check entitlement — the route must do that first."""
        pack = get_pack(pack_slug)
        if not pack or not pack.get("r2_key"):
            raise ValueError(f"No downloadable file for pack: {pack_slug}")
        client = StlPackService._r2_client()
        return client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": config.R2_BUCKET,
                "Key": pack["r2_key"],
                "ResponseContentDisposition": f'attachment; filename="{pack_slug}.zip"',
            },
            ExpiresIn=int(ttl_seconds),
        )

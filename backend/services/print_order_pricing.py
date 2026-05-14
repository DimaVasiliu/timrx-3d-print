"""
Print-on-demand pricing — server-side authoritative calculator.

DESIGN NOTES
============

The previous implementation priced everything in USD and FX-converted to
the customer's currency. That had two problems:
  • UK customers saw inflated shipping because we converted US-export rates
    to GBP, not Royal Mail's actual domestic rates.
  • The product itself looked too cheap (just filament cost), so customers
    perceived shipping as the dominant cost — bad psychology.

The new model is:
  • Per-currency NATIVE tables (no FX inside pricing — only as a fallback
    display hint for unsupported countries).
  • A real "production fee" that covers setup, cleaning, QC, standard
    packaging, payment-processing fees and the failed-print reserve.
  • Print-time charge that covers electricity + printer wear (the old
    model ignored this).
  • Retail material rates (not wholesale filament cost).
  • Minimum order per currency — protects margin on tiny items.
  • Free-shipping threshold — drives basket expansion and lets the
    customer "earn" delivery, which is psychologically much stronger
    than slashing shipping for everyone.
  • Optional premium packaging upsell — high-margin add-on.

The customer-visible price is dominated by Production (not Shipping),
which makes the order feel like a premium custom collectible instead of
a low-cost commodity print.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple


# ─────────────────────────────────────────────────────────────────────
# 1.  STATIC CATALOGS
# ─────────────────────────────────────────────────────────────────────
# Material density only — pricing per material lives in PRICING[currency]
# so each market sees its own native rates.
MATERIALS: Dict[str, Dict[str, Dict[str, Any]]] = {
    "fdm": {
        "pla":     {"label": "PLA",          "density": 1.24},
        "plaplus": {"label": "PLA+ (tough)", "density": 1.24},
        "petg":    {"label": "PETG",         "density": 1.27},
        "abs":     {"label": "ABS",          "density": 1.04},
        "tpu":     {"label": "TPU (flex)",   "density": 1.21},
        "silk":    {"label": "PLA Silk",     "density": 1.24},
    },
    "resin": {
        "std":   {"label": "Standard Resin", "density": 1.10},
        "tough": {"label": "Tough Resin",    "density": 1.13},
        "clear": {"label": "Clear Resin",    "density": 1.10},
        "flex":  {"label": "Flexible Resin", "density": 1.10},
    },
}

COLOR_LABELS: Dict[str, str] = {
    "black": "Black", "white": "White", "gray": "Gray", "red": "Red",
    "orange": "Orange", "yellow": "Yellow", "green": "Green", "teal": "Teal",
    "blue": "Blue", "navy": "Navy", "purple": "Purple", "pink": "Pink",
    "gold": "Gold", "silver": "Silver",
}

# Quality affects detail + print-time. Premium feel — Fine and Ultra are
# real upcharges, not just slightly slower.
QUALITY_MULT = {"draft": 0.85, "standard": 1.0, "fine": 1.4, "ultra": 2.0}

# Finish ladder leans more premium: painted is hand-work and should cost
# noticeably more.  Raw stays as the baseline.
FINISH_MULT  = {"raw": 1.0, "sanded": 1.15, "primed": 1.35, "painted": 2.2}


# ─────────────────────────────────────────────────────────────────────
# 2.  PER-CURRENCY PRICING TABLES (native rates — NOT FX'd)
# ─────────────────────────────────────────────────────────────────────
# Each block is the authoritative truth for that market.  Edit here to
# adjust pricing.  Three native currencies are supported; everything
# else falls back to EUR.
PRICING: Dict[str, Dict[str, Any]] = {
    # ── United Kingdom (Royal Mail Tracked) ───────────────────────────
    "GBP": {
        "production_fee":           8.50,   # setup + cleaning + QC + std packaging + Mollie fee reserve + 5% failed-print reserve
        "print_time_per_hour":      0.80,   # electricity + nozzle/build-plate wear
        "min_order":                14.00,  # absolute floor per unit (post quality/finish)
        "packaging_premium":        3.99,   # optional gift box + crinkle + certificate
        "free_shipping_threshold":  45.00,
        "shipping": {
            "standard":  4.49,   # Royal Mail Tracked 48
            "express":   7.99,   # Royal Mail Tracked 24
            "priority": 12.99,   # Special Delivery Guaranteed
        },
        "materials": {
            "fdm": {
                "pla":     78,    # £/kg — retail, not wholesale
                "plaplus": 95,
                "petg":   105,
                "abs":    115,
                "tpu":    145,
                "silk":   110,
            },
            "resin": {
                "std":   145,
                "tough": 175,
                "clear": 190,
                "flex":  220,
            },
        },
    },

    # ── Eurozone (DPD / Royal Mail International Tracked) ─────────────
    "EUR": {
        "production_fee":           9.99,
        "print_time_per_hour":      0.95,
        "min_order":                16.00,
        "packaging_premium":        4.99,
        "free_shipping_threshold":  55.00,
        "shipping": {
            "standard":  7.49,
            "express":  13.99,
            "priority": 22.99,
        },
        "materials": {
            "fdm": {
                "pla":     92, "plaplus": 112, "petg":   122,
                "abs":    135, "tpu":     170, "silk":   130,
            },
            "resin": {
                "std":   170, "tough": 205, "clear": 220, "flex":  255,
            },
        },
    },

    # ── US / CA / AU (DHL Economy or USPS/Royal Mail Int'l Tracked) ──
    "USD": {
        "production_fee":          11.99,
        "print_time_per_hour":      1.10,
        "min_order":                18.00,
        "packaging_premium":        5.99,
        "free_shipping_threshold":  65.00,
        "shipping": {
            "standard": 11.99,
            "express":  24.99,
            "priority": 42.99,
        },
        "materials": {
            "fdm": {
                "pla":    100, "plaplus": 122, "petg":   135,
                "abs":    148, "tpu":     185, "silk":   142,
            },
            "resin": {
                "std":   185, "tough": 225, "clear": 245, "flex":  285,
            },
        },
    },
}

# Currency mapping by shipping country.
USD_COUNTRIES = {"US", "CA", "AU"}
GBP_COUNTRIES = {"GB"}
# Everything else (EU bucket + JP + OTHER) bills in EUR.


class PriceError(ValueError):
    """Raised when input parameters cannot be priced."""


# ─────────────────────────────────────────────────────────────────────
# 3.  RESULT TYPE
# ─────────────────────────────────────────────────────────────────────
@dataclass
class PriceBreakdown:
    """Authoritative price breakdown returned to the route + stored on the order row."""

    currency: str
    weight_g: int
    time_min: int

    # Per-unit
    per_unit_base: float           # before quantity multiplier — useful for "qty × £14 = £42" UX
    quantity: int
    quantity_discount_pct: float

    # Order-level (all in native currency)
    subtotal: float                # production × qty × (1 - bulk_discount)
    packaging: float               # 0 if not selected
    shipping: float                # 0 if free-shipping kicked in
    shipping_label: str            # 'Tracked delivery (Royal Mail 48)' or 'Free standard delivery'
    free_shipping_unlocked: bool   # for UI badge
    free_shipping_remaining: float # currency amount user needs to add to unlock free ship
    total: float

    # Display helpers
    material_label: str
    color_label: str

    # Cents for storage / Mollie / PayPal — integer-safe.
    subtotal_cents: int
    packaging_cents: int
    shipping_cents: int
    total_cents: int

    # Trust-building copy fragments the frontend / email can use
    trust_points: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ─────────────────────────────────────────────────────────────────────
# 4.  CURRENCY SELECTION
# ─────────────────────────────────────────────────────────────────────
def pick_currency(country: Optional[str]) -> str:
    """Auto-detect currency from shipping country."""
    c = (country or "").upper()
    if c in USD_COUNTRIES:
        return "USD"
    if c in GBP_COUNTRIES:
        return "GBP"
    return "EUR"


def _table(currency: str) -> Dict[str, Any]:
    """Return the pricing table for a currency, falling back to EUR."""
    return PRICING.get(currency) or PRICING["EUR"]


# ─────────────────────────────────────────────────────────────────────
# 5.  SPEC VALIDATION + GEOMETRY → WEIGHT
# ─────────────────────────────────────────────────────────────────────
def _validate_spec(spec: Dict[str, Any]) -> Tuple[Dict[str, Any], str, Dict[str, Any]]:
    process = (spec.get("process") or "fdm").lower()
    if process not in MATERIALS:
        raise PriceError(f"Unknown process '{process}'")

    material_id = (spec.get("material") or "").lower()
    catalog = MATERIALS[process]
    if material_id not in catalog:
        raise PriceError(f"Unknown material '{material_id}' for process '{process}'")
    mat = catalog[material_id]

    color = (spec.get("color") or "").lower()
    if color and color not in COLOR_LABELS:
        raise PriceError(f"Unknown color '{color}'")

    return mat, process, {
        "material_id":      material_id,
        "color":            color,
        "quality":          (spec.get("quality") or "standard").lower(),
        "finish":           (spec.get("finish")  or "raw").lower(),
        "infill_pct":       int(spec.get("infill_pct") or 20),
        "quantity":         max(1, min(100, int(spec.get("quantity") or 1))),
        "premium_packaging": bool(spec.get("premium_packaging") or False),
    }


def _bbox_mm(spec: Dict[str, Any]) -> Optional[Tuple[float, float, float]]:
    dims = spec.get("scaled_dimensions_mm") or spec.get("dimensions_mm")
    if isinstance(dims, (list, tuple)) and len(dims) == 3:
        try:
            d = tuple(float(x) for x in dims)
            if all(v > 0 for v in d):
                return d  # type: ignore[return-value]
        except (TypeError, ValueError):
            pass
    return None


def _estimate_weight_and_time(
    bbox_mm: Tuple[float, float, float],
    process: str,
    infill_pct: int,
    density: float,
    quality_mult: float,
) -> Tuple[float, float]:
    """
    Return (weight_g, time_minutes).

    Calibration note: bounding-box volume × an "object fraction" is the
    only thing we can compute without an actual slicer. We use:
      FDM:   object_fraction = 0.18  (was 0.32 — was 80% high vs reality)
      Resin: object_fraction = 0.12  (resin parts are usually hollowed)
    """
    bx, by, bz = bbox_mm
    vol_cm3 = (bx * by * bz) / 1000.0

    if process == "resin":
        object_fraction = 0.12
        effective_solid = object_fraction  # no infill on resin
    else:
        object_fraction = 0.18
        infill = max(10, min(100, infill_pct))
        # Walls ~30% solid, rest = infill of the interior
        effective_solid = object_fraction * (0.30 + 0.70 * (infill / 100.0))

    solid_vol_cm3 = vol_cm3 * effective_solid
    weight_g = solid_vol_cm3 * density

    # Print-time minutes per gram, multiplied by quality (fine layers print slower).
    base_min_per_g = 0.85 if process == "resin" else 1.6
    time_min = weight_g * base_min_per_g * quality_mult

    return weight_g, time_min


# ─────────────────────────────────────────────────────────────────────
# 6.  MAIN COMPUTE
# ─────────────────────────────────────────────────────────────────────
def compute(
    spec: Dict[str, Any],
    country: Optional[str],
    speed: str = "standard",
) -> PriceBreakdown:
    """
    Compute the authoritative price for an order.

    Raises PriceError on invalid spec / dimensions.
    """
    mat, process, sv = _validate_spec(spec)
    bbox = _bbox_mm(spec)
    if not bbox:
        raise PriceError("Missing or invalid scaled_dimensions_mm in spec")

    currency = pick_currency(country)
    P = _table(currency)

    # ── Geometry → weight + time ────────────────────────────────────
    quality_mult = QUALITY_MULT.get(sv["quality"], 1.0)
    finish_mult  = FINISH_MULT.get(sv["finish"], 1.0)
    weight_g, time_min = _estimate_weight_and_time(
        bbox, process, sv["infill_pct"], float(mat["density"]), quality_mult,
    )

    # ── Per-unit production cost (native currency) ──────────────────
    material_rate = float(P["materials"][process][sv["material_id"]])
    material_cost = (weight_g / 1000.0) * material_rate * finish_mult * quality_mult
    print_time_cost = (time_min / 60.0) * float(P["print_time_per_hour"])
    production_fee = float(P["production_fee"])

    per_unit_raw = production_fee + material_cost + print_time_cost

    # Minimum order floor — protects margin on tiny items where production
    # fee alone wouldn't cover overhead.
    per_unit = max(per_unit_raw, float(P["min_order"]))

    # ── Quantity discount ───────────────────────────────────────────
    # Tighter than before: -2.5% per extra unit, capped at 15%, no
    # discount on first unit.  Rewards small "+1 extra" upsell, doesn't
    # try to win bulk-print contracts.
    qty = sv["quantity"]
    qty_discount_pct = min(0.15, max(0.0, (qty - 1) * 0.025))
    subtotal = per_unit * qty * (1 - qty_discount_pct)

    # ── Packaging upgrade (one fee per ORDER, not per unit) ─────────
    packaging = float(P["packaging_premium"]) if sv["premium_packaging"] else 0.0

    # ── Shipping (native rate, free over threshold for STANDARD only) ───
    speed_key = speed if speed in ("standard", "express", "priority") else "standard"
    shipping_full = float(P["shipping"][speed_key])

    pre_shipping_total = subtotal + packaging
    free_shipping_threshold = float(P["free_shipping_threshold"])
    free_shipping_unlocked = (
        speed_key == "standard"
        and pre_shipping_total >= free_shipping_threshold
    )
    shipping = 0.0 if free_shipping_unlocked else shipping_full
    free_shipping_remaining = max(0.0, free_shipping_threshold - pre_shipping_total)

    shipping_label = _shipping_label(currency, speed_key, free_shipping_unlocked)

    # ── Total ───────────────────────────────────────────────────────
    total = subtotal + packaging + shipping

    # ── Trust copy (frontend renders these as chips) ────────────────
    trust_points = [
        "Produced on demand",
        "Tracked delivery",
        "Quality-checked before shipping",
        "Securely packaged",
    ]

    return PriceBreakdown(
        currency=currency,
        weight_g=int(round(weight_g * qty)),
        time_min=int(round(time_min * qty)),
        per_unit_base=round(per_unit, 2),
        quantity=qty,
        quantity_discount_pct=round(qty_discount_pct, 3),
        subtotal=round(subtotal, 2),
        packaging=round(packaging, 2),
        shipping=round(shipping, 2),
        shipping_label=shipping_label,
        free_shipping_unlocked=free_shipping_unlocked,
        free_shipping_remaining=round(free_shipping_remaining, 2),
        total=round(total, 2),
        material_label=str(mat["label"]),
        color_label=COLOR_LABELS.get(sv["color"], sv["color"] or "—"),
        subtotal_cents=int(round(subtotal * 100)),
        packaging_cents=int(round(packaging * 100)),
        shipping_cents=int(round(shipping * 100)),
        total_cents=int(round(total * 100)),
        trust_points=trust_points,
    )


def _shipping_label(currency: str, speed: str, free: bool) -> str:
    """Human-readable shipping line for the receipt / email."""
    if free:
        return "Free Tracked delivery"
    if currency == "GBP":
        return {
            "standard": "Tracked delivery — Royal Mail Tracked 48",
            "express":  "Express delivery — Royal Mail Tracked 24",
            "priority": "Priority delivery — Special Delivery Guaranteed",
        }.get(speed, "Tracked delivery")
    if currency == "USD":
        return {
            "standard": "Tracked international delivery (7–14 days)",
            "express":  "Express international delivery (3–5 days)",
            "priority": "Priority international delivery (1–3 days)",
        }.get(speed, "Tracked delivery")
    # EUR
    return {
        "standard": "Tracked delivery (5–8 days)",
        "express":  "Express delivery (2–3 days)",
        "priority": "Priority delivery (1–2 days)",
    }.get(speed, "Tracked delivery")

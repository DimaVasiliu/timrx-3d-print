"""
Print-on-demand pricing — server-side authoritative calculator.

DESIGN NOTES
============

The previous implementation priced everything in USD and FX-converted to
the customer's currency. That had two problems:
  • UK customers saw inflated shipping because we converted US-export rates
    to USD, not Royal Mail's actual domestic rates.
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

ENABLED_PROCESSES = {"fdm"}

COLOR_LABELS: Dict[str, str] = {
    "black": "Black",
    "jade_white": "Jade White",
    "white": "Jade White",
    "gray": "Gray",
    "dark_gray": "Dark Gray",
    "red": "Red",
    "orange": "Orange",
    "yellow": "Yellow",
    "bambu_green": "Bambu Green",
    "green": "Bambu Green",
    "blue": "Blue",
    "lake_blue": "Lake Blue",
    "cyan": "Cyan",
    "magenta": "Magenta",
    "lime_green": "Lime Green",
    "forest_green": "Forest Green",
    "cream": "Cream",
    "silver": "Silver",
    "peanut_brown": "Peanut Brown",
}

# Quality affects detail + print-time. Premium feel — Fine and Ultra are
# real upcharges, not just slightly slower.
QUALITY_MULT = {"draft": 0.85, "standard": 1.0, "fine": 1.4, "ultra": 2.0}

# Finish ladder leans more premium: painted is hand-work and should cost
# noticeably more.  Raw stays as the baseline.
FINISH_MULT  = {"raw": 1.0, "sanded": 1.15, "primed": 1.35, "painted": 2.2}

# Small custom prints often hit the minimum-order floor.  Without a
# material-aware floor, changing PLA -> PETG/Resin appears to do nothing in
# the modal, which is both confusing and commercially wrong.
MATERIAL_FLOOR_PREMIUMS: Dict[str, Dict[str, float]] = {
    "fdm": {
        "pla": 0.00,
        "plaplus": 1.20,
        "petg": 2.00,
        "abs": 2.40,
        "tpu": 3.80,
        "silk": 2.50,
    },
    "resin": {
        "std": 4.00,
        "tough": 6.00,
        "clear": 7.00,
        "flex": 9.00,
    },
}

SIZE_CLASS_MULT = {
    "mini": 1.00,       # 50-74mm
    "collectible": 1.12, # 75-124mm
    "display": 1.32,   # 125-174mm
    "showpiece": 1.58, # 175-224mm
    "oversized": 1.95,
}

SIZE_CLASS_LABELS = {
    "mini": "Mini collectible",
    "collectible": "Collectible",
    "display": "Display piece",
    "showpiece": "Showpiece",
    "oversized": "Oversized production",
}

BUILD_VOLUME_MM = (256.0, 256.0, 256.0)


def _infill_floor_multiplier(process: str, infill_pct: int) -> float:
    """Make stronger FDM infill visibly affect even minimum-floor mini prints."""
    if process == "resin":
        return 1.0
    infill = max(10, min(100, int(infill_pct or 20)))
    if infill <= 20:
        return 1.0
    return 1.0 + min(0.35, (infill - 20) * 0.007)


# ─────────────────────────────────────────────────────────────────────
# 2.  PER-CURRENCY PRICING TABLES (native rates — NOT FX'd)
# ─────────────────────────────────────────────────────────────────────
# Each block is the authoritative truth for that market.  Edit here to
# adjust pricing.  Three native currencies are supported; everything
# else falls back to EUR.
PRICING: Dict[str, Dict[str, Any]] = {

    # ── US / CA / AU (DHL Economy or USPS/Royal Mail Int'l Tracked) ──
    "USD": {
        "production_fee":          13.50,
        "print_time_per_hour":      1.45,
        "min_order":                19.95,
        "packaging_premium":        5.99,
        "free_shipping_threshold":  79.00,
        "shipping": {
            "small":     {"standard": 13.95, "express": 26.95, "priority": 44.95},
            "parcel":    {"standard": 18.95, "express": 32.95, "priority": 54.95},
            "medium":    {"standard": 28.95, "express": 44.95, "priority": 74.95},
            "oversized": {"standard": 49.95, "express": 74.95, "priority": 119.95},
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

    # ── Eurozone (DPD / Royal Mail International Tracked) ─────────────
    "EUR": {
        "production_fee":          11.50,
        "print_time_per_hour":      1.25,
        "min_order":               17.95,
        "packaging_premium":        4.99,
        "free_shipping_threshold": 65.00,
        "shipping": {
            "small":     {"standard":  8.95, "express": 15.95, "priority": 24.95},
            "parcel":    {"standard": 11.95, "express": 18.95, "priority": 28.95},
            "medium":    {"standard": 16.95, "express": 24.95, "priority": 36.95},
            "oversized": {"standard": 29.95, "express": 42.95, "priority": 64.95},
        },
        "materials": {
            "fdm": {
                "pla":      92, "plaplus": 112, "petg":    122,
                "abs":     135, "tpu":     170, "silk":    130,
            },
            "resin": {
                "std":   170, "tough": 205, "clear": 220, "flex":  255,
            },
        },
    },
}

# Currency mapping by shipping country.
USD_COUNTRIES = {"US", "CA", "AU"}
UK_COUNTRIES = {"GB"}
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
    per_unit_base: float           # before quantity multiplier — useful for "qty × $14 = $42" UX
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
    size_class: str
    shipping_tier: str

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
    if c in UK_COUNTRIES:
        return "USD"
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
    if process not in ENABLED_PROCESSES:
        raise PriceError("Resin ordering is not available on the current Bambu Lab P1S fulfilment setup")

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
        "parts_count":       max(1, min(24, int(spec.get("parts_count") or spec.get("part_count") or 1))),
    }


def _bbox_mm(spec: Dict[str, Any]) -> Optional[Tuple[float, float, float]]:
    dims = spec.get("scaled_dimensions_mm") or spec.get("dimensions_mm")
    if isinstance(dims, (list, tuple)) and len(dims) == 3:
        try:
            d = tuple(float(x) for x in dims)
            if all(v > 0 for v in d):
                over = [v for v, max_v in zip(d, BUILD_VOLUME_MM) if v > max_v]
                if over:
                    raise PriceError("Scaled dimensions exceed the current 256 × 256 × 256mm printer volume")
                return d  # type: ignore[return-value]
        except PriceError:
            raise
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


def _size_class(bbox_mm: Tuple[float, float, float]) -> str:
    max_dim = max(bbox_mm)
    if max_dim < 75:
        return "mini"
    if max_dim < 125:
        return "collectible"
    if max_dim < 175:
        return "display"
    if max_dim < 225:
        return "showpiece"
    return "oversized"


def _shipping_tier(
    bbox_mm: Tuple[float, float, float],
    total_weight_g: float,
    quantity: int,
    parts_count: int,
) -> str:
    """Map model + order bulk to a realistic parcel tier."""
    max_dim = max(bbox_mm)
    packed_weight_g = total_weight_g + 140 + (quantity - 1) * 35 + (parts_count - 1) * 25

    if max_dim <= 160 and packed_weight_g <= 500:
        return "small"
    if max_dim <= 350 and packed_weight_g <= 2000:
        return "parcel"
    if max_dim <= 450 and packed_weight_g <= 5000:
        return "medium"
    return "oversized"


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
    size_class = _size_class(bbox)
    size_mult = SIZE_CLASS_MULT.get(size_class, 1.0)

    # ── Per-unit production cost (native currency) ──────────────────
    material_rate = float(P["materials"][process][sv["material_id"]])
    material_cost = (weight_g / 1000.0) * material_rate * finish_mult * quality_mult
    print_time_cost = (time_min / 60.0) * float(P["print_time_per_hour"])
    production_fee = float(P["production_fee"])

    parts_overhead = max(0, sv["parts_count"] - 1) * float(P["production_fee"]) * 0.28
    per_unit_raw = (production_fee + material_cost + print_time_cost + parts_overhead) * size_mult

    # Minimum order floor — protects margin on tiny items where production
    # fee alone wouldn't cover overhead. The floor is material-aware so
    # premium materials still change the estimate on small models.
    material_floor_premium = MATERIAL_FLOOR_PREMIUMS.get(process, {}).get(sv["material_id"], 0.0)
    infill_floor_mult = _infill_floor_multiplier(process, sv["infill_pct"])
    per_unit_floor = (float(P["min_order"]) + material_floor_premium) * size_mult * infill_floor_mult
    per_unit = max(per_unit_raw, per_unit_floor)

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
    tier = _shipping_tier(bbox, weight_g * qty, qty, sv["parts_count"])
    shipping_full = float(P["shipping"][tier][speed_key])

    pre_shipping_total = subtotal + packaging
    free_shipping_threshold = float(P["free_shipping_threshold"])
    free_shipping_unlocked = (
        speed_key == "standard"
        and pre_shipping_total >= free_shipping_threshold
    )
    shipping = 0.0 if free_shipping_unlocked else shipping_full
    free_shipping_remaining = max(0.0, free_shipping_threshold - pre_shipping_total)

    shipping_label = _shipping_label(currency, speed_key, free_shipping_unlocked, tier)

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
        size_class=SIZE_CLASS_LABELS.get(size_class, size_class),
        shipping_tier=tier,
        subtotal_cents=int(round(subtotal * 100)),
        packaging_cents=int(round(packaging * 100)),
        shipping_cents=int(round(shipping * 100)),
        total_cents=int(round(total * 100)),
        trust_points=trust_points,
    )


def _shipping_label(currency: str, speed: str, free: bool, tier: str = "small") -> str:
    """Human-readable shipping line for the receipt / email."""
    if free:
        return "Free secure packaging & tracked delivery"
    tier_hint = "oversized " if tier == "oversized" else ""
    if currency == "USD":
        return {
            "standard": f"Secure packaging & {tier_hint}tracked delivery",
            "express":  f"Express secure packaging & {tier_hint}tracked delivery",
            "priority": f"Priority secure packaging & {tier_hint}tracked delivery",
        }.get(speed, "Secure packaging & tracked delivery")
    if currency == "USD":
        return {
            "standard": f"Secure packaging & {tier_hint}tracked international delivery",
            "express":  f"Express secure packaging & {tier_hint}tracked international delivery",
            "priority": f"Priority secure packaging & {tier_hint}tracked international delivery",
        }.get(speed, "Secure packaging & tracked delivery")
    # EUR
    return {
        "standard": f"Secure packaging & {tier_hint}tracked delivery",
        "express":  f"Express secure packaging & {tier_hint}tracked delivery",
        "priority": f"Priority secure packaging & {tier_hint}tracked delivery",
    }.get(speed, "Secure packaging & tracked delivery")

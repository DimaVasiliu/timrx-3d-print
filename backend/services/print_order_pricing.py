"""
Print-on-demand pricing — server-side authoritative calculator.

The frontend shows a live estimate, but the final price charged to a
customer is ALWAYS recomputed here.  Never trust the price the browser
posts.  This module is a pure-function mirror of the JS pricing in
Frontend/js/main.js → initTimrxOrderModal/computeEstimate, with one
addition: currency selection by shipping country.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional, Tuple


# ── Catalogs (must stay aligned with frontend MATERIALS/COLORS) ──────
MATERIALS: Dict[str, Dict[str, Dict[str, Any]]] = {
    "fdm": {
        "pla":     {"label": "PLA",            "density": 1.24, "rate_per_kg": 38},
        "plaplus": {"label": "PLA+ (tough)",   "density": 1.24, "rate_per_kg": 52},
        "petg":    {"label": "PETG",           "density": 1.27, "rate_per_kg": 58},
        "abs":     {"label": "ABS",            "density": 1.04, "rate_per_kg": 62},
        "tpu":     {"label": "TPU (flex)",     "density": 1.21, "rate_per_kg": 78},
        "silk":    {"label": "PLA Silk",       "density": 1.24, "rate_per_kg": 62},
    },
    "resin": {
        "std":   {"label": "Standard Resin", "density": 1.10, "rate_per_kg": 95},
        "tough": {"label": "Tough Resin",    "density": 1.13, "rate_per_kg": 120},
        "clear": {"label": "Clear Resin",    "density": 1.10, "rate_per_kg": 130},
        "flex":  {"label": "Flexible Resin", "density": 1.10, "rate_per_kg": 160},
    },
}

COLOR_LABELS: Dict[str, str] = {
    "black": "Black", "white": "White", "gray": "Gray", "red": "Red",
    "orange": "Orange", "yellow": "Yellow", "green": "Green", "teal": "Teal",
    "blue": "Blue", "navy": "Navy", "purple": "Purple", "pink": "Pink",
    "gold": "Gold", "silver": "Silver",
}

QUALITY_MULT = {"draft": 0.85, "standard": 1.0, "fine": 1.35, "ultra": 1.8}
FINISH_MULT  = {"raw": 1.0, "sanded": 1.15, "primed": 1.3, "painted": 1.8}
SPEED_FEE_USD = {"standard": 0, "express": 18, "priority": 42}
SHIP_BASE_USD = {"US": 9, "CA": 14, "GB": 14, "EU": 16, "AU": 22, "JP": 22, "OTHER": 28}
BASE_FEE_USD = 6.50

# Currency mapping by shipping country.
USD_COUNTRIES = {"US", "CA", "AU"}
GBP_COUNTRIES = {"GB"}

# Naive FX from USD baseline.  Adjust quarterly or wire to an FX API.
FX_FROM_USD = {"USD": 1.0, "EUR": 0.92, "GBP": 0.79}


class PriceError(ValueError):
    """Raised when input parameters cannot be priced."""


@dataclass
class PriceBreakdown:
    currency: str
    weight_g: int
    time_min: int
    per_unit: float
    subtotal: float
    shipping: float
    total: float
    quantity: int
    quantity_discount_pct: float
    material_label: str
    color_label: str
    # Cents for storage / Mollie / PayPal — integer-safe.
    subtotal_cents: int
    shipping_cents: int
    total_cents: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def pick_currency(country: Optional[str]) -> str:
    """Auto-detect currency from shipping country."""
    c = (country or "").upper()
    if c in USD_COUNTRIES:
        return "USD"
    if c in GBP_COUNTRIES:
        return "GBP"
    return "EUR"


def _validate_spec(spec: Dict[str, Any]) -> Tuple[Dict[str, Any], str, Dict[str, Any]]:
    """
    Pull validated fields out of the user-supplied spec.
    Raises PriceError on bad input.
    """
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
        "material_id": material_id,
        "color": color,
        "quality": (spec.get("quality") or "standard").lower(),
        "finish": (spec.get("finish") or "raw").lower(),
        "infill_pct": int(spec.get("infill_pct") or 20),
        "quantity": max(1, min(100, int(spec.get("quantity") or 1))),
    }


def _bbox_mm(spec: Dict[str, Any]) -> Optional[Tuple[float, float, float]]:
    """Extract a valid scaled bbox from the spec."""
    dims = spec.get("scaled_dimensions_mm") or spec.get("dimensions_mm")
    if isinstance(dims, (list, tuple)) and len(dims) == 3:
        try:
            d = tuple(float(x) for x in dims)
            if all(v > 0 for v in d):
                return d  # type: ignore[return-value]
        except (TypeError, ValueError):
            pass
    return None


def compute(
    spec: Dict[str, Any],
    country: Optional[str],
    speed: str = "standard",
) -> PriceBreakdown:
    """
    Compute the authoritative price for an order.

    Args:
        spec: User-submitted print specification.
              Required keys: process, material, color, quality, finish,
              infill_pct (FDM), quantity, scaled_dimensions_mm.
        country: ISO country code from shipping address (e.g. 'US').
        speed: 'standard' | 'express' | 'priority'.

    Returns:
        PriceBreakdown with USD-or-EUR pricing.

    Raises:
        PriceError on invalid spec / dimensions.
    """
    mat, process, sv = _validate_spec(spec)
    bbox = _bbox_mm(spec)
    if not bbox:
        raise PriceError("Missing or invalid scaled_dimensions_mm in spec")

    quality_mult = QUALITY_MULT.get(sv["quality"], 1.0)
    finish_mult  = FINISH_MULT.get(sv["finish"], 1.0)
    qty = sv["quantity"]

    # ── Material weight & time (USD baseline) ────────────────────────
    vol_cm3 = (bbox[0] * bbox[1] * bbox[2]) / 1000.0
    object_fraction = 0.32  # printed object ~32% of bbox volume

    if process == "resin":
        effective_solid_fraction = 1.0
        infill_pct = 100
    else:
        infill_pct = max(10, min(100, sv["infill_pct"]))
        effective_solid_fraction = 0.30 + (1 - 0.30) * (infill_pct / 100.0)

    solid_vol_cm3 = vol_cm3 * object_fraction * effective_solid_fraction
    weight_g = solid_vol_cm3 * float(mat["density"])

    base_min_per_g = 0.85 if process == "resin" else 1.6
    time_min = weight_g * base_min_per_g * quality_mult

    # ── Per-unit subtotal in USD ─────────────────────────────────────
    material_cost = (weight_g / 1000.0) * float(mat["rate_per_kg"])
    per_unit_usd = BASE_FEE_USD + (material_cost * quality_mult * finish_mult)

    # Bulk discount: -3% per extra unit, capped at 25%
    qty_discount_pct = min(0.25, max(0.0, (qty - 1) * 0.03))
    subtotal_usd = per_unit_usd * qty * (1 - qty_discount_pct)

    # ── Shipping in USD ──────────────────────────────────────────────
    country_norm = (country or "").upper() or "OTHER"
    ship_base = SHIP_BASE_USD.get(country_norm, SHIP_BASE_USD["OTHER"])
    speed_fee = SPEED_FEE_USD.get(speed, 0)
    shipping_usd = float(ship_base + speed_fee)

    total_usd = subtotal_usd + shipping_usd

    # ── Currency conversion ──────────────────────────────────────────
    currency = pick_currency(country)
    fx = FX_FROM_USD.get(currency, 1.0)
    subtotal = round(subtotal_usd * fx, 2)
    shipping = round(shipping_usd * fx, 2)
    total = round(total_usd * fx, 2)

    return PriceBreakdown(
        currency=currency,
        weight_g=int(round(weight_g * qty)),
        time_min=int(round(time_min * qty)),
        per_unit=round(per_unit_usd * fx, 2),
        subtotal=subtotal,
        shipping=shipping,
        total=total,
        quantity=qty,
        quantity_discount_pct=round(qty_discount_pct, 3),
        material_label=str(mat["label"]),
        color_label=COLOR_LABELS.get(sv["color"], sv["color"] or "—"),
        subtotal_cents=int(round(subtotal * 100)),
        shipping_cents=int(round(shipping * 100)),
        total_cents=int(round(total * 100)),
    )

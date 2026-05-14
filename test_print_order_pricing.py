import importlib.util
import sys
from pathlib import Path


_PRICING_PATH = Path(__file__).resolve().parents[1] / "backend" / "services" / "print_order_pricing.py"
_SPEC = importlib.util.spec_from_file_location("print_order_pricing_under_test", _PRICING_PATH)
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)
compute = _MODULE.compute
PriceError = _MODULE.PriceError


def _spec(**overrides):
    spec = {
        "process": "fdm",
        "material": "pla",
        "color": "black",
        "quality": "standard",
        "finish": "raw",
        "infill_pct": 20,
        "quantity": 1,
        "scaled_dimensions_mm": [60, 60, 60],
    }
    spec.update(overrides)
    return spec


def test_uk_60mm_default_keeps_product_value_above_delivery():
    quote = compute(_spec(), country="GB", speed="standard")

    assert quote.currency == "GBP"
    assert quote.per_unit_base == 14.95
    assert quote.shipping == 4.95
    assert quote.subtotal > quote.shipping
    assert quote.total == 19.90
    assert quote.shipping_label == "Secure packaging & tracked delivery"


def test_small_premium_materials_change_the_quote_even_at_floor():
    pla = compute(_spec(material="pla"), country="GB", speed="standard")
    petg = compute(_spec(material="petg"), country="GB", speed="standard")
    silk = compute(_spec(material="silk"), country="GB", speed="standard")

    assert petg.total > pla.total
    assert silk.total > petg.total


def test_size_increases_production_value_and_can_change_parcel_tier():
    mini = compute(_spec(scaled_dimensions_mm=[60, 60, 60]), country="GB", speed="standard")
    display = compute(_spec(scaled_dimensions_mm=[150, 150, 150]), country="GB", speed="standard")
    oversized = compute(_spec(scaled_dimensions_mm=[256, 256, 256]), country="GB", speed="standard")

    assert display.subtotal > mini.subtotal
    assert oversized.subtotal > display.subtotal
    assert mini.shipping_tier == "small"
    assert oversized.shipping_tier in {"parcel", "medium", "oversized"}


def test_standard_delivery_becomes_included_over_threshold():
    quote = compute(_spec(quantity=4), country="GB", speed="standard")

    assert quote.free_shipping_unlocked is True
    assert quote.shipping == 0
    assert quote.shipping_label == "Free secure packaging & tracked delivery"


def test_single_piece_must_fit_current_256mm_build_volume():
    try:
        compute(_spec(scaled_dimensions_mm=[257, 80, 80]), country="GB", speed="standard")
    except PriceError as exc:
        assert "256" in str(exc)
    else:
        raise AssertionError("Expected oversized single-piece print to be rejected")


def test_current_fulfilment_rejects_resin_until_resin_printer_exists():
    try:
        compute(_spec(process="resin", material="std", color="gray"), country="GB", speed="standard")
    except PriceError as exc:
        assert "Resin ordering is not available" in str(exc)
    else:
        raise AssertionError("Expected resin order to be rejected for P1S fulfilment")


def test_bambu_lab_color_ids_are_accepted():
    quote = compute(_spec(color="bambu_green"), country="GB", speed="standard")

    assert quote.color_label == "Bambu Green"

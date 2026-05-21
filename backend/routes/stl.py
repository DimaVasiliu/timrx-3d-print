"""
/api/stl routes — STL pack storefront.

- POST /api/stl/checkout   Create a Mollie payment for a pack -> {checkout_url}
- GET  /api/stl/download   Entitlement-gated R2 download link -> {download_url}
- GET  /api/stl/my-packs   List the packs the signed-in user owns
- GET  /api/stl/catalog    Public catalog (slug, title, price) — no auth

Downloads are served straight from Cloudflare R2 via a short-lived pre-signed
URL, so the backend never streams the large ZIP itself.
"""

from flask import Blueprint, request, jsonify, g

from backend.middleware import require_session, no_cache
from backend.services.stl_pack_service import StlPackService, get_pack, STL_PACKS, ALL_ACCESS

bp = Blueprint("stl", __name__)


@bp.route("/catalog", methods=["GET"])
def stl_catalog():
    """Public — the price list. Useful for the storefront / debugging."""
    packs = [
        {"slug": slug, "title": p["title"], "price_gbp": p["price_gbp"]}
        for slug, p in STL_PACKS.items()
    ]
    return jsonify({
        "ok": True,
        "packs": packs,
        "all_access": {"slug": "*", "title": ALL_ACCESS["title"],
                       "price_gbp": ALL_ACCESS["price_gbp"]},
    })


@bp.route("/checkout", methods=["POST"])
@no_cache
@require_session
def stl_checkout():
    """Start a Mollie checkout for an STL pack."""
    data = request.get_json(silent=True) or {}
    pack_slug = (data.get("pack_slug") or "").strip()

    if not pack_slug:
        return jsonify({"ok": False, "error": "pack_slug is required"}), 400

    if not get_pack(pack_slug):
        return jsonify({"ok": False, "error": "unknown_pack",
                        "message": "That pack does not exist."}), 404

    if not StlPackService.payments_available():
        return jsonify({"ok": False, "error": "payment_unavailable",
                        "message": "Payment service is not available."}), 503

    email = (getattr(g, "identity", None) or {}).get("email")
    try:
        result = StlPackService.create_checkout(g.identity_id, email, pack_slug)
        return jsonify({"ok": True, "checkout_url": result["checkout_url"]})
    except ValueError as e:
        return jsonify({"ok": False, "error": "checkout_failed", "message": str(e)}), 400
    except Exception as e:  # noqa: BLE001
        print(f"[STL] checkout error: {e}")
        return jsonify({"ok": False, "error": "checkout_failed",
                        "message": "Could not start checkout."}), 502


@bp.route("/download", methods=["GET"])
@no_cache
@require_session
def stl_download():
    """Return a short-lived R2 download URL — only if the user owns the pack."""
    pack_slug = (request.args.get("pack") or "").strip()

    if not pack_slug:
        return jsonify({"ok": False, "error": "pack_required"}), 400

    if not get_pack(pack_slug):
        return jsonify({"ok": False, "error": "unknown_pack"}), 404

    if not StlPackService.has_entitlement(g.identity_id, pack_slug):
        return jsonify({"ok": False, "error": "not_owned",
                        "message": "Purchase this pack to download it."}), 403

    if not StlPackService.storage_available():
        return jsonify({"ok": False, "error": "storage_unavailable",
                        "message": "Downloads are temporarily unavailable."}), 503

    try:
        url = StlPackService.presign_download(pack_slug)
        return jsonify({"ok": True, "download_url": url})
    except Exception as e:  # noqa: BLE001
        print(f"[STL] download error: {e}")
        return jsonify({"ok": False, "error": "download_failed",
                        "message": "Could not generate the download link."}), 502


@bp.route("/my-packs", methods=["GET"])
@no_cache
@require_session
def stl_my_packs():
    """List the packs the signed-in user owns."""
    ents = StlPackService.list_entitlements(g.identity_id)
    owns_all = any(e.get("pack_slug") == "*" for e in ents)

    packs = []
    for e in ents:
        slug = e.get("pack_slug")
        pk = get_pack(slug)
        purchased_at = e.get("created_at")
        packs.append({
            "slug": slug,
            "title": (pk["title"] if pk else slug),
            "purchased_at": purchased_at.isoformat()
            if hasattr(purchased_at, "isoformat") else purchased_at,
        })

    return jsonify({"ok": True, "owns_all_access": owns_all, "packs": packs})

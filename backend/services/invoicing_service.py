"""
Invoicing Service — creates invoices + receipts with PDF generation.

Flow (called from Mollie webhook after purchase is recorded):
1. create_invoice_for_purchase() — DB rows (idempotent via UNIQUE purchase_id)
2. generate_invoice_pdf() / generate_receipt_pdf() — fpdf2 PDF bytes
3. Upload PDFs to S3
4. Send email with both PDFs attached

All methods are safe to call from webhooks — never throw.
"""

from __future__ import annotations

import io
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from backend.db import get_conn, fetch_one, transaction, Tables


# ── Text sanitizer for PDF (Helvetica doesn't support Unicode) ──
def sanitize_pdf_text(text: str) -> str:
    """
    Replace Unicode characters that Helvetica can't render with ASCII equivalents.
    This prevents 'character outside the range' errors in fpdf2.
    """
    if not text:
        return text

    # Character replacements: Unicode → ASCII
    replacements = {
        '–': '-',      # en-dash → hyphen
        '—': '-',      # em-dash → hyphen
        ''': "'",      # left single quote
        ''': "'",      # right single quote
        '"': '"',      # left double quote
        '"': '"',      # right double quote
        '…': '...',    # ellipsis
        '•': '*',      # bullet
        '×': 'x',      # multiplication sign
        '÷': '/',      # division sign
        '≤': '<=',     # less than or equal
        '≥': '>=',     # greater than or equal
        '≠': '!=',     # not equal
        '±': '+/-',    # plus-minus
        '€': 'EUR',    # euro (keep £ as it's in Latin-1)
        '™': '(TM)',   # trademark
        '®': '(R)',    # registered
        '©': '(C)',    # copyright
        '\u00a0': ' ', # non-breaking space
    }

    for unicode_char, ascii_char in replacements.items():
        text = text.replace(unicode_char, ascii_char)

    # Remove any remaining non-Latin-1 characters (keep £ which is \u00a3)
    # Latin-1 range is 0x00-0xFF
    result = []
    for char in text:
        if ord(char) <= 0xFF:
            result.append(char)
        else:
            result.append('?')  # Replace unknown chars with ?

    return ''.join(result)

# ── Logo cache ──────────────────────────────────────────────
_logo_bytes: Optional[bytes] = None
_logo_loaded = False


def _load_logo() -> Optional[bytes]:
    """Load TimrX logo PNG.  Tries Render paths, local paths, then public URL.  Cached."""
    global _logo_bytes, _logo_loaded
    if _logo_loaded:
        return _logo_bytes

    _logo_loaded = True

    from backend.config import config

    # Build list of candidate paths (ordered by priority)
    candidates = [
        # Render deployment paths (most likely in production)
        Path("/opt/render/project/src/backend/assets/logo.png"),
        Path("/opt/render/project/src/assets/logo.png"),
        # APP_DIR-relative paths (APP_DIR = meshy/ in this project)
        config.APP_DIR / "assets" / "logo.png",
        config.APP_DIR / "backend" / "assets" / "logo.png",
        # Local development paths
        config.APP_DIR / ".." / ".." / "Frontend" / "img" / "logo.png",
        config.APP_DIR / ".." / ".." / "Frontend" / "img" / "logo (1).png",
    ]

    for p in candidates:
        try:
            resolved = p.resolve()
            if resolved.is_file():
                _logo_bytes = resolved.read_bytes()
                print(f"[INVOICE] Logo loaded from {resolved} ({len(_logo_bytes)} bytes)")
                return _logo_bytes
        except Exception:
            continue

    # Fallback: download from public URL
    try:
        import requests as _req
        resp = _req.get("https://timrx.live/img/logo.png", timeout=10)
        if resp.status_code == 200 and len(resp.content) > 100:
            _logo_bytes = resp.content
            print(f"[INVOICE] Logo downloaded from web ({len(_logo_bytes)} bytes)")
            return _logo_bytes
    except Exception as e:
        print(f"[INVOICE] Could not download logo: {e}")

    print("[INVOICE] Logo not found — PDFs will render without logo")
    return None


# ── Invoice number helpers ──────────────────────────────────

def _next_invoice_number(cur) -> str:
    """Generate INV-YYYY-NNNN using the DB sequence."""
    year = datetime.now(timezone.utc).year
    cur.execute("SELECT nextval('timrx_billing.invoice_number_seq')")
    seq = cur.fetchone()["nextval"]
    return f"INV-{year}-{seq:04d}"


def _next_receipt_number(cur) -> str:
    """Generate RCPT-YYYY-NNNN using the DB sequence."""
    year = datetime.now(timezone.utc).year
    cur.execute("SELECT nextval('timrx_billing.receipt_number_seq')")
    seq = cur.fetchone()["nextval"]
    return f"RCPT-{year}-{seq:04d}"


class InvoicingService:
    """Service for creating invoices, generating PDFs, and emailing them."""

    # ── DB operations ──────────────────────────────────────

    @staticmethod
    def create_invoice_for_purchase(
        purchase_id: str,
        identity_id: str,
        plan_code: str,
        plan_name: str,
        credits: int,
        amount_gbp: float,
        customer_email: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Create invoice + receipt rows for a purchase.

        Idempotent: UNIQUE(purchase_id) on invoices prevents duplicates.
        Returns {"invoice": {...}, "receipt": {...}} or None if already exists.
        """
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # Generate numbers
                    invoice_number = _next_invoice_number(cur)
                    receipt_number = _next_receipt_number(cur)

                    now = datetime.now(timezone.utc)

                    # Insert invoice (idempotent via uq_invoices_purchase)
                    cur.execute(
                        f"""
                        INSERT INTO {Tables.INVOICES}
                            (identity_id, purchase_id, invoice_number, status,
                             currency, subtotal, tax_amount, total,
                             customer_email, issued_at)
                        VALUES (%s, %s, %s, 'paid', 'GBP', %s, 0, %s, %s, %s)
                        ON CONFLICT (purchase_id) WHERE purchase_id IS NOT NULL
                        DO NOTHING
                        RETURNING *
                        """,
                        (
                            identity_id,
                            purchase_id,
                            invoice_number,
                            amount_gbp,
                            amount_gbp,
                            customer_email,
                            now,
                        ),
                    )
                    invoice = fetch_one(cur)

                    if not invoice:
                        # Already exists — idempotent skip
                        print(f"[INVOICE] Invoice already exists for purchase {purchase_id}")
                        return None

                    invoice_id = str(invoice["id"])

                    # Insert line item
                    cur.execute(
                        f"""
                        INSERT INTO {Tables.INVOICE_ITEMS}
                            (invoice_id, description, quantity, unit_price, total, sort_order)
                        VALUES (%s, %s, 1, %s, %s, 0)
                        RETURNING *
                        """,
                        (
                            invoice_id,
                            f"{plan_name} — {credits:,} Credits",
                            amount_gbp,
                            amount_gbp,
                        ),
                    )
                    item = fetch_one(cur)

                    # Insert receipt
                    cur.execute(
                        f"""
                        INSERT INTO {Tables.RECEIPTS}
                            (invoice_id, identity_id, receipt_number,
                             amount_paid, currency, payment_method, paid_at)
                        VALUES (%s, %s, %s, %s, 'GBP', 'mollie', %s)
                        ON CONFLICT (invoice_id) DO NOTHING
                        RETURNING *
                        """,
                        (invoice_id, identity_id, receipt_number, amount_gbp, now),
                    )
                    receipt = fetch_one(cur)

                conn.commit()

                print(
                    f"[INVOICE] Created {invoice_number} + {receipt_number} "
                    f"for purchase {purchase_id}"
                )
                return {
                    "invoice": invoice,
                    "receipt": receipt,
                    "items": [item] if item else [],
                }

        except Exception as e:
            print(f"[INVOICE] Error creating invoice for purchase {purchase_id}: {e}")
            return None

    @staticmethod
    def get_invoice(invoice_id: str) -> Optional[Dict[str, Any]]:
        """Fetch invoice by ID."""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT * FROM {Tables.INVOICES} WHERE id::text = %s",
                        (invoice_id,),
                    )
                    return fetch_one(cur)
        except Exception as e:
            print(f"[INVOICE] Error fetching invoice {invoice_id}: {e}")
            return None

    @staticmethod
    def get_receipt(receipt_id: str) -> Optional[Dict[str, Any]]:
        """Fetch receipt by ID."""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT * FROM {Tables.RECEIPTS} WHERE id::text = %s",
                        (receipt_id,),
                    )
                    return fetch_one(cur)
        except Exception as e:
            print(f"[INVOICE] Error fetching receipt {receipt_id}: {e}")
            return None

    @staticmethod
    def get_invoice_items(invoice_id: str) -> List[Dict[str, Any]]:
        """Fetch line items for an invoice."""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT * FROM {Tables.INVOICE_ITEMS}
                        WHERE invoice_id::text = %s
                        ORDER BY sort_order
                        """,
                        (invoice_id,),
                    )
                    return cur.fetchall() or []
        except Exception as e:
            print(f"[INVOICE] Error fetching items for invoice {invoice_id}: {e}")
            return []

    # ── PDF generation ─────────────────────────────────────

    @staticmethod
    def _place_logo(pdf, x, y, w=28):
        """Place the TimrX logo on the PDF. Returns True if placed."""
        logo = _load_logo()
        if not logo:
            return False
        try:
            tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
            tmp.write(logo)
            tmp.close()
            pdf.image(tmp.name, x=x, y=y, w=w)
            os.unlink(tmp.name)
            return True
        except Exception:
            return False

    @staticmethod
    def _draw_from_block(pdf, x, y):
        """Draw the 'From' address block (TimrX business details)."""
        BLACK = (30, 30, 30)
        GRAY = (120, 120, 120)
        pdf.set_text_color(*BLACK)
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_xy(x, y)
        pdf.cell(80, 5, "TimrX")
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(*GRAY)
        for i, line in enumerate([
            "9 Thomas Court",
            "New Mossford Way",
            "London, IG6 1FJ",
            "United Kingdom",
            "support@timrx.live",
        ]):
            pdf.set_xy(x, y + 6 + i * 5)
            pdf.cell(80, 5, line)
        return y + 6 + 5 * 5  # return y after last line

    @staticmethod
    def _draw_footer(pdf, lm, rm):
        """Draw the professional footer line."""
        GRAY = (120, 120, 120)
        LGRAY = (200, 200, 200)
        pdf.set_y(-25)
        pdf.set_draw_color(*LGRAY)
        pdf.line(lm, pdf.get_y(), rm, pdf.get_y())
        pdf.ln(3)
        pdf.set_font("Helvetica", "", 7)
        pdf.set_text_color(*GRAY)
        pdf.cell(
            rm - lm, 4,
            "TimrX  |  9 Thomas Court, New Mossford Way, London IG6 1FJ  |  timrx.live  |  support@timrx.live",
            align="C",
        )

    @staticmethod
    def generate_invoice_pdf(
        invoice: Dict[str, Any],
        items: List[Dict[str, Any]],
    ) -> bytes:
        """Generate a professional invoice PDF. Returns raw bytes."""
        from fpdf import FPDF

        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=25)
        pdf.add_page()

        LM = 15
        RM = 195
        CW = RM - LM
        BLACK = (30, 30, 30)
        GRAY = (120, 120, 120)
        LGRAY = (200, 200, 200)

        # -- Logo (top-left)
        InvoicingService._place_logo(pdf, LM, 15)

        # -- Title (top-right)
        pdf.set_text_color(*BLACK)
        pdf.set_font("Helvetica", "B", 28)
        pdf.set_xy(RM - 80, 15)
        pdf.cell(80, 12, "Invoice", align="R")

        # -- Invoice details (right column below title)
        invoice_num = sanitize_pdf_text(str(invoice["invoice_number"]))
        issued = invoice.get("issued_at")
        if issued:
            date_str = issued.strftime("%d %B %Y") if hasattr(issued, "strftime") else str(issued)[:10]
        else:
            date_str = datetime.now(timezone.utc).strftime("%d %B %Y")

        y = 32
        pdf.set_font("Helvetica", "", 9)
        for label, val in [
            ("Invoice number", invoice_num),
            ("Date of issue", sanitize_pdf_text(date_str)),
            ("Date due", sanitize_pdf_text(date_str)),
        ]:
            pdf.set_text_color(*GRAY)
            pdf.set_xy(115, y)
            pdf.cell(40, 5, label)
            pdf.set_text_color(*BLACK)
            pdf.cell(40, 5, val, align="R")
            y += 6

        # -- From (left)
        y_from = 55
        y_after_addr = InvoicingService._draw_from_block(pdf, LM, y_from)

        # -- Bill To (right)
        pdf.set_text_color(*BLACK)
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_xy(115, y_from)
        pdf.cell(80, 5, "Bill to", align="R")
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(*GRAY)
        email = sanitize_pdf_text(invoice.get("customer_email") or "-")
        pdf.set_xy(115, y_from + 6)
        pdf.cell(80, 5, email, align="R")

        # -- Amount due banner
        amount = float(invoice["total"])
        y_banner = y_after_addr + 8
        pdf.set_font("Helvetica", "B", 14)
        pdf.set_text_color(*BLACK)
        pdf.set_xy(LM, y_banner)
        pdf.cell(CW, 10, sanitize_pdf_text(f"\u00a3{amount:.2f} due {date_str}"), align="C")

        # -- Line items table
        y_tbl = y_banner + 16
        pdf.set_draw_color(*LGRAY)
        pdf.line(LM, y_tbl, RM, y_tbl)

        y_tbl += 2
        pdf.set_xy(LM, y_tbl)
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*GRAY)
        pdf.cell(90, 7, "Description")
        pdf.cell(20, 7, "Qty", align="C")
        pdf.cell(35, 7, "Unit price", align="R")
        pdf.cell(35, 7, "Amount", align="R")

        y_tbl += 9
        pdf.line(LM, y_tbl, RM, y_tbl)

        pdf.set_text_color(*BLACK)
        pdf.set_font("Helvetica", "", 9)
        for item in items:
            pdf.set_xy(LM, y_tbl + 2)
            desc = sanitize_pdf_text(str(item.get("description", "")))
            pdf.cell(90, 8, desc)
            pdf.cell(20, 8, str(item.get("quantity", 1)), align="C")
            pdf.cell(35, 8, f"\u00a3{float(item['unit_price']):.2f}", align="R")
            pdf.cell(35, 8, f"\u00a3{float(item['total']):.2f}", align="R")
            y_tbl += 10

        pdf.line(LM, y_tbl, RM, y_tbl)

        # -- Totals (right-aligned)
        yt = y_tbl + 6
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(*GRAY)
        pdf.set_xy(RM - 80, yt)
        pdf.cell(45, 7, "Subtotal")
        pdf.set_text_color(*BLACK)
        pdf.cell(35, 7, f"\u00a3{float(invoice['subtotal']):.2f}", align="R")

        tax = float(invoice.get("tax_amount", 0))
        if tax > 0:
            yt += 7
            pdf.set_text_color(*GRAY)
            pdf.set_xy(RM - 80, yt)
            pdf.cell(45, 7, "Tax")
            pdf.set_text_color(*BLACK)
            pdf.cell(35, 7, f"\u00a3{tax:.2f}", align="R")

        yt += 7
        pdf.set_text_color(*GRAY)
        pdf.set_xy(RM - 80, yt)
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(45, 7, "Total")
        pdf.set_text_color(*BLACK)
        pdf.cell(35, 7, f"\u00a3{amount:.2f}", align="R")

        yt += 10
        pdf.line(RM - 80, yt, RM, yt)

        yt += 4
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_text_color(*BLACK)
        pdf.set_xy(RM - 80, yt)
        pdf.cell(45, 8, "Amount due")
        pdf.cell(35, 8, f"\u00a3{amount:.2f}", align="R")

        # -- Footer
        InvoicingService._draw_footer(pdf, LM, RM)

        return pdf.output()

    @staticmethod
    def generate_receipt_pdf(
        receipt: Dict[str, Any],
        invoice: Dict[str, Any],
        items: Optional[List[Dict[str, Any]]] = None,
    ) -> bytes:
        """Generate a professional payment receipt PDF. Returns raw bytes."""
        from fpdf import FPDF

        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=25)
        pdf.add_page()

        LM = 15
        RM = 195
        CW = RM - LM
        BLACK = (30, 30, 30)
        GRAY = (120, 120, 120)
        LGRAY = (200, 200, 200)
        GREEN = (0, 150, 75)

        # -- Logo (top-left)
        InvoicingService._place_logo(pdf, LM, 15)

        # -- Title (top-right)
        pdf.set_text_color(*BLACK)
        pdf.set_font("Helvetica", "B", 28)
        pdf.set_xy(RM - 80, 15)
        pdf.cell(80, 12, "Receipt", align="R")

        # -- Receipt details (right column below title)
        receipt_num = sanitize_pdf_text(str(receipt["receipt_number"]))
        inv_num = sanitize_pdf_text(str(invoice.get("invoice_number", "-")))
        paid_at = receipt.get("paid_at")
        if paid_at:
            date_str = paid_at.strftime("%d %B %Y") if hasattr(paid_at, "strftime") else str(paid_at)[:10]
        else:
            date_str = datetime.now(timezone.utc).strftime("%d %B %Y")

        y = 32
        pdf.set_font("Helvetica", "", 9)
        for label, val in [
            ("Receipt number", receipt_num),
            ("Invoice number", inv_num),
            ("Date of issue", sanitize_pdf_text(date_str)),
        ]:
            pdf.set_text_color(*GRAY)
            pdf.set_xy(115, y)
            pdf.cell(40, 5, label)
            pdf.set_text_color(*BLACK)
            pdf.cell(40, 5, val, align="R")
            y += 6

        # -- From (left)
        y_from = 55
        y_after_addr = InvoicingService._draw_from_block(pdf, LM, y_from)

        # -- Bill To (right)
        pdf.set_text_color(*BLACK)
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_xy(115, y_from)
        pdf.cell(80, 5, "Bill to", align="R")
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(*GRAY)
        email = sanitize_pdf_text(invoice.get("customer_email") or "-")
        pdf.set_xy(115, y_from + 6)
        pdf.cell(80, 5, email, align="R")

        # -- Amount paid banner (green)
        amount = float(receipt["amount_paid"])
        y_banner = y_after_addr + 8
        pdf.set_font("Helvetica", "B", 14)
        pdf.set_text_color(*GREEN)
        pdf.set_xy(LM, y_banner)
        pdf.cell(CW, 10, sanitize_pdf_text(f"\u00a3{amount:.2f} paid on {date_str}"), align="C")

        # -- Line items table (if items provided)
        y_tbl = y_banner + 16
        if items:
            pdf.set_draw_color(*LGRAY)
            pdf.line(LM, y_tbl, RM, y_tbl)

            y_tbl += 2
            pdf.set_xy(LM, y_tbl)
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_text_color(*GRAY)
            pdf.cell(90, 7, "Description")
            pdf.cell(20, 7, "Qty", align="C")
            pdf.cell(35, 7, "Unit price", align="R")
            pdf.cell(35, 7, "Amount", align="R")

            y_tbl += 9
            pdf.line(LM, y_tbl, RM, y_tbl)

            pdf.set_text_color(*BLACK)
            pdf.set_font("Helvetica", "", 9)
            for item in items:
                pdf.set_xy(LM, y_tbl + 2)
                desc = sanitize_pdf_text(str(item.get("description", "")))
                pdf.cell(90, 8, desc)
                pdf.cell(20, 8, str(item.get("quantity", 1)), align="C")
                pdf.cell(35, 8, f"\u00a3{float(item['unit_price']):.2f}", align="R")
                pdf.cell(35, 8, f"\u00a3{float(item['total']):.2f}", align="R")
                y_tbl += 10

            pdf.line(LM, y_tbl, RM, y_tbl)

        # -- Totals (right-aligned)
        yt = y_tbl + 6
        total = float(invoice.get("total", amount))
        subtotal = float(invoice.get("subtotal", total))

        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(*GRAY)
        pdf.set_xy(RM - 80, yt)
        pdf.cell(45, 7, "Subtotal")
        pdf.set_text_color(*BLACK)
        pdf.cell(35, 7, f"\u00a3{subtotal:.2f}", align="R")

        tax = float(invoice.get("tax_amount", 0))
        if tax > 0:
            yt += 7
            pdf.set_text_color(*GRAY)
            pdf.set_xy(RM - 80, yt)
            pdf.cell(45, 7, "Tax")
            pdf.set_text_color(*BLACK)
            pdf.cell(35, 7, f"\u00a3{tax:.2f}", align="R")

        yt += 7
        pdf.set_text_color(*GRAY)
        pdf.set_xy(RM - 80, yt)
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(45, 7, "Total")
        pdf.set_text_color(*BLACK)
        pdf.cell(35, 7, f"\u00a3{total:.2f}", align="R")

        yt += 10
        pdf.set_draw_color(*LGRAY)
        pdf.line(RM - 80, yt, RM, yt)

        yt += 4
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_text_color(*GREEN)
        pdf.set_xy(RM - 80, yt)
        pdf.cell(45, 8, "Amount paid")
        pdf.cell(35, 8, f"\u00a3{amount:.2f}", align="R")

        # -- Payment history
        yt += 16
        pdf.set_text_color(*BLACK)
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_xy(LM, yt)
        pdf.cell(CW, 7, "Payment history")

        yt += 10
        pdf.set_draw_color(*LGRAY)
        pdf.line(LM, yt, RM, yt)

        yt += 2
        pdf.set_xy(LM, yt)
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_text_color(*GRAY)
        pdf.cell(40, 7, "Payment")
        pdf.cell(45, 7, "Date")
        pdf.cell(40, 7, "Amount", align="R")
        pdf.cell(55, 7, "Receipt number", align="R")

        yt += 9
        pdf.line(LM, yt, RM, yt)

        yt += 2
        pdf.set_xy(LM, yt)
        pdf.set_font("Helvetica", "", 8)
        pdf.set_text_color(*BLACK)
        method = sanitize_pdf_text((receipt.get("payment_method") or "Mollie").title())
        pdf.cell(40, 7, method)
        pdf.cell(45, 7, sanitize_pdf_text(date_str))
        pdf.cell(40, 7, f"\u00a3{amount:.2f}", align="R")
        pdf.cell(55, 7, receipt_num, align="R")

        yt += 9
        pdf.line(LM, yt, RM, yt)

        # -- Footer
        InvoicingService._draw_footer(pdf, LM, RM)

        return pdf.output()

    # ── S3 upload ──────────────────────────────────────────

    @staticmethod
    def _upload_pdf(pdf_bytes: bytes, s3_key: str) -> Optional[str]:
        """Upload PDF bytes to S3 and return URL."""
        try:
            from backend.services.s3_service import upload_bytes_to_s3
            result = upload_bytes_to_s3(
                data_bytes=pdf_bytes,
                content_type="application/pdf",
                key=s3_key,
            )
            url = result if isinstance(result, str) else result.get("url")
            print(f"[INVOICE] PDF uploaded: {s3_key}")
            return url
        except Exception as e:
            print(f"[INVOICE] Error uploading PDF {s3_key}: {e}")
            return None

    @staticmethod
    def _update_invoice_pdf(invoice_id: str, s3_key: str, url: str) -> None:
        """Set pdf_s3_key and pdf_url on invoice row."""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {Tables.INVOICES}
                        SET pdf_s3_key = %s, pdf_url = %s
                        WHERE id::text = %s
                        """,
                        (s3_key, url, invoice_id),
                    )
                conn.commit()
        except Exception as e:
            print(f"[INVOICE] Error updating invoice PDF path: {e}")

    @staticmethod
    def _update_receipt_pdf(receipt_id: str, s3_key: str, url: str) -> None:
        """Set pdf_s3_key and pdf_url on receipt row."""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {Tables.RECEIPTS}
                        SET pdf_s3_key = %s, pdf_url = %s
                        WHERE id::text = %s
                        """,
                        (s3_key, url, receipt_id),
                    )
                conn.commit()
        except Exception as e:
            print(f"[INVOICE] Error updating receipt PDF path: {e}")

    # ── Orchestrator ───────────────────────────────────────

    @staticmethod
    def process_purchase_invoice(
        purchase_id: str,
        identity_id: str,
        plan_code: str,
        plan_name: str,
        credits: int,
        amount_gbp: float,
        customer_email: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Full invoice pipeline: create DB rows → PDFs → S3 → email.

        Idempotent and safe — never throws, never blocks credit granting.
        Returns summary dict or None.
        """
        invoice_pdf = None
        receipt_pdf = None
        inv_url = None
        rcpt_url = None
        pdf_error = None

        try:
            # 1. Create invoice + receipt (idempotent)
            result = InvoicingService.create_invoice_for_purchase(
                purchase_id=purchase_id,
                identity_id=identity_id,
                plan_code=plan_code,
                plan_name=plan_name,
                credits=credits,
                amount_gbp=amount_gbp,
                customer_email=customer_email,
            )

            if not result:
                # Already exists or error
                return None

            invoice = result["invoice"]
            receipt = result["receipt"]
            items = result["items"]
            invoice_id = str(invoice["id"])
            receipt_id = str(receipt["id"]) if receipt else None
            invoice_number = invoice["invoice_number"]
            receipt_number = receipt["receipt_number"] if receipt else None
            year = datetime.now(timezone.utc).year

            # 2. Generate PDFs (with error handling - don't fail the whole pipeline)
            try:
                invoice_pdf = InvoicingService.generate_invoice_pdf(invoice, items)
                receipt_pdf = InvoicingService.generate_receipt_pdf(receipt, invoice, items) if receipt else None
                print(f"[INVOICE] PDFs generated: invoice={len(invoice_pdf) if invoice_pdf else 0} bytes, receipt={len(receipt_pdf) if receipt_pdf else 0} bytes")
            except Exception as pdf_err:
                pdf_error = str(pdf_err)
                print(f"[INVOICE] PDF generation failed: {pdf_error}")

            # 3. Upload to S3 (only if PDFs were generated)
            if invoice_pdf:
                inv_key = f"invoices/{year}/{invoice_number}.pdf"
                inv_url = InvoicingService._upload_pdf(invoice_pdf, inv_key)
                if inv_url:
                    InvoicingService._update_invoice_pdf(invoice_id, inv_key, inv_url)

            if receipt_pdf and receipt_id and receipt_number:
                rcpt_key = f"receipts/{year}/{receipt_number}.pdf"
                rcpt_url = InvoicingService._upload_pdf(receipt_pdf, rcpt_key)
                if rcpt_url:
                    InvoicingService._update_receipt_pdf(receipt_id, rcpt_key, rcpt_url)

            # 4. Send email to customer (ALWAYS attempt, with or without PDFs)
            if customer_email:
                try:
                    from backend.emailer import send_invoice_email, send_purchase_receipt
                    logo = _load_logo()

                    # If we have PDFs, send full invoice email with attachments
                    if invoice_pdf and receipt_pdf:
                        send_invoice_email(
                            to_email=customer_email,
                            invoice_number=invoice_number,
                            receipt_number=receipt_number or "",
                            plan_name=sanitize_pdf_text(plan_name),
                            credits=credits,
                            amount_gbp=amount_gbp,
                            invoice_pdf=invoice_pdf,
                            receipt_pdf=receipt_pdf,
                            logo_bytes=logo,
                        )
                        print(f"[INVOICE] Email sent to {customer_email} with PDF attachments")
                    else:
                        # Fallback: send HTML receipt without PDFs
                        print(f"[INVOICE] Sending fallback email (no PDFs) to {customer_email}")
                        send_purchase_receipt(
                            to_email=customer_email,
                            plan_name=sanitize_pdf_text(plan_name),
                            credits=credits,
                            amount_gbp=amount_gbp,
                        )
                        print(f"[INVOICE] Fallback email sent to {customer_email} (PDF error: {pdf_error})")
                except Exception as email_err:
                    print(f"[INVOICE] Email failed (PDFs still saved): {email_err}")

            print(
                f"[INVOICE] Pipeline complete: {invoice_number} + {receipt_number} "
                f"for purchase {purchase_id}"
            )
            return {
                "invoice_id": invoice_id,
                "receipt_id": receipt_id,
                "invoice_number": invoice_number,
                "receipt_number": receipt_number,
                "invoice_pdf_url": inv_url,
                "receipt_pdf_url": rcpt_url,
            }

        except Exception as e:
            print(f"[INVOICE] Pipeline error for purchase {purchase_id}: {e}")
            return None

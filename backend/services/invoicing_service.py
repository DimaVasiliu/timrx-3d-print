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
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from backend.db import get_conn, fetch_one, transaction, Tables

# ── Logo cache ──────────────────────────────────────────────
_logo_bytes: Optional[bytes] = None
_logo_loaded = False


def _load_logo() -> Optional[bytes]:
    """Load TimrX logo PNG.  Tries local paths then public URL.  Cached."""
    global _logo_bytes, _logo_loaded
    if _logo_loaded:
        return _logo_bytes

    _logo_loaded = True

    # Try local paths (development)
    from backend.config import config
    candidates = [
        config.APP_DIR / "backend" / "assets" / "logo.png",       # server (APP_DIR = repo root)
        config.APP_DIR / "assets" / "logo.png",                    # local alt
        config.APP_DIR / ".." / ".." / "Frontend" / "img" / "logo (1).png",  # local dev
        config.APP_DIR / ".." / ".." / "Frontend" / "img" / "logo.png",
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
        resp = _req.get("https://timrx.live/logo.png", timeout=10)
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
    def generate_invoice_pdf(
        invoice: Dict[str, Any],
        items: List[Dict[str, Any]],
    ) -> bytes:
        """Generate a professional invoice PDF.  Returns raw bytes."""
        from fpdf import FPDF

        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=25)
        pdf.add_page()

        # -- Logo
        logo = _load_logo()
        if logo:
            try:
                tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
                tmp.write(logo)
                tmp.close()
                pdf.image(tmp.name, x=10, y=10, w=30)
                os.unlink(tmp.name)
            except Exception:
                pass

        # -- Header
        pdf.set_font("Helvetica", "B", 22)
        pdf.set_xy(120, 10)
        pdf.cell(80, 10, "INVOICE", align="R")

        pdf.set_font("Helvetica", "", 10)
        pdf.set_xy(120, 22)
        pdf.cell(80, 5, f"No: {invoice['invoice_number']}", align="R")

        issued = invoice.get("issued_at")
        if issued:
            date_str = issued.strftime("%d %B %Y") if hasattr(issued, "strftime") else str(issued)[:10]
        else:
            date_str = datetime.now(timezone.utc).strftime("%d %B %Y")
        pdf.set_xy(120, 28)
        pdf.cell(80, 5, f"Date: {date_str}", align="R")

        # -- From
        pdf.set_xy(10, 45)
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(80, 6, "From")
        pdf.set_font("Helvetica", "", 9)
        pdf.set_xy(10, 52)
        pdf.cell(80, 5, "TimrX - 3D Print Hub")
        pdf.set_xy(10, 57)
        pdf.cell(80, 5, "support@timrx.live")

        # -- Bill To
        pdf.set_xy(120, 45)
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(80, 6, "Bill To", align="R")
        pdf.set_font("Helvetica", "", 9)
        email = invoice.get("customer_email", "—")
        pdf.set_xy(120, 52)
        pdf.cell(80, 5, email, align="R")

        # -- Line items table
        y_start = 72
        pdf.set_xy(10, y_start)

        # Table header
        pdf.set_fill_color(40, 40, 40)
        pdf.set_text_color(255, 255, 255)
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(90, 8, "  Description", fill=True)
        pdf.cell(25, 8, "Qty", align="C", fill=True)
        pdf.cell(35, 8, "Unit Price", align="R", fill=True)
        pdf.cell(35, 8, "Total  ", align="R", fill=True)
        pdf.ln()

        # Table rows
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "", 9)
        for item in items:
            pdf.cell(90, 8, f"  {item['description']}")
            pdf.cell(25, 8, str(item.get("quantity", 1)), align="C")
            pdf.cell(35, 8, f"\u00a3{float(item['unit_price']):.2f}", align="R")
            pdf.cell(35, 8, f"\u00a3{float(item['total']):.2f}  ", align="R")
            pdf.ln()

        # -- Totals
        y_totals = pdf.get_y() + 6
        pdf.set_xy(120, y_totals)
        pdf.set_font("Helvetica", "", 9)
        pdf.cell(45, 7, "Subtotal:", align="R")
        pdf.cell(35, 7, f"\u00a3{float(invoice['subtotal']):.2f}  ", align="R")
        pdf.ln()

        tax = float(invoice.get("tax_amount", 0))
        if tax > 0:
            pdf.set_x(120)
            pdf.cell(45, 7, "Tax:", align="R")
            pdf.cell(35, 7, f"\u00a3{tax:.2f}  ", align="R")
            pdf.ln()

        pdf.set_x(120)
        pdf.set_font("Helvetica", "B", 11)
        pdf.cell(45, 9, "Total:", align="R")
        pdf.cell(35, 9, f"\u00a3{float(invoice['total']):.2f}  ", align="R")

        # -- Footer
        pdf.set_y(-30)
        pdf.set_font("Helvetica", "", 8)
        pdf.set_text_color(130, 130, 130)
        pdf.cell(0, 5, "TimrX - 3D Print Hub  |  timrx.live  |  support@timrx.live", align="C")

        return pdf.output()

    @staticmethod
    def generate_receipt_pdf(
        receipt: Dict[str, Any],
        invoice: Dict[str, Any],
    ) -> bytes:
        """Generate a payment receipt PDF.  Returns raw bytes."""
        from fpdf import FPDF

        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=25)
        pdf.add_page()

        # -- Logo
        logo = _load_logo()
        if logo:
            try:
                tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
                tmp.write(logo)
                tmp.close()
                pdf.image(tmp.name, x=10, y=10, w=30)
                os.unlink(tmp.name)
            except Exception:
                pass

        # -- Header
        pdf.set_font("Helvetica", "B", 22)
        pdf.set_xy(120, 10)
        pdf.cell(80, 10, "RECEIPT", align="R")

        pdf.set_font("Helvetica", "", 10)
        pdf.set_xy(120, 22)
        pdf.cell(80, 5, f"No: {receipt['receipt_number']}", align="R")

        paid_at = receipt.get("paid_at")
        if paid_at:
            date_str = paid_at.strftime("%d %B %Y") if hasattr(paid_at, "strftime") else str(paid_at)[:10]
        else:
            date_str = datetime.now(timezone.utc).strftime("%d %B %Y")
        pdf.set_xy(120, 28)
        pdf.cell(80, 5, f"Date: {date_str}", align="R")

        # -- PAID stamp
        pdf.set_font("Helvetica", "B", 28)
        pdf.set_text_color(0, 160, 80)
        pdf.set_xy(10, 42)
        pdf.cell(50, 14, "PAID", border=1)
        pdf.set_text_color(0, 0, 0)

        # -- Details
        pdf.set_xy(10, 65)
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(80, 6, "Payment Details")
        pdf.ln()

        pdf.set_font("Helvetica", "", 9)
        details = [
            ("Receipt No:", receipt["receipt_number"]),
            ("Invoice No:", invoice.get("invoice_number", "—")),
            ("Customer:", invoice.get("customer_email", "—")),
            ("Payment Method:", (receipt.get("payment_method") or "mollie").title()),
            ("Currency:", receipt.get("currency", "GBP")),
            ("Date:", date_str),
        ]
        for label, value in details:
            pdf.cell(45, 7, label)
            pdf.cell(100, 7, str(value))
            pdf.ln()

        # -- Amount
        pdf.ln(6)
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(45, 10, "Amount Paid:")
        pdf.cell(100, 10, f"\u00a3{float(receipt['amount_paid']):.2f}")

        # -- Footer
        pdf.set_y(-30)
        pdf.set_font("Helvetica", "", 8)
        pdf.set_text_color(130, 130, 130)
        pdf.cell(0, 5, "TimrX - 3D Print Hub  |  timrx.live  |  support@timrx.live", align="C")

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

            # 2. Generate PDFs
            invoice_pdf = InvoicingService.generate_invoice_pdf(invoice, items)
            receipt_pdf = InvoicingService.generate_receipt_pdf(receipt, invoice) if receipt else None

            # 3. Upload to S3
            inv_key = f"invoices/{year}/{invoice_number}.pdf"
            inv_url = InvoicingService._upload_pdf(invoice_pdf, inv_key)
            if inv_url:
                InvoicingService._update_invoice_pdf(invoice_id, inv_key, inv_url)

            rcpt_url = None
            if receipt_pdf and receipt_id and receipt_number:
                rcpt_key = f"receipts/{year}/{receipt_number}.pdf"
                rcpt_url = InvoicingService._upload_pdf(receipt_pdf, rcpt_key)
                if rcpt_url:
                    InvoicingService._update_receipt_pdf(receipt_id, rcpt_key, rcpt_url)

            # 4. Send email with PDFs
            if customer_email:
                try:
                    from backend.emailer import send_invoice_email
                    logo = _load_logo()
                    send_invoice_email(
                        to_email=customer_email,
                        invoice_number=invoice_number,
                        receipt_number=receipt_number or "",
                        plan_name=plan_name,
                        credits=credits,
                        amount_gbp=amount_gbp,
                        invoice_pdf=invoice_pdf,
                        receipt_pdf=receipt_pdf or b"",
                        logo_bytes=logo,
                    )
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

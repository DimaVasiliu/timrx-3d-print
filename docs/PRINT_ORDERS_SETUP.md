# Print-on-demand orders — setup guide

End-to-end flow:

```
Browser → POST /api/print-orders → backend creates DB row + payment session
   → checkout_url returned → browser redirects to Mollie/PayPal
   → user pays → provider hits POST /api/print-orders/webhook/{mollie|paypal}
   → backend marks order paid → emails admin + customer
   → user redirected to /3dprint?print_order=TX-PR-NNNNNN
```

## 1. Run the migration

```sql
\i migrations/066_print_orders.sql
```

Creates:
- `timrx_billing.print_orders` table
- `timrx_billing.print_order_number_seq` sequence
- `timrx_billing.touch_print_orders_updated_at()` trigger function

## 2. Environment variables

Add to your Render service (or `.env`):

| Variable | Required | Default | Notes |
| --- | --- | --- | --- |
| `MOLLIE_API_KEY` | ✅ | — | Already set if credit purchases work |
| `PAYPAL_CLIENT_ID` | ⚠️ if PayPal | — | From PayPal Developer dashboard |
| `PAYPAL_CLIENT_SECRET` | ⚠️ if PayPal | — | From PayPal Developer dashboard |
| `PAYPAL_WEBHOOK_ID` | ⚠️ if PayPal | — | Create webhook in dashboard, copy ID |
| `PAYPAL_ENV` | ⚠️ if PayPal | `sandbox` | `live` or `sandbox` |
| `PRINT_ORDER_ADMIN_EMAIL` | ❌ | `admin@timrx.live` | Where new-order notifications land |
| `PRINT_ORDER_FROM_EMAIL` | ❌ | `no-reply@timrx.live` | Customer receipt From: header |
| `PRINT_ORDER_FROM_NAME` | ❌ | `TimrX Print` | Customer receipt From: display name |
| `EMAIL_PROVIDER` / `EMAIL_*` | ✅ | — | Existing config (SES or SMTP) |
| `FRONTEND_BASE_URL` | ✅ | — | e.g. `https://timrx.live` |
| `PUBLIC_BASE_URL` | ✅ | — | e.g. `https://3d.timrx.live` |

The customer-receipt From address (`no-reply@timrx.live`) must be a
verified sender in your email provider (SES) or your SMTP relay must
permit it.

## 3. PayPal one-time setup

1. Create a REST app at https://developer.paypal.com/dashboard/applications/
2. Copy the **Client ID** and **Secret** into `PAYPAL_CLIENT_ID` / `PAYPAL_CLIENT_SECRET`.
3. Under "Webhooks" for the app:
   - URL: `https://3d.timrx.live/api/print-orders/webhook/paypal`
   - Events to subscribe to:
     - `CHECKOUT.ORDER.APPROVED`
     - `PAYMENT.CAPTURE.COMPLETED`
     - `PAYMENT.CAPTURE.DENIED`
     - `PAYMENT.CAPTURE.REFUNDED`
     - `CHECKOUT.ORDER.VOIDED`
4. Copy the **Webhook ID** that PayPal returns → `PAYPAL_WEBHOOK_ID`.
5. Set `PAYPAL_ENV=live` once you've tested in sandbox.

## 4. Mollie webhook URL

The print-order flow uses a different webhook from credit purchases:

- Credit purchases:  `POST /api/billing/webhook/mollie`
- **Print orders:**   `POST /api/print-orders/webhook/mollie`

You don't need to register this URL in the Mollie dashboard — it's
created per-payment in `_create_mollie_payment` via `webhookUrl`.

## 5. Endpoints reference

| Method | Path | Auth | Purpose |
| --- | --- | --- | --- |
| POST | `/api/print-orders/quote` | required | Server-side price quote (no DB write) |
| POST | `/api/print-orders` | required + verified email | Create order + payment session |
| GET  | `/api/print-orders` | required | List the caller's orders |
| GET  | `/api/print-orders/<id_or_number>` | required | One order (owner only) |
| POST | `/api/print-orders/webhook/mollie` | provider | Mollie payment notification |
| POST | `/api/print-orders/webhook/paypal` | provider, signed | PayPal payment notification |

## 6. Pricing

Server-side authoritative pricing lives in
`backend/services/print_order_pricing.py`. The browser shows a live
estimate using a JS twin of the same formula, but **only** the
server-computed `total_cents` is sent to Mollie/PayPal.

Currency selection: `US`, `CA`, `AU` → USD. Everywhere else → EUR.
Adjust `USD_COUNTRIES` and `FX_FROM_USD` in `print_order_pricing.py`
quarterly or wire to a real FX feed.

## 7. Emails

- **Admin notification** → `admin@timrx.live` (override with
  `PRINT_ORDER_ADMIN_EMAIL`). Includes model link, full spec, shipping
  address, payment details and totals.
- **Customer receipt** → from `no-reply@timrx.live`, Reply-To
  `admin@timrx.live`. Includes spec summary, shipping address, totals.

Both fire only **after** the webhook confirms payment.

## 8. Fulfillment workflow

1. New order email lands at `admin@timrx.live`.
2. Click the **Open file ↗** link to download the GLB.
3. Slice & print per the spec rows.
4. Send the QC photo to the customer (reply to their address).
5. Mark order shipped:
   ```sql
   UPDATE timrx_billing.print_orders
   SET status = 'shipped', shipped_at = NOW()
   WHERE order_number = 'TX-PR-001027';
   ```
6. (Future) — add an admin dashboard route at `/admin/print-orders` if
   manual SQL gets repetitive.

## 9. Refunds

Refund directly in the Mollie or PayPal dashboard, then mirror state:

```sql
UPDATE timrx_billing.print_orders
SET status = 'refunded', refunded_at = NOW()
WHERE order_number = 'TX-PR-001027';
```

## 10. Smoke test

```bash
# 1. Verify backend route is registered
curl -s https://3d.timrx.live/api/print-orders/quote \
  -X POST -H 'content-type: application/json' \
  -b "timrx_sid=$YOUR_SESSION" \
  -d '{"spec":{"process":"fdm","material":"pla","color":"black","quality":"standard","finish":"raw","infill_pct":20,"quantity":1,"scaled_dimensions_mm":[60,100,60]},"shipping":{"country":"US","speed":"standard"}}'

# Expect: {"ok":true,"currency_detected":"USD","providers_available":["mollie",...],"quote":{...}}
```

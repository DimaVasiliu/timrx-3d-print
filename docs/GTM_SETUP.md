# Google Tag Manager + GA4 + Google Ads — TimrX Setup Guide

This guide finishes the implementation that ships with this commit. The code
side is done; you still need to configure the GTM container itself + a few
GA4/Ads tags inside the GTM UI.

## TL;DR — what already works in code

- A GTM container snippet is on every customer-facing page (both `timrx.live`
  and `3d.timrx.live`).
- `Frontend/js/analytics.js` is the **only** module that writes to
  `window.dataLayer`. All other code goes through `window.TimrXAnalytics.*`.
- Five canonical events are wired:
  - `purchase` — fires after Mollie webhook + credit grant **finalise**
    successfully. Server-issued via the new `analytics_events` queue.
  - `begin_checkout` — fires the moment the user is redirected to Mollie.
    Client-side, value is the plan price.
  - `sign_up` — server-issued the first time an identity attaches+verifies
    an email (anonymous-first model).
  - `email_verified` — server-issued on every successful email verification.
  - `generation_started` / `generation_completed` — helpers exposed on
    `window.TimrXAnalytics`; not yet wired (optional).
- All events carry a stable `event_id` and survive page-reload / SPA replay /
  cross-subdomain navigation.
- Conversion firing is gated by:
  1. Server-side `event_id` UNIQUE constraint (`analytics_events` table).
  2. Server marks `fired_at` once the browser acks → drops from queue.
  3. Browser localStorage allow-list (defence in depth).
  4. GA4 native `transaction_id` dedup.

## What you need to do once before going live

### Step 0 — Run the new migration

```bash
psql "$DATABASE_URL" -f Backend/meshy/migrations/070_analytics_events.sql
```

### Step 1 — GTM container (already configured)

Production container ID: **`GTM-TH8DB6S5`** (already baked into the code on
both `timrx.live` and `3d.timrx.live`). Manage it at
<https://tagmanager.google.com>.

If you need a different container for staging, swap `GTM_ID` in
`Frontend/js/analytics-config.js` *and* update every page's inline snippet:

```bash
grep -rl "GTM-TH8DB6S5" TimrX/Frontend TimrX/Backend/meshy/docs \
  | xargs sed -i '' 's/GTM-TH8DB6S5/GTM-YOURSTAGING/g'
```

### Step 2 — Configure tags inside the GTM container

Create the following three tags + matching triggers + variables.

**Variables (Custom Event / Data Layer Variables):**

| Variable Name | Type | Data Layer key |
|---|---|---|
| `dlv.transaction_id` | Data Layer Variable | `transaction_id` |
| `dlv.value` | Data Layer Variable | `value` |
| `dlv.currency` | Data Layer Variable | `currency` |
| `dlv.event_id` | Data Layer Variable | `event_id` |
| `dlv.items` | Data Layer Variable | `ecommerce.items` (version 2) |
| `dlv.plan_code` | Data Layer Variable | `plan_code` |
| `dlv.credit_type` | Data Layer Variable | `credit_type` |

**Triggers (Custom Event):**

| Trigger Name | Custom Event name |
|---|---|
| `evt - purchase` | `purchase` |
| `evt - begin_checkout` | `begin_checkout` |
| `evt - sign_up` | `sign_up` |
| `evt - email_verified` | `email_verified` |

**Tags:**

1. **GA4 Configuration** (Google Analytics: GA4 Configuration)
   - Measurement ID: your GA4 `G-XXXXXXXXXX`.
   - Trigger: `All Pages`.
   - Fields to set: `cookie_domain = .timrx.live` (so the same client_id
     follows users across `timrx.live` ↔ `3d.timrx.live`).
   - Send to server container: off (unless you run sGTM).

2. **GA4 Event — purchase** (Google Analytics: GA4 Event)
   - Configuration Tag: select your GA4 Configuration tag.
   - Event Name: `purchase`.
   - Event Parameters:
     - `transaction_id` → `{{dlv.transaction_id}}`
     - `value` → `{{dlv.value}}`
     - `currency` → `{{dlv.currency}}`
     - `items` → `{{dlv.items}}`
     - `event_id` → `{{dlv.event_id}}` (for GA4 dedup)
   - Trigger: `evt - purchase`.

3. **Google Ads Conversion — purchase** (Google Ads Conversion Tracking)
   - Conversion ID: `AW-18162436469`
   - Conversion Label: `ruWaCPm54qwcEPWSw9RD`
   - Conversion Value: `{{dlv.value}}`
   - Currency Code: `{{dlv.currency}}`
   - Transaction ID: `{{dlv.transaction_id}}` ← **critical** for Ads dedup
   - Trigger: `evt - purchase`.

4. **Google Ads Conversion Linker** (Conversion Linker)
   - Trigger: `All Pages`.
   - Cookie Domain: `auto`.

5. (Optional) **GA4 Events — sign_up / email_verified / begin_checkout**
   - One GA4 Event tag per custom event, mirroring the trigger names.

### Step 3 — Verify in Preview mode

1. In GTM, click **Preview**, enter `https://timrx.live`.
2. Make a real purchase end-to-end (use a £1 test pack or Mollie test mode).
3. After redirect back, watch the Tag Assistant tab:
   - `begin_checkout` should have fired *before* the redirect.
   - `purchase` should fire either immediately on return (if the webhook
     beat the redirect) or within a few seconds (when visibilitychange
     re-polls the queue). Event ID will be `purchase:<mollie_payment_id>`.
4. Open GA4 → Configure → DebugView. You should see the same `purchase`
   with `transaction_id`, `value`, `currency`, `items`.
5. In Google Ads → Tools → Conversions, the conversion should register as
   "Recording" within ~3 hours.

## Operational notes

- **Refund / chargeback**: a server-side enqueue for a refund event is *not*
  implemented in this commit. Google Ads handles refunds via the bulk uploads
  API or the offline conversion adjustments UI — TimrX should keep refunds
  manual until volume justifies automating.
- **Cookie consent**: `analytics.js` honours `window.__TIMRX_ANALYTICS_OPT_IN__`
  — wire your consent banner to flip this to `false` on reject.
- **Anonymous-first**: events are scoped to `identity_id`, which exists for
  anonymous sessions too. We never expose PII (no email, no name) in the
  dataLayer.
- **Cross-subdomain**: same GTM container is loaded on both subdomains; GA4's
  `cookie_domain = .timrx.live` keeps client_id continuous. The pending-
  conversion queue is also subdomain-agnostic — a user redirected via
  3d.timrx.live → timrx.live after Mollie will still see the queued event.

## Files in this implementation

```
Backend/meshy/
├── migrations/070_analytics_events.sql        # queue table
├── backend/services/analytics_events_service.py
├── backend/routes/analytics.py                # GET /pending, POST /ack
├── backend/services/mollie_service.py         # enqueue on credit grant
├── backend/routes/auth.py                     # enqueue on email verify
└── docs/GTM_SETUP.md                          # this file

Frontend/
├── js/analytics-config.js                     # IDs (GTM-TH8DB6S5 + AW-18162436469)
├── js/analytics.js                            # the only dataLayer wrapper
├── js/main.js                                 # imports analytics.js (workspace)
├── js/credits.js                              # begin_checkout forward
├── js/workspace-credits.js                    # emits timrx:identity:confirmed
└── *.html                                     # GTM snippet on every page
```

## Local validation cheat sheet

```bash
# Backend: parse + smoke-test
cd Backend/meshy && python3 -c "
import ast
for f in [
  'backend/services/analytics_events_service.py',
  'backend/routes/analytics.py',
  'backend/services/mollie_service.py',
  'backend/routes/auth.py',
]:
  ast.parse(open(f).read()); print('OK', f)
"

# Frontend: ES module check
cd Frontend && node --input-type=module --check < js/analytics.js && echo OK
```

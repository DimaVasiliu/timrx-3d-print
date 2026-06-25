# TimrX Smoke Tests

Automated tests for anonymous flows: health, jobs, and history.

## Test Coverage

| Category | Tests |
|----------|-------|
| **Health** | Health check |
| **Jobs** | Create text-to-3d job, poll status |
| **History** | List, add item, update, delete |

## Prerequisites

```bash
pip install requests python-dotenv
```

## Running Locally

### 1. Start the backend server

```bash
cd TimrX/Backend/meshy

# Bootstrap local config
cp .env.example .env

# Canonical local entrypoint (same app object used in deploy)
flask --app app_modular:app run --host 0.0.0.0 --port 5001
```

Alternative:

```bash
cd TimrX/Backend/meshy
python app_modular.py
```

### 2. Run tests against localhost

```bash
cd tests

# Default: runs against http://localhost:5001
python smoke_test.py

# Verbose mode (shows request details)
VERBOSE=1 python smoke_test.py

# View test plan only
python smoke_test.py --plan
```

## Running Against Render (Production)

```bash
cd TimrX/Backend/meshy/tests

# Run against Render deployment
API_BASE=https://timrx-3d-print-1.onrender.com python smoke_test.py

# With verbose output
API_BASE=https://timrx-3d-print-1.onrender.com VERBOSE=1 python smoke_test.py
```

## Expected Output

```
TimrX Smoke Tests
API Base: http://localhost:5001
Run ID: a1b2c3d4

============================================================
1. HEALTH CHECK
============================================================
  ✓ Health check

============================================================
2. JOB TESTS (Anonymous)
============================================================
  ✓ Create text-to-3d job
  ✓ Poll job status

============================================================
3. HISTORY TESTS (Anonymous)
============================================================
  ✓ List history
  ✓ Add history item
  ✓ List history
  ✓ Update history item

============================================================
4. CLEANUP
============================================================
  ✓ Delete history item

============================================================
SUMMARY
============================================================
  Total:  8
  Passed: 8
  Failed: 0

All tests passed!
```

## Environment Variables

Core local setup:

| Variable | Required? | Description |
|----------|-----------|-------------|
| `DATABASE_URL` | Recommended | Enables persistent jobs/history instead of degraded no-DB mode |
| `ALLOWED_ORIGINS` | Recommended | Keeps local frontend requests working explicitly |
| `ADMIN_TOKEN` or `ADMIN_EMAILS` | Recommended | Enables admin routes |
| `MESHY_API_KEY` | Required for Meshy generation | Main 3D generation provider |

Optional feature providers:

| Variable | Description |
|----------|-------------|
| `OPENAI_API_KEY` | Enables OpenAI-powered image/text features |
| `GEMINI_API_KEY` | Enables Gemini-backed image flows |
| `PIAPI_API_KEY` | Enables Seedance / PiAPI video flows |
| `GOOGLE_CLOUD_PROJECT` + `GOOGLE_APPLICATION_CREDENTIALS_JSON` | Required if `VIDEO_PROVIDER=vertex` |
| `MOLLIE_API_KEY` / `STRIPE_SECRET_KEY` | Required only for the payment provider you enable |
| `EMAIL_ENABLED`, `EMAIL_PROVIDER`, `SES_FROM_EMAIL`, `AWS_*` | Required if you want magic codes / receipts to send |

Service-level defaults and optional flags are documented in [`.env.example`](../.env.example).

Startup validation now warns about missing production-critical config such as database, admin auth, provider keys, payment setup, and webhook base URLs.

## Database Modes

The 3D backend now exposes its DB mode explicitly in:

- `/api/health`
- `/api/status`
- `/api/_mod/health`
- `/api/_mod/status`

If `DATABASE_URL` is missing, the service starts in **degraded** mode instead of pretending everything is fully available.

### Full mode

Use this for realistic local testing:

- persistent jobs/history across restarts
- durable worker + stale-job recovery
- session-backed identity, wallet, purchases, subscriptions
- magic-code auth and payment/webhook reconciliation
- community routes and DB-backed asset ownership checks

### Degraded mode

What local testers should expect without `DATABASE_URL`:

- jobs/history only use local per-process fallbacks where implemented
- restart-safe persistence and multi-worker visibility are disabled
- stale-job recovery, rescue loops, and durable worker leadership do not run
- session/email restore, wallet/billing, subscriptions, and payment reconciliation require the DB
- community routes and DB-backed asset proxy/ownership checks are unavailable

In degraded local mode, `job_store.json` is runtime state only. It is safe to delete when you want to clear local per-process job state; the backend recreates it automatically on the next local job write.

Health responses include a `database` object with:

- `mode`: `full` or `degraded`
- `reason`: why DB-backed mode is unavailable
- `disabled_capabilities`: the exact degraded-mode limitations

So if `/api/health` says `ok: true` but `database.mode: degraded`, the HTTP app is up and serving, but DB-backed behavior is intentionally limited.

| Variable | Default | Description |
|----------|---------|-------------|
| `API_BASE` | `http://localhost:5001` | Backend API URL |
| `VERBOSE` | `false` | Show detailed request/response info |

## Troubleshooting

### Tests fail with connection errors

1. Ensure the backend is running
2. Check the API_BASE URL is correct
3. For Render, wait for cold start (first request may timeout)

### Database errors

Ensure `DATABASE_URL` is set and the database has the required tables:
- `history_items`
- `active_jobs` (for job recovery)

Run `schema.sql` to create tables if needed.

## Canonical Entrypoint

The checked-in runtime entrypoint is:

```bash
gunicorn app_modular:app
```

Use `app_modular:app` for deploys, Flask local runs, and any smoke-test setup so all environments target the same Flask app object.

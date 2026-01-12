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

# Create/edit .env with required variables
# DATABASE_URL=postgresql://...
# MESHY_API_KEY=...

# Run the server
python app.py
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
API_BASE=https://timrx-3d-print.onrender.com python smoke_test.py

# With verbose output
API_BASE=https://timrx-3d-print.onrender.com VERBOSE=1 python smoke_test.py
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

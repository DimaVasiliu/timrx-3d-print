# Backend Migration Plan

## Current State: Hybrid Architecture
The codebase has two parallel systems:
- `app_modular.py` - canonical Flask runtime entrypoint used locally and in deploys
- `app.py.backup` (legacy monolith snapshot) - historical source used during migration
- `backend/` module - Modular structure (partially complete)

## Target State: Fully Modular
All runtime code flows through `app_modular.py`, with migrated logic living in `backend/`.

---

## Directory Structure

```
meshy/
├── app_modular.py              # Canonical Flask entrypoint (`app_modular:app`)
├── app.py.backup               # Legacy monolith snapshot kept for migration reference
├── backend/
│   ├── __init__.py             # Package init
│   ├── config.py               # ✅ DONE - Configuration
│   ├── db.py                   # ✅ DONE - Database utilities
│   ├── middleware.py           # ✅ DONE - Session/identity decorators
│   ├── emailer.py              # ✅ DONE - Email utilities
│   │
│   ├── utils/
│   │   ├── __init__.py         # ⏳ CREATED - Package init
│   │   ├── helpers.py          # ⏳ CREATED - Utility functions
│   │   └── error_handlers.py   # ⏳ CREATED - HTTP error handlers
│   │
│   ├── services/
│   │   ├── __init__.py         # ✅ DONE - Package init
│   │   ├── identity_service.py # ✅ DONE - Identity management
│   │   ├── wallet_service.py   # ✅ DONE - Wallet operations
│   │   ├── pricing_service.py  # ✅ DONE - Action pricing
│   │   ├── reservation_service.py # ✅ DONE - Credit reservations
│   │   ├── purchase_service.py # ✅ DONE - Purchase handling
│   │   ├── job_service.py      # ✅ DONE - Job dispatch (needs merge)
│   │   ├── magic_code_service.py # ✅ DONE - Magic codes
│   │   ├── mollie_service.py   # ✅ DONE - Payment gateway
│   │   ├── admin_service.py    # ✅ DONE - Admin operations
│   │   ├── email_service.py    # ✅ DONE - Email service
│   │   ├── s3_service.py       # ⏳ CREATED - S3 uploads
│   │   ├── history_service.py  # ⏳ CREATED - History items
│   │   ├── meshy_service.py    # ⏳ CREATED - Meshy API
│   │   ├── openai_service.py   # ⏳ CREATED - OpenAI API
│   │   ├── async_dispatch.py   # ⏳ CREATED - Background jobs
│   │   └── credits_helper.py   # ⏳ CREATED - Credit helpers
│   │
│   └── routes/
│       ├── __init__.py         # ✅ DONE - Blueprint registration
│       ├── me.py               # ✅ DONE - /api/me
│       ├── billing.py          # ✅ DONE - /api/billing
│       ├── auth.py             # ✅ DONE - /api/auth
│       ├── admin.py            # ✅ DONE - /api/admin
│       ├── jobs.py             # ✅ DONE - /api/jobs (needs merge)
│       ├── credits.py          # ✅ DONE - /api/credits
│       ├── text_to_3d.py       # ⏳ CREATED - /api/text-to-3d
│       ├── image_to_3d.py      # ⏳ CREATED - /api/image-to-3d
│       ├── mesh_operations.py  # ⏳ CREATED - /api/mesh
│       ├── image_gen.py        # ⏳ CREATED - /api/image
│       ├── history.py          # ⏳ CREATED - /api/history
│       ├── community.py        # ⏳ CREATED - /api/community
│       ├── assets.py           # ⏳ CREATED - /api/assets
│       ├── frontend.py         # ⏳ CREATED - Static files
│       └── health.py           # ⏳ CREATED - Health checks
```

Legend:
- ✅ DONE = Already implemented and working
- ⏳ CREATED = File created, awaiting code migration
- ❌ TODO = Not started

---

## Migration Phases

### Phase 1: Extract Pure Functions (No dependencies)
Priority: HIGH - Can be done immediately

| File | Status | Source Lines | Functions |
|------|--------|--------------|-----------|
| utils/helpers.py | ⏳ | ~200 | sanitize_filename, normalize_*, etc. |
| utils/error_handlers.py | ⏳ | ~80 | HTTP error handlers |
| services/s3_service.py | ⏳ | ~400 | S3 upload functions |

### Phase 2: Extract API Clients
Priority: HIGH - Required for routes

| File | Status | Source Lines | Functions |
|------|--------|--------------|-----------|
| services/meshy_service.py | ⏳ | ~300 | mesh_post, mesh_get, normalize_* |
| services/openai_service.py | ⏳ | ~100 | openai_image_generate |

### Phase 3: Extract Business Logic
Priority: MEDIUM - Core services

| File | Status | Source Lines | Functions |
|------|--------|--------------|-----------|
| services/async_dispatch.py | ⏳ | ~350 | _dispatch_*_async functions |
| services/credits_helper.py | ⏳ | ~200 | start_paid_job, finalize_*, etc. |
| services/history_service.py | ⏳ | ~400 | save_*_to_normalized_db |

### Phase 4: Extract Routes (One at a time)
Priority: MEDIUM - Can be done incrementally

| File | Status | Source Lines | Routes |
|------|--------|--------------|--------|
| routes/health.py | ⏳ | ~30 | /api/health, /api/db-check |
| routes/frontend.py | ⏳ | ~100 | Static file serving |
| routes/assets.py | ⏳ | ~200 | /api/assets/*, /api/proxy-glb |
| routes/community.py | ⏳ | ~350 | /api/community/* |
| routes/history.py | ⏳ | ~500 | /api/history/* |
| routes/image_gen.py | ⏳ | ~250 | /api/image/* |
| routes/text_to_3d.py | ⏳ | ~400 | /api/text-to-3d/* |
| routes/image_to_3d.py | ⏳ | ~350 | /api/image-to-3d/* |
| routes/mesh_operations.py | ⏳ | ~400 | /api/mesh/* |

### Phase 5: Merge Duplicates
Priority: LOW - After routes migrated

| Issue | Resolution |
|-------|------------|
| job_service.py duplication | Merge legacy monolith functions into backend/services/job_service.py |
| jobs.py route duplication | Remove duplicate routes from the legacy monolith snapshot |
| db connection duplication | Use backend/db.py exclusively |

### Phase 6: Final Cleanup
Priority: LOW - Last step

- Remove remaining migrated code from the legacy monolith snapshot
- Update imports throughout
- Test all routes work correctly
- `app_modular.py` remains the canonical runtime entrypoint

---

## How to Migrate a Function

1. **Copy** the function to its target file
2. **Update imports** in the new file
3. **Add export** to __init__.py if needed
4. **Test** the function works in isolation
5. **Update callers** to import from new location
6. **Remove** from the legacy monolith snapshot only after all callers updated
7. **Run tests** to verify nothing broke

---

## Files Created (Ready for Migration)

### Services (backend/services/)
- `s3_service.py` - S3 upload/download functions
- `history_service.py` - History item management
- `meshy_service.py` - Meshy API client
- `openai_service.py` - OpenAI API client
- `async_dispatch.py` - Background job processing
- `credits_helper.py` - Credit system helpers

### Routes (backend/routes/)
- `text_to_3d.py` - Text-to-3D generation routes
- `image_to_3d.py` - Image-to-3D generation routes
- `mesh_operations.py` - Mesh remesh/texture/rig routes
- `image_gen.py` - OpenAI image generation routes
- `history.py` - History CRUD routes
- `community.py` - Community feed routes
- `assets.py` - Asset download routes
- `frontend.py` - Static file serving
- `health.py` - Health check routes

### Utils (backend/utils/)
- `helpers.py` - Utility functions
- `error_handlers.py` - HTTP error handlers

---

## Notes

- Each file contains documentation of what needs to be migrated
- Source line numbers reference the legacy monolith snapshot (`app.py.backup`)
- Migration can be done incrementally - no big-bang required
- Existing routes in backend/ continue to work during migration

---

## Canonical Runtime Commands

Use the same app object everywhere:

```bash
# Local dev
flask --app app_modular:app run --host 0.0.0.0 --port 5001

# Deploy / Procfile
gunicorn app_modular:app --workers 2 --threads 4 --timeout 300
```

The runtime contract for this repo is now `app_modular:app`.

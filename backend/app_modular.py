"""Shim for Render/Gunicorn.

Allows `gunicorn app_modular:app` while the real app lives at
`backend.app_modular:app`.
"""

from backend.app_modular import app  # noqa: F401

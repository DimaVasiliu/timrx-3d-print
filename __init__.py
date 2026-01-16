"""
TimrX Backend Module
--------------------
Modular backend for anonymous-first credits + purchases system.

This package contains:
- config: Application configuration
- db: Database connection utilities
- emailer: Email sending utilities
- middleware: Session/identity decorators for routes
- routes/: Flask blueprints for API endpoints
- services/: Business logic services
"""

__version__ = "1.0.0"

# Convenient imports
from .config import config
from .routes import register_blueprints

"""
Config stub - re-exports from backend.config for import compatibility.

This file exists at the project root so that `from config import config`
works regardless of sys.path order. All actual configuration is in
backend/config.py.
"""

# Re-export everything from backend.config
from backend.config import *
from backend.config import config, Config

# Make config the default export
__all__ = ['config', 'Config']

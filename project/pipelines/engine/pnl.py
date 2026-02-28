"""Compatibility wrapper for the canonical engine pnl.

This module re-exports from `project/engine/pnl.py` to prevent logic drift
between duplicate engine implementations.
"""

from engine.pnl import *  # noqa: F401,F403

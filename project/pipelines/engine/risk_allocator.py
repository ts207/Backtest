"""Compatibility wrapper for the canonical engine risk allocator.

This module re-exports from `project/engine/risk_allocator.py` to prevent logic drift
between duplicate engine implementations.
"""

from engine.risk_allocator import *  # noqa: F401,F403

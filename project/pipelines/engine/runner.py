"""Compatibility wrapper for the canonical engine runner.

This module re-exports from `project/engine/runner.py` to prevent logic drift
between duplicate runner implementations.
"""

from engine.runner import *  # noqa: F401,F403

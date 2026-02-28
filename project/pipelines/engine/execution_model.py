"""Compatibility wrapper for the canonical engine execution model.

This module re-exports from `project/engine/execution_model.py` to prevent logic drift
between duplicate engine implementations.
"""

from engine.execution_model import *  # noqa: F401,F403

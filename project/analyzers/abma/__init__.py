"""Auction Boundary Microstructure Asymmetry (ABMA) analyzer package."""

from .config_v1 import ABMAConfig, DEFAULT_ABMA_CONFIG
from .controls import sample_matched_controls
from .evaluate import evaluate_stabilization
from .events import extract_auction_boundary_events
from .metrics import compute_structural_metrics
from .report import build_report_artifacts
from .schema import CONTROL_SUMMARY_COLUMNS, DELTA_COLUMNS, EVENT_COLUMNS, output_schema

__all__ = [
    "ABMAConfig",
    "DEFAULT_ABMA_CONFIG",
    "extract_auction_boundary_events",
    "sample_matched_controls",
    "compute_structural_metrics",
    "evaluate_stabilization",
    "build_report_artifacts",
    "CONTROL_SUMMARY_COLUMNS",
    "DELTA_COLUMNS",
    "EVENT_COLUMNS",
    "output_schema",
]

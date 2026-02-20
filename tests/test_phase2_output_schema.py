"""
Phase 2 output schema test.

Validates that every Phase 2 result CSV/parquet satisfies the data-semantic
constraints the user defined:
  - `condition` ∉ {mean_reversion, continuation, carry, breakout, ...}  (not a rule template)
  - `condition` ∉ strings containing "__"                               (no legacy format)
  - `conditioning` may contain severity_bucket_* or quantile_*          (allowed)
  - `rule_template` is from the known small enum
  - `condition_source` is from the allowed set
  - `compile_eligible` is boolean-coercible
  - `condition` is always executable OR is "all"/"__BLOCKED__"

These tests run against any Phase 2 CSV/parquet found under
data/reports/phase2/<run_id>/<event_type>/phase2_candidates.csv
to regression-test the schema contract after every discovery run.

When no real phase2 files exist, falls back to a tiny synthetic fixture.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1] / "project"
DATA_ROOT = Path(__file__).resolve().parents[1] / "data"
sys.path.insert(0, str(PROJECT_ROOT))

_RULE_TEMPLATE_ENUM = {"mean_reversion", "continuation", "carry", "breakout"}
_CONDITION_SOURCE_ENUM = {"runtime", "bucket_non_runtime", "unconditional", "permissive_fallback", "blocked"}

# ── Helpers ────────────────────────────────────────────────────────────────

def _collect_phase2_frames() -> list[pd.DataFrame]:
    """Collect all phase2_candidates.csv files for any known run IDs."""
    frames = []
    phase2_root = DATA_ROOT / "reports" / "phase2"
    if phase2_root.exists():
        for event_dir in phase2_root.rglob("phase2_candidates.csv"):
            try:
                df = pd.read_csv(event_dir)
                if not df.empty:
                    frames.append(df)
            except Exception:
                pass
    # Also check parquet
    for event_dir in phase2_root.rglob("*.parquet") if phase2_root.exists() else []:
        try:
            df = pd.read_parquet(event_dir)
            if not df.empty:
                frames.append(df)
        except Exception:
            pass
    return frames


def _synthetic_fixture() -> pd.DataFrame:
    """Minimal synthetic Phase 2 output covering all routing branches."""
    return pd.DataFrame([
        {
            "candidate_id": "LIQUIDATION_CASCADE_mean_reversion_5m_BTCUSDT_all",
            "rule_template": "mean_reversion",
            "conditioning": "all",
            "condition": "all",
            "condition_source": "unconditional",
            "compile_eligible": True,
        },
        {
            "candidate_id": "LIQUIDATION_CASCADE_continuation_5m_BTCUSDT_vol_regime_high",
            "rule_template": "continuation",
            "conditioning": "vol_regime_high",
            "condition": "vol_regime_high",
            "condition_source": "runtime",
            "compile_eligible": True,
        },
        {
            "candidate_id": "LIQUIDATION_CASCADE_mean_reversion_5m_BTCUSDT_severity_bucket_top_10pct",
            "rule_template": "mean_reversion",
            "conditioning": "severity_bucket_top_10pct",
            "condition": "all",
            "condition_source": "bucket_non_runtime",
            "compile_eligible": True,
        },
    ])


# ── Fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def phase2_frames():
    frames = _collect_phase2_frames()
    if not frames:
        return [_synthetic_fixture()]
    return frames


# ── Tests ──────────────────────────────────────────────────────────────────

class TestPhase2OutputSchema:
    """Phase 2 output schema / data-semantic constraints."""

    def test_condition_is_not_a_rule_template_name(self, phase2_frames):
        """'condition' must never equal a rule template name."""
        for df in phase2_frames:
            if "condition" not in df.columns:
                continue
            bad = df[df["condition"].str.lower().isin(_RULE_TEMPLATE_ENUM)]
            assert bad.empty, (
                f"Phase 2 output has {len(bad)} row(s) where 'condition' equals a rule template: "
                f"{bad['condition'].unique().tolist()}"
            )

    def test_condition_has_no_legacy_double_underscore(self, phase2_frames):
        """'condition' must not contain '__' in post-fix output (files with condition_source column)."""
        for df in phase2_frames:
            if "condition" not in df.columns:
                continue
            # Only assert on post-fix files that have condition_source column.
            # Pre-fix files may still have 'all__...' values from the old code.
            if "condition_source" not in df.columns:
                continue
            bad = df[
                df["condition"].str.contains("__", na=False)
                & (df["condition"] != "__BLOCKED__")  # BLOCKED sentinel is intentional
            ]
            assert bad.empty, (
                f"Phase 2 output (post-fix, has condition_source) has {len(bad)} row(s) where "
                f"'condition' contains legacy '__' format: {bad['condition'].unique().tolist()}"
            )

    def test_rule_template_column_is_from_enum(self, phase2_frames):
        """'rule_template' must be from the known enum."""
        for df in phase2_frames:
            if "rule_template" not in df.columns:
                continue
            values = set(df["rule_template"].dropna().str.lower().unique())
            unknown = values - _RULE_TEMPLATE_ENUM
            assert not unknown, (
                f"Phase 2 output has unknown rule_template values: {sorted(unknown)}"
            )

    def test_condition_source_is_from_enum_when_present(self, phase2_frames):
        """'condition_source', when present, must be from the allowed set."""
        for df in phase2_frames:
            if "condition_source" not in df.columns:
                continue  # column is new; older runs may not have it
            values = set(df["condition_source"].dropna().unique())
            unknown = values - _CONDITION_SOURCE_ENUM
            assert not unknown, (
                f"Phase 2 output has unknown condition_source values: {sorted(unknown)}"
            )

    def test_compile_eligible_when_present(self, phase2_frames):
        """'compile_eligible', when present, must be boolean-coercible."""
        for df in phase2_frames:
            if "compile_eligible" not in df.columns:
                continue
            try:
                df["compile_eligible"].fillna(True).astype(bool)
            except Exception as exc:
                pytest.fail(f"'compile_eligible' column cannot be coerced to bool: {exc}")

    def test_blocked_rows_never_have_runtime_source(self, phase2_frames):
        """Rows with condition='__BLOCKED__' must have condition_source='blocked'."""
        for df in phase2_frames:
            if "condition" not in df.columns or "condition_source" not in df.columns:
                continue
            blocked = df[df["condition"] == "__BLOCKED__"]
            if blocked.empty:
                continue
            wrong = blocked[blocked["condition_source"] != "blocked"]
            assert wrong.empty, (
                f"{len(wrong)} row(s) have condition='__BLOCKED__' but condition_source != 'blocked': "
                f"{wrong['condition_source'].unique().tolist()}"
            )

    def test_conditioning_column_may_contain_bucket_labels(self, phase2_frames):
        """'conditioning' may contain severity_bucket_* — this is expected and valid.

        In post-fix output (files with condition_source column), the corresponding
        'condition' field must be 'all' or '__BLOCKED__', never a vol/session condition.
        """
        for df in phase2_frames:
            if "conditioning" not in df.columns:
                continue
            mask = df["conditioning"].str.contains("severity_bucket_|quantile_", na=False, regex=True)
            if not mask.any():
                continue
            # Only enforce the routing invariant on post-fix files
            if "condition" in df.columns and "condition_source" in df.columns:
                bucket_rows = df[mask]
                bad = bucket_rows[~bucket_rows["condition"].isin(["all", "__BLOCKED__", ""])]
                assert bad.empty, (
                    f"Post-fix rows with severity_bucket/quantile conditioning must have "
                    f"condition='all' or '__BLOCKED__', found: "
                    f"{bad[['conditioning','condition']].head(5).to_dict(orient='records')}"
                )

    def test_runtime_condition_rows_have_executable_condition(self, phase2_frames):
        """Rows with condition_source='runtime' must have an executable condition string."""
        from strategy_dsl.contract_v1 import is_executable_condition
        for df in phase2_frames:
            if "condition" not in df.columns or "condition_source" not in df.columns:
                continue
            runtime_rows = df[df["condition_source"] == "runtime"]
            for _, row in runtime_rows.iterrows():
                cond = str(row.get("condition", ""))
                assert is_executable_condition(cond), (
                    f"Row with condition_source='runtime' has non-executable condition '{cond}' "
                    f"(candidate_id={row.get('candidate_id')})"
                )

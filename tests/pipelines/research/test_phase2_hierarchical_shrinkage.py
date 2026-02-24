from __future__ import annotations

import pandas as pd

from pipelines.research.phase2_candidate_discovery import _apply_hierarchical_shrinkage


def _base_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "BTCUSDT",
                "canonical_family": "POSITIONING_EXTREMES",
                "canonical_event_type": "FUNDING_EXTREME_ONSET",
                "runtime_event_type": "funding_extreme_onset",
                "event_type": "FUNDING_EXTREME_ONSET",
                "template_verb": "mean_reversion",
                "rule_template": "mean_reversion",
                "horizon": "15m",
                "state_id": "CROWDING_STATE",
                "conditioning": "all",
                "expectancy": 0.10,
                "p_value": 0.001,
                "n_events": 10,
                "sample_size": 10,
                "std_return": 0.12,
            },
            {
                "symbol": "BTCUSDT",
                "canonical_family": "POSITIONING_EXTREMES",
                "canonical_event_type": "FUNDING_EXTREME_ONSET",
                "runtime_event_type": "funding_extreme_onset",
                "event_type": "FUNDING_EXTREME_ONSET",
                "template_verb": "mean_reversion",
                "rule_template": "mean_reversion",
                "horizon": "15m",
                "state_id": "POST_EXTREME_CARRY_STATE",
                "conditioning": "all",
                "expectancy": 0.02,
                "p_value": 0.001,
                "n_events": 500,
                "sample_size": 500,
                "std_return": 0.08,
            },
            {
                "symbol": "BTCUSDT",
                "canonical_family": "POSITIONING_EXTREMES",
                "canonical_event_type": "LIQUIDATION_CASCADE",
                "runtime_event_type": "LIQUIDATION_CASCADE",
                "event_type": "LIQUIDATION_CASCADE",
                "template_verb": "mean_reversion",
                "rule_template": "mean_reversion",
                "horizon": "15m",
                "state_id": "POST_LIQUIDATION_STATE",
                "conditioning": "all",
                "expectancy": 0.01,
                "p_value": 0.001,
                "n_events": 700,
                "sample_size": 700,
                "std_return": 0.10,
            },
        ]
    )


def test_hierarchical_shrinkage_small_n_pools_toward_event_more():
    df = _base_rows()
    out = _apply_hierarchical_shrinkage(
        df,
        lambda_state=100.0,
        lambda_event=300.0,
        lambda_family=1000.0,
    )
    by_state = {row["state_id"]: row for row in out.to_dict(orient="records")}

    crowding = by_state["CROWDING_STATE"]
    post_carry = by_state["POST_EXTREME_CARRY_STATE"]

    # Small-N state should be strongly pooled.
    assert abs(crowding["effect_shrunk_state"] - crowding["effect_raw"]) > 1e-4
    assert crowding["shrinkage_weight_state"] < post_carry["shrinkage_weight_state"]

    # Large-N state should stay close to raw estimate.
    assert abs(post_carry["effect_shrunk_state"] - post_carry["effect_raw"]) < 0.01


def test_hierarchical_shrinkage_preserves_raw_and_adds_contract_columns():
    df = _base_rows()
    out = _apply_hierarchical_shrinkage(df)

    required = {
        "effect_raw",
        "effect_shrunk_family",
        "effect_shrunk_event",
        "effect_shrunk_state",
        "shrinkage_weight_family",
        "shrinkage_weight_event",
        "shrinkage_weight_state",
        "p_value_raw",
        "p_value_shrunk",
        "p_value_for_fdr",
    }
    missing = required - set(out.columns)
    assert not missing
    assert (out["effect_raw"] == df["expectancy"]).all()
    assert ((out["p_value_for_fdr"] >= 0.0) & (out["p_value_for_fdr"] <= 1.0)).all()


def test_shrunk_p_value_for_fdr_increases_when_small_n_effect_is_pooled_down():
    df = pd.DataFrame(
        [
            {
                "symbol": "BTCUSDT",
                "canonical_family": "LIQUIDITY_DISLOCATION",
                "canonical_event_type": "LIQUIDITY_VACUUM",
                "runtime_event_type": "LIQUIDITY_VACUUM",
                "event_type": "LIQUIDITY_VACUUM",
                "template_verb": "continuation",
                "rule_template": "continuation",
                "horizon": "5m",
                "state_id": "REFILL_LAG_STATE",
                "conditioning": "all",
                "expectancy": 0.50,
                "p_value": 0.0001,
                "n_events": 5,
                "sample_size": 5,
                "std_return": 0.10,
            },
            {
                "symbol": "BTCUSDT",
                "canonical_family": "LIQUIDITY_DISLOCATION",
                "canonical_event_type": "LIQUIDITY_VACUUM",
                "runtime_event_type": "LIQUIDITY_VACUUM",
                "event_type": "LIQUIDITY_VACUUM",
                "template_verb": "continuation",
                "rule_template": "continuation",
                "horizon": "5m",
                "state_id": "LOW_LIQUIDITY_STATE",
                "conditioning": "all",
                "expectancy": 0.00,
                "p_value": 0.5,
                "n_events": 1000,
                "sample_size": 1000,
                "std_return": 0.10,
            },
        ]
    )

    out = _apply_hierarchical_shrinkage(df, lambda_state=100.0, lambda_event=300.0, lambda_family=1000.0)
    pooled_row = out[out["n_events"] == 5].iloc[0]
    assert pooled_row["effect_shrunk_state"] < pooled_row["effect_raw"]
    assert pooled_row["p_value_shrunk"] != pooled_row["p_value_raw"]
    assert pooled_row["p_value_for_fdr"] == pooled_row["p_value_shrunk"]


def test_adaptive_lambda_single_state_uses_lambda_max():
    df = pd.DataFrame(
        [
            {
                "symbol": "BTCUSDT",
                "canonical_family": "FLOW_EXHAUSTION",
                "canonical_event_type": "FORCED_FLOW_EXHAUSTION",
                "runtime_event_type": "FORCED_FLOW_EXHAUSTION",
                "event_type": "FORCED_FLOW_EXHAUSTION",
                "template_verb": "exhaustion_reversal",
                "rule_template": "exhaustion_reversal",
                "horizon": "60m",
                "state_id": "EXHAUSTION_STATE",
                "conditioning": "all",
                "expectancy": 0.03,
                "p_value": 0.01,
                "n_events": 400,
                "sample_size": 400,
                "std_return": 0.11,
            }
        ]
    )
    out = _apply_hierarchical_shrinkage(df, adaptive_lambda=True, adaptive_lambda_max=5000.0)
    row = out.iloc[0]
    assert row["lambda_state_status"] in {"single_child", "no_state"}
    if row["lambda_state_status"] == "single_child":
        assert row["lambda_state"] == 5000.0


def test_adaptive_lambda_insufficient_data_skips_state_pooling():
    df = pd.DataFrame(
        [
            {
                "symbol": "BTCUSDT",
                "canonical_family": "LIQUIDITY_DISLOCATION",
                "canonical_event_type": "DEPTH_COLLAPSE",
                "runtime_event_type": "DEPTH_COLLAPSE",
                "event_type": "DEPTH_COLLAPSE",
                "template_verb": "mean_reversion",
                "rule_template": "mean_reversion",
                "horizon": "5m",
                "state_id": "LOW_LIQUIDITY_STATE",
                "conditioning": "all",
                "expectancy": 0.04,
                "p_value": 0.02,
                "n_events": 10,
                "sample_size": 10,
                "std_return": 0.12,
            },
            {
                "symbol": "BTCUSDT",
                "canonical_family": "LIQUIDITY_DISLOCATION",
                "canonical_event_type": "DEPTH_COLLAPSE",
                "runtime_event_type": "DEPTH_COLLAPSE",
                "event_type": "DEPTH_COLLAPSE",
                "template_verb": "mean_reversion",
                "rule_template": "mean_reversion",
                "horizon": "5m",
                "state_id": "REFILL_LAG_STATE",
                "conditioning": "all",
                "expectancy": -0.01,
                "p_value": 0.4,
                "n_events": 8,
                "sample_size": 8,
                "std_return": 0.10,
            },
        ]
    )
    out = _apply_hierarchical_shrinkage(
        df,
        adaptive_lambda=True,
        adaptive_lambda_min_total_samples=1000,
    )
    assert (out["lambda_state_status"] == "insufficient_data").all()
    assert (out["shrinkage_weight_state"] == 1.0).all()
    assert (out["effect_shrunk_state"] == out["effect_raw"]).all()

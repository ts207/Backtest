import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "project"))

from analyzers.abma_v2_funding import DEFAULT_ABMA_FUNDING_CONFIG, output_schema


def test_abma_v2_funding_constants_are_frozen() -> None:
    config = DEFAULT_ABMA_FUNDING_CONFIG
    assert config.def_version == "v2_funding"
    assert config.event_window_minutes == 30
    assert config.microprice_half_life_seconds == 60
    assert config.trade_sign_entropy_trades == 50
    assert config.control_set_size == 5
    assert config.bootstrap_samples == 1000


def test_abma_v2_funding_output_schema_matches_doc() -> None:
    schema = output_schema()
    assert set(schema.keys()) == {"event", "control_summary", "delta"}

    event_cols = set(schema["event"])
    control_cols = set(schema["control_summary"])
    delta_cols = set(schema["delta"])

    expected_event = {
        "event_id",
        "symbol",
        "session_date",
        "t0",
        "window_end",
        "abma_def_version",
        "spread_half_life_sec",
        "microprice_var_decay_rate",
        "trade_sign_entropy_mean",
        "controls_used",
    }
    expected_control = {
        "control_spread_half_life_mean",
        "control_microprice_var_decay_mean",
        "control_trade_sign_entropy_mean",
    }
    expected_delta = {
        "delta_spread_half_life_sec",
        "delta_microprice_var_decay_rate",
        "delta_trade_sign_entropy_mean",
    }

    assert expected_event == event_cols
    assert expected_control == control_cols
    assert expected_delta == delta_cols

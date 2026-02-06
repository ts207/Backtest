"""Output schema for ABMA Phase-1 analyzer (v2 funding)."""

EVENT_COLUMNS = [
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
]

CONTROL_SUMMARY_COLUMNS = [
    "control_spread_half_life_mean",
    "control_microprice_var_decay_mean",
    "control_trade_sign_entropy_mean",
]

DELTA_COLUMNS = [
    "delta_spread_half_life_sec",
    "delta_microprice_var_decay_rate",
    "delta_trade_sign_entropy_mean",
]


def output_schema() -> dict[str, list[str]]:
    return {
        "event": list(EVENT_COLUMNS),
        "control_summary": list(CONTROL_SUMMARY_COLUMNS),
        "delta": list(DELTA_COLUMNS),
    }

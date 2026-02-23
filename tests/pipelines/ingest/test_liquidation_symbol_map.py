"""
PR-5 / F-8: Liquidation symbol map validation.

Tests that _assert_cm_mapping_complete and _assert_events_per_symbol
raise ValueError with clear messages on bad input, and pass on good input.
"""
from __future__ import annotations

import pytest

from pipelines.ingest.ingest_binance_um_liquidation_snapshot import (
    CM_SYMBOL_MAP,
    _KNOWN_CM_CONTRACTS,
    _assert_cm_mapping_complete,
    _assert_events_per_symbol,
    _to_cm_contract,
)


# ---------------------------------------------------------------------------
# _assert_cm_mapping_complete
# ---------------------------------------------------------------------------

class TestAssertCmMappingComplete:
    def test_known_btc_variants_pass(self):
        _assert_cm_mapping_complete(["BTC", "BTCUSDT", "BTCUSD", "BTCUSD_PERP"])

    def test_known_eth_variants_pass(self):
        _assert_cm_mapping_complete(["ETH", "ETHUSDT", "ETHUSD", "ETHUSD_PERP"])

    def test_mixed_known_symbols_pass(self):
        _assert_cm_mapping_complete(["BTC", "ETH"])

    def test_empty_symbol_list_passes(self):
        # No symbols → no constraint to violate.
        _assert_cm_mapping_complete([])

    def test_sol_raises(self):
        """SOL has no CM perpetual; _to_cm_contract returns 'SOL' which is not in
        _KNOWN_CM_CONTRACTS.  Must raise ValueError before any network call."""
        with pytest.raises(ValueError, match="Symbol mapping validation failed"):
            _assert_cm_mapping_complete(["SOL"])

    def test_sol_error_includes_symbol_name(self):
        with pytest.raises(ValueError, match="SOL"):
            _assert_cm_mapping_complete(["SOL"])

    def test_unknown_symbol_raises(self):
        with pytest.raises(ValueError, match="Symbol mapping validation failed"):
            _assert_cm_mapping_complete(["XRP"])

    def test_mixed_known_and_unknown_raises(self):
        """Even one unmapped symbol in a larger list must trigger failure."""
        with pytest.raises(ValueError, match="SOL"):
            _assert_cm_mapping_complete(["BTC", "SOL", "ETH"])

    def test_error_includes_derived_contract(self):
        """Error message should show the (wrong) derived CM contract so users understand
        what _to_cm_contract returned."""
        with pytest.raises(ValueError, match="->"):
            _assert_cm_mapping_complete(["SOL"])

    def test_all_known_cm_contract_values_pass(self):
        """Every value in CM_SYMBOL_MAP should map to itself and pass."""
        _assert_cm_mapping_complete(list(CM_SYMBOL_MAP.values()))

    def test_known_contracts_set_is_nonempty(self):
        assert len(_KNOWN_CM_CONTRACTS) >= 2


# ---------------------------------------------------------------------------
# _assert_events_per_symbol
# ---------------------------------------------------------------------------

class TestAssertEventsPerSymbol:
    def test_all_symbols_have_events_passes(self):
        _assert_events_per_symbol({"BTC": 500, "ETH": 200})

    def test_single_symbol_with_events_passes(self):
        _assert_events_per_symbol({"BTC": 1})

    def test_empty_dict_raises(self):
        with pytest.raises(ValueError, match="empty"):
            _assert_events_per_symbol({})

    def test_single_symbol_zero_events_raises(self):
        with pytest.raises(ValueError, match="Per-symbol event validation failed"):
            _assert_events_per_symbol({"BTC": 0})

    def test_zero_events_error_includes_symbol_name(self):
        with pytest.raises(ValueError, match="ETH"):
            _assert_events_per_symbol({"BTC": 100, "ETH": 0})

    def test_one_zero_in_multi_symbol_raises(self):
        with pytest.raises(ValueError, match="Per-symbol event validation failed"):
            _assert_events_per_symbol({"BTC": 100, "ETH": 0})

    def test_all_zero_raises_and_lists_all(self):
        with pytest.raises(ValueError, match="2 symbol"):
            _assert_events_per_symbol({"BTC": 0, "ETH": 0})

    def test_boundary_one_event_passes(self):
        """Exactly 1 event is sufficient."""
        _assert_events_per_symbol({"BTC": 1})


# ---------------------------------------------------------------------------
# _to_cm_contract round-trip sanity (guards against regression in the mapper)
# ---------------------------------------------------------------------------

class TestToCmContract:
    @pytest.mark.parametrize("sym, expected", [
        ("BTC",         "BTCUSD_PERP"),
        ("BTCUSDT",     "BTCUSD_PERP"),
        ("BTCUSD",      "BTCUSD_PERP"),
        ("BTCUSD_PERP", "BTCUSD_PERP"),
        ("ETH",         "ETHUSD_PERP"),
        ("ETHUSDT",     "ETHUSD_PERP"),
        ("ETHUSD",      "ETHUSD_PERP"),
        ("ETHUSD_PERP", "ETHUSD_PERP"),
    ])
    def test_known_symbols_map_to_known_cm_contracts(self, sym, expected):
        assert _to_cm_contract(sym) == expected
        assert _to_cm_contract(sym) in _KNOWN_CM_CONTRACTS

    @pytest.mark.parametrize("sym", ["SOL", "XRP", "MATIC", "AVAX"])
    def test_unknown_symbols_produce_unmapped_contracts(self, sym):
        """Unknown symbols must NOT end up in _KNOWN_CM_CONTRACTS."""
        assert _to_cm_contract(sym) not in _KNOWN_CM_CONTRACTS

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Set

from pipelines._lib.io_utils import (
    choose_partition_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
)

CONDITION_KEY_ALIASES = {
    "funding_bps": "funding_rate_bps",
}

CANONICAL_EVENT_JOIN_KEYS = {
    "event_type",
    "timestamp",
    "enter_ts",
    "exit_ts",
    "event_id",
    "symbol",
    "run_id",
    "signal_column",
    "runtime_event_type",
    "canonical_event_type",
    "canonical_family",
}

SOFT_DEFAULT_CONDITION_KEYS = {
    "severity_bucket",
    "vol_regime",
    "carry_state",
    "funding_rate_bps",
    "funding_bps",
    "regime_vol_liquidity",
    "vpin",
}


def _norm(value: object) -> str:
    return str(value or "").strip()


def _collect_columns_from_partition_candidates(paths: Iterable[Path]) -> Set[str]:
    out: Set[str] = set()
    src = choose_partition_dir(list(paths))
    files = list_parquet_files(src) if src else []
    if not files:
        return out
    frame = read_parquet([files[0]])
    if frame.empty:
        return out
    out.update({_norm(col) for col in frame.columns if _norm(col)})
    return out


def _collect_columns_from_file_candidates(paths: Iterable[Path]) -> Set[str]:
    out: Set[str] = set()
    for path in paths:
        if not Path(path).exists():
            continue
        frame = read_parquet([Path(path)])
        if frame.empty:
            continue
        out.update({_norm(col) for col in frame.columns if _norm(col)})
        break
    return out


def load_symbol_joined_condition_contract(
    *,
    data_root: Path,
    run_id: str,
    symbol: str,
    timeframe: str = "5m",
) -> Dict[str, Set[str]]:
    keys: Set[str] = set()
    sym = _norm(symbol).upper()
    rid = _norm(run_id)
    tf = _norm(timeframe) or "5m"

    feature_paths = [
        run_scoped_lake_path(data_root, rid, "features", "perp", sym, tf, "features_v1"),
        Path(data_root) / "lake" / "features" / "perp" / sym / tf / "features_v1",
    ]
    feature_keys = _collect_columns_from_partition_candidates(feature_paths)
    keys.update(feature_keys)

    context_file_paths = [
        run_scoped_lake_path(data_root, rid, "context", "market_state", sym, f"{tf}.parquet"),
        Path(data_root) / "lake" / "context" / "market_state" / sym / f"{tf}.parquet",
    ]
    context_keys = _collect_columns_from_file_candidates(context_file_paths)
    keys.update(context_keys)

    keys.update(CANONICAL_EVENT_JOIN_KEYS)
    for alias, canonical in CONDITION_KEY_ALIASES.items():
        if canonical in keys:
            keys.add(alias)
    keys = {k for k in keys if k}
    return {
        "keys": keys,
        "feature_keys": feature_keys,
        "context_keys": context_keys,
        "event_keys": set(CANONICAL_EVENT_JOIN_KEYS),
    }


def load_symbol_joined_condition_keys(
    *,
    data_root: Path,
    run_id: str,
    symbol: str,
    timeframe: str = "5m",
    include_soft_defaults: bool = False,
) -> Set[str]:
    contract = load_symbol_joined_condition_contract(
        data_root=data_root,
        run_id=run_id,
        symbol=symbol,
        timeframe=timeframe,
    )
    keys = set(contract.get("keys", set()))
    if include_soft_defaults:
        keys.update(SOFT_DEFAULT_CONDITION_KEYS)
    return keys


def normalize_condition_keys(keys: Iterable[str]) -> Set[str]:
    out: Set[str] = set()
    for key in keys:
        token = _norm(key)
        if not token:
            continue
        out.add(token)
        out.add(token.lower())
        alias = CONDITION_KEY_ALIASES.get(token)
        if alias:
            out.add(alias)
            out.add(alias.lower())
        for alias_key, canonical in CONDITION_KEY_ALIASES.items():
            if token == canonical:
                out.add(alias_key)
                out.add(alias_key.lower())
    return out


def missing_condition_keys(required_keys: Iterable[str], available_keys: Iterable[str]) -> Set[str]:
    available_norm = normalize_condition_keys(available_keys)
    missing: Set[str] = set()
    for key in required_keys:
        token = _norm(key)
        if not token:
            continue
        if token in available_norm:
            continue
        if token.lower() in available_norm:
            continue
        alias = CONDITION_KEY_ALIASES.get(token, token)
        if alias in available_norm or alias.lower() in available_norm:
            continue
        missing.add(token)
    return missing


def format_available_key_sample(keys: Iterable[str], limit: int = 50) -> str:
    values = sorted({_norm(key) for key in keys if _norm(key)})
    if not values:
        return "<none>"
    if len(values) <= int(limit):
        return ", ".join(values)
    head = values[: int(limit)]
    return ", ".join(head) + f", ... (+{len(values) - int(limit)} more)"

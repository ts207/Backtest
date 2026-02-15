from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir, list_parquet_files, read_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from strategies.registry import list_strategies

EVENT_FAMILY_STRATEGY_ROUTING: Dict[str, Dict[str, str]] = {
    "vol_shock_relaxation": {
        "execution_family": "breakout_mechanics",
        "base_strategy": "vol_compression_v1",
    },
    "vol_aftershock_window": {
        "execution_family": "breakout_mechanics",
        "base_strategy": "vol_compression_v1",
    },
    "range_compression_breakout_window": {
        "execution_family": "breakout_mechanics",
        "base_strategy": "vol_compression_v1",
    },
    "liquidity_refill_lag_window": {
        "execution_family": "breakout_mechanics",
        "base_strategy": "liquidity_refill_lag_v1",
    },
    "liquidity_absence_window": {
        "execution_family": "breakout_mechanics",
        "base_strategy": "liquidity_absence_gate_v1",
    },
    "liquidity_vacuum": {
        "execution_family": "breakout_mechanics",
        "base_strategy": "liquidity_vacuum_v1",
    },
    "funding_extreme_reversal_window": {
        "execution_family": "carry_imbalance",
        "base_strategy": "funding_extreme_reversal_v1",
    },
    "directional_exhaustion_after_forced_flow": {
        "execution_family": "exhaustion_overshoot",
        "base_strategy": "forced_flow_exhaustion_v1",
    },
    "cross_venue_desync": {
        "execution_family": "spread_dislocation",
        "base_strategy": "cross_venue_desync_v1",
    },
}

BACKTEST_READY_BASE_STRATEGIES = set(list_strategies())


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        out = float(value)
        if pd.isna(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def _safe_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _candidate_index(candidate_id: str) -> int:
    if not candidate_id:
        return 0
    tail = candidate_id.rsplit("_", 1)[-1]
    return int(tail) if tail.isdigit() else 0


def _load_candidate_detail(source_path: Path, candidate_index: int) -> Dict[str, object]:
    if not source_path.exists():
        return {}
    try:
        if source_path.suffix.lower() == ".json":
            payload = json.loads(source_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                candidates = payload.get("candidates", [])
                if isinstance(candidates, list) and candidate_index < len(candidates) and isinstance(candidates[candidate_index], dict):
                    return dict(candidates[candidate_index])
                return payload if all(isinstance(v, (str, int, float, bool, type(None))) for v in payload.values()) else {}
            if isinstance(payload, list) and candidate_index < len(payload) and isinstance(payload[candidate_index], dict):
                return dict(payload[candidate_index])
        if source_path.suffix.lower() == ".csv":
            df = pd.read_csv(source_path)
            if df.empty:
                return {}
            if candidate_index >= len(df):
                candidate_index = len(df) - 1
            return df.iloc[int(candidate_index)].to_dict()
    except Exception:
        return {}
    return {}


def _sanitize_id(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", str(value).strip().lower()).strip("_")


def _symbol_scope_from_row(row: Dict[str, object], symbols: List[str]) -> Dict[str, object]:
    run_symbols = [str(s).strip().upper() for s in symbols if str(s).strip()]
    candidate_symbol = str(row.get("candidate_symbol", "")).strip().upper()
    if not candidate_symbol:
        raw_symbol = str(row.get("symbol", "")).strip().upper()
        if raw_symbol:
            candidate_symbol = raw_symbol
    if not candidate_symbol:
        condition = str(row.get("condition", "")).strip().lower()
        if condition.startswith("symbol_"):
            candidate_symbol = condition.removeprefix("symbol_").upper()
    if not candidate_symbol:
        candidate_symbol = run_symbols[0] if len(run_symbols) == 1 else "ALL"
    return {"candidate_symbol": candidate_symbol, "run_symbols": run_symbols}


def _risk_controls_from_action(action: str) -> Dict[str, object]:
    controls: Dict[str, object] = {
        "entry_delay_bars": 0,
        "size_scale": 1.0,
        "block_entries": False,
        "reentry_mode": "immediate",
    }
    if action.startswith("delay_"):
        controls["entry_delay_bars"] = _safe_int(action.split("_")[-1], 0)
        return controls
    if action.startswith("risk_throttle_"):
        controls["size_scale"] = _safe_float(action.split("_")[-1], 1.0)
        controls["block_entries"] = bool(controls["size_scale"] <= 0.0)
        return controls
    if action == "entry_gate_skip":
        controls["size_scale"] = 0.0
        controls["block_entries"] = True
        return controls
    if action == "reenable_at_half_life":
        controls["entry_delay_bars"] = 8
        controls["reentry_mode"] = "half_life"
        return controls
    return controls


def _route_event_family(event: str) -> Optional[Dict[str, str]]:
    key = str(event).strip().lower()
    return EVENT_FAMILY_STRATEGY_ROUTING.get(key)


def _manual_backtest_command_for_strategy(base_strategy: str, symbols_csv: str) -> str:
    if base_strategy in BACKTEST_READY_BASE_STRATEGIES:
        return (
            f"./.venv/bin/python project/pipelines/backtest/backtest_strategies.py "
            f"--run_id <manual_backtest_run_id> --symbols {symbols_csv} "
            f"--strategies {base_strategy} --force 1"
        )
    return (
        "Backtest adapter required for this strategy template. Implement/choose a supported strategy id, then run:\n"
        f"./.venv/bin/python project/pipelines/backtest/backtest_strategies.py "
        f"--run_id <manual_backtest_run_id> --symbols {symbols_csv} --strategies <strategy_id> --force 1"
    )


def _parse_symbol_scores(value: object) -> Dict[str, float]:
    if isinstance(value, dict):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except Exception:
            return {}
    out: Dict[str, float] = {}
    for symbol, score in parsed.items():
        symbol_key = str(symbol).strip().upper()
        if not symbol_key:
            continue
        out[symbol_key] = _safe_float(score, 0.0)
    return out


def _resolve_deployment_scope(
    candidate_symbol: str,
    run_symbols: List[str],
    symbol_scores: Dict[str, float],
    rollout_eligible: bool,
) -> Dict[str, object]:
    normalized_run_symbols = [str(symbol).strip().upper() for symbol in run_symbols if str(symbol).strip()]
    if not normalized_run_symbols:
        return {"deployment_type": "single_symbol", "deployment_symbols": [], "rollout_eligible": False}

    candidate_symbol = str(candidate_symbol).strip().upper()
    if candidate_symbol and candidate_symbol != "ALL":
        target = candidate_symbol if candidate_symbol in normalized_run_symbols else normalized_run_symbols[0]
        return {"deployment_type": "single_symbol", "deployment_symbols": [target], "rollout_eligible": False}

    if len(normalized_run_symbols) == 1:
        return {"deployment_type": "single_symbol", "deployment_symbols": normalized_run_symbols, "rollout_eligible": False}

    if rollout_eligible:
        return {
            "deployment_type": "multi_symbol",
            "deployment_symbols": normalized_run_symbols,
            "rollout_eligible": True,
        }

    best_symbol = normalized_run_symbols[0]
    if symbol_scores:
        ranked = sorted(
            ((symbol, score) for symbol, score in symbol_scores.items() if symbol in normalized_run_symbols),
            key=lambda item: item[1],
            reverse=True,
        )
        if ranked:
            best_symbol = ranked[0][0]
    return {"deployment_type": "single_symbol", "deployment_symbols": [best_symbol], "rollout_eligible": False}


def _build_edge_strategy_candidate(
    row: Dict[str, object],
    detail: Dict[str, object],
    symbols: List[str],
) -> Dict[str, object]:
    event = str(row.get("event", "")).strip()
    candidate_id = str(row.get("candidate_id", "")).strip()
    edge_score = _safe_float(row.get("edge_score"), 0.0)
    stability_proxy = _safe_float(row.get("stability_proxy"), 0.0)
    expectancy_per_trade = _safe_float(row.get("expectancy_per_trade"), _safe_float(row.get("expected_return_proxy"), 0.0))
    robustness_score = _safe_float(row.get("robustness_score"), stability_proxy)
    event_frequency = _safe_float(row.get("event_frequency"), 0.0)
    capacity_proxy = _safe_float(row.get("capacity_proxy"), 0.0)
    profit_density_score = _safe_float(
        row.get("profit_density_score"),
        max(0.0, expectancy_per_trade) * max(0.0, robustness_score) * max(0.0, event_frequency),
    )
    n_events = _safe_int(row.get("n_events"), 0)
    status = str(row.get("status", "PROMOTED")).strip().upper()

    condition = str(detail.get("condition", "all"))
    action = str(detail.get("action", "no_action"))
    selection_score = profit_density_score if profit_density_score > 0.0 else (0.65 * edge_score) + (0.35 * stability_proxy)
    controls = _risk_controls_from_action(action)
    route = _route_event_family(event)
    execution_family = route["execution_family"] if route else "unmapped"
    base_strategy = route["base_strategy"] if route else "unmapped"
    routing_reason = "" if route else f"Unknown event family `{event}`; no strategy routing is defined."
    backtest_ready = bool(route is not None and base_strategy in BACKTEST_READY_BASE_STRATEGIES)

    strategy_candidate_id = _sanitize_id(f"{event}_{condition}_{action}_{candidate_id}")
    symbols_csv = ",".join(symbols)
    manual_backtest_command = _manual_backtest_command_for_strategy(base_strategy, symbols_csv)
    symbol_scope = _symbol_scope_from_row(row=row, symbols=symbols)
    symbol_scores = _parse_symbol_scores(row.get("symbol_scores", {}))
    rollout_eligible = bool(row.get("rollout_eligible", False))
    deployment_scope = _resolve_deployment_scope(
        candidate_symbol=str(symbol_scope["candidate_symbol"]),
        run_symbols=symbol_scope["run_symbols"],
        symbol_scores=symbol_scores,
        rollout_eligible=rollout_eligible,
    )
    deployment_symbols = deployment_scope["deployment_symbols"]
    strategy_instances = [
        {
            "strategy_id": f"{base_strategy}_{symbol}",
            "base_strategy": base_strategy,
            "symbol": symbol,
            "strategy_params": {
                "promotion_thresholds": {
                    "edge_score": edge_score,
                    "expectancy_per_trade": expectancy_per_trade,
                    "stability_proxy": stability_proxy,
                    "robustness_score": robustness_score,
                    "event_frequency": event_frequency,
                    "capacity_proxy": capacity_proxy,
                    "profit_density_score": profit_density_score,
                    "selection_score": selection_score,
                    "symbol_score": _safe_float(symbol_scores.get(symbol), selection_score),
                },
                "risk_controls": controls,
                "condition": condition,
                "action": action,
            },
        }
        for symbol in deployment_symbols
    ]

    notes: List[str] = [
        f"Derived from promoted edge candidate {candidate_id} ({event}).",
        "Translate risk controls into strategy params/overlays before manual backtest execution.",
    ]
    if controls.get("block_entries"):
        notes.append("Action implies full entry block; use as rejection/guard condition, not a standalone trading strategy.")
    if not backtest_ready:
        if route is None:
            notes.append(routing_reason)
        else:
            notes.append(
                f"`{base_strategy}` is a strategy template. Add/choose a concrete backtest implementation before execution."
            )
    if symbol_scope["candidate_symbol"] == "ALL" and not deployment_scope["rollout_eligible"] and len(symbols) > 1:
        notes.append("Cross-symbol rollout disabled: scores are not similar enough across symbols.")

    return {
        "strategy_candidate_id": strategy_candidate_id,
        "source_type": "edge_candidate",
        "execution_family": execution_family,
        "base_strategy": base_strategy,
        "backtest_ready": backtest_ready,
        "backtest_ready_reason": routing_reason if route is None else "",
        "event": event,
        "condition": condition,
        "action": action,
        "status": status,
        "n_events": n_events,
        "edge_score": edge_score,
        "expectancy_per_trade": expectancy_per_trade,
        "stability_proxy": stability_proxy,
        "robustness_score": robustness_score,
        "event_frequency": event_frequency,
        "capacity_proxy": capacity_proxy,
        "profit_density_score": profit_density_score,
        "selection_score": selection_score,
        "symbols": symbols,
        "candidate_symbol": symbol_scope["candidate_symbol"],
        "run_symbols": symbol_scope["run_symbols"],
        "symbol_scores": symbol_scores,
        "rollout_eligible": deployment_scope["rollout_eligible"],
        "deployment_type": deployment_scope["deployment_type"],
        "deployment_symbols": deployment_symbols,
        "strategy_instances": strategy_instances,
        "risk_controls": controls,
        "manual_backtest_command": manual_backtest_command,
        "notes": notes,
    }


def _load_alpha_bundle_candidate(run_id: str, symbols: List[str]) -> Dict[str, object] | None:
    candidates = [
        DATA_ROOT / "feature_store" / "alpha_bundle" / run_id / "alpha_bundle_scores.parquet",
        DATA_ROOT / "feature_store" / "alpha_bundle" / "alpha_bundle_scores.parquet",
        DATA_ROOT / "feature_store" / "alpha_bundle" / "alpha_bundle_scores.csv",
    ]
    frame = pd.DataFrame()
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            if candidate.is_dir():
                frame = read_parquet(list_parquet_files(candidate))
            elif candidate.suffix.lower() == ".csv":
                frame = pd.read_csv(candidate)
            else:
                frame = read_parquet([candidate])
        except Exception:
            continue
        if not frame.empty:
            break
    if frame.empty:
        return None

    if "symbol" in frame.columns:
        frame = frame[frame["symbol"].astype(str).isin(symbols)].copy()
    if frame.empty:
        return None

    score_col = "score" if "score" in frame.columns else "score_pre_gate" if "score_pre_gate" in frame.columns else None
    if score_col is None:
        return None
    score = pd.to_numeric(frame[score_col], errors="coerce").dropna()
    if score.empty:
        return None

    symbols_csv = ",".join(symbols)
    alpha_strategy = "onchain_flow_v1"
    alpha_backtest_ready = alpha_strategy in BACKTEST_READY_BASE_STRATEGIES
    strategy_instances = [
        {
            "strategy_id": f"{alpha_strategy}_{symbol}",
            "base_strategy": alpha_strategy,
            "symbol": symbol,
            "strategy_params": {
                "promotion_thresholds": {"selection_score": float(score.abs().mean())},
                "risk_controls": {
                    "entry_delay_bars": 0,
                    "size_scale": 1.0,
                    "block_entries": False,
                    "reentry_mode": "immediate",
                },
                "condition": "cross_signal_composite",
                "action": "score_rank",
            },
        }
        for symbol in symbols
    ]
    return {
        "strategy_candidate_id": _sanitize_id(f"alpha_bundle_{run_id}"),
        "source_type": "alpha_bundle",
        "execution_family": "onchain_flow",
        "base_strategy": alpha_strategy,
        "backtest_ready": alpha_backtest_ready,
        "backtest_ready_reason": "" if alpha_backtest_ready else "On-chain strategy adapter required.",
        "event": "alpha_bundle",
        "condition": "cross_signal_composite",
        "action": "score_rank",
        "status": "PROMOTED",
        "n_events": int(len(score)),
        "edge_score": float(score.abs().mean()),
        "expectancy_per_trade": float(score.abs().mean()),
        "stability_proxy": float((score.abs() > score.abs().quantile(0.5)).mean()),
        "robustness_score": 1.0,
        "event_frequency": 1.0,
        "capacity_proxy": 0.0,
        "profit_density_score": float(score.abs().mean()),
        "selection_score": float(score.abs().mean()),
        "symbols": symbols,
        "candidate_symbol": "ALL" if len(symbols) > 1 else symbols[0],
        "run_symbols": symbols,
        "symbol_scores": {},
        "rollout_eligible": True if len(symbols) > 1 else False,
        "deployment_type": "multi_symbol" if len(symbols) > 1 else "single_symbol",
        "deployment_symbols": symbols,
        "strategy_instances": strategy_instances,
        "risk_controls": {
            "entry_delay_bars": 0,
            "size_scale": 1.0,
            "block_entries": False,
            "reentry_mode": "immediate",
        },
        "manual_backtest_command": _manual_backtest_command_for_strategy(alpha_strategy, symbols_csv),
        "notes": [
            "AlphaBundle candidate is routed to an on-chain execution template for manual validation.",
            "Keep AlphaBundle and mainline candidates under identical robustness and promotion standards.",
        ],
    }


def _render_summary_md(run_id: str, candidates: List[Dict[str, object]]) -> str:
    lines = [
        "# Strategy Candidate Selection",
        "",
        f"- Run ID: `{run_id}`",
        f"- Candidate count: `{len(candidates)}`",
        "",
        "## Ranked candidates",
        "",
        "| rank | strategy_candidate_id | source_type | event | action | selection_score | n_events |",
        "|---:|---|---|---|---|---:|---:|",
    ]
    for idx, item in enumerate(candidates, start=1):
        lines.append(
            f"| {idx} | `{item['strategy_candidate_id']}` | `{item['source_type']}` | "
            f"`{item['event']}` | `{item['action']}` | {item['selection_score']:.6f} | {int(item['n_events'])} |"
        )
    return "\n".join(lines) + "\n"


def _render_manual_instructions(run_id: str, symbols: List[str], candidates: List[Dict[str, object]]) -> str:
    symbols_csv = ",".join(symbols)
    lines = [
        "# Manual Backtest Instructions",
        "",
        f"Run ID: `{run_id}`",
        f"Symbols: `{symbols_csv}`",
        "",
        "## 1) Review strategy candidates",
        f"- Open `data/reports/strategy_builder/{run_id}/strategy_candidates.json`.",
        "- Select candidate IDs for manual backtest translation.",
        "",
        "## 2) Run manual backtest command template",
        "```bash",
        "./.venv/bin/python project/pipelines/backtest/backtest_strategies.py \\",
        "  --run_id <manual_backtest_run_id> \\",
        f"  --symbols {symbols_csv} \\",
        "  --strategies vol_compression_v1 \\",
        "  --force 1",
        "```",
        "",
        "## 3) Candidate-specific notes",
    ]
    for item in candidates:
        notes = item.get("notes", [])
        lines.append(f"- `{item['strategy_candidate_id']}`:")
        for note in notes[:3]:
            lines.append(f"  - {note}")
        lines.append(f"  - Base strategy template: `{item.get('base_strategy', 'unknown')}`")
        lines.append(f"  - Backtest ready now: `{bool(item.get('backtest_ready', False))}`")
    lines.append("")
    lines.append("## 4) Acceptance checklist")
    lines.append("- Verify the translated strategy matches condition/action intent from the source edge.")
    lines.append("- Compare outcomes across candidates using identical fees/slippage assumptions.")
    lines.append("- Keep AlphaBundle and mainline candidates under the same promotion criteria.")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build manual-backtest strategy candidates from promoted edges (multi-symbol aware)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True, help="Comma-separated discovery symbols sharing one run_id")
    parser.add_argument("--top_k_per_event", type=int, default=2)
    parser.add_argument("--max_candidates", type=int, default=20)
    parser.add_argument("--min_edge_score", type=float, default=0.0)
    parser.add_argument("--include_alpha_bundle", type=int, default=1)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    symbols = []
    seen_symbols = set()
    for raw_symbol in str(args.symbols).split(","):
        symbol = raw_symbol.strip().upper()
        if not symbol or symbol in seen_symbols:
            continue
        symbols.append(symbol)
        seen_symbols.add(symbol)
    if not symbols:
        print("--symbols must include at least one symbol", file=sys.stderr)
        return 1
    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "strategy_builder" / args.run_id
    ensure_dir(out_dir)

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    params = {
        "run_id": args.run_id,
        "symbols": symbols,
        "top_k_per_event": int(args.top_k_per_event),
        "max_candidates": int(args.max_candidates),
        "min_edge_score": float(args.min_edge_score),
        "include_alpha_bundle": int(args.include_alpha_bundle),
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("build_strategy_candidates", args.run_id, params, inputs, outputs)

    try:
        edge_csv = DATA_ROOT / "reports" / "edge_candidates" / args.run_id / "edge_candidates_normalized.csv"
        inputs.append({"path": str(edge_csv), "rows": None, "start_ts": None, "end_ts": None})

        if edge_csv.exists():
            edge_df = pd.read_csv(edge_csv)
        else:
            edge_df = pd.DataFrame(
                columns=[
                    "run_id",
                    "candidate_symbol",
                    "run_symbols",
                    "event",
                    "candidate_id",
                    "status",
                    "edge_score",
                    "expected_return_proxy",
                    "expectancy_per_trade",
                    "stability_proxy",
                    "robustness_score",
                    "event_frequency",
                    "capacity_proxy",
                    "profit_density_score",
                    "n_events",
                    "source_path",
                ]
            )

        if not edge_df.empty:
            edge_df["edge_score"] = pd.to_numeric(edge_df["edge_score"], errors="coerce").fillna(0.0)
            edge_df["stability_proxy"] = pd.to_numeric(edge_df["stability_proxy"], errors="coerce").fillna(0.0)
            edge_df = edge_df[edge_df["edge_score"] >= float(args.min_edge_score)].copy()

        strategy_rows: List[Dict[str, object]] = []
        if not edge_df.empty:
            edge_df["expectancy_per_trade"] = pd.to_numeric(edge_df.get("expectancy_per_trade"), errors="coerce").fillna(
                pd.to_numeric(edge_df.get("expected_return_proxy"), errors="coerce").fillna(0.0)
            )
            edge_df["robustness_score"] = pd.to_numeric(edge_df.get("robustness_score"), errors="coerce").fillna(edge_df["stability_proxy"])
            edge_df["event_frequency"] = pd.to_numeric(edge_df.get("event_frequency"), errors="coerce").fillna(0.0)
            edge_df["profit_density_score"] = pd.to_numeric(edge_df.get("profit_density_score"), errors="coerce")
            fallback_pds = (
                edge_df["expectancy_per_trade"].clip(lower=0.0)
                * edge_df["robustness_score"].clip(lower=0.0)
                * edge_df["event_frequency"].clip(lower=0.0)
            )
            edge_df["profit_density_score"] = edge_df["profit_density_score"].fillna(fallback_pds)
            edge_df["selection_score"] = edge_df["profit_density_score"]
            edge_df.loc[edge_df["selection_score"] <= 0.0, "selection_score"] = (
                0.65 * edge_df["edge_score"] + 0.35 * edge_df["stability_proxy"]
            )
            edge_df = edge_df.sort_values(["event", "selection_score"], ascending=[True, False]).reset_index(drop=True)

            for event, group in edge_df.groupby("event", sort=True):
                selected = group.head(int(args.top_k_per_event))
                for _, row in selected.iterrows():
                    source_path = Path(str(row.get("source_path", "")))
                    detail = _load_candidate_detail(source_path=source_path, candidate_index=_candidate_index(str(row.get("candidate_id", ""))))
                    strategy_rows.append(_build_edge_strategy_candidate(row=row.to_dict(), detail=detail, symbols=symbols))

        strategy_rows = sorted(strategy_rows, key=lambda x: float(x["selection_score"]), reverse=True)
        strategy_rows = strategy_rows[: int(args.max_candidates)]

        if int(args.include_alpha_bundle):
            alpha_candidate = _load_alpha_bundle_candidate(run_id=args.run_id, symbols=symbols)
            if alpha_candidate is not None:
                strategy_rows.append(alpha_candidate)

        strategy_rows = sorted(strategy_rows, key=lambda x: float(x["selection_score"]), reverse=True)
        for rank, row in enumerate(strategy_rows, start=1):
            row["rank"] = rank

        out_json = out_dir / "strategy_candidates.json"
        out_deploy = out_dir / "deployment_manifest.json"
        out_md = out_dir / "selection_summary.md"
        out_manual = out_dir / "manual_backtest_instructions.md"

        out_json.write_text(json.dumps(strategy_rows, indent=2), encoding="utf-8")
        deployment_manifest = {
            "run_id": args.run_id,
            "strategies": [
                {
                    "strategy_candidate_id": row["strategy_candidate_id"],
                    "base_strategy": row["base_strategy"],
                    "deployment_type": row.get("deployment_type", "single_symbol"),
                    "deployment_symbols": row.get("deployment_symbols", []),
                    "strategy_instances": row.get("strategy_instances", []),
                }
                for row in strategy_rows
            ],
        }
        out_deploy.write_text(json.dumps(deployment_manifest, indent=2), encoding="utf-8")
        out_md.write_text(_render_summary_md(run_id=args.run_id, candidates=strategy_rows), encoding="utf-8")
        out_manual.write_text(
            _render_manual_instructions(run_id=args.run_id, symbols=symbols, candidates=strategy_rows),
            encoding="utf-8",
        )

        outputs.append({"path": str(out_json), "rows": int(len(strategy_rows)), "start_ts": None, "end_ts": None})
        outputs.append({"path": str(out_deploy), "rows": int(len(strategy_rows)), "start_ts": None, "end_ts": None})
        outputs.append({"path": str(out_md), "rows": int(len(strategy_rows)), "start_ts": None, "end_ts": None})
        outputs.append({"path": str(out_manual), "rows": int(len(strategy_rows)), "start_ts": None, "end_ts": None})

        finalize_manifest(
            manifest,
            "success",
            stats={
                "strategy_candidate_count": int(len(strategy_rows)),
                "edge_rows_seen": int(len(edge_df)),
                "included_alpha_bundle": bool(int(args.include_alpha_bundle)),
            },
        )
        return 0
    except Exception as exc:
        logging.exception("Strategy candidate build failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())

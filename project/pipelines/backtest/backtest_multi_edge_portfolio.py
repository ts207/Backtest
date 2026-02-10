from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from engine.runner import _load_symbol_data, _strategy_returns
from pipelines._lib.config import load_configs
from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.universe import compute_monthly_top_n_symbols, resolve_requested_symbols
from portfolio.edge_contract import EdgeContractValidationError, load_approved_edge_contracts
from portfolio.multi_edge_allocator import PortfolioConstraints, run_multi_edge_allocation
from strategies.overlay_registry import apply_overlay


def _mode_list(raw: str, default_modes: str) -> List[str]:
    value = raw.strip() if str(raw).strip() else default_modes
    return [x.strip() for x in value.split(",") if x.strip()]


def _build_edge_frames(
    *,
    run_id: str,
    symbols: List[str],
    edges: List[Dict[str, object]],
    base_params: Dict[str, object],
    cost_bps: float,
) -> Dict[str, pd.DataFrame]:
    edge_frames: Dict[str, pd.DataFrame] = {}

    for edge in edges:
        edge_id = str(edge["edge_id"])
        strategy_name = str(edge["strategy"])

        edge_params = dict(base_params)
        edge_params.update(dict(edge.get("params", {})))

        overlays = [str(o).strip() for o in edge.get("overlays", []) if str(o).strip()]
        for overlay_name in overlays:
            edge_params = apply_overlay(overlay_name, edge_params)

        symbol_frames: List[pd.DataFrame] = []
        for symbol in symbols:
            try:
                bars, features = _load_symbol_data(DATA_ROOT, symbol, run_id=run_id)
            except Exception as exc:  # pragma: no cover - defensive guard
                logging.warning("Skipping %s for edge %s: %s", symbol, edge_id, exc)
                continue
            result = _strategy_returns(symbol, bars, features, strategy_name, edge_params, cost_bps)
            frame = result.data.copy()
            frame["edge_id"] = edge_id
            symbol_frames.append(frame)

        if symbol_frames:
            combined = pd.concat(symbol_frames, ignore_index=True).sort_values(["timestamp", "symbol"]).reset_index(drop=True)
        else:
            combined = pd.DataFrame(columns=["timestamp", "symbol", "pos", "ret", "pnl", "edge_id"])
        edge_frames[edge_id] = combined
    return edge_frames


def _build_edge_matrices(
    edge_frames: Dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
    edge_returns_series = {}
    all_symbols: set[str] = set()

    for edge_id, frame in edge_frames.items():
        if frame.empty:
            edge_returns_series[edge_id] = pd.Series(dtype=float)
            continue
        grouped = frame.groupby("timestamp", sort=True)["pnl"].sum()
        edge_returns_series[edge_id] = grouped
        all_symbols.update(frame["symbol"].astype(str).unique().tolist())

    edge_returns = pd.concat(edge_returns_series, axis=1).fillna(0.0)
    edge_returns = edge_returns.sort_index()

    edge_symbol_positions: Dict[str, pd.DataFrame] = {}
    edge_symbol_pnl: Dict[str, pd.DataFrame] = {}
    for symbol in sorted(all_symbols):
        pos_map = {}
        pnl_map = {}
        for edge_id, frame in edge_frames.items():
            subset = frame[frame["symbol"] == symbol]
            if subset.empty:
                pos_map[edge_id] = pd.Series(dtype=float)
                pnl_map[edge_id] = pd.Series(dtype=float)
                continue
            grouped = subset.groupby("timestamp", sort=True)
            pos_map[edge_id] = grouped["pos"].last()
            pnl_map[edge_id] = grouped["pnl"].sum()

        pos_df = pd.concat(pos_map, axis=1).reindex(edge_returns.index).fillna(0.0)
        pnl_df = pd.concat(pnl_map, axis=1).reindex(edge_returns.index).fillna(0.0)
        edge_symbol_positions[symbol] = pos_df
        edge_symbol_pnl[symbol] = pnl_df

    return edge_returns, edge_symbol_positions, edge_symbol_pnl


def _edge_contribution(weights: pd.DataFrame, edge_returns: pd.DataFrame) -> pd.DataFrame:
    if weights.empty or edge_returns.empty:
        return pd.DataFrame(columns=["edge_id", "total_pnl", "mean_weight", "marginal_sharpe"])

    w = weights.set_index("timestamp").reindex(edge_returns.index).fillna(0.0)
    w = w.reindex(columns=edge_returns.columns).fillna(0.0)
    weighted = w * edge_returns

    rows = []
    for edge_id in edge_returns.columns:
        pnl = weighted[edge_id]
        std = float(pnl.std()) if len(pnl) else 0.0
        sharpe = float((pnl.mean() / std) * (365 * 24 * 4) ** 0.5) if std > 0 else 0.0
        rows.append(
            {
                "edge_id": edge_id,
                "total_pnl": float(pnl.sum()),
                "mean_weight": float(w[edge_id].mean()) if len(w) else 0.0,
                "marginal_sharpe": sharpe,
            }
        )
    return pd.DataFrame(rows).sort_values("total_pnl", ascending=False).reset_index(drop=True)


def _symbol_contribution(
    *,
    weights: pd.DataFrame,
    edge_returns: pd.DataFrame,
    edge_symbol_pnl: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    if weights.empty or edge_returns.empty or not edge_symbol_pnl:
        return pd.DataFrame(columns=["symbol", "total_pnl", "positive_days_ratio"])

    w = weights.set_index("timestamp").reindex(edge_returns.index).fillna(0.0)
    w = w.reindex(columns=edge_returns.columns).fillna(0.0)

    rows = []
    for symbol, pnl_df in edge_symbol_pnl.items():
        aligned = pnl_df.reindex(index=edge_returns.index, columns=edge_returns.columns).fillna(0.0)
        contrib = (w * aligned).sum(axis=1)
        pos_ratio = float((contrib > 0).mean()) if len(contrib) else 0.0
        rows.append(
            {
                "symbol": symbol,
                "total_pnl": float(contrib.sum()),
                "positive_days_ratio": pos_ratio,
            }
        )
    return pd.DataFrame(rows).sort_values("total_pnl", ascending=False).reset_index(drop=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backtest multi-edge portfolio")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", default="TOP10")
    parser.add_argument("--modes", default="")
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    config_paths = [
        str(PROJECT_ROOT / "configs" / "pipeline.yaml"),
        str(PROJECT_ROOT / "configs" / "fees.yaml"),
        str(PROJECT_ROOT / "configs" / "portfolio.yaml"),
    ]
    config_paths.extend(args.config)
    config = load_configs(config_paths)

    portfolio_cfg = dict(config.get("multi_edge_portfolio", {}))
    constraints_cfg = dict(portfolio_cfg.get("constraints", {}))
    allocator_cfg = dict(portfolio_cfg.get("allocator", {}))
    universe_cfg = dict(portfolio_cfg.get("universe", {}))

    fee_bps = float(config.get("fee_bps_per_side", 4.0))
    slippage_bps = float(config.get("slippage_bps_per_fill", 2.0))
    cost_bps = fee_bps + slippage_bps

    base_params = {
        "trade_day_timezone": str(config.get("trade_day_timezone", "UTC")),
        "one_trade_per_day": True,
    }

    requested_symbols = resolve_requested_symbols(
        args.symbols,
        data_root=DATA_ROOT,
        run_id=args.run_id,
        seed_symbols=universe_cfg.get("seed_symbols", []),
    )
    if not requested_symbols:
        logging.error("No symbols available for multi-edge backtest")
        return 1

    modes = _mode_list(args.modes, str(allocator_cfg.get("default_modes", "equal_risk,score_weighted,constrained_optimizer")))
    if not modes:
        logging.error("No allocation modes selected")
        return 1

    edges_cfg = portfolio_cfg.get("edges", [])
    try:
        approved_edges = load_approved_edge_contracts(edges_cfg)
    except EdgeContractValidationError as exc:
        logging.error("Edge contract validation failed: %s", exc)
        return 1

    params = {
        "run_id": args.run_id,
        "symbols": requested_symbols,
        "modes": modes,
        "constraints": constraints_cfg,
        "objective": portfolio_cfg.get("objective", {}),
        "cost_bps": cost_bps,
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("backtest_multi_edge_portfolio", args.run_id, params, inputs, outputs)
    stats: Dict[str, object] = {}

    out_dir = DATA_ROOT / "lake" / "trades" / "backtests" / "multi_edge_portfolio" / args.run_id
    report_dir = DATA_ROOT / "reports" / "multi_edge_portfolio" / args.run_id
    ensure_dir(out_dir)
    ensure_dir(report_dir)

    if not args.force and (out_dir / "metrics.json").exists():
        finalize_manifest(manifest, "success", stats={"skipped": True})
        return 0

    try:
        edge_frames = _build_edge_frames(
            run_id=args.run_id,
            symbols=requested_symbols,
            edges=approved_edges,
            base_params=base_params,
            cost_bps=cost_bps,
        )
        edge_returns, edge_symbol_positions, edge_symbol_pnl = _build_edge_matrices(edge_frames)

        if edge_returns.empty:
            raise ValueError("No edge returns generated; ensure run data exists for requested symbols")

        start_ts = pd.Timestamp(edge_returns.index.min())
        end_ts = pd.Timestamp(edge_returns.index.max())
        top_n = int(universe_cfg.get("top_n", 10))
        lookback_days = int(universe_cfg.get("lookback_days", 90))
        monthly_top_symbols = compute_monthly_top_n_symbols(
            data_root=DATA_ROOT,
            run_id=args.run_id,
            symbols=requested_symbols,
            top_n=top_n,
            lookback_days=lookback_days,
            start_ts=start_ts,
            end_ts=end_ts,
            fallback_seed=universe_cfg.get("seed_symbols", []),
        )

        constraints = PortfolioConstraints(
            max_drawdown_pct=float(constraints_cfg.get("max_drawdown_pct", 0.20)),
            cvar_1d_99_pct=float(constraints_cfg.get("cvar_1d_99_pct", 0.025)),
            gross_exposure_max=float(constraints_cfg.get("gross_exposure_max", 1.50)),
            net_exposure_max=float(constraints_cfg.get("net_exposure_max", 0.60)),
            single_symbol_weight_max=float(constraints_cfg.get("single_symbol_weight_max", 0.20)),
            single_edge_risk_contrib_max=float(constraints_cfg.get("single_edge_risk_contrib_max", 0.25)),
            turnover_budget_daily=float(constraints_cfg.get("turnover_budget_daily", 0.30)),
        )

        mode_metrics: Dict[str, Dict[str, object]] = {}
        selected_mode = None
        selected_score = None

        for mode in modes:
            alloc = run_multi_edge_allocation(
                edge_returns=edge_returns,
                edge_symbol_positions=edge_symbol_positions,
                monthly_allowed_symbols=monthly_top_symbols,
                constraints=constraints,
                mode=mode,
                lookback_bars=int(allocator_cfg.get("lookback_bars", 384)),
                return_tilt=float(allocator_cfg.get("return_tilt", 0.35)),
            )

            contribution_df = _edge_contribution(alloc.weights, edge_returns)
            symbol_contrib_df = _symbol_contribution(weights=alloc.weights, edge_returns=edge_returns, edge_symbol_pnl=edge_symbol_pnl)

            portfolio_path = out_dir / f"portfolio_returns_{mode}.csv"
            weights_path = out_dir / f"edge_weights_{mode}.csv"
            exposure_path = out_dir / f"symbol_exposures_{mode}.csv"
            contribution_path = out_dir / f"edge_contribution_{mode}.csv"
            symbol_contrib_path = out_dir / f"symbol_contribution_{mode}.csv"
            metrics_path = out_dir / f"metrics_{mode}.json"

            alloc.portfolio.to_csv(portfolio_path, index=False)
            alloc.weights.to_csv(weights_path, index=False)
            alloc.symbol_exposures.to_csv(exposure_path, index=False)
            contribution_df.to_csv(contribution_path, index=False)
            symbol_contrib_df.to_csv(symbol_contrib_path, index=False)

            mode_payload = dict(alloc.metrics)
            mode_payload["paths"] = {
                "portfolio": str(portfolio_path),
                "weights": str(weights_path),
                "symbol_exposures": str(exposure_path),
                "edge_contribution": str(contribution_path),
                "symbol_contribution": str(symbol_contrib_path),
            }
            unique_days = max(1, alloc.portfolio["timestamp"].astype(str).str[:10].nunique()) if not alloc.portfolio.empty else 1
            mode_payload["estimated_cost_drag"] = float(mode_payload.get("avg_daily_turnover", 0.0)) * (cost_bps / 10000.0) * unique_days
            if not alloc.weights.empty and not alloc.portfolio.empty:
                weighted = (
                    alloc.weights.set_index("timestamp")
                    .reindex(edge_returns.index)
                    .fillna(0.0)
                    .reindex(columns=edge_returns.columns)
                    * edge_returns
                )
                edge_std_sum = float(weighted.std().sum()) if not weighted.empty else 0.0
                portfolio_std = float(alloc.portfolio["portfolio_pnl"].std()) if len(alloc.portfolio) else 0.0
                mode_payload["diversification_benefit"] = float(1.0 - (portfolio_std / edge_std_sum)) if edge_std_sum > 0 else 0.0
            else:
                mode_payload["diversification_benefit"] = 0.0
            metrics_path.write_text(json.dumps(mode_payload, indent=2, sort_keys=True), encoding="utf-8")

            outputs.extend(
                [
                    {"path": str(portfolio_path), "rows": int(len(alloc.portfolio)), "start_ts": None, "end_ts": None},
                    {"path": str(weights_path), "rows": int(len(alloc.weights)), "start_ts": None, "end_ts": None},
                    {"path": str(exposure_path), "rows": int(len(alloc.symbol_exposures)), "start_ts": None, "end_ts": None},
                    {"path": str(contribution_path), "rows": int(len(contribution_df)), "start_ts": None, "end_ts": None},
                    {"path": str(symbol_contrib_path), "rows": int(len(symbol_contrib_df)), "start_ts": None, "end_ts": None},
                    {"path": str(metrics_path), "rows": 1, "start_ts": None, "end_ts": None},
                ]
            )

            mode_metrics[mode] = mode_payload
            pass_bonus = 1 if mode_payload.get("constraints_pass") else 0
            score = (pass_bonus, float(mode_payload.get("net_total_return", 0.0)))
            if selected_score is None or score > selected_score:
                selected_score = score
                selected_mode = mode

        summary = {
            "run_id": args.run_id,
            "objective": portfolio_cfg.get("objective", {"target_metric": "net_total_return"}),
            "universe": {
                "requested": args.symbols,
                "resolved_symbols": requested_symbols,
                "monthly_top_symbols": monthly_top_symbols,
                "lookback_days": lookback_days,
                "top_n": top_n,
            },
            "constraints": constraints_cfg,
            "edge_ids": [str(edge["edge_id"]) for edge in approved_edges],
            "modes": mode_metrics,
            "selected_mode": selected_mode,
        }

        metrics_out = out_dir / "metrics.json"
        metrics_out.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        outputs.append({"path": str(metrics_out), "rows": 1, "start_ts": None, "end_ts": None})

        report_lines = [
            "# Multi-Edge Portfolio Report",
            "",
            f"Run ID: `{args.run_id}`",
            f"Selected mode: `{selected_mode}`",
            "",
            "## Modes",
        ]
        for mode, payload in mode_metrics.items():
            report_lines.append(
                f"- {mode}: net_total_return={payload.get('net_total_return', 0.0):.2%}, "
                f"max_drawdown={payload.get('max_drawdown', 0.0):.2%}, "
                f"constraints_pass={payload.get('constraints_pass')}"
            )
        report_path = report_dir / "summary.md"
        report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
        report_json = report_dir / "summary.json"
        report_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

        outputs.append({"path": str(report_path), "rows": len(report_lines), "start_ts": None, "end_ts": None})
        outputs.append({"path": str(report_json), "rows": 1, "start_ts": None, "end_ts": None})

        stats["selected_mode"] = selected_mode
        stats["symbols"] = requested_symbols
        stats["edges"] = [str(edge["edge_id"]) for edge in approved_edges]
        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:  # pragma: no cover
        logging.exception("Multi-edge backtest failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())

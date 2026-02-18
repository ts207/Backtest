from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines.research.analyze_conditional_expectancy import build_walk_forward_split_labels

REQUIRED_WF_SPLITS = ("train", "validation", "test")
REQUIRED_WF_STRATEGY_KEYS = ("total_trades", "net_pnl", "stressed_net_pnl")
REQUIRED_WF_REGIME_KEYS = ("max_regime_share", "regime_consistent")
REQUIRED_WF_DRAWDOWN_CLUSTER_KEYS = (
    "max_loss_cluster_len",
    "cluster_loss_concentration",
    "tail_conditional_drawdown_95",
)
REQUIRED_RETURNS_COLUMNS = (
    "timestamp",
    "symbol",
    "pos",
    "pnl",
    "close",
    "high",
    "low",
)


def _sanitize(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", str(value).strip().lower()).strip("_")


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)
        if np.isnan(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def _to_float_strict(value: object, *, field_name: str) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid numeric field '{field_name}': {value!r}") from exc
    if not np.isfinite(out):
        raise ValueError(f"Invalid numeric field '{field_name}': {value!r}")
    return out


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _load_blueprints(path: Path) -> List[Dict[str, object]]:
    if not path.exists():
        raise FileNotFoundError(f"Blueprint file not found: {path}")
    rows: List[Dict[str, object]] = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"Invalid blueprint row at line {line_no}: expected object")
        rows.append(payload)
    return rows


def _count_entries(frame: pd.DataFrame) -> int:
    if frame.empty or "pos" not in frame.columns:
        return 0
    ordered = frame.copy()
    if "symbol" not in ordered.columns:
        ordered["symbol"] = "ALL"
    entries = 0
    for _, sub in ordered.sort_values(["symbol", "timestamp"]).groupby("symbol", sort=True):
        pos = pd.to_numeric(sub["pos"], errors="coerce").fillna(0.0)
        if "position_scale" in sub.columns:
            scale = pd.to_numeric(sub["position_scale"], errors="coerce").fillna(1.0)
            pos = pos * scale
        prior = pos.shift(1).fillna(0.0)
        entries += int(((prior == 0.0) & (pos != 0.0)).sum())
    return int(entries)


def _assign_splits(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    out = out.dropna(subset=["timestamp"]).copy()
    if "symbol" not in out.columns:
        out["symbol"] = "ALL"
    out["split_label"] = build_walk_forward_split_labels(out, time_col="timestamp", symbol_col="symbol")
    return out


def _split_pnl(frame: pd.DataFrame) -> Dict[str, float]:
    if frame.empty:
        return {"train": 0.0, "validation": 0.0, "test": 0.0}
    pnl = pd.to_numeric(frame.get("pnl"), errors="coerce").fillna(0.0)
    out = {}
    for split in ("train", "validation", "test"):
        out[split] = float(pnl[frame["split_label"] == split].sum())
    return out


def _stressed_pnl(frame: pd.DataFrame) -> Dict[str, float]:
    if frame.empty:
        return {"train": 0.0, "validation": 0.0, "test": 0.0}
    gross = pd.to_numeric(frame.get("gross_pnl"), errors="coerce").fillna(pd.to_numeric(frame.get("pnl"), errors="coerce").fillna(0.0))
    trading_cost = pd.to_numeric(frame.get("trading_cost"), errors="coerce").fillna(0.0)
    funding = pd.to_numeric(frame.get("funding_pnl"), errors="coerce").fillna(0.0)
    borrow = pd.to_numeric(frame.get("borrow_cost"), errors="coerce").fillna(0.0)
    stressed = gross - (2.0 * trading_cost) + funding - borrow
    out = {}
    for split in ("train", "validation", "test"):
        out[split] = float(stressed[frame["split_label"] == split].sum())
    return out


def _symbol_pass_rate(frame: pd.DataFrame) -> float:
    if frame.empty:
        return 0.0
    grouped = frame.groupby("symbol", sort=True)["pnl"].sum()
    if grouped.empty:
        return 0.0
    return float((grouped > 0.0).mean())


def _regime_dominance_share(frame: pd.DataFrame) -> float:
    if frame.empty:
        return 1.0
    close = pd.to_numeric(frame.get("close"), errors="coerce")
    high = pd.to_numeric(frame.get("high"), errors="coerce")
    low = pd.to_numeric(frame.get("low"), errors="coerce")
    valid = close.notna() & high.notna() & low.notna() & (close > 0)
    if not valid.any():
        return 1.0
    regime_proxy = ((high - low) / close).where(valid)
    median = float(regime_proxy.median()) if regime_proxy.notna().any() else 0.0
    regime = np.where(regime_proxy >= median, "high_vol", "low_vol")
    pnl = pd.to_numeric(frame.get("pnl"), errors="coerce").fillna(0.0)
    reg = pd.DataFrame({"regime": regime, "pnl_abs": pnl.abs()})
    by_regime = reg.groupby("regime", sort=True)["pnl_abs"].sum()
    total = float(by_regime.sum())
    if total <= 0.0:
        return 1.0
    return float(by_regime.max() / total)


_NUMERIC_CONDITION_PATTERN = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|==|>|<)\s*(-?\d+(?:\.\d+)?)\s*$")


def _parameter_neighborhood_stability(blueprint: Dict[str, object], split_pnl: Dict[str, float]) -> bool:
    entry = blueprint.get("entry", {}) if isinstance(blueprint.get("entry"), dict) else {}
    conditions = entry.get("conditions", []) if isinstance(entry.get("conditions"), list) else []
    has_numeric_threshold = any(_NUMERIC_CONDITION_PATTERN.match(str(cond or "")) for cond in conditions)
    train = _to_float(split_pnl.get("train"), 0.0)
    validation = _to_float(split_pnl.get("validation"), 0.0)
    if not has_numeric_threshold:
        return (train > 0.0) and (validation > 0.0)
    # Heuristic neighborhood gate: adjacent threshold variants should preserve sign;
    # in v1 we proxy this with train/validation sign consistency and bounded magnitude drift.
    if not ((train > 0.0) and (validation > 0.0)):
        return False
    denom = max(abs(train), abs(validation), 1e-9)
    relative_drift = abs(train - validation) / denom
    return bool(relative_drift <= 0.75)


def _load_strategy_returns(engine_dir: Path, strategy_id: str) -> pd.DataFrame:
    path = engine_dir / f"strategy_returns_{strategy_id}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing strategy returns for {strategy_id}: {path}")
    try:
        frame = pd.read_csv(path)
    except Exception as exc:
        raise ValueError(f"Failed reading strategy returns for {strategy_id}: {path}") from exc
    missing = [col for col in REQUIRED_RETURNS_COLUMNS if col not in frame.columns]
    if missing:
        raise ValueError(
            f"Strategy returns missing required columns for {strategy_id}: {missing} in {path}"
        )
    return frame


def _load_walkforward_summary(run_id: str, *, required: bool) -> Dict[str, object]:
    path = DATA_ROOT / "reports" / "eval" / run_id / "walkforward_summary.json"
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Missing walkforward summary: {path}")
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"Invalid walkforward summary JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid walkforward summary payload (expected object): {path}")
    return payload


def _extract_strategy_wf_metrics(
    wf_per_strategy: Dict[str, object],
    *,
    strategy_id: str,
) -> Dict[str, Dict[str, float]]:
    strategy_wf = wf_per_strategy.get(strategy_id, {})
    if not isinstance(strategy_wf, dict):
        raise ValueError(f"Invalid walkforward strategy payload for {strategy_id}")

    out: Dict[str, Dict[str, float]] = {}
    for split_label in REQUIRED_WF_SPLITS:
        split_row = strategy_wf.get(split_label, {})
        if not isinstance(split_row, dict):
            raise ValueError(f"Missing walkforward split '{split_label}' for {strategy_id}")
        parsed: Dict[str, float] = {}
        for key in REQUIRED_WF_STRATEGY_KEYS:
            parsed[key] = _to_float_strict(
                split_row.get(key),
                field_name=f"{strategy_id}.{split_label}.{key}",
            )
        out[split_label] = parsed
    return out


def _extract_strategy_wf_regime_metrics(
    wf_per_strategy_regime: Dict[str, object],
    *,
    strategy_id: str,
) -> Dict[str, Dict[str, float | bool]]:
    strategy_wf = wf_per_strategy_regime.get(strategy_id, {})
    if not isinstance(strategy_wf, dict):
        raise ValueError(f"Invalid walkforward strategy regime payload for {strategy_id}")

    out: Dict[str, Dict[str, float | bool]] = {}
    for split_label in REQUIRED_WF_SPLITS:
        split_row = strategy_wf.get(split_label, {})
        if not isinstance(split_row, dict):
            raise ValueError(f"Missing walkforward regime split '{split_label}' for {strategy_id}")
        parsed: Dict[str, float | bool] = {}
        for key in REQUIRED_WF_REGIME_KEYS:
            if key == "regime_consistent":
                parsed[key] = _as_bool(split_row.get(key))
            else:
                parsed[key] = _to_float_strict(
                    split_row.get(key),
                    field_name=f"{strategy_id}.{split_label}.{key}",
                )
        out[split_label] = parsed
    return out


def _extract_strategy_wf_drawdown_cluster_metrics(
    wf_per_strategy_drawdown: Dict[str, object],
    *,
    strategy_id: str,
) -> Dict[str, Dict[str, float]]:
    strategy_wf = wf_per_strategy_drawdown.get(strategy_id, {})
    if not isinstance(strategy_wf, dict):
        raise ValueError(f"Invalid walkforward strategy drawdown payload for {strategy_id}")

    out: Dict[str, Dict[str, float]] = {}
    for split_label in REQUIRED_WF_SPLITS:
        split_row = strategy_wf.get(split_label, {})
        if not isinstance(split_row, dict):
            raise ValueError(f"Missing walkforward drawdown split '{split_label}' for {strategy_id}")
        parsed: Dict[str, float] = {}
        for key in REQUIRED_WF_DRAWDOWN_CLUSTER_KEYS:
            parsed[key] = _to_float_strict(
                split_row.get(key),
                field_name=f"{strategy_id}.{split_label}.{key}",
            )
        out[split_label] = parsed
    return out


def _loss_cluster_lengths(pnl_series: pd.Series) -> List[int]:
    values = pd.to_numeric(pnl_series, errors="coerce").fillna(0.0).to_numpy(dtype=float)
    runs: List[int] = []
    run_len = 0
    for value in values:
        if value < 0.0:
            run_len += 1
        elif run_len > 0:
            runs.append(run_len)
            run_len = 0
    if run_len > 0:
        runs.append(run_len)
    return runs


def _fallback_drawdown_cluster_metrics(frame: pd.DataFrame, split_label: str) -> Dict[str, float]:
    sub = frame[frame["split_label"] == split_label].copy()
    if sub.empty:
        return {
            "max_loss_cluster_len": 0.0,
            "cluster_loss_concentration": 0.0,
            "tail_conditional_drawdown_95": 0.0,
        }
    sub = sub.sort_values("timestamp")
    pnl_ts = pd.to_numeric(sub.get("pnl"), errors="coerce").fillna(0.0).groupby(sub["timestamp"], sort=True).sum()
    clusters = _loss_cluster_lengths(pnl_ts)
    max_len = float(max(clusters)) if clusters else 0.0

    values = pnl_ts.to_numpy(dtype=float)
    loss_magnitudes: List[float] = []
    start = None
    for idx, value in enumerate(values):
        if value < 0.0 and start is None:
            start = idx
        elif value >= 0.0 and start is not None:
            loss_magnitudes.append(float(np.abs(values[start:idx].sum())))
            start = None
    if start is not None:
        loss_magnitudes.append(float(np.abs(values[start:].sum())))
    if not loss_magnitudes:
        concentration = 0.0
    else:
        sorted_losses = sorted(loss_magnitudes, reverse=True)
        k = max(1, int(np.ceil(len(sorted_losses) * 0.10)))
        concentration = float(sum(sorted_losses[:k]) / max(sum(sorted_losses), 1e-9))

    equity = (1.0 + pnl_ts.cumsum()).astype(float)
    peak = equity.cummax().replace(0.0, np.nan)
    drawdown = ((equity - peak) / peak).replace([np.inf, -np.inf], np.nan).dropna()
    if drawdown.empty:
        tail_dd = 0.0
    else:
        threshold = float(drawdown.quantile(0.05))
        tail = drawdown[drawdown <= threshold]
        tail_dd = float(tail.mean()) if not tail.empty else float(threshold)
    return {
        "max_loss_cluster_len": max_len,
        "cluster_loss_concentration": concentration,
        "tail_conditional_drawdown_95": tail_dd,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Promote strategy blueprints using backtest gating rules")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--min_trades", type=int, default=100)
    parser.add_argument("--min_cross_symbol_pass_rate", type=float, default=0.60)
    parser.add_argument("--regime_max_share", type=float, default=0.80)
    parser.add_argument("--max_loss_cluster_len", type=int, default=64)
    parser.add_argument("--max_cluster_loss_concentration", type=float, default=0.50)
    parser.add_argument("--min_tail_conditional_drawdown_95", type=float, default=-0.20)
    # Backward-compatible alias; if provided, overrides regime_max_share.
    parser.add_argument("--max_regime_dominance_share", type=float, default=0.999)
    parser.add_argument("--blueprints_path", default=None)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--allow_fallback_evidence", type=int, default=0)
    args = parser.parse_args()

    blueprints_path = (
        Path(args.blueprints_path)
        if args.blueprints_path
        else DATA_ROOT / "reports" / "strategy_blueprints" / args.run_id / "blueprints.jsonl"
    )
    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "promotions" / args.run_id
    ensure_dir(out_dir)
    promoted_path = out_dir / "promoted_blueprints.jsonl"
    report_path = out_dir / "promotion_report.json"

    params = {
        "run_id": args.run_id,
        "min_trades": int(args.min_trades),
        "min_cross_symbol_pass_rate": float(args.min_cross_symbol_pass_rate),
        "regime_max_share": float(args.regime_max_share),
        "max_loss_cluster_len": int(args.max_loss_cluster_len),
        "max_cluster_loss_concentration": float(args.max_cluster_loss_concentration),
        "min_tail_conditional_drawdown_95": float(args.min_tail_conditional_drawdown_95),
        "max_regime_dominance_share": float(args.max_regime_dominance_share),
        "blueprints_path": str(blueprints_path),
        "allow_fallback_evidence": int(args.allow_fallback_evidence),
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("promote_blueprints", args.run_id, params, inputs, outputs)

    try:
        blueprints = _load_blueprints(blueprints_path)
        inputs.append({"path": str(blueprints_path), "rows": int(len(blueprints)), "start_ts": None, "end_ts": None})
        fallback_enabled = bool(int(args.allow_fallback_evidence))
        require_wf = bool((not fallback_enabled) and bool(blueprints))
        walkforward_summary = _load_walkforward_summary(args.run_id, required=require_wf)
        wf_per_strategy = (
            walkforward_summary.get("per_strategy_split_metrics", {})
            if isinstance(walkforward_summary.get("per_strategy_split_metrics", {}), dict)
            else {}
        )
        wf_per_strategy_regime = (
            walkforward_summary.get("per_strategy_regime_metrics", {})
            if isinstance(walkforward_summary.get("per_strategy_regime_metrics", {}), dict)
            else {}
        )
        wf_per_strategy_drawdown = (
            walkforward_summary.get("per_strategy_drawdown_cluster_metrics", {})
            if isinstance(walkforward_summary.get("per_strategy_drawdown_cluster_metrics", {}), dict)
            else {}
        )
        if require_wf and (not wf_per_strategy):
            raise ValueError("walkforward_summary.json missing non-empty per_strategy_split_metrics")
        if require_wf and (not wf_per_strategy_regime):
            raise ValueError("walkforward_summary.json missing non-empty per_strategy_regime_metrics")
        if require_wf and (not wf_per_strategy_drawdown):
            raise ValueError("walkforward_summary.json missing non-empty per_strategy_drawdown_cluster_metrics")

        regime_threshold = float(args.regime_max_share)
        if "--max_regime_dominance_share" in sys.argv:
            regime_threshold = float(args.max_regime_dominance_share)

        engine_dir = DATA_ROOT / "runs" / args.run_id / "engine"
        tested_rows: List[Dict[str, object]] = []
        survivors: List[Dict[str, object]] = []
        evidence_mode_counts = {"walkforward_strategy": 0, "fallback": 0}

        for bp in blueprints:
            bp_id = str(bp.get("id", "")).strip()
            if not bp_id:
                continue
            strategy_id = f"dsl_interpreter_v1__{_sanitize(bp_id)}"
            evidence_mode = "fallback"
            returns = _load_strategy_returns(engine_dir=engine_dir, strategy_id=strategy_id)

            frame = _assign_splits(returns)
            trades = _count_entries(frame)
            split_pnl = _split_pnl(frame)
            stressed_split_pnl = _stressed_pnl(frame)
            symbol_pass_rate = _symbol_pass_rate(frame)
            regime_dominance_share = _regime_dominance_share(frame)
            regime_source = "fallback"
            regime_wf: Dict[str, Dict[str, float | bool]] | None = None
            drawdown_source = "fallback"
            drawdown_wf: Dict[str, Dict[str, float]] | None = None
            strategy_wf = wf_per_strategy.get(strategy_id, {}) if isinstance(wf_per_strategy.get(strategy_id, {}), dict) else {}
            if strategy_wf:
                evidence_mode = "walkforward_strategy"
                parsed_wf = _extract_strategy_wf_metrics(
                    wf_per_strategy,
                    strategy_id=strategy_id,
                )
                if isinstance(wf_per_strategy_regime.get(strategy_id), dict):
                    regime_wf = _extract_strategy_wf_regime_metrics(
                        wf_per_strategy_regime,
                        strategy_id=strategy_id,
                    )
                    regime_source = "walkforward_strategy"
                elif not fallback_enabled:
                    raise ValueError(f"Missing walkforward regime evidence for {strategy_id}")
                if isinstance(wf_per_strategy_drawdown.get(strategy_id), dict):
                    drawdown_wf = _extract_strategy_wf_drawdown_cluster_metrics(
                        wf_per_strategy_drawdown,
                        strategy_id=strategy_id,
                    )
                    drawdown_source = "walkforward_strategy"
                elif not fallback_enabled:
                    raise ValueError(f"Missing walkforward drawdown-cluster evidence for {strategy_id}")
                trades = int(sum(int(parsed_wf[s]["total_trades"]) for s in REQUIRED_WF_SPLITS))
                for split_label in REQUIRED_WF_SPLITS:
                    split_pnl[split_label] = float(parsed_wf[split_label]["net_pnl"])
                    stressed_split_pnl[split_label] = float(parsed_wf[split_label]["stressed_net_pnl"])
            elif not fallback_enabled:
                raise ValueError(f"Missing walkforward strategy evidence for {strategy_id}")
            evidence_mode_counts[evidence_mode] += 1

            if drawdown_wf is None:
                drawdown_wf = {
                    split_label: _fallback_drawdown_cluster_metrics(frame, split_label)
                    for split_label in REQUIRED_WF_SPLITS
                }

            eval_spec = bp.get("evaluation", {}) if isinstance(bp.get("evaluation"), dict) else {}
            robust_flags = eval_spec.get("robustness_flags", {}) if isinstance(eval_spec.get("robustness_flags"), dict) else {}
            regime_required = _as_bool(robust_flags.get("regime_stability_required", True))

            gates = {
                "min_trades": bool(trades >= int(args.min_trades)),
                "parameter_neighborhood_stability": bool(_parameter_neighborhood_stability(bp, split_pnl)),
                "cross_symbol_pass_rate": bool(symbol_pass_rate >= float(args.min_cross_symbol_pass_rate)),
                "regime_stability": bool(
                    (
                        bool(regime_wf["train"]["regime_consistent"])
                        and bool(regime_wf["validation"]["regime_consistent"])
                    )
                    if (regime_required and regime_wf is not None)
                    else (
                        (regime_dominance_share <= float(regime_threshold))
                        if regime_required
                        else True
                    )
                ),
                "cost_stress_train_validation_positive": bool(
                    (_to_float(stressed_split_pnl.get("train"), 0.0) > 0.0)
                    and (_to_float(stressed_split_pnl.get("validation"), 0.0) > 0.0)
                ),
                "drawdown_cluster_len": bool(
                    max(
                        _to_float(drawdown_wf["train"]["max_loss_cluster_len"]),
                        _to_float(drawdown_wf["validation"]["max_loss_cluster_len"]),
                    )
                    <= float(args.max_loss_cluster_len)
                ),
                "drawdown_cluster_concentration": bool(
                    max(
                        _to_float(drawdown_wf["train"]["cluster_loss_concentration"]),
                        _to_float(drawdown_wf["validation"]["cluster_loss_concentration"]),
                    )
                    <= float(args.max_cluster_loss_concentration)
                ),
                "tail_conditional_drawdown": bool(
                    min(
                        _to_float(drawdown_wf["train"]["tail_conditional_drawdown_95"]),
                        _to_float(drawdown_wf["validation"]["tail_conditional_drawdown_95"]),
                    )
                    >= float(args.min_tail_conditional_drawdown_95)
                ),
            }
            promote = all(gates.values())
            fail_reasons = [name for name, passed in gates.items() if not passed]

            tested = {
                "blueprint_id": bp_id,
                "strategy_id": strategy_id,
                "event_type": str(bp.get("event_type", "")),
                "family": str(bp.get("event_type", "")),
                "trades": int(trades),
                "split_pnl": split_pnl,
                "stressed_split_pnl": stressed_split_pnl,
                "symbol_pass_rate": float(symbol_pass_rate),
                "regime_dominance_share": float(regime_dominance_share),
                "regime_split_metrics": regime_wf if regime_wf is not None else {},
                "regime_evidence_source": regime_source,
                "drawdown_cluster_metrics": drawdown_wf if drawdown_wf is not None else {},
                "drawdown_evidence_source": drawdown_source,
                "gates": gates,
                "fail_reasons": fail_reasons,
                "evidence_mode": evidence_mode,
                "promoted": bool(promote),
            }
            tested_rows.append(tested)
            if promote:
                promoted_row = dict(bp)
                promoted_row["promotion"] = tested
                survivors.append(promoted_row)

        lines = [json.dumps(row, sort_keys=True) for row in survivors]
        promoted_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

        report = {
            "run_id": args.run_id,
            "tested_count": int(len(tested_rows)),
            "survivors_count": int(len(survivors)),
            "integrity_checks": {
                "artifacts_validated": True,
                "walkforward_strategy_evidence_required": bool(not fallback_enabled),
                "walkforward_summary_validated": bool(bool(walkforward_summary) or fallback_enabled),
            },
            "evidence_mode_counts": evidence_mode_counts,
            "tested": tested_rows,
            "thresholds": {
                "min_trades": int(args.min_trades),
                "min_cross_symbol_pass_rate": float(args.min_cross_symbol_pass_rate),
                "regime_max_share": float(regime_threshold),
                "max_regime_dominance_share": float(regime_threshold),
                "max_loss_cluster_len": int(args.max_loss_cluster_len),
                "max_cluster_loss_concentration": float(args.max_cluster_loss_concentration),
                "min_tail_conditional_drawdown_95": float(args.min_tail_conditional_drawdown_95),
                "cost_stress_rule": "2x trading_cost on train+validation must stay positive",
            },
        }
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

        outputs.append({"path": str(promoted_path), "rows": int(len(survivors)), "start_ts": None, "end_ts": None})
        outputs.append({"path": str(report_path), "rows": int(len(tested_rows)), "start_ts": None, "end_ts": None})
        finalize_manifest(
            manifest,
            "success",
            stats={
                "tested_count": int(len(tested_rows)),
                "survivors_count": int(len(survivors)),
                "walkforward_strategy_evidence_required": bool(not fallback_enabled),
                "evidence_mode_counts": evidence_mode_counts,
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())

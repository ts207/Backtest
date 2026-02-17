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
        return []
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
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _load_walkforward_summary(run_id: str) -> Dict[str, object]:
    path = DATA_ROOT / "reports" / "eval" / run_id / "walkforward_summary.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Promote strategy blueprints using backtest gating rules")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--min_trades", type=int, default=100)
    parser.add_argument("--min_cross_symbol_pass_rate", type=float, default=0.60)
    parser.add_argument("--max_regime_dominance_share", type=float, default=0.999)
    parser.add_argument("--blueprints_path", default=None)
    parser.add_argument("--out_dir", default=None)
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
        "max_regime_dominance_share": float(args.max_regime_dominance_share),
        "blueprints_path": str(blueprints_path),
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("promote_blueprints", args.run_id, params, inputs, outputs)

    try:
        blueprints = _load_blueprints(blueprints_path)
        inputs.append({"path": str(blueprints_path), "rows": int(len(blueprints)), "start_ts": None, "end_ts": None})
        walkforward_summary = _load_walkforward_summary(args.run_id)
        wf_per_split = (
            walkforward_summary.get("per_split_metrics", {})
            if isinstance(walkforward_summary.get("per_split_metrics", {}), dict)
            else {}
        )
        wf_final = (
            walkforward_summary.get("final_test_metrics", {})
            if isinstance(walkforward_summary.get("final_test_metrics", {}), dict)
            else {}
        )

        engine_dir = DATA_ROOT / "runs" / args.run_id / "engine"
        tested_rows: List[Dict[str, object]] = []
        survivors: List[Dict[str, object]] = []

        for bp in blueprints:
            bp_id = str(bp.get("id", "")).strip()
            if not bp_id:
                continue
            strategy_id = f"dsl_interpreter_v1__{_sanitize(bp_id)}"
            evidence_mode = "fallback"
            returns = _load_strategy_returns(engine_dir=engine_dir, strategy_id=strategy_id)
            if returns.empty:
                continue

            frame = _assign_splits(returns)
            trades = _count_entries(frame)
            split_pnl = _split_pnl(frame)
            stressed_split_pnl = _stressed_pnl(frame)
            symbol_pass_rate = _symbol_pass_rate(frame)
            regime_dominance_share = _regime_dominance_share(frame)
            if wf_per_split:
                wf_meta = wf_final.get("metadata", {}) if isinstance(wf_final.get("metadata", {}), dict) else {}
                wf_strategy_ids = (
                    wf_meta.get("strategy_ids", [])
                    if isinstance(wf_meta.get("strategy_ids", []), list)
                    else []
                )
                if (not wf_strategy_ids) or (strategy_id in [str(s) for s in wf_strategy_ids]):
                    evidence_mode = "walkforward"
                    for split_label in ("train", "validation", "test"):
                        row = wf_per_split.get(split_label, {})
                        if isinstance(row, dict):
                            split_pnl[split_label] = _to_float(row.get("stressed_net_pnl"), split_pnl.get(split_label, 0.0))
                            stressed_split_pnl[split_label] = _to_float(
                                row.get("stressed_net_pnl"), stressed_split_pnl.get(split_label, 0.0)
                            )
                    trades = max(
                        trades,
                        int(_to_float(wf_per_split.get("train", {}).get("total_trades", 0), 0))
                        + int(_to_float(wf_per_split.get("validation", {}).get("total_trades", 0), 0))
                        + int(_to_float(wf_per_split.get("test", {}).get("total_trades", 0), 0)),
                    )

            eval_spec = bp.get("evaluation", {}) if isinstance(bp.get("evaluation"), dict) else {}
            robust_flags = eval_spec.get("robustness_flags", {}) if isinstance(eval_spec.get("robustness_flags"), dict) else {}
            regime_required = _as_bool(robust_flags.get("regime_stability_required", True))

            gates = {
                "min_trades": bool(trades >= int(args.min_trades)),
                "parameter_neighborhood_stability": bool(_parameter_neighborhood_stability(bp, split_pnl)),
                "cross_symbol_pass_rate": bool(symbol_pass_rate >= float(args.min_cross_symbol_pass_rate)),
                "regime_stability": bool(
                    (regime_dominance_share <= float(args.max_regime_dominance_share))
                    if regime_required
                    else True
                ),
                "cost_stress_train_validation_positive": bool(
                    (_to_float(stressed_split_pnl.get("train"), 0.0) > 0.0)
                    and (_to_float(stressed_split_pnl.get("validation"), 0.0) > 0.0)
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
            "tested": tested_rows,
            "thresholds": {
                "min_trades": int(args.min_trades),
                "min_cross_symbol_pass_rate": float(args.min_cross_symbol_pass_rate),
                "max_regime_dominance_share": float(args.max_regime_dominance_share),
                "cost_stress_rule": "2x trading_cost on train+validation must stay positive",
            },
        }
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

        outputs.append({"path": str(promoted_path), "rows": int(len(survivors)), "start_ts": None, "end_ts": None})
        outputs.append({"path": str(report_path), "rows": int(len(tested_rows)), "start_ts": None, "end_ts": None})
        finalize_manifest(
            manifest,
            "success",
            stats={"tested_count": int(len(tested_rows)), "survivors_count": int(len(survivors))},
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())

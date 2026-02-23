from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from eval.splits import build_time_splits
from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.execution_costs import resolve_execution_costs
from pipelines._lib.run_manifest import finalize_manifest, start_manifest

INITIAL_EQUITY = 1_000_000.0
BARS_PER_YEAR_5M = 365 * 24 * 12
REQUIRED_SPLIT_METRIC_KEYS = ["total_trades", "ending_equity", "sharpe_annualized", "max_drawdown"]
REQUIRED_STRATEGY_RETURN_COLUMNS = [
    "timestamp",
    "pos",
    "pnl",
    "gross_pnl",
    "trading_cost",
    "funding_pnl",
    "borrow_cost",
]
REGIME_LABELS = ("low", "mid", "high")


def _to_float_strict(value: object, *, label: str) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be numeric, got `{value}`") from exc
    if not np.isfinite(out):
        raise ValueError(f"{label} must be finite, got `{value}`")
    return out


def _load_split_metrics_strict(metrics_path: Path, *, split_label: str, split_run_id: str) -> Dict[str, object]:
    if not metrics_path.exists():
        raise ValueError(f"Missing metrics.json for split={split_label} run_id={split_run_id}: {metrics_path}")
    try:
        payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"Invalid metrics.json for split={split_label} run_id={split_run_id}: {metrics_path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"metrics.json must be an object for split={split_label} run_id={split_run_id}: {metrics_path}")

    for key in REQUIRED_SPLIT_METRIC_KEYS:
        if key not in payload:
            raise ValueError(
                f"metrics.json missing required key `{key}` for split={split_label} run_id={split_run_id}: {metrics_path}"
            )
        _to_float_strict(payload.get(key), label=f"{split_label}.{key}")

    cost_decomposition = payload.get("cost_decomposition", {})
    if not isinstance(cost_decomposition, dict):
        raise ValueError(
            f"metrics.json cost_decomposition must be object for split={split_label} run_id={split_run_id}: {metrics_path}"
        )
    if "net_alpha" not in cost_decomposition:
        raise ValueError(
            f"metrics.json missing cost_decomposition.net_alpha for split={split_label} run_id={split_run_id}: {metrics_path}"
        )
    _to_float_strict(cost_decomposition.get("net_alpha"), label=f"{split_label}.cost_decomposition.net_alpha")
    return payload


def _run_split_backtest(cmd: List[str]) -> int:
    return subprocess.run(cmd).returncode


def _strategy_id_from_path(path: Path) -> str:
    name = path.name
    prefix = "strategy_returns_"
    suffix = ".csv"
    if not name.startswith(prefix) or not name.endswith(suffix):
        return ""
    strategy_id = name[len(prefix) : -len(suffix)].strip()
    return strategy_id


def _sanitize_id(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", str(value).strip().lower()).strip("_")


def _load_blueprints_raw(path: Path) -> List[Dict[str, object]]:
    if not path.exists():
        raise ValueError(f"Blueprint file not found: {path}")
    text = path.read_text(encoding="utf-8")
    rows: List[Dict[str, object]] = []
    if path.suffix.lower() == ".json":
        payload = json.loads(text)
        if not isinstance(payload, list):
            raise ValueError(f"Blueprint JSON must contain a list: {path}")
        for idx, row in enumerate(payload, start=1):
            if not isinstance(row, dict):
                raise ValueError(f"Invalid blueprint object at index {idx} in {path}")
            rows.append(dict(row))
    else:
        for line_no, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError(f"Invalid blueprint object at line {line_no} in {path}")
            rows.append(dict(row))
    if not rows:
        raise ValueError(f"No blueprint rows found in {path}")
    return rows


def _expected_blueprint_strategy_ids(
    *,
    blueprints_path: Path,
    event_type: str,
    top_k: int,
    cli_symbols: List[str],
) -> List[str]:
    raw_rows = _load_blueprints_raw(blueprints_path)
    # B1: Hard fail if ANY blueprint has promotion_track != 'standard'.
    # Fallback blueprints bypass BH-FDR and are banned from evaluation. [INV_NO_FALLBACK_IN_MEASUREMENT]
    fallback_rows = [
        row for row in raw_rows
        if str((row.get("lineage") or {}).get("promotion_track", "standard")) != "standard"
    ]
    if fallback_rows:
        bad_ids = ", ".join(str(row.get("id", "<unknown>")) for row in fallback_rows[:5])
        extra = f" (+{len(fallback_rows) - 5} more)" if len(fallback_rows) > 5 else ""
        raise ValueError(
            f"EVALUATION GUARD [INV_NO_FALLBACK_IN_MEASUREMENT]: "
            f"{len(fallback_rows)} blueprint(s) have promotion_track != 'standard': {bad_ids}{extra}. "
            "Fallback-only blueprints bypass BH-FDR and cannot appear in evaluation artifacts. "
            "Remediation: set spec/gates.yaml gate_v1_fallback.promotion_eligible_regardless_of_fdr: false "
            "and re-run discovery to produce standard-track survivors only."
        )
    cli_set = {str(symbol).strip().upper() for symbol in cli_symbols if str(symbol).strip()}
    out: List[str] = []
    seen = set()
    for row in raw_rows:
        row_event = str(row.get("event_type", "")).strip()
        if str(event_type).strip() != "all" and row_event != str(event_type).strip():
            continue
        scope = row.get("symbol_scope", {})
        scope_symbols: List[str] = []
        if isinstance(scope, dict):
            raw_scope_symbols = scope.get("symbols", [])
            if isinstance(raw_scope_symbols, list):
                scope_symbols = [str(s).strip().upper() for s in raw_scope_symbols if str(s).strip()]
        if cli_set:
            scope_symbols = [s for s in scope_symbols if s in cli_set]
        if not scope_symbols:
            continue
        bp_id = str(row.get("id", "")).strip()
        if not bp_id:
            raise ValueError(f"Blueprint row missing id in {blueprints_path}")
        strategy_id = f"dsl_interpreter_v1__{_sanitize_id(bp_id)}"
        if strategy_id in seen:
            continue
        out.append(strategy_id)
        seen.add(strategy_id)
        if len(out) >= max(1, int(top_k)):
            break
    if not out:
        raise ValueError(
            "No blueprint strategies selected for walkforward. "
            f"path={blueprints_path}, event_type={event_type}, top_k={int(top_k)}"
        )
    return out


def _annualized_sharpe(pnl_series: pd.Series) -> float:
    if pnl_series.empty:
        return 0.0
    std = float(pnl_series.std())
    if not np.isfinite(std) or std <= 0.0:
        return 0.0
    mean = float(pnl_series.mean())
    return float((mean / std) * np.sqrt(BARS_PER_YEAR_5M))


def _safe_metric_float(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    if not np.isfinite(out):
        return float(default)
    return float(out)


def _annualized_sortino(pnl_series: pd.Series) -> float:
    if pnl_series.empty:
        return 0.0
    downside = pnl_series[pnl_series < 0.0]
    downside_std = float(downside.std()) if not downside.empty else 0.0
    if not np.isfinite(downside_std) or downside_std <= 0.0:
        return 0.0
    mean = float(pnl_series.mean())
    return float((mean / downside_std) * np.sqrt(BARS_PER_YEAR_5M))


def _max_drawdown_from_returns(pnl_series: pd.Series) -> float:
    if pnl_series.empty:
        return 0.0
    equity = (1.0 + pnl_series).cumprod()
    peak = equity.cummax().replace(0.0, np.nan)
    drawdown = (equity / peak - 1.0).replace([np.inf, -np.inf], np.nan).dropna()
    if drawdown.empty:
        return 0.0
    return float(drawdown.min())


def _annual_return_from_returns(pnl_series: pd.Series) -> float:
    if pnl_series.empty:
        return 0.0
    gross = float((1.0 + pnl_series).prod())
    if not np.isfinite(gross) or gross <= 0.0:
        return -1.0
    periods = max(1, int(len(pnl_series)))
    return float(gross ** (float(BARS_PER_YEAR_5M) / float(periods)) - 1.0)


def _standardized_metrics_from_returns(pnl_series: pd.Series) -> Dict[str, object]:
    returns = pd.to_numeric(pnl_series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna().astype(float)
    if returns.empty:
        return {
            "sharpe": 0.0,
            "sortino": 0.0,
            "calmar": 0.0,
            "max_drawdown": 0.0,
            "annual_return": 0.0,
            "metrics_source": "fallback",
        }

    try:
        import empyrical as ep  # Optional dependency from empyrical-reloaded package.

        sharpe = _safe_metric_float(ep.sharpe_ratio(returns, annualization=BARS_PER_YEAR_5M), 0.0)
        sortino = _safe_metric_float(ep.sortino_ratio(returns, annualization=BARS_PER_YEAR_5M), 0.0)
        calmar = _safe_metric_float(ep.calmar_ratio(returns, annualization=BARS_PER_YEAR_5M), 0.0)
        max_drawdown = _safe_metric_float(ep.max_drawdown(returns), _max_drawdown_from_returns(returns))
        annual_return = _safe_metric_float(ep.annual_return(returns, annualization=BARS_PER_YEAR_5M), _annual_return_from_returns(returns))
        return {
            "sharpe": float(sharpe),
            "sortino": float(sortino),
            "calmar": float(calmar),
            "max_drawdown": float(max_drawdown),
            "annual_return": float(annual_return),
            "metrics_source": "empyrical_reloaded",
        }
    except Exception:
        sharpe = _annualized_sharpe(returns)
        sortino = _annualized_sortino(returns)
        max_drawdown = _max_drawdown_from_returns(returns)
        annual_return = _annual_return_from_returns(returns)
        calmar = float(annual_return / abs(max_drawdown)) if max_drawdown < 0.0 else 0.0
        return {
            "sharpe": float(sharpe),
            "sortino": float(sortino),
            "calmar": float(calmar),
            "max_drawdown": float(max_drawdown),
            "annual_return": float(annual_return),
            "metrics_source": "fallback",
        }


def _compute_drawdown(equity_series: pd.Series) -> float:
    if equity_series.empty:
        return 0.0
    peak = equity_series.cummax().replace(0.0, np.nan)
    drawdown = ((equity_series - peak) / peak).replace([np.inf, -np.inf], np.nan).dropna()
    if drawdown.empty:
        return 0.0
    return float(drawdown.min())


def _entry_count(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    ordered = frame.copy()
    if "symbol" not in ordered.columns:
        ordered["symbol"] = "ALL"
    ordered = ordered.sort_values(["symbol", "timestamp"]).reset_index(drop=True)

    entries = 0
    for _, group in ordered.groupby("symbol", sort=True):
        pos = pd.to_numeric(group.get("pos"), errors="coerce").fillna(0.0)
        if "position_scale" in group.columns:
            scale = pd.to_numeric(group.get("position_scale"), errors="coerce").fillna(1.0)
        elif "allocated_position_scale" in group.columns:
            scale = pd.to_numeric(group.get("allocated_position_scale"), errors="coerce").fillna(1.0)
        elif "requested_position_scale" in group.columns:
            scale = pd.to_numeric(group.get("requested_position_scale"), errors="coerce").fillna(1.0)
        else:
            scale = pd.Series(1.0, index=group.index, dtype=float)
        effective_pos = pos * scale
        prior = effective_pos.shift(1).fillna(0.0)
        entries += int(((prior.abs() <= 1e-12) & (effective_pos.abs() > 1e-12)).sum())
    return int(entries)


def _strategy_metrics_from_frame(frame: pd.DataFrame) -> Dict[str, object]:
    if frame.empty:
        return {
            "total_trades": 0,
            "net_pnl": 0.0,
            "stressed_net_pnl": 0.0,
            "ending_equity": INITIAL_EQUITY,
            "sharpe_annualized": 0.0,
            "max_drawdown": 0.0,
            "gate_precheck": {"has_trades": False, "stressed_non_negative": True},
        }

    out = frame.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    out = out.dropna(subset=["timestamp"]).copy()
    if out.empty:
        return {
            "total_trades": 0,
            "net_pnl": 0.0,
            "stressed_net_pnl": 0.0,
            "ending_equity": INITIAL_EQUITY,
            "sharpe_annualized": 0.0,
            "max_drawdown": 0.0,
            "gate_precheck": {"has_trades": False, "stressed_non_negative": True},
        }

    if "symbol" not in out.columns:
        out["symbol"] = "ALL"

    out["pnl"] = pd.to_numeric(out.get("pnl"), errors="coerce").fillna(0.0)
    out["gross_pnl"] = pd.to_numeric(out.get("gross_pnl"), errors="coerce").fillna(out["pnl"])
    out["trading_cost"] = pd.to_numeric(out.get("trading_cost"), errors="coerce").fillna(0.0)
    out["funding_pnl"] = pd.to_numeric(out.get("funding_pnl"), errors="coerce").fillna(0.0)
    out["borrow_cost"] = pd.to_numeric(out.get("borrow_cost"), errors="coerce").fillna(0.0)

    entries = _entry_count(out)
    pnl_ts = out.groupby("timestamp", sort=True)["pnl"].sum()
    stressed = out["gross_pnl"] - (2.0 * out["trading_cost"]) + out["funding_pnl"] - out["borrow_cost"]
    stressed_ts = out.assign(stressed_pnl=stressed).groupby("timestamp", sort=True)["stressed_pnl"].sum()
    equity = INITIAL_EQUITY * (1.0 + pnl_ts.cumsum())

    net_pnl = float(pnl_ts.sum())
    stressed_net_pnl = float(stressed_ts.sum())
    ending_equity = float(equity.iloc[-1]) if not equity.empty else INITIAL_EQUITY

    return {
        "total_trades": int(entries),
        "net_pnl": net_pnl,
        "stressed_net_pnl": stressed_net_pnl,
        "ending_equity": ending_equity,
        "sharpe_annualized": _annualized_sharpe(pnl_ts),
        "max_drawdown": _compute_drawdown(equity),
        "gate_precheck": {
            "has_trades": bool(entries > 0),
            "stressed_non_negative": bool(stressed_net_pnl >= 0.0),
        },
    }


def _regime_metrics_from_frame(frame: pd.DataFrame, *, regime_max_share: float) -> Dict[str, object]:
    if frame.empty:
        return {
            "regime_pnl_share": {label: 0.0 for label in REGIME_LABELS},
            "max_regime_share": 1.0,
            "regime_consistent": False,
            "regime_max_share": float(regime_max_share),
        }

    out = frame.copy()
    out = out.sort_values("timestamp").reset_index(drop=True)
    pnl = pd.to_numeric(out.get("pnl"), errors="coerce").fillna(0.0).astype(float)
    gross = pd.to_numeric(out.get("gross_pnl"), errors="coerce").fillna(pnl).astype(float)
    proxy = gross.abs().rolling(window=96, min_periods=8).mean()
    if proxy.notna().sum() < 3:
        proxy = pnl.abs()
    proxy = proxy.ffill().fillna(0.0)
    ranked = proxy.rank(method="first")
    if int(ranked.notna().sum()) < 3:
        regime = pd.Series(["mid"] * len(out), index=out.index, dtype="object")
    else:
        regime = pd.qcut(ranked, q=3, labels=list(REGIME_LABELS))

    reg = pd.DataFrame(
        {
            "regime": regime.astype(str),
            "pnl_abs": pnl.abs().astype(float),
        }
    )
    by_regime = reg.groupby("regime", sort=True)["pnl_abs"].sum()
    total = float(by_regime.sum())
    if total <= 0.0:
        shares = {label: 0.0 for label in REGIME_LABELS}
        max_share = 1.0
    else:
        shares = {label: float(by_regime.get(label, 0.0) / total) for label in REGIME_LABELS}
        max_share = float(max(shares.values()))
    return {
        "regime_pnl_share": shares,
        "max_regime_share": max_share,
        "regime_consistent": bool(max_share <= float(regime_max_share)),
        "regime_max_share": float(regime_max_share),
    }


def _loss_cluster_lengths(pnl_ts: pd.Series) -> List[int]:
    values = pd.to_numeric(pnl_ts, errors="coerce").fillna(0.0).to_numpy(dtype=float)
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


def _drawdown_cluster_metrics_from_frame(
    frame: pd.DataFrame,
    *,
    drawdown_cluster_top_frac: float,
    drawdown_tail_q: float,
) -> Dict[str, object]:
    if frame.empty:
        return {
            "max_loss_cluster_len": 0,
            "p95_loss_cluster_len": 0.0,
            "loss_cluster_count": 0,
            "cluster_loss_concentration": 0.0,
            "tail_conditional_drawdown_95": 0.0,
        }

    out = frame.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    out = out.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    if out.empty:
        return {
            "max_loss_cluster_len": 0,
            "p95_loss_cluster_len": 0.0,
            "loss_cluster_count": 0,
            "cluster_loss_concentration": 0.0,
            "tail_conditional_drawdown_95": 0.0,
        }

    pnl_ts = pd.to_numeric(out.get("pnl"), errors="coerce").fillna(0.0).groupby(out["timestamp"], sort=True).sum()
    clusters = _loss_cluster_lengths(pnl_ts)
    cluster_count = int(len(clusters))
    max_len = int(max(clusters)) if clusters else 0
    p95_len = float(np.percentile(clusters, 95)) if clusters else 0.0

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
        k = max(1, int(np.ceil(len(sorted_losses) * float(drawdown_cluster_top_frac))))
        concentration = float(sum(sorted_losses[:k]) / max(sum(sorted_losses), 1e-9))

    equity = INITIAL_EQUITY * (1.0 + pnl_ts.cumsum())
    peak = equity.cummax().replace(0.0, np.nan)
    drawdown = ((equity - peak) / peak).replace([np.inf, -np.inf], np.nan).dropna()
    if drawdown.empty:
        tail_conditional_drawdown = 0.0
    else:
        threshold = float(drawdown.quantile(float(drawdown_tail_q)))
        tail = drawdown[drawdown <= threshold]
        tail_conditional_drawdown = float(tail.mean()) if not tail.empty else float(threshold)

    return {
        "max_loss_cluster_len": max_len,
        "p95_loss_cluster_len": p95_len,
        "loss_cluster_count": cluster_count,
        "cluster_loss_concentration": concentration,
        "tail_conditional_drawdown_95": tail_conditional_drawdown,
    }


def _load_per_strategy_split_metrics_strict(
    split_run_id: str,
    *,
    split_label: str,
    expected_strategy_ids: List[str] | None = None,
    allow_unexpected_strategy_files: bool = False,
    regime_max_share: float = 0.80,
    drawdown_cluster_top_frac: float = 0.10,
    drawdown_tail_q: float = 0.05,
) -> tuple[Dict[str, Dict[str, object]], Dict[str, object]]:
    engine_dir = DATA_ROOT / "runs" / split_run_id / "engine"
    if not engine_dir.exists():
        raise ValueError(f"Missing engine directory for split={split_label} run_id={split_run_id}: {engine_dir}")
    strategy_files = sorted(engine_dir.glob("strategy_returns_*.csv"))
    if not strategy_files:
        raise ValueError(
            f"No strategy return artifacts found for split={split_label} run_id={split_run_id} in {engine_dir}"
        )

    out: Dict[str, Dict[str, object]] = {}
    observed_strategy_ids: set[str] = set()
    for path in strategy_files:
        strategy_id = _strategy_id_from_path(path)
        if not strategy_id:
            raise ValueError(f"Invalid strategy return filename for split={split_label} run_id={split_run_id}: {path.name}")
        observed_strategy_ids.add(strategy_id)
        try:
            frame = pd.read_csv(path)
        except Exception as exc:
            raise ValueError(f"Failed reading strategy return file for split={split_label} run_id={split_run_id}: {path}") from exc
        missing_cols = [col for col in REQUIRED_STRATEGY_RETURN_COLUMNS if col not in frame.columns]
        if missing_cols:
            raise ValueError(
                f"Strategy return file missing required columns for split={split_label} run_id={split_run_id}: "
                f"{path.name} missing {missing_cols}"
            )
        strategy_metrics = _strategy_metrics_from_frame(frame)
        strategy_metrics.update(_regime_metrics_from_frame(frame, regime_max_share=float(regime_max_share)))
        strategy_metrics.update(
            _drawdown_cluster_metrics_from_frame(
                frame,
                drawdown_cluster_top_frac=float(drawdown_cluster_top_frac),
                drawdown_tail_q=float(drawdown_tail_q),
            )
        )
        out[strategy_id] = strategy_metrics
    expected_count = 0
    unexpected: List[str] = []
    if expected_strategy_ids is not None:
        expected = {str(x).strip() for x in expected_strategy_ids if str(x).strip()}
        expected_count = int(len(expected))
        missing = sorted(expected - set(out.keys()))
        if missing:
            raise ValueError(
                f"Missing expected strategy returns for split={split_label} run_id={split_run_id}: {missing}"
            )
        unexpected = sorted(set(out.keys()) - expected)
        if unexpected and not bool(allow_unexpected_strategy_files):
            raise ValueError(
                f"Unexpected strategy returns for split={split_label} run_id={split_run_id}: {unexpected}"
            )
        if unexpected and bool(allow_unexpected_strategy_files):
            out = {sid: metrics for sid, metrics in out.items() if sid in expected}
    else:
        expected_count = int(len(out))

    diagnostics = {
        "expected_strategy_count": int(expected_count),
        "observed_strategy_count": int(len(observed_strategy_ids)),
        "unexpected_strategy_ids": unexpected,
    }
    return out, diagnostics


def _load_split_pnl_series_strict(
    split_run_id: str,
    *,
    split_label: str,
    expected_strategy_ids: List[str] | None = None,
    allow_unexpected_strategy_files: bool = False,
) -> pd.Series:
    engine_dir = DATA_ROOT / "runs" / split_run_id / "engine"
    if not engine_dir.exists():
        raise ValueError(f"Missing engine directory for split={split_label} run_id={split_run_id}: {engine_dir}")
    strategy_files = sorted(engine_dir.glob("strategy_returns_*.csv"))
    if not strategy_files:
        raise ValueError(
            f"No strategy return artifacts found for split={split_label} run_id={split_run_id} in {engine_dir}"
        )

    observed_map: Dict[str, Path] = {}
    for path in strategy_files:
        strategy_id = _strategy_id_from_path(path)
        if not strategy_id:
            raise ValueError(f"Invalid strategy return filename for split={split_label} run_id={split_run_id}: {path.name}")
        observed_map[strategy_id] = path

    selected_strategy_ids = sorted(observed_map.keys())
    if expected_strategy_ids is not None:
        expected = {str(x).strip() for x in expected_strategy_ids if str(x).strip()}
        missing = sorted(expected - set(observed_map.keys()))
        if missing:
            raise ValueError(
                f"Missing expected strategy returns for split={split_label} run_id={split_run_id}: {missing}"
            )
        unexpected = sorted(set(observed_map.keys()) - expected)
        if unexpected and not bool(allow_unexpected_strategy_files):
            raise ValueError(
                f"Unexpected strategy returns for split={split_label} run_id={split_run_id}: {unexpected}"
            )
        selected_strategy_ids = sorted(expected) if bool(allow_unexpected_strategy_files) else sorted(observed_map.keys())

    combined = pd.Series(dtype=float)
    for strategy_id in selected_strategy_ids:
        path = observed_map[strategy_id]
        try:
            frame = pd.read_csv(path)
        except Exception as exc:
            raise ValueError(f"Failed reading strategy return file for split={split_label} run_id={split_run_id}: {path}") from exc
        missing_cols = [col for col in REQUIRED_STRATEGY_RETURN_COLUMNS if col not in frame.columns]
        if missing_cols:
            raise ValueError(
                f"Strategy return file missing required columns for split={split_label} run_id={split_run_id}: "
                f"{path.name} missing {missing_cols}"
            )
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
        frame["pnl"] = pd.to_numeric(frame["pnl"], errors="coerce").fillna(0.0)
        frame = frame.dropna(subset=["timestamp"])
        if frame.empty:
            continue
        pnl_ts = frame.groupby("timestamp", sort=True)["pnl"].sum()
        combined = pnl_ts.astype(float) if combined.empty else combined.add(pnl_ts.astype(float), fill_value=0.0)
    return combined.sort_index() if not combined.empty else pd.Series(dtype=float)




def _infer_backtest_family_dir(*, strategies: str, blueprints_path: str | None) -> str:
    # Mirrors backtest_strategies.py output layout.
    if blueprints_path:
        return "dsl"
    items = [s.strip() for s in str(strategies).split(",") if s.strip()]
    families = set()
    for strategy_id in items:
        if strategy_id.startswith("dsl_interpreter_v1__"):
            families.add("dsl")
            continue
        if strategy_id.startswith("vol_compression_v1"):
            families.add("breakout")
            continue
        if strategy_id.startswith("funding_extreme_reversal_v1"):
            families.add("carry")
            continue
        if strategy_id.startswith("cross_venue_desync_v1"):
            families.add("spread")
            continue
        if strategy_id.startswith("liquidity_") or strategy_id.startswith("forced_flow_exhaustion_v1"):
            families.add("mean_reversion")
            continue
        families.add("unknown")
    if len(families) == 1:
        return next(iter(families))
    if not families:
        return "unknown"
    return "hybrid"

def _build_backtest_cmd(
    *,
    split_run_id: str,
    symbols: str,
    start: str,
    end: str,
    force: int,
    fees_bps: float | None,
    slippage_bps: float | None,
    cost_bps: float | None,
    strategies: str | None,
    overlays: str,
    blueprints_path: str | None,
    blueprints_top_k: int,
    blueprints_filter_event_type: str,
    clean_engine_artifacts: int,
    config_paths: List[str],
    timeframe: str = "5m",
) -> List[str]:
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "pipelines" / "backtest" / "backtest_strategies.py"),
        "--run_id",
        split_run_id,
        "--symbols",
        symbols,
        "--start",
        start,
        "--end",
        end,
        "--force",
        str(int(force)),
        "--clean_engine_artifacts",
        str(int(clean_engine_artifacts)),
        "--timeframe",
        str(timeframe),
    ]
    if fees_bps is not None:
        cmd.extend(["--fees_bps", str(float(fees_bps))])
    if slippage_bps is not None:
        cmd.extend(["--slippage_bps", str(float(slippage_bps))])
    if cost_bps is not None:
        cmd.extend(["--cost_bps", str(float(cost_bps))])
    if strategies and str(strategies).strip():
        cmd.extend(["--strategies", str(strategies)])
    if blueprints_path and str(blueprints_path).strip():
        cmd.extend(
            [
                "--blueprints_path",
                str(blueprints_path),
                "--blueprints_top_k",
                str(int(blueprints_top_k)),
                "--blueprints_filter_event_type",
                str(blueprints_filter_event_type),
            ]
        )
    if overlays:
        cmd.extend(["--overlays", str(overlays)])
    for config_path in config_paths:
        cmd.extend(["--config", str(config_path)])
    return cmd


def main() -> int:
    parser = argparse.ArgumentParser(description="Run deterministic walk-forward backtest evaluation")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--embargo_days", type=int, default=1)  # B2: default 1, never 0
    parser.add_argument("--allow_zero_trigger_coverage", type=int, default=0)
    parser.add_argument("--train_frac", type=float, default=0.6)
    parser.add_argument("--validation_frac", type=float, default=0.2)
    parser.add_argument("--force", type=int, default=1)
    parser.add_argument("--fees_bps", type=float, default=None)
    parser.add_argument("--slippage_bps", type=float, default=None)
    parser.add_argument("--cost_bps", type=float, default=None)
    parser.add_argument("--strategies", default=None)
    parser.add_argument("--overlays", default="")
    parser.add_argument("--timeframe", default="5m", help="Bar timeframe for data loading (e.g. '5m', '15m')")
    parser.add_argument("--blueprints_path", default=None)
    parser.add_argument("--blueprints_top_k", type=int, default=10)
    parser.add_argument("--blueprints_filter_event_type", default="all")
    parser.add_argument("--regime_max_share", type=float, default=0.80)
    parser.add_argument("--drawdown_cluster_top_frac", type=float, default=0.10)
    parser.add_argument("--drawdown_tail_q", type=float, default=0.05)
    parser.add_argument("--allow_unexpected_strategy_files", type=int, default=0)
    parser.add_argument("--clean_engine_artifacts", type=int, default=1)
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()

    strategy_mode = bool(args.strategies and str(args.strategies).strip())
    blueprint_mode = bool(args.blueprints_path and str(args.blueprints_path).strip())
    if strategy_mode and blueprint_mode:
        print("run_walkforward: --strategies and --blueprints_path are mutually exclusive.", file=sys.stderr)
        return 1
    if not strategy_mode and not blueprint_mode:
        print("run_walkforward: provide either --strategies or --blueprints_path.", file=sys.stderr)
        return 1
    if not (0.0 < float(args.drawdown_cluster_top_frac) <= 1.0):
        print("run_walkforward: --drawdown_cluster_top_frac must be within (0,1].", file=sys.stderr)
        return 1
    if not (0.0 < float(args.drawdown_tail_q) < 1.0):
        print("run_walkforward: --drawdown_tail_q must be within (0,1).", file=sys.stderr)
        return 1
    # B2: Hard fail if embargo is zero — autocorrelation from persistent regime states bleeds
    # across train/validation boundaries, inflating OOS Sharpe. [INV_EMBARGO_NONZERO]
    if int(args.embargo_days) < 1:
        print(
            f"EVALUATION GUARD [INV_EMBARGO_NONZERO]: --embargo_days={args.embargo_days} is below "
            "the required minimum of 1. Autocorrelation from persistent regime states bleeds across "
            "train/validation boundaries when embargo=0, inflating OOS Sharpe. "
            "Remediation: use --embargo_days 1 or higher.",
            file=sys.stderr,
        )
        return 1

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "eval" / args.run_id
    ensure_dir(out_dir)
    summary_path = out_dir / "walkforward_summary.json"

    resolved_costs = resolve_execution_costs(
        project_root=PROJECT_ROOT,
        config_paths=args.config,
        fees_bps=args.fees_bps,
        slippage_bps=args.slippage_bps,
        cost_bps=args.cost_bps,
    )

    params = {
        "run_id": args.run_id,
        "symbols": args.symbols,
        "start": args.start,
        "end": args.end,
        "embargo_days": int(args.embargo_days),
        "train_frac": float(args.train_frac),
        "validation_frac": float(args.validation_frac),
        "strategies": str(args.strategies or ""),
        "blueprints_path": str(args.blueprints_path or ""),
        "regime_max_share": float(args.regime_max_share),
        "drawdown_cluster_top_frac": float(args.drawdown_cluster_top_frac),
        "drawdown_tail_q": float(args.drawdown_tail_q),
        "allow_unexpected_strategy_files": int(args.allow_unexpected_strategy_files),
        "clean_engine_artifacts": int(args.clean_engine_artifacts),
        "config": [str(path) for path in args.config],
        "execution_cost_config_digest": resolved_costs.config_digest,
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("run_walkforward", args.run_id, params, inputs, outputs)

    try:
        windows = build_time_splits(
            start=args.start,
            end=args.end,
            train_frac=float(args.train_frac),
            validation_frac=float(args.validation_frac),
            embargo_days=int(args.embargo_days),
        )
        split_labels = [str(window.label) for window in windows]
        if "test" not in split_labels:
            raise ValueError(
                "Walkforward split plan must include a `test` window. "
                f"labels={split_labels}, start={args.start}, end={args.end}, "
                f"train_frac={float(args.train_frac)}, validation_frac={float(args.validation_frac)}, "
                f"embargo_days={int(args.embargo_days)}"
            )

        expected_strategy_ids: List[str] | None
        if strategy_mode:
            expected_strategy_ids = [s.strip() for s in str(args.strategies).split(",") if s.strip()]
        else:
            expected_strategy_ids = _expected_blueprint_strategy_ids(
                blueprints_path=Path(str(args.blueprints_path)),
                event_type=str(args.blueprints_filter_event_type),
                top_k=int(args.blueprints_top_k),
                cli_symbols=[s.strip().upper() for s in str(args.symbols).split(",") if s.strip()],
            )

        split_rows: List[Dict[str, object]] = []
        for split in windows:
            split_run_id = f"{args.run_id}__wf_{split.label}"
            cmd = _build_backtest_cmd(
                split_run_id=split_run_id,
                symbols=str(args.symbols),
                start=split.start.isoformat(),
                end=split.end.isoformat(),
                force=int(args.force),
                fees_bps=args.fees_bps,
                slippage_bps=args.slippage_bps,
                cost_bps=args.cost_bps,
                strategies=args.strategies,
                overlays=str(args.overlays or ""),
                blueprints_path=args.blueprints_path,
                blueprints_top_k=int(args.blueprints_top_k),
                blueprints_filter_event_type=str(args.blueprints_filter_event_type),
                clean_engine_artifacts=int(args.clean_engine_artifacts),
                config_paths=[str(path) for path in args.config],
                timeframe=str(args.timeframe),
            )
            rc = _run_split_backtest(cmd)
            if rc != 0:
                raise RuntimeError(f"Backtest failed for split={split.label} run_id={split_run_id}")

            family_dir = _infer_backtest_family_dir(strategies=str(args.strategies), blueprints_path=(str(args.blueprints_path) if args.blueprints_path else None))
            metrics_path = DATA_ROOT / 'lake' / 'trades' / 'backtests' / family_dir / split_run_id / 'metrics.json'
            metrics = _load_split_metrics_strict(metrics_path, split_label=split.label, split_run_id=split_run_id)
            per_strategy_metrics, strategy_set_diag = _load_per_strategy_split_metrics_strict(
                split_run_id=split_run_id,
                split_label=split.label,
                expected_strategy_ids=expected_strategy_ids,
                allow_unexpected_strategy_files=bool(int(args.allow_unexpected_strategy_files)),
                regime_max_share=float(args.regime_max_share),
                drawdown_cluster_top_frac=float(args.drawdown_cluster_top_frac),
                drawdown_tail_q=float(args.drawdown_tail_q),
            )
            stressed_net_pnl = _to_float_strict(
                metrics.get("cost_decomposition", {}).get("net_alpha"),
                label=f"{split.label}.cost_decomposition.net_alpha",
            )
            split_rows.append(
                {
                    "label": split.label,
                    "run_id": split_run_id,
                    "start": split.start.isoformat(),
                    "end": split.end.isoformat(),
                    "metrics_path": str(metrics_path),
                    "metrics": metrics,
                    "per_strategy_metrics": per_strategy_metrics,
                    "strategy_set_diagnostics": strategy_set_diag,
                    "stressed_net_pnl": stressed_net_pnl,
                }
            )
            outputs.append({"path": str(metrics_path), "rows": 1, "start_ts": split.start.isoformat(), "end_ts": split.end.isoformat()})

        per_split_metrics = {
            row["label"]: {
                "run_id": row["run_id"],
                "start": row["start"],
                "end": row["end"],
                "total_trades": int(_to_float_strict(row["metrics"].get("total_trades"), label=f"{row['label']}.total_trades")),
                "ending_equity": _to_float_strict(row["metrics"].get("ending_equity"), label=f"{row['label']}.ending_equity"),
                "sharpe_annualized": _to_float_strict(
                    row["metrics"].get("sharpe_annualized"),
                    label=f"{row['label']}.sharpe_annualized",
                ),
                "max_drawdown": _to_float_strict(row["metrics"].get("max_drawdown"), label=f"{row['label']}.max_drawdown"),
                "stressed_net_pnl": _to_float_strict(row.get("stressed_net_pnl"), label=f"{row['label']}.stressed_net_pnl"),
                "gate_precheck": {
                    "has_trades": bool(int(_to_float_strict(row["metrics"].get("total_trades"), label=f"{row['label']}.total_trades")) > 0),
                    "stressed_non_negative": bool(
                        _to_float_strict(row.get("stressed_net_pnl"), label=f"{row['label']}.stressed_net_pnl") >= 0.0
                    ),
                },
            }
            for row in split_rows
        }
        per_strategy_split_metrics: Dict[str, Dict[str, Dict[str, object]]] = {}
        per_strategy_regime_metrics: Dict[str, Dict[str, Dict[str, object]]] = {}
        per_strategy_drawdown_cluster_metrics: Dict[str, Dict[str, Dict[str, object]]] = {}
        observed_strategy_ids: set[str] = set()
        unexpected_strategy_ids: set[str] = set()
        for row in split_rows:
            split_label = str(row["label"])
            split_run_id = str(row["run_id"])
            split_start = str(row["start"])
            split_end = str(row["end"])
            strategy_set_diagnostics = row.get("strategy_set_diagnostics", {})
            strategy_payload = row.get("per_strategy_metrics", {})
            if not isinstance(strategy_payload, dict):
                continue
            for strategy_id, strategy_metrics in strategy_payload.items():
                if not isinstance(strategy_metrics, dict):
                    continue
                observed_strategy_ids.add(str(strategy_id))
                metric_row = dict(strategy_metrics)
                metric_row["run_id"] = split_run_id
                metric_row["start"] = split_start
                metric_row["end"] = split_end
                per_strategy_split_metrics.setdefault(str(strategy_id), {})[split_label] = metric_row
                per_strategy_regime_metrics.setdefault(str(strategy_id), {})[split_label] = {
                    "run_id": split_run_id,
                    "start": split_start,
                    "end": split_end,
                    "regime_pnl_share": dict(metric_row.get("regime_pnl_share", {})),
                    "max_regime_share": float(metric_row.get("max_regime_share", 1.0)),
                    "regime_consistent": bool(metric_row.get("regime_consistent", False)),
                    "regime_max_share": float(metric_row.get("regime_max_share", float(args.regime_max_share))),
                }
                per_strategy_drawdown_cluster_metrics.setdefault(str(strategy_id), {})[split_label] = {
                    "run_id": split_run_id,
                    "start": split_start,
                    "end": split_end,
                    "max_loss_cluster_len": int(metric_row.get("max_loss_cluster_len", 0)),
                    "p95_loss_cluster_len": float(metric_row.get("p95_loss_cluster_len", 0.0)),
                    "loss_cluster_count": int(metric_row.get("loss_cluster_count", 0)),
                    "cluster_loss_concentration": float(metric_row.get("cluster_loss_concentration", 0.0)),
                    "tail_conditional_drawdown_95": float(metric_row.get("tail_conditional_drawdown_95", 0.0)),
                }
            if isinstance(strategy_set_diagnostics, dict):
                unexpected_strategy_ids.update(
                    str(x).strip()
                    for x in strategy_set_diagnostics.get("unexpected_strategy_ids", [])
                    if str(x).strip()
                )

        per_strategy_split_metrics = {
            strategy_id: {
                split_label: split_metrics[split_label]
                for split_label in sorted(split_metrics.keys())
            }
            for strategy_id, split_metrics in sorted(per_strategy_split_metrics.items())
        }
        per_strategy_regime_metrics = {
            strategy_id: {
                split_label: split_metrics[split_label]
                for split_label in sorted(split_metrics.keys())
            }
            for strategy_id, split_metrics in sorted(per_strategy_regime_metrics.items())
        }
        per_strategy_drawdown_cluster_metrics = {
            strategy_id: {
                split_label: split_metrics[split_label]
                for split_label in sorted(split_metrics.keys())
            }
            for strategy_id, split_metrics in sorted(per_strategy_drawdown_cluster_metrics.items())
        }
        test_row = next((row for row in split_rows if row["label"] == "test"), None)
        if test_row is None:
            raise ValueError("Walkforward execution did not produce a `test` split row.")
        final_test_metrics = test_row["metrics"]
        if not isinstance(final_test_metrics, dict) or not final_test_metrics:
            raise ValueError("Walkforward final_test_metrics is missing or invalid.")
        test_returns = _load_split_pnl_series_strict(
            split_run_id=str(test_row["run_id"]),
            split_label="test",
            expected_strategy_ids=expected_strategy_ids,
            allow_unexpected_strategy_files=bool(int(args.allow_unexpected_strategy_files)),
        )
        standardized_metrics = _standardized_metrics_from_returns(test_returns)
        final_test_metrics = dict(final_test_metrics)
        final_test_metrics["standardized_metrics"] = dict(standardized_metrics)
        integrity_checks = {
            "artifacts_validated": True,
            "required_test_present": True,
            "config_passthrough_count": int(len(args.config)),
            "allow_unexpected_strategy_files": bool(int(args.allow_unexpected_strategy_files)),
        }
            summary = {
                "run_id": args.run_id,
                "cost_config_digest": str(params.get("execution_cost_config_digest", "")),
                "splits": [w.to_dict() for w in windows],
        
            "per_split_metrics": per_split_metrics,
            "per_strategy_split_metrics": per_strategy_split_metrics,
            "per_strategy_regime_metrics": per_strategy_regime_metrics,
            "per_strategy_drawdown_cluster_metrics": per_strategy_drawdown_cluster_metrics,
            "final_test_metrics": final_test_metrics,
            "standardized_metrics": standardized_metrics,
            "tested_splits": len(split_rows),
            "expected_strategy_count": int(len(expected_strategy_ids or [])),
            "observed_strategy_count": int(len(observed_strategy_ids)),
            "unexpected_strategy_ids": sorted(unexpected_strategy_ids),
            "integrity_checks": integrity_checks,
        }
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        outputs.append({"path": str(summary_path), "rows": int(len(split_rows)), "start_ts": None, "end_ts": None})
        finalize_manifest(
            manifest,
            "success",
            stats={
                "tested_splits": int(len(split_rows)),
                "summary_path": str(summary_path),
                "artifacts_validated": True,
                "required_test_present": True,
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())


# --- Trigger coverage aggregation (best-effort) ---
def _copy_trigger_coverage(data_root, split_run_id, wf_out_dir):
    import shutil, os
    src = Path(data_root) / "runs" / split_run_id / "engine" / "trigger_coverage.json"
    if src.exists():
        dst = Path(wf_out_dir) / f"trigger_coverage__{split_run_id}.json"
        shutil.copyfile(src, dst)

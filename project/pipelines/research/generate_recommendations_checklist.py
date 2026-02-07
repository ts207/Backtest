from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT / "data"))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate run-level checklist aligned with docs/recommendations.md promotion gates."
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument(
        "--reports_root",
        default=str(DATA_ROOT / "reports"),
        help="Root reports directory (default: data/reports)",
    )
    parser.add_argument(
        "--runs_root",
        default=str(DATA_ROOT / "runs"),
        help="Root runs directory (default: data/runs)",
    )
    parser.add_argument(
        "--out_dir",
        default="",
        help="Optional output directory (default: data/runs/<run_id>/research_checklist)",
    )
    parser.add_argument("--cost_bps", type=float, default=6.0)
    parser.add_argument("--min_trade_count", type=int, default=20)
    parser.add_argument("--max_drawdown_pct", type=float, default=20.0)
    parser.add_argument("--max_missing_ohlcv_pct", type=float, default=1.0)
    return parser.parse_args()


def _find_summary_json(reports_root: Path, run_id: str) -> Path | None:
    candidates = [
        reports_root / "vol_compression_expansion_v1" / run_id / "summary.json",
        reports_root / "by_run" / run_id / "backtest" / "vol_compression_expansion_v1" / "summary.json",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def _extract_fee_row(summary: dict[str, Any], cost_bps: float) -> dict[str, Any] | None:
    rows = summary.get("fee_sensitivity", [])
    if not isinstance(rows, list):
        return None
    for row in rows:
        try:
            if abs(float(row.get("fee_bps_per_side", -1.0)) - cost_bps) < 1e-9:
                return row
        except (TypeError, ValueError):
            continue
    return None


def _max_missing_ohlcv_pct(summary: dict[str, Any]) -> float | None:
    quality = summary.get("data_quality", {})
    if not isinstance(quality, dict):
        return None
    symbols = quality.get("symbols", {})
    if not isinstance(symbols, dict):
        return None

    observed: list[float] = []
    for payload in symbols.values():
        if not isinstance(payload, dict):
            continue
        monthly = payload.get("pct_missing_ohlcv", {})
        if not isinstance(monthly, dict):
            continue
        for month_payload in monthly.values():
            if not isinstance(month_payload, dict):
                continue
            value = month_payload.get("pct_missing_ohlcv")
            if value is None:
                continue
            try:
                observed.append(float(value) * 100.0)
            except (TypeError, ValueError):
                continue
    return max(observed) if observed else None


def _stability_checks(summary: dict[str, Any]) -> dict[str, Any]:
    checks = summary.get("stability_checks", {})
    return checks if isinstance(checks, dict) else {}


def _gate_result(name: str, passed: bool, observed: Any, threshold: Any, note: str = "") -> dict[str, Any]:
    return {
        "name": name,
        "passed": bool(passed),
        "observed": observed,
        "threshold": threshold,
        "note": note,
    }


def _build_payload(summary: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    reasons: list[str] = []
    gates: list[dict[str, Any]] = []

    fee_row = _extract_fee_row(summary, float(args.cost_bps))
    if fee_row is None:
        gates.append(
            _gate_result(
                "positive_net_return_after_costs",
                False,
                None,
                f"> 0.0 at {args.cost_bps} bps",
                "missing fee sensitivity row for configured cost",
            )
        )
        reasons.append("missing fee sensitivity row for configured cost")
    else:
        net_return = float(fee_row.get("net_return", 0.0) or 0.0)
        passed = net_return > 0.0
        gates.append(
            _gate_result(
                "positive_net_return_after_costs",
                passed,
                net_return,
                "> 0.0",
            )
        )
        if not passed:
            reasons.append(f"net return not positive at {args.cost_bps} bps ({net_return:.6f})")

    total_trades = int(summary.get("total_trades", 0) or 0)
    trade_count_ok = total_trades >= int(args.min_trade_count)
    gates.append(_gate_result("minimum_trade_count", trade_count_ok, total_trades, args.min_trade_count))
    if not trade_count_ok:
        reasons.append(f"trade count below threshold ({total_trades} < {args.min_trade_count})")

    max_drawdown = float(summary.get("max_drawdown", 0.0) or 0.0)
    max_drawdown_pct = abs(max_drawdown) * 100.0
    drawdown_ok = max_drawdown_pct <= float(args.max_drawdown_pct)
    gates.append(_gate_result("max_drawdown_cap", drawdown_ok, max_drawdown_pct, args.max_drawdown_pct))
    if not drawdown_ok:
        reasons.append(f"max drawdown exceeds threshold ({max_drawdown_pct:.2f}% > {args.max_drawdown_pct:.2f}%)")

    missing_ohlcv_pct = _max_missing_ohlcv_pct(summary)
    if missing_ohlcv_pct is None:
        gates.append(
            _gate_result(
                "data_quality_missing_ohlcv",
                False,
                None,
                f"<= {args.max_missing_ohlcv_pct}",
                "no monthly missing OHLCV diagnostics available",
            )
        )
        reasons.append("missing data quality diagnostics for OHLCV coverage")
    else:
        data_quality_ok = missing_ohlcv_pct <= float(args.max_missing_ohlcv_pct)
        gates.append(
            _gate_result(
                "data_quality_missing_ohlcv",
                data_quality_ok,
                missing_ohlcv_pct,
                args.max_missing_ohlcv_pct,
            )
        )
        if not data_quality_ok:
            reasons.append(
                f"missing OHLCV exceeds threshold ({missing_ohlcv_pct:.4f}% > {args.max_missing_ohlcv_pct:.4f}%)"
            )

    stability = _stability_checks(summary)
    has_stability = bool(stability)
    gates.append(
        _gate_result(
            "stability_checks_present",
            has_stability,
            sorted(stability.keys()),
            "non-empty dict",
            "can be produced by a dedicated research stage",
        )
    )
    if not has_stability:
        reasons.append("stability checks are missing")

    decision = "PROMOTE" if all(gate["passed"] for gate in gates) else "KEEP_RESEARCH"
    return {
        "run_id": summary.get("run_id"),
        "decision": decision,
        "gates": gates,
        "failure_reasons": reasons,
        "config": {
            "cost_bps": float(args.cost_bps),
            "min_trade_count": int(args.min_trade_count),
            "max_drawdown_pct": float(args.max_drawdown_pct),
            "max_missing_ohlcv_pct": float(args.max_missing_ohlcv_pct),
        },
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Research Checklist",
        "",
        f"- Run ID: `{payload.get('run_id')}`",
        f"- Decision: **{payload.get('decision')}**",
        "",
        "## Gate Results",
        "",
    ]
    for gate in payload.get("gates", []):
        status = "PASS" if gate.get("passed") else "FAIL"
        lines.append(
            f"- **{gate.get('name')}**: {status} (observed={gate.get('observed')}, threshold={gate.get('threshold')})"
        )
        if gate.get("note"):
            lines.append(f"  - note: {gate.get('note')}")

    reasons = payload.get("failure_reasons", [])
    lines.extend(["", "## Failure Reasons", ""])
    if reasons:
        for reason in reasons:
            lines.append(f"- {reason}")
    else:
        lines.append("- None")

    return "\n".join(lines) + "\n"


def main() -> int:
    args = _parse_args()
    reports_root = Path(args.reports_root)
    summary_path = _find_summary_json(reports_root, args.run_id)
    if summary_path is None:
        print(f"summary.json not found for run_id={args.run_id}")
        return 1

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    payload = _build_payload(summary, args)

    out_dir = Path(args.out_dir) if args.out_dir else Path(args.runs_root) / args.run_id / "research_checklist"
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / "checklist.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    md_path = out_dir / "checklist.md"
    md_path.write_text(_render_markdown(payload), encoding="utf-8")

    print(json.dumps({"decision": payload["decision"], "out_dir": str(out_dir)}, indent=2, sort_keys=True))
    return 0 if payload["decision"] == "PROMOTE" else 1


if __name__ == "__main__":
    raise SystemExit(main())

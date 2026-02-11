from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from strategies.overlay_registry import get_overlay

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check whether an overlay satisfies promotion contract.")
    parser.add_argument("--overlay", required=True, help="Overlay name from edges/*.json")
    parser.add_argument(
        "--reports_root",
        default=str(PROJECT_ROOT / "data" / "reports"),
        help="Root containing report artifacts (default: data/reports)",
    )
    parser.add_argument(
        "--out_dir",
        default="",
        help="Output directory for verdict files. Default: data/reports/overlay_promotion/<overlay>",
    )
    return parser.parse_args()


def _find_summary_json(reports_root: Path, run_id: str) -> Path | None:
    candidates = [
        reports_root / "by_run" / run_id / "backtest" / "vol_compression_expansion_v1" / "summary.json",
        reports_root / "vol_compression_expansion_v1" / run_id / "summary.json",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def _extract_net_return(summary: dict[str, Any], cost_bps: float) -> float | None:
    rows = summary.get("fee_sensitivity", [])
    if not isinstance(rows, list):
        return None
    for row in rows:
        if abs(float(row.get("fee_bps_per_side", -1.0)) - float(cost_bps)) < 1e-9:
            try:
                return float(row.get("net_return"))
            except (TypeError, ValueError):
                return None
    return None


def _is_overlay_bound(summary: dict[str, Any], overlay_name: str) -> bool:
    payload = summary.get("overlay_bindings", {})
    if not isinstance(payload, dict):
        return False
    for strategy_data in payload.values():
        if not isinstance(strategy_data, dict):
            continue
        applied = strategy_data.get("applied_overlays", [])
        if isinstance(applied, list) and overlay_name in applied:
            return True
    return False


def _check_single_run(
    summary: dict[str, Any],
    overlay_name: str,
    objective_metric: str,
    cost_bps: float,
    constraints: dict[str, Any],
    stability: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []

    if not _is_overlay_bound(summary, overlay_name):
        reasons.append("overlay not found in summary.overlay_bindings")

    if objective_metric == "net_total_return":
        net_return = _extract_net_return(summary, cost_bps)
        if net_return is None:
            reasons.append(f"missing fee_sensitivity.net_return for cost_bps={cost_bps}")
    else:
        reasons.append(f"unsupported objective metric in checker: {objective_metric}")

    max_dd_limit = constraints.get("max_drawdown_pct")
    max_drawdown = summary.get("max_drawdown")
    if max_dd_limit is not None:
        if max_drawdown is None:
            reasons.append("missing summary.max_drawdown")
        else:
            drawdown_pct = abs(float(max_drawdown) * 100.0)
            if drawdown_pct > float(max_dd_limit):
                reasons.append(f"max_drawdown_pct violated ({drawdown_pct:.2f} > {float(max_dd_limit):.2f})")

    risk_metrics = summary.get("risk_metrics", {})
    if not isinstance(risk_metrics, dict):
        risk_metrics = {}

    tail_cfg = constraints.get("tail_loss") if isinstance(constraints, dict) else None
    if isinstance(tail_cfg, dict) and tail_cfg:
        metric = str(tail_cfg.get("metric", "")).strip()
        max_abs_return = tail_cfg.get("max_abs_return")
        if metric:
            observed = risk_metrics.get(metric)
            if observed is None:
                reasons.append(f"missing risk_metrics.{metric}")
            elif max_abs_return is not None and abs(float(observed)) > float(max_abs_return):
                reasons.append(f"tail_loss violated for {metric} ({abs(float(observed)):.6f} > {float(max_abs_return):.6f})")

    turnover_cfg = constraints.get("turnover_budget") if isinstance(constraints, dict) else None
    if isinstance(turnover_cfg, dict) and turnover_cfg:
        max_bps_day = turnover_cfg.get("max_bps_per_day")
        if max_bps_day is not None:
            observed = risk_metrics.get("bps_per_day")
            if observed is None:
                reasons.append("missing risk_metrics.bps_per_day")
            elif float(observed) > float(max_bps_day):
                reasons.append(f"turnover bps/day violated ({float(observed):.2f} > {float(max_bps_day):.2f})")
        max_trades_day = turnover_cfg.get("max_trades_per_day")
        if max_trades_day is not None:
            observed = risk_metrics.get("trades_per_day")
            if observed is None:
                reasons.append("missing risk_metrics.trades_per_day")
            elif float(observed) > float(max_trades_day):
                reasons.append(f"turnover trades/day violated ({float(observed):.2f} > {float(max_trades_day):.2f})")

    exposure_cfg = constraints.get("exposure_limits") if isinstance(constraints, dict) else None
    if isinstance(exposure_cfg, dict) and exposure_cfg:
        exposure_map = {
            "max_gross": "max_gross",
            "max_per_symbol": "max_per_symbol",
            "max_time_in_market": "max_time_in_market",
        }
        for key, metric_key in exposure_map.items():
            if key in exposure_cfg and exposure_cfg[key] is not None:
                observed = risk_metrics.get(metric_key)
                if observed is None:
                    reasons.append(f"missing risk_metrics.{metric_key}")
                elif float(observed) > float(exposure_cfg[key]):
                    reasons.append(f"exposure {metric_key} violated ({float(observed):.4f} > {float(exposure_cfg[key]):.4f})")

    stability_checks = summary.get("stability_checks", {})
    if not isinstance(stability_checks, dict):
        stability_checks = {}

    sign_consistency_min = stability.get("sign_consistency_min")
    observed_consistency = stability_checks.get("sign_consistency")
    if observed_consistency is None:
        reasons.append("missing stability_checks.sign_consistency")
    elif float(observed_consistency) < float(sign_consistency_min):
        reasons.append(
            f"sign_consistency below threshold ({float(observed_consistency):.3f} < {float(sign_consistency_min):.3f})"
        )

    required_ci_excludes_0 = bool(stability.get("effect_ci_excludes_0"))
    observed_ci_excludes_0 = stability_checks.get("effect_ci_excludes_0")
    if observed_ci_excludes_0 is None:
        reasons.append("missing stability_checks.effect_ci_excludes_0")
    elif bool(observed_ci_excludes_0) != required_ci_excludes_0:
        reasons.append(
            "effect_ci_excludes_0 mismatch "
            f"(observed={bool(observed_ci_excludes_0)} required={required_ci_excludes_0})"
        )

    max_regime_flips = int(stability.get("max_regime_flip_count", 0))
    observed_flips = stability_checks.get("regime_flip_count")
    if observed_flips is None:
        reasons.append("missing stability_checks.regime_flip_count")
    elif int(observed_flips) > max_regime_flips:
        reasons.append(f"regime_flip_count violated ({int(observed_flips)} > {max_regime_flips})")

    return reasons


def main() -> int:
    args = _parse_args()
    reports_root = Path(args.reports_root)
    overlay_name = args.overlay.strip()
    overlay = get_overlay(overlay_name)
    output_dir = (
        Path(args.out_dir)
        if args.out_dir
        else reports_root / "overlay_promotion" / overlay_name
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    reasons: list[dict[str, Any]] = []

    if overlay.get("status") != "APPROVED":
        reasons.append({"scope": "spec", "reason": f"overlay status is {overlay.get('status')}, expected APPROVED"})

    objective = overlay.get("objective", {})
    constraints = overlay.get("constraints", {})
    stability = overlay.get("stability", {})
    cost_bps = float(overlay.get("cost_bps_validated", 0.0))

    for evidence in overlay.get("run_ids_evidence", []):
        run_id = str(evidence.get("run_id", "")).strip()
        summary_path = _find_summary_json(reports_root, run_id)
        if summary_path is None:
            reasons.append({"scope": run_id or "unknown", "reason": "summary.json not found for evidence run"})
            continue
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        run_reasons = _check_single_run(
            summary=summary,
            overlay_name=overlay_name,
            objective_metric=str(objective.get("target_metric", "")),
            cost_bps=cost_bps,
            constraints=constraints,
            stability=stability,
        )
        for reason in run_reasons:
            reasons.append({"scope": run_id, "reason": reason, "summary_path": str(summary_path)})

    verdict = "PASS_PROMOTION" if not reasons else "FAIL_PROMOTION"
    payload = {
        "overlay": overlay_name,
        "verdict": verdict,
        "reasons": reasons,
        "checked_evidence_runs": len(overlay.get("run_ids_evidence", [])),
    }

    json_path = output_dir / "promotion_verdict.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    lines = [
        f"# Overlay Promotion Verdict: {overlay_name}",
        "",
        f"- Verdict: **{verdict}**",
        f"- Evidence runs checked: `{payload['checked_evidence_runs']}`",
        "",
    ]
    if reasons:
        lines.append("## Reasons")
        lines.append("")
        for row in reasons:
            lines.append(f"- [{row.get('scope', 'unknown')}] {row.get('reason', 'unknown reason')}")
    else:
        lines.append("All promotion checks passed.")

    md_path = output_dir / "promotion_verdict.md"
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if verdict == "PASS_PROMOTION" else 1


if __name__ == "__main__":
    raise SystemExit(main())

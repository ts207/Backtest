from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import choose_partition_dir, list_parquet_files, read_parquet, run_scoped_lake_path


def _read_promoted(path: Path) -> List[Dict[str, object]]:
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, dict):
        payload = payload.get("candidates", [])
    return payload if isinstance(payload, list) else []


def _find_phase1_events_csv(event_type: str, run_id: str) -> Path | None:
    base = DATA_ROOT / "reports" / event_type / run_id
    if not base.exists():
        return None
    cands = sorted(base.glob("*events.csv"))
    return cands[0] if cands else None


def _load_bars(symbol: str, run_id: str, timeframe: str) -> pd.DataFrame:
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", "perp", symbol, f"bars_{timeframe}"),
        DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}",
    ]
    bars_dir = choose_partition_dir(candidates)
    if not bars_dir:
        return pd.DataFrame()
    bars = read_parquet(list_parquet_files(bars_dir))
    if bars.empty:
        return bars
    bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True, errors="coerce")
    bars = bars.sort_values("timestamp").reset_index(drop=True)
    return bars




def _table_text(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except ImportError:
        return df.to_string(index=False)

def _infer_direction(candidate: Dict[str, object]) -> int:
    val = candidate.get("expected_return_proxy")
    try:
        v = float(val)
    except (TypeError, ValueError):
        return 1
    return -1 if v < 0 else 1


def _simulate_candidate(
    events: pd.DataFrame,
    bars_by_symbol: Dict[str, pd.DataFrame],
    direction: int,
    horizon_bars: int,
    total_cost_per_side_bps: float,
) -> pd.DataFrame:
    out: List[Dict[str, object]] = []
    if events.empty:
        return pd.DataFrame(out)

    for row in events.itertuples(index=False):
        symbol = getattr(row, "symbol", None)
        if symbol is None or symbol not in bars_by_symbol:
            continue
        bars = bars_by_symbol[symbol]
        if bars.empty:
            continue

        event_end = getattr(row, "exit_ts", None)
        if event_end is None:
            event_end = getattr(row, "enter_ts", None)
        event_end = pd.to_datetime(event_end, utc=True, errors="coerce")
        if pd.isna(event_end):
            continue

        entry_idx = bars.index[bars["timestamp"] > event_end]
        if len(entry_idx) == 0:
            continue
        i = int(entry_idx[0])
        j = i + max(1, int(horizon_bars)) - 1
        if j >= len(bars):
            continue

        entry_price = float(bars.at[i, "open"])
        exit_price = float(bars.at[j, "close"])
        if entry_price <= 0:
            continue

        gross = direction * ((exit_price / entry_price) - 1.0)
        cost = 2.0 * (total_cost_per_side_bps / 10_000.0)
        net = gross - cost

        out.append(
            {
                "symbol": symbol,
                "event_end_ts": event_end.isoformat(),
                "entry_ts": bars.at[i, "timestamp"].isoformat(),
                "exit_ts": bars.at[j, "timestamp"].isoformat(),
                "entry_price": entry_price,
                "exit_price": exit_price,
                "direction": int(direction),
                "gross_return": float(gross),
                "net_return": float(net),
            }
        )

    return pd.DataFrame(out)


def run_promoted_edge_audits(
    run_id: str,
    timeframe: str = "15m",
    horizon_bars: int = 1,
    top_n: int = 3,
    fee_bps_per_side: float = 8.0,
    spread_bps_per_side: float = 2.0,
) -> Dict[str, object]:
    phase2_root = DATA_ROOT / "reports" / "phase2" / run_id
    out_root = DATA_ROOT / "reports" / "promotion_audits" / run_id
    out_root.mkdir(parents=True, exist_ok=True)

    total_cost_per_side_bps = float(fee_bps_per_side) + float(spread_bps_per_side)
    event_dirs = sorted([p for p in phase2_root.iterdir() if p.is_dir()]) if phase2_root.exists() else []

    family_summaries: List[Dict[str, object]] = []

    for event_dir in event_dirs:
        event_type = event_dir.name
        prom_path = event_dir / "promoted_candidates.json"
        if not prom_path.exists():
            continue

        promoted = _read_promoted(prom_path)
        if not promoted:
            continue

        promoted = sorted(promoted, key=lambda x: float(x.get("edge_score", 0.0) or 0.0), reverse=True)[: max(1, int(top_n))]

        events_csv = _find_phase1_events_csv(event_type, run_id)
        if events_csv is None:
            family_summaries.append(
                {
                    "event_type": event_type,
                    "status": "missing_events",
                    "promoted_candidates": len(promoted),
                    "source_path": str(prom_path),
                }
            )
            continue

        events = pd.read_csv(events_csv)
        if events.empty or "symbol" not in events.columns:
            family_summaries.append(
                {
                    "event_type": event_type,
                    "status": "invalid_events",
                    "promoted_candidates": len(promoted),
                    "events_path": str(events_csv),
                }
            )
            continue

        for ts_col in ["enter_ts", "exit_ts"]:
            if ts_col in events.columns:
                events[ts_col] = pd.to_datetime(events[ts_col], utc=True, errors="coerce")

        bars_by_symbol = {sym: _load_bars(sym, run_id, timeframe) for sym in sorted(events["symbol"].dropna().astype(str).unique())}

        rows = []
        for cand in promoted:
            direction = _infer_direction(cand)
            trades = _simulate_candidate(events, bars_by_symbol, direction, horizon_bars, total_cost_per_side_bps)
            if trades.empty:
                rows.append(
                    {
                        "candidate_id": cand.get("candidate_id"),
                        "edge_score": cand.get("edge_score"),
                        "expected_return_proxy": cand.get("expected_return_proxy"),
                        "direction": direction,
                        "trades": 0,
                        "mean_net_return": 0.0,
                        "total_net_return": 0.0,
                    }
                )
                continue

            trades["candidate_id"] = str(cand.get("candidate_id", ""))
            rows.append(
                {
                    "candidate_id": cand.get("candidate_id"),
                    "edge_score": cand.get("edge_score"),
                    "expected_return_proxy": cand.get("expected_return_proxy"),
                    "direction": direction,
                    "trades": int(len(trades)),
                    "mean_net_return": float(trades["net_return"].mean()),
                    "total_net_return": float(trades["net_return"].sum()),
                }
            )

        event_out = out_root / event_type
        event_out.mkdir(parents=True, exist_ok=True)
        summary_df = pd.DataFrame(rows)
        summary_df.to_csv(event_out / "candidate_audit_summary.csv", index=False)

        summary_payload = {
            "run_id": run_id,
            "event_type": event_type,
            "timeframe": timeframe,
            "horizon_bars": int(horizon_bars),
            "total_cost_per_side_bps": float(total_cost_per_side_bps),
            "events_path": str(events_csv),
            "candidates": rows,
        }
        (event_out / "summary.json").write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

        md_lines = [
            f"# Promotion Audit Summary: {event_type}",
            "",
            f"- run_id: `{run_id}`",
            f"- timeframe: `{timeframe}`",
            f"- horizon_bars: `{horizon_bars}`",
            f"- total_cost_per_side_bps: `{total_cost_per_side_bps}`",
            "",
            "## Candidate Results",
            "",
            _table_text(summary_df) if not summary_df.empty else "None",
            "",
            "Method: entry at first bar open after event end (`exit_ts` fallback `enter_ts`), fixed horizon exit, no tunable filters beyond promoted candidate direction sign inference.",
        ]
        (event_out / "summary.md").write_text("\n".join(md_lines), encoding="utf-8")

        family_summaries.append(
            {
                "event_type": event_type,
                "status": "ok",
                "promoted_candidates": len(promoted),
                "summary_path": str(event_out / "summary.md"),
                "events_path": str(events_csv),
            }
        )

    payload = {
        "run_id": run_id,
        "family_count": len(family_summaries),
        "families": family_summaries,
        "timeframe": timeframe,
        "horizon_bars": int(horizon_bars),
        "total_cost_per_side_bps": float(total_cost_per_side_bps),
    }
    (out_root / "summary.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    top_md = [
        "# Promoted Edge Audits",
        "",
        f"- run_id: `{run_id}`",
        f"- families processed: {len(family_summaries)}",
        f"- horizon_bars: {horizon_bars}",
        f"- total_cost_per_side_bps: {total_cost_per_side_bps}",
        "",
    ]
    if family_summaries:
        top_md.append(_table_text(pd.DataFrame(family_summaries)))
    else:
        top_md.append("No phase2 promoted candidate families found.")
    (out_root / "summary.md").write_text("\n".join(top_md), encoding="utf-8")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Run fixed, non-tunable promotion audits per promoted event family")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--timeframe", default="15m")
    parser.add_argument("--horizon_bars", type=int, default=1)
    parser.add_argument("--top_n", type=int, default=3)
    parser.add_argument("--fee_bps_per_side", type=float, default=8.0)
    parser.add_argument("--spread_bps_per_side", type=float, default=2.0)
    args = parser.parse_args()

    payload = run_promoted_edge_audits(
        run_id=args.run_id,
        timeframe=args.timeframe,
        horizon_bars=args.horizon_bars,
        top_n=args.top_n,
        fee_bps_per_side=args.fee_bps_per_side,
        spread_bps_per_side=args.spread_bps_per_side,
    )
    print(json.dumps({"status": "ok", "run_id": args.run_id, "family_count": payload["family_count"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

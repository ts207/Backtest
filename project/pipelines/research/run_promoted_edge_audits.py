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

PREFERRED_VARIANTS = {
    "vol_shock_relaxation": "_0",
    "directional_exhaustion_after_forced_flow": "_1",
    "range_compression_breakout_window": "_0",
    "liquidity_refill_lag_window": "_1",
    "liquidity_absence_window": "_0",
    "funding_extreme_reversal_window": "_0",
    "vol_aftershock_window": "_0",
}

PRIMARY_STRATEGY_ORDER = [
    "directional_exhaustion_after_forced_flow",
    "liquidity_refill_lag_window",
    "vol_shock_relaxation",
]


def _collapse_promoted_variants(event_type: str, promoted: List[Dict[str, object]]) -> List[Dict[str, object]]:
    preferred_suffix = PREFERRED_VARIANTS.get(event_type)
    if not preferred_suffix:
        return promoted
    preferred = [row for row in promoted if str(row.get("candidate_id", "")).endswith(preferred_suffix)]
    if preferred:
        return sorted(preferred, key=lambda x: float(x.get("edge_score", 0.0) or 0.0), reverse=True)[:1]
    return sorted(promoted, key=lambda x: float(x.get("edge_score", 0.0) or 0.0), reverse=True)[:1]


def _event_direction(event_type: str, row: object, bars: pd.DataFrame) -> int:
    if event_type == "directional_exhaustion_after_forced_flow":
        raw = getattr(row, "direction", 0)
        try:
            base = int(raw)
        except (TypeError, ValueError):
            base = 0
        return -base if base else 0

    event_end = getattr(row, "exit_ts", None)
    if event_end is None:
        event_end = getattr(row, "enter_ts", None)
    if event_end is None:
        event_end = getattr(row, "anchor_ts", None)
    event_end = pd.to_datetime(event_end, utc=True, errors="coerce")
    if pd.isna(event_end) or bars.empty:
        return 0

    prev_idx = bars.index[bars["timestamp"] <= event_end]
    if len(prev_idx) == 0:
        next_idx = bars.index[bars["timestamp"] >= event_end]
        if len(next_idx) == 0:
            return 0
        i = int(next_idx[0])
    else:
        i = int(prev_idx[-1])
    impulse = float(bars.at[i, "close"]) - float(bars.at[i, "open"])
    return -1 if impulse > 0 else 1


def _is_invalidated(event_type: str, row: object, event_end: pd.Timestamp) -> bool:
    if event_type != "directional_exhaustion_after_forced_flow":
        return False
    invalid_cols = ["opposite_liquidation_cluster", "opposite_liq_cluster", "invalidate"]
    for col in invalid_cols:
        val = getattr(row, col, None)
        if val is not None:
            try:
                return bool(int(val))
            except (TypeError, ValueError):
                continue
    return False


def _simulate_candidate(
    event_type: str,
    events: pd.DataFrame,
    bars_by_symbol: Dict[str, pd.DataFrame],
    horizon_bars: int,
    fee_bps_per_side: float,
    spread_bps_per_side: float,
) -> tuple[pd.DataFrame, Dict[str, int], pd.DataFrame]:
    out: List[Dict[str, object]] = []
    used_events: List[Dict[str, object]] = []
    stats = {
        "events_loaded": int(len(events)),
        "events_with_symbol": 0,
        "events_with_bars": 0,
        "events_with_valid_end": 0,
        "events_after_invalidation": 0,
        "events_with_direction": 0,
        "events_with_entry": 0,
        "events_with_horizon": 0,
        "events_traded": 0,
    }
    if events.empty:
        return pd.DataFrame(out), stats, pd.DataFrame(used_events)

    for row in events.itertuples(index=False):
        symbol = getattr(row, "symbol", None)
        if symbol is None:
            continue
        stats["events_with_symbol"] += 1
        if symbol not in bars_by_symbol:
            continue
        bars = bars_by_symbol[symbol]
        if bars.empty:
            continue
        stats["events_with_bars"] += 1

        event_end = getattr(row, "exit_ts", None)
        if event_end is None:
            event_end = getattr(row, "enter_ts", None)
        if event_end is None:
            event_end = getattr(row, "anchor_ts", None)
        event_end = pd.to_datetime(event_end, utc=True, errors="coerce")
        if pd.isna(event_end):
            continue
        stats["events_with_valid_end"] += 1

        if _is_invalidated(event_type, row, event_end):
            continue
        stats["events_after_invalidation"] += 1

        direction = _event_direction(event_type, row, bars)
        if direction == 0:
            continue
        stats["events_with_direction"] += 1

        entry_idx = bars.index[bars["timestamp"] >= event_end]
        if len(entry_idx) == 0:
            continue
        i = int(entry_idx[0])
        stats["events_with_entry"] += 1

        j = i + max(1, int(horizon_bars)) - 1
        if j >= len(bars):
            continue
        stats["events_with_horizon"] += 1

        entry_price = float(bars.at[i, "open"])
        exit_price = float(bars.at[j, "close"])
        if entry_price <= 0:
            continue

        gross = direction * ((exit_price / entry_price) - 1.0)
        fee_cost = 2.0 * (float(fee_bps_per_side) / 10_000.0)
        slippage_cost = 2.0 * (float(spread_bps_per_side) / 10_000.0)
        total_cost = fee_cost + slippage_cost
        net = gross - total_cost
        stats["events_traded"] += 1

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
                "fee_cost_return": float(fee_cost),
                "slippage_cost_return": float(slippage_cost),
                "total_cost_return": float(total_cost),
                "net_return": float(net),
            }
        )
        used_events.append(
            {
                "symbol": symbol,
                "event_end_ts": event_end.isoformat(),
                "event_direction": int(direction),
                "entry_ts": bars.at[i, "timestamp"].isoformat(),
                "exit_ts": bars.at[j, "timestamp"].isoformat(),
            }
        )

    return pd.DataFrame(out), stats, pd.DataFrame(used_events)


def _overlap_within_15m(a: pd.DataFrame, b: pd.DataFrame) -> tuple[int, pd.DataFrame]:
    if a.empty or b.empty:
        return 0, pd.DataFrame()
    aa = a[["symbol", "entry_ts", "net_return"]].copy().rename(columns={"entry_ts": "entry_ts_a", "net_return": "net_return_a"})
    bb = b[["symbol", "entry_ts", "net_return"]].copy().rename(columns={"entry_ts": "entry_ts_b", "net_return": "net_return_b"})
    aa["entry_ts_a"] = pd.to_datetime(aa["entry_ts_a"], utc=True, errors="coerce")
    bb["entry_ts_b"] = pd.to_datetime(bb["entry_ts_b"], utc=True, errors="coerce")
    aa = aa.dropna(subset=["entry_ts_a", "symbol"]).sort_values(["entry_ts_a", "symbol"]).reset_index(drop=True)
    bb = bb.dropna(subset=["entry_ts_b", "symbol"]).sort_values(["entry_ts_b", "symbol"]).reset_index(drop=True)
    if aa.empty or bb.empty:
        return 0, pd.DataFrame()
    merged = pd.merge_asof(
        aa,
        bb,
        left_on="entry_ts_a",
        right_on="entry_ts_b",
        by="symbol",
        tolerance=pd.Timedelta(minutes=15),
        direction="nearest",
    ).dropna(subset=["entry_ts_b"])
    return int(len(merged)), merged


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

    horizon_bars = 1
    total_cost_per_side_bps = float(fee_bps_per_side) + float(spread_bps_per_side)
    event_dirs = sorted([p for p in phase2_root.iterdir() if p.is_dir()]) if phase2_root.exists() else []

    family_summaries: List[Dict[str, object]] = []

    selected_dirs: List[Path] = []
    by_name = {p.name: p for p in event_dirs}
    for event_name in PRIMARY_STRATEGY_ORDER:
        if event_name in by_name:
            selected_dirs.append(by_name[event_name])

    for event_dir in selected_dirs:
        event_type = event_dir.name
        prom_path = event_dir / "promoted_candidates.json"
        if not prom_path.exists():
            continue

        promoted = _read_promoted(prom_path)
        if not promoted:
            continue

        promoted = _collapse_promoted_variants(event_type, promoted)

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
        event_out = out_root / event_type
        event_out.mkdir(parents=True, exist_ok=True)

        rows = []
        for cand in promoted:
            trades, event_counts, used_events = _simulate_candidate(event_type, events, bars_by_symbol, horizon_bars, fee_bps_per_side, spread_bps_per_side)
            if trades.empty:
                rows.append(
                    {
                        "candidate_id": cand.get("candidate_id"),
                        "edge_score": cand.get("edge_score"),
                        "expected_return_proxy": cand.get("expected_return_proxy"),
                        "direction": "event_driven",
                        "trades": 0,
                        "mean_net_return": 0.0,
                        "total_net_return": 0.0,
                        "gross_return": 0.0,
                        "max_dd": 0.0,
                        "win_rate": 0.0,
                        "sharpe": 0.0,
                        **event_counts,
                    }
                )
                (event_out / f"events_used_{cand.get('candidate_id','candidate')}.csv").write_text("", encoding="utf-8")
                continue

            eq = trades["net_return"].cumsum()
            max_dd = float((eq - eq.cummax()).min()) if not eq.empty else 0.0
            win_rate = float((trades["net_return"] > 0).mean()) if len(trades) else 0.0
            sharpe = 0.0
            if len(trades) > 1 and float(trades["net_return"].std(ddof=1) or 0.0) > 0:
                sharpe = float((trades["net_return"].mean() / trades["net_return"].std(ddof=1)) * (len(trades) ** 0.5))

            symbol_means = trades.groupby("symbol", as_index=False)["net_return"].mean()
            stable_by_symbol = bool((symbol_means["net_return"] > 0).all()) if not symbol_means.empty else False
            acceptance = bool((trades["net_return"].sum() > 0) and stable_by_symbol and (len(trades) >= 80))
            if event_type == "vol_shock_relaxation" and len(trades) < 100:
                acceptance = False

            trades["candidate_id"] = str(cand.get("candidate_id", ""))
            rows.append(
                {
                    "candidate_id": cand.get("candidate_id"),
                    "edge_score": cand.get("edge_score"),
                    "expected_return_proxy": cand.get("expected_return_proxy"),
                    "direction": "event_driven",
                    "trades": int(len(trades)),
                    "mean_net_return": float(trades["net_return"].mean()),
                    "total_net_return": float(trades["net_return"].sum()),
                    "gross_return": float(trades["gross_return"].sum()),
                    "max_dd": max_dd,
                    "win_rate": win_rate,
                    "sharpe": sharpe,
                    "stable_sign_btc_eth": stable_by_symbol,
                    "accepted": acceptance,
                    "monitor_only": bool(event_type == "vol_shock_relaxation" and len(trades) < 100),
                    "fees_paid_return": float(trades["fee_cost_return"].sum()),
                    "slippage_paid_return": float(trades["slippage_cost_return"].sum()),
                    "avg_gross_return": float(trades["gross_return"].mean()),
                    "avg_net_return": float(trades["net_return"].mean()),
                    **event_counts,
                }
            )

            trades.to_csv(event_out / f"trades_{cand.get('candidate_id','candidate')}.csv", index=False)
            used_events.to_csv(event_out / f"events_used_{cand.get('candidate_id','candidate')}.csv", index=False)

        summary_df = pd.DataFrame(rows)
        summary_df.to_csv(event_out / "candidate_audit_summary.csv", index=False)

        summary_payload = {
            "run_id": run_id,
            "event_type": event_type,
            "timeframe": timeframe,
            "horizon_bars": int(horizon_bars),
            "total_cost_per_side_bps": float(total_cost_per_side_bps),
            "fee_bps_per_side": float(fee_bps_per_side),
            "spread_bps_per_side": float(spread_bps_per_side),
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

    surviving = [x for x in family_summaries if x.get("status") == "ok"]
    overlap_report: List[Dict[str, object]] = []
    if len(surviving) >= 2:
        trade_frames: Dict[str, pd.DataFrame] = {}
        for fam in surviving:
            event_type = str(fam["event_type"])
            event_out = out_root / event_type
            first_trade = sorted(event_out.glob("trades_*.csv"))
            if first_trade:
                trade_frames[event_type] = pd.read_csv(first_trade[0])
                trade_frames[event_type]["entry_ts"] = pd.to_datetime(trade_frames[event_type]["entry_ts"], utc=True, errors="coerce")
        names = sorted(trade_frames.keys())
        for i in range(len(names)):
            for j in range(i + 1, len(names)):
                a, b = names[i], names[j]
                ta = trade_frames[a]
                tb = trade_frames[b]
                if ta.empty or tb.empty:
                    continue
                overlap_n, merged = _overlap_within_15m(ta, tb)
                pnl_corr = float(merged["net_return_a"].corr(merged["net_return_b"])) if len(merged) >= 2 else 0.0
                if pd.isna(pnl_corr):
                    pnl_corr = 0.0
                cofire = float(overlap_n / max(1, min(len(ta), len(tb))))
                independence = "mixed"
                if pnl_corr > 0.6:
                    independence = "same_edge"
                elif pnl_corr < 0.3:
                    independence = "additive"
                overlap_report.append({
                    "edge_a": a,
                    "edge_b": b,
                    "timestamp_overlap_15m": int(overlap_n),
                    "pnl_corr": pnl_corr,
                    "cofire_pct": cofire,
                    "independence": independence,
                })

    payload = {
        "run_id": run_id,
        "family_count": len(family_summaries),
        "families": family_summaries,
        "timeframe": timeframe,
        "horizon_bars": int(horizon_bars),
        "total_cost_per_side_bps": float(total_cost_per_side_bps),
        "independence": overlap_report,
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

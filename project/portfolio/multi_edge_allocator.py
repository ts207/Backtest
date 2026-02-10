from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Sequence

import numpy as np
import pandas as pd

BARS_PER_YEAR_15M = 365 * 24 * 4


@dataclass
class PortfolioConstraints:
    max_drawdown_pct: float = 0.20
    cvar_1d_99_pct: float = 0.025
    gross_exposure_max: float = 1.50
    net_exposure_max: float = 0.60
    single_symbol_weight_max: float = 0.20
    single_edge_risk_contrib_max: float = 0.25
    turnover_budget_daily: float = 0.30


@dataclass
class AllocationResult:
    portfolio: pd.DataFrame
    weights: pd.DataFrame
    symbol_exposures: pd.DataFrame
    metrics: Dict[str, object]


def _clip_and_scale(weights: pd.Series, max_weight: float, gross_cap: float) -> pd.Series:
    out = weights.clip(lower=0.0, upper=max_weight)
    gross = float(out.abs().sum())
    if gross > gross_cap and gross > 0:
        out = out * (gross_cap / gross)
    return out


def _equal_risk_weights(vol: pd.Series, max_weight: float, gross_cap: float) -> pd.Series:
    inv = 1.0 / vol.replace(0, np.nan)
    inv = inv.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    if float(inv.sum()) <= 0:
        inv = pd.Series(1.0, index=vol.index)
    out = inv / float(inv.sum())
    return _clip_and_scale(out, max_weight=max_weight, gross_cap=gross_cap)


def _score_weighted_weights(mu: pd.Series, vol: pd.Series, max_weight: float, gross_cap: float, tilt: float) -> pd.Series:
    base = _equal_risk_weights(vol, max_weight=max_weight, gross_cap=gross_cap)
    score = (mu.clip(lower=0.0) / vol.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    if float(score.sum()) > 0:
        score = score / float(score.sum())
    else:
        score = base
    out = (1.0 - tilt) * base + float(tilt) * score
    return _clip_and_scale(out, max_weight=max_weight, gross_cap=gross_cap)


def _constrained_optimizer_weights(
    mu: pd.Series,
    cov: pd.DataFrame,
    vol: pd.Series,
    max_weight: float,
    gross_cap: float,
    tilt: float,
    steps: int = 60,
) -> pd.Series:
    w = _score_weighted_weights(mu, vol, max_weight=max_weight, gross_cap=gross_cap, tilt=tilt)
    if cov.empty:
        return w

    cov = cov.reindex(index=w.index, columns=w.index).fillna(0.0)
    mu = mu.reindex(w.index).fillna(0.0)
    risk_aversion = 5.0
    step_size = 0.05

    w_arr = w.values.astype(float)
    mu_arr = mu.values.astype(float)
    cov_arr = cov.values.astype(float)
    for _ in range(steps):
        grad = mu_arr - risk_aversion * (cov_arr @ w_arr)
        w_arr = w_arr + step_size * grad
        w_arr = np.clip(w_arr, 0.0, max_weight)
        gross = float(np.abs(w_arr).sum())
        if gross > gross_cap and gross > 0:
            w_arr = w_arr * (gross_cap / gross)

    out = pd.Series(w_arr, index=w.index)
    return _clip_and_scale(out, max_weight=max_weight, gross_cap=gross_cap)


def _compute_mode_weights(
    *,
    mode: str,
    mu: pd.Series,
    cov: pd.DataFrame,
    vol: pd.Series,
    max_weight: float,
    gross_cap: float,
    tilt: float,
) -> pd.Series:
    if mode == "equal_risk":
        return _equal_risk_weights(vol, max_weight=max_weight, gross_cap=gross_cap)
    if mode == "score_weighted":
        return _score_weighted_weights(mu, vol, max_weight=max_weight, gross_cap=gross_cap, tilt=tilt)
    if mode == "constrained_optimizer":
        return _constrained_optimizer_weights(
            mu,
            cov,
            vol,
            max_weight=max_weight,
            gross_cap=gross_cap,
            tilt=tilt,
        )
    raise ValueError(f"unsupported allocation mode: {mode}")


def _apply_turnover_budget(
    *,
    prev_w: pd.Series,
    next_w: pd.Series,
    day_turnover_used: float,
    turnover_budget_daily: float,
) -> tuple[pd.Series, float, float]:
    delta = float((next_w - prev_w).abs().sum())
    remaining = max(0.0, float(turnover_budget_daily) - day_turnover_used)
    if delta <= remaining or delta == 0.0:
        return next_w, delta, day_turnover_used + delta

    alpha = remaining / delta if delta > 0 else 0.0
    out = prev_w + (next_w - prev_w) * alpha
    used = day_turnover_used + remaining
    return out, remaining, used


def _apply_exposure_caps(
    *,
    weights: pd.Series,
    edge_symbol_positions_t: pd.DataFrame,
    constraints: PortfolioConstraints,
) -> tuple[pd.Series, pd.Series]:
    if edge_symbol_positions_t.empty:
        return weights, pd.Series(dtype=float)

    aligned = edge_symbol_positions_t.reindex(index=weights.index).fillna(0.0)
    exposures = aligned.mul(weights, axis=0).sum(axis=0)

    scale = 1.0
    max_abs_symbol = float(exposures.abs().max()) if not exposures.empty else 0.0
    if max_abs_symbol > float(constraints.single_symbol_weight_max) and max_abs_symbol > 0:
        scale = min(scale, float(constraints.single_symbol_weight_max) / max_abs_symbol)

    gross = float(exposures.abs().sum()) if not exposures.empty else 0.0
    if gross > float(constraints.gross_exposure_max) and gross > 0:
        scale = min(scale, float(constraints.gross_exposure_max) / gross)

    net = abs(float(exposures.sum())) if not exposures.empty else 0.0
    if net > float(constraints.net_exposure_max) and net > 0:
        scale = min(scale, float(constraints.net_exposure_max) / net)

    if scale < 1.0:
        weights = weights * scale
        exposures = exposures * scale

    return weights, exposures


def _max_drawdown(equity_curve: pd.Series) -> float:
    if equity_curve.empty:
        return 0.0
    peak = equity_curve.cummax()
    drawdown = (equity_curve - peak) / peak
    return float(abs(drawdown.min()))


def _annualized_sharpe(returns: pd.Series) -> float:
    vals = returns.dropna()
    if vals.empty:
        return 0.0
    std = float(vals.std())
    if std == 0.0:
        return 0.0
    return float((vals.mean() / std) * np.sqrt(BARS_PER_YEAR_15M))


def _cvar_1d_99(returns_15m: pd.Series) -> float:
    if returns_15m.empty:
        return 0.0
    daily = returns_15m.groupby(pd.to_datetime(returns_15m.index).floor("D")).sum()
    if daily.empty:
        return 0.0
    q = float(np.quantile(daily.values, 0.01))
    tail = daily[daily <= q]
    if tail.empty:
        return 0.0
    return float(abs(tail.mean()))


def _risk_contrib_max(weights_df: pd.DataFrame, edge_returns: pd.DataFrame) -> float:
    if weights_df.empty or edge_returns.empty:
        return 0.0
    w = weights_df.mean(axis=0).reindex(edge_returns.columns).fillna(0.0)
    cov = edge_returns.cov().reindex(index=w.index, columns=w.index).fillna(0.0)
    w_arr = w.values.astype(float)
    cov_arr = cov.values.astype(float)
    port_var = float(w_arr.T @ cov_arr @ w_arr)
    if port_var <= 0:
        return 0.0
    marginal = cov_arr @ w_arr
    rc = w_arr * marginal / port_var
    return float(np.max(np.abs(rc)))


def run_multi_edge_allocation(
    *,
    edge_returns: pd.DataFrame,
    edge_symbol_positions: Dict[str, pd.DataFrame],
    monthly_allowed_symbols: Dict[str, Sequence[str]],
    constraints: PortfolioConstraints,
    mode: str,
    lookback_bars: int,
    return_tilt: float,
) -> AllocationResult:
    if edge_returns.empty:
        empty = pd.DataFrame(columns=["timestamp", "portfolio_ret", "portfolio_pnl"])
        return AllocationResult(
            portfolio=empty,
            weights=pd.DataFrame(),
            symbol_exposures=pd.DataFrame(),
            metrics={
                "mode": mode,
                "net_total_return": 0.0,
                "ending_equity": 1_000_000.0,
                "max_drawdown": 0.0,
                "sharpe_annualized": 0.0,
                "cvar_1d_99": 0.0,
                "avg_daily_turnover": 0.0,
                "max_symbol_abs_exposure": 0.0,
                "max_gross_exposure": 0.0,
                "max_net_exposure": 0.0,
                "max_edge_risk_contrib": 0.0,
                "constraints_pass": True,
                "constraint_breaches": [],
            },
        )

    edge_returns = edge_returns.sort_index()
    edges = list(edge_returns.columns)
    symbols = sorted(edge_symbol_positions.keys())

    prev_w = pd.Series(0.0, index=edges)
    all_weights = []
    all_exposures = []
    portfolio_rows = []

    current_day = None
    daily_turnover_used = 0.0
    daily_turnovers: Dict[pd.Timestamp, float] = {}

    for i, ts in enumerate(edge_returns.index):
        day = pd.Timestamp(ts).floor("D")
        if current_day is None or day != current_day:
            current_day = day
            daily_turnover_used = 0.0

        hist = edge_returns.iloc[max(0, i - int(lookback_bars)) : i]
        if hist.empty:
            w = prev_w.copy()
        else:
            mu = hist.mean().fillna(0.0)
            if len(hist) < 2:
                cov = pd.DataFrame(0.0, index=edge_returns.columns, columns=edge_returns.columns)
            else:
                cov = hist.cov().fillna(0.0)
            vol = hist.std().replace(0, np.nan).fillna(1e-6)
            w = _compute_mode_weights(
                mode=mode,
                mu=mu,
                cov=cov,
                vol=vol,
                max_weight=float(constraints.single_edge_risk_contrib_max),
                gross_cap=float(constraints.gross_exposure_max),
                tilt=float(return_tilt),
            )

        month_key = pd.Timestamp(ts).strftime("%Y-%m")
        allowed_symbols = set(monthly_allowed_symbols.get(month_key, symbols))
        if allowed_symbols:
            live_edges = []
            for edge in edges:
                active = False
                for symbol in allowed_symbols:
                    symbol_pos = edge_symbol_positions.get(symbol)
                    if symbol_pos is None or ts not in symbol_pos.index or edge not in symbol_pos.columns:
                        continue
                    if float(symbol_pos.at[ts, edge]) != 0.0:
                        active = True
                        break
                live_edges.append(active)
            live_mask = pd.Series(live_edges, index=edges)
            w = w.where(live_mask, 0.0)

        w, turnover_delta, daily_turnover_used = _apply_turnover_budget(
            prev_w=prev_w,
            next_w=w,
            day_turnover_used=daily_turnover_used,
            turnover_budget_daily=float(constraints.turnover_budget_daily),
        )
        daily_turnovers[day] = daily_turnovers.get(day, 0.0) + turnover_delta

        edge_symbol_matrix = pd.DataFrame(index=edges, columns=symbols, data=0.0)
        for symbol in symbols:
            symbol_pos = edge_symbol_positions.get(symbol)
            if symbol_pos is None or ts not in symbol_pos.index:
                continue
            row = symbol_pos.loc[ts].reindex(edges).fillna(0.0)
            edge_symbol_matrix[symbol] = row.values

        w, exposures = _apply_exposure_caps(weights=w, edge_symbol_positions_t=edge_symbol_matrix, constraints=constraints)

        pnl_t = float((w * edge_returns.loc[ts].reindex(edges).fillna(0.0)).sum())
        portfolio_rows.append({"timestamp": ts, "portfolio_ret": pnl_t, "portfolio_pnl": pnl_t})

        w_row = {"timestamp": ts, **{edge: float(w.get(edge, 0.0)) for edge in edges}}
        all_weights.append(w_row)
        exp_row = {"timestamp": ts, **{symbol: float(exposures.get(symbol, 0.0)) for symbol in symbols}}
        all_exposures.append(exp_row)

        prev_w = w.copy()

    portfolio = pd.DataFrame(portfolio_rows)
    weights_df = pd.DataFrame(all_weights)
    exposure_df = pd.DataFrame(all_exposures)

    port_series = portfolio.set_index("timestamp")["portfolio_pnl"] if not portfolio.empty else pd.Series(dtype=float)
    equity = 1_000_000.0 * (1.0 + port_series.cumsum()) if not port_series.empty else pd.Series(dtype=float)
    max_dd = _max_drawdown(equity)
    cvar = _cvar_1d_99(port_series)

    max_symbol_abs_exposure = float(exposure_df.drop(columns=["timestamp"]).abs().max().max()) if not exposure_df.empty else 0.0
    gross_exposure = (
        exposure_df.drop(columns=["timestamp"]).abs().sum(axis=1).max() if not exposure_df.empty else 0.0
    )
    net_exposure = (
        exposure_df.drop(columns=["timestamp"]).sum(axis=1).abs().max() if not exposure_df.empty else 0.0
    )

    avg_daily_turnover = float(np.mean(list(daily_turnovers.values()))) if daily_turnovers else 0.0
    max_edge_rc = _risk_contrib_max(weights_df.drop(columns=["timestamp"], errors="ignore"), edge_returns)

    breaches: List[str] = []
    if max_dd > float(constraints.max_drawdown_pct):
        breaches.append("max_drawdown_pct")
    if cvar > float(constraints.cvar_1d_99_pct):
        breaches.append("cvar_1d_99_pct")
    if float(gross_exposure) > float(constraints.gross_exposure_max):
        breaches.append("gross_exposure_max")
    if float(net_exposure) > float(constraints.net_exposure_max):
        breaches.append("net_exposure_max")
    if max_symbol_abs_exposure > float(constraints.single_symbol_weight_max):
        breaches.append("single_symbol_weight_max")
    if max_edge_rc > float(constraints.single_edge_risk_contrib_max):
        breaches.append("single_edge_risk_contrib_max")
    if avg_daily_turnover > float(constraints.turnover_budget_daily):
        breaches.append("turnover_budget_daily")

    metrics = {
        "mode": mode,
        "net_total_return": float(port_series.sum()) if not port_series.empty else 0.0,
        "ending_equity": float(equity.iloc[-1]) if not equity.empty else 1_000_000.0,
        "max_drawdown": float(max_dd),
        "sharpe_annualized": _annualized_sharpe(port_series),
        "cvar_1d_99": float(cvar),
        "avg_daily_turnover": float(avg_daily_turnover),
        "max_symbol_abs_exposure": float(max_symbol_abs_exposure),
        "max_gross_exposure": float(gross_exposure),
        "max_net_exposure": float(net_exposure),
        "max_edge_risk_contrib": float(max_edge_rc),
        "constraints_pass": len(breaches) == 0,
        "constraint_breaches": breaches,
    }

    return AllocationResult(
        portfolio=portfolio,
        weights=weights_df,
        symbol_exposures=exposure_df,
        metrics=metrics,
    )

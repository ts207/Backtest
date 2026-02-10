from __future__ import annotations

from pathlib import Path

import pandas as pd


def yearly_sign_consistency(portfolio_path: Path) -> float:
    """Compute dominant-sign yearly consistency from a portfolio returns file."""
    if not portfolio_path.exists():
        return 0.0
    df = pd.read_csv(portfolio_path)
    if df.empty:
        return 0.0
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="mixed")
    df["year"] = df["timestamp"].dt.year
    yearly = df.groupby("year", as_index=False)["portfolio_pnl"].sum()
    if yearly.empty:
        return 0.0

    signs = yearly["portfolio_pnl"].apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0)
    non_zero = signs[signs != 0]
    if non_zero.empty:
        return 0.0

    dominant = non_zero.mode().iloc[0]
    return float((non_zero == dominant).mean())


def symbol_positive_pnl_ratio(symbol_contrib_path: Path) -> float:
    """Compute fraction of symbols with positive total pnl from symbol contribution file."""
    if not symbol_contrib_path.exists():
        return 0.0
    df = pd.read_csv(symbol_contrib_path)
    if df.empty or "total_pnl" not in df.columns:
        return 0.0
    return float((pd.to_numeric(df["total_pnl"], errors="coerce").fillna(0.0) > 0).mean())

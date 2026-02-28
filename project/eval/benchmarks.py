from __future__ import annotations
import numpy as np
import pandas as pd
from typing import Dict

BARS_PER_YEAR = 105120  # 5m bars


def compute_buy_hold_sharpe(close: pd.Series) -> Dict[str, float]:
    ret = close.pct_change(fill_method=None).dropna()
    if ret.empty:
        return {"sharpe_annualized": 0.0, "annualized_return": 0.0, "annualized_vol": 0.0}
    ann_ret = float(ret.mean() * BARS_PER_YEAR)
    ann_vol = float(ret.std() * np.sqrt(BARS_PER_YEAR))
    sharpe = ann_ret / ann_vol if ann_vol > 0 else 0.0
    return {"sharpe_annualized": sharpe, "annualized_return": ann_ret, "annualized_vol": ann_vol}


def compute_btc_beta(strategy_returns: pd.Series, btc_returns: pd.Series) -> Dict[str, float]:
    aligned = pd.DataFrame({"strat": strategy_returns, "btc": btc_returns}).dropna()
    if len(aligned) < 10:
        return {"beta": float("nan"), "r_squared": float("nan")}
    cov = float(np.cov(aligned["strat"], aligned["btc"])[0, 1])
    var_btc = float(aligned["btc"].var())
    beta = cov / var_btc if var_btc > 0 else float("nan")
    corr = float(aligned["strat"].corr(aligned["btc"]))
    return {"beta": beta, "r_squared": corr ** 2}


def compute_exposure_summary(positions: pd.Series) -> Dict[str, float]:
    gross = positions.abs()
    turnover = positions.diff().abs()
    return {
        "gross_exposure_mean": float(gross.mean()),
        "net_exposure_mean": float(positions.mean()),
        "turnover_mean": float(turnover.mean()),
        "long_fraction": float((positions > 0).mean()),
        "short_fraction": float((positions < 0).mean()),
    }

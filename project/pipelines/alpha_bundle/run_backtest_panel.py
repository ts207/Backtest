import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def load_bars(bars_root: Path, symbols: list[str]) -> pd.DataFrame:
    frames = []
    for sym in symbols:
        # expect parquet under bars_root/<sym>.parquet or bars_root/<sym>/bars.parquet
        p1 = bars_root / f"{sym}.parquet"
        p2 = bars_root / sym / "bars.parquet"
        if p1.exists():
            df = pd.read_parquet(p1)
        elif p2.exists():
            df = pd.read_parquet(p2)
        else:
            # fall back to any parquet in the folder
            cand = list((bars_root / sym).glob("*.parquet")) if (bars_root / sym).exists() else []
            if not cand:
                raise FileNotFoundError(f"No bars parquet found for {sym} under {bars_root}")
            df = pd.read_parquet(cand[0])
        if 'ts_event' not in df.columns:
            raise ValueError(f"Bars for {sym} must contain ts_event")
        df = df.copy()
        df['symbol'] = sym
        frames.append(df)
    out = pd.concat(frames, ignore_index=True)
    return out


def main() -> int:
    p = argparse.ArgumentParser(description='Simple multi-asset panel backtest from per-symbol scores.')
    p.add_argument('--scores_path', required=True, help='Parquet with columns: ts_event, symbol, score')
    p.add_argument('--bars_root', required=True, help='Root folder for per-symbol bar parquets')
    p.add_argument('--symbols', required=True, help='Comma-separated symbols')
    p.add_argument('--price_col', default='close', help='Price column in bars (close or mid)')
    p.add_argument('--gross_leverage', type=float, default=2.0)
    p.add_argument('--market_neutral', action='store_true')
    p.add_argument('--cost_bps_per_turnover', type=float, default=2.0)
    p.add_argument('--out_path', default='data/backtest_results/panel_backtest.parquet')

    args = p.parse_args()

    scores = pd.read_parquet(args.scores_path)
    for col in ['ts_event', 'symbol', 'score']:
        if col not in scores.columns:
            raise ValueError(f"scores must contain column {col}")

    symbols = [s.strip() for s in args.symbols.split(',') if s.strip()]
    bars = load_bars(Path(args.bars_root), symbols)

    if args.price_col not in bars.columns:
        raise ValueError(f"bars must contain price column {args.price_col}")

    bars = bars[['ts_event', 'symbol', args.price_col]].rename(columns={args.price_col: 'price'})

    # compute 1-step forward log return per symbol
    bars = bars.sort_values(['symbol', 'ts_event'], kind='mergesort')
    bars['price_fwd'] = bars.groupby('symbol')['price'].shift(-1)
    bars['ret_fwd'] = np.log(bars['price_fwd'] / bars['price'])
    bars = bars.dropna(subset=['ret_fwd'])

    panel = scores.merge(bars[['ts_event', 'symbol', 'ret_fwd']], on=['ts_event', 'symbol'], how='inner')

    # weights per timestamp: proportional to score
    panel = panel.sort_values(['ts_event', 'symbol'], kind='mergesort')
    panel['w_raw'] = panel['score']

    if args.market_neutral:
        panel['w_raw'] = panel['w_raw'] - panel.groupby('ts_event')['w_raw'].transform('mean')

    # normalize to gross leverage
    gross = panel.groupby('ts_event')['w_raw'].apply(lambda x: float(np.sum(np.abs(x))))
    panel = panel.join(gross.rename('gross_raw'), on='ts_event')
    panel['w'] = np.where(panel['gross_raw'] > 0, panel['w_raw'] * (args.gross_leverage / panel['gross_raw']), 0.0)

    # turnover cost
    panel['w_prev'] = panel.groupby('symbol')['w'].shift(1).fillna(0.0)
    panel['turnover'] = np.abs(panel['w'] - panel['w_prev'])

    # pnl per row
    panel['pnl_row'] = panel['w_prev'] * panel['ret_fwd']

    # costs applied at rebalance (per bar)
    cost_rate = args.cost_bps_per_turnover / 10000.0
    panel['cost_row'] = cost_rate * panel['turnover']

    by_t = panel.groupby('ts_event').agg(
        pnl=('pnl_row', 'sum'),
        costs=('cost_row', 'sum'),
        gross=('w', lambda x: float(np.sum(np.abs(x)))),
        net=('w', 'sum'),
        coverage=('w', 'size'),
    ).reset_index()

    by_t['ret_net'] = by_t['pnl'] - by_t['costs']
    by_t['equity'] = (1.0 + by_t['ret_net']).cumprod()

    out_path = Path(args.out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    by_t.to_parquet(out_path, index=False)

    print(str(out_path))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

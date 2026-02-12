import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode('utf-8')).hexdigest()


def canonical_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def main() -> int:
    p = argparse.ArgumentParser(description='Calibrate score to expected return mapping (linear OLS).')
    p.add_argument('--scores_path', required=True, help='Parquet with columns: ts_event,symbol,score')
    p.add_argument('--labels_path', required=True, help='Parquet with columns: ts_event,symbol,y')
    p.add_argument('--vol_path', default='', help='Optional parquet or directory containing ewma_vol per row (ts_event,symbol,ewma_vol). If omitted, assumes scores file already has ewma_vol column.')
    p.add_argument('--horizon_bars', type=int, required=True)
    p.add_argument('--vol_col', default='ewma_vol')
    p.add_argument('--label_col', default='y')
    p.add_argument('--out_dir', default='data/model_registry')
    p.add_argument('--min_rows', type=int, default=10000)
    args = p.parse_args()

    scores = pd.read_parquet(args.scores_path)
    labels = pd.read_parquet(args.labels_path)

    df = scores.merge(labels[['ts_event','symbol',args.label_col]], on=['ts_event','symbol'], how='inner')

    if args.vol_path:
        vp = Path(args.vol_path)
        if vp.is_dir():
            parts = []
            for f in sorted(vp.glob('*.parquet')):
                parts.append(pd.read_parquet(f))
            vol = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        else:
            vol = pd.read_parquet(vp)
        df = df.merge(vol[['ts_event','symbol',args.vol_col]], on=['ts_event','symbol'], how='left')

    if args.vol_col not in df.columns:
        raise ValueError(f"Missing vol column '{args.vol_col}'. Provide --vol_path or include it in scores.")

    df = df.dropna(subset=['score', args.vol_col, args.label_col])

    if len(df) < args.min_rows:
        raise ValueError(f'Not enough rows after merge/NaN drop: {len(df)} < {args.min_rows}')

    # x = score * sigma * sqrt(h)
    x = df['score'].astype('float64') * df[args.vol_col].astype('float64') * np.sqrt(float(args.horizon_bars))
    y = df[args.label_col].astype('float64')

    # OLS through origin: beta = sum(x*y)/sum(x^2)
    denom = float((x * x).sum())
    beta = float((x * y).sum() / denom) if denom > 0 else 0.0

    # Diagnostics
    y_hat = beta * x
    resid = y - y_hat
    mse = float((resid * resid).mean())
    corr = float(np.corrcoef(y, y_hat)[0,1]) if len(y) > 2 else 0.0

    artifact = {
        'type': 'ScoreToMuLinearOLS',
        'horizon_bars': args.horizon_bars,
        'beta_score': beta,
        'x_def': 'x = score * vol * sqrt(horizon_bars)',
        'label_col': args.label_col,
        'vol_col': args.vol_col,
        'n_rows': int(len(df)),
        'mse': mse,
        'corr_y_yhat': corr,
    }

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    artifact_hash = sha256_str(canonical_json(artifact))
    artifact['artifact_hash'] = artifact_hash

    out_path = out_dir / f'ScoreToMu_{artifact_hash[:12]}.json'
    out_path.write_text(canonical_json(artifact), encoding='utf-8')

    print(str(out_path))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

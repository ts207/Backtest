import sys
sys.path.insert(0, 'project')
import pandas as pd
import numpy as np

idx = pd.date_range('2026-01-01', periods=6, freq='5min', tz='UTC')
pnl = pd.Series([100.0, 200.0, 100.0, -500.0, -600.0, 100.0], index=idx)
INITIAL_EQUITY = 10000.0
equity = INITIAL_EQUITY + pnl.fillna(0.0).cumsum()
peak = equity.cummax()
dd = (peak - equity) / peak
print('Equity:')
print(equity)
print('DD:')
print(dd)
breaches = dd[dd > 0.04]
print('Breaches:')
print(breaches)
print('First breach idx:', breaches.index[0] if not breaches.empty else 'None')

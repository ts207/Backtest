# Context Features

## Funding Persistence (read-only)

`funding_persistence` is a frozen **context/risk** module, not a trading signal.

### Definition contract

- Definition version is fixed: `fp_def_version = "v1"`.
- No direction, alpha score, or entry-side output is emitted.
- No parameter sweeps are exposed in mainline pipelines.

### Output columns

Per symbol + bar:

- `fp_active` (`0/1`): inside persistence window.
- `fp_age_bars` (`int`): bars since event start; `0` when inactive.
- `fp_event_id` (`str|null`): stable event id while active.
- `fp_enter_ts` (`timestamp|null`): event start timestamp.
- `fp_exit_ts` (`timestamp|null`): filled on event close row; otherwise null.
- `fp_severity` (`float`): frozen standardized persistence intensity.
- `fp_norm_due` (`0/1`): `1` when `fp_age_bars >= 96` while active.
- `fp_def_version` (`str`): frozen definition tag.

### Intended usage (allowed)

- Entry filters (e.g., skip entry when `fp_active=1`).
- Risk throttling by state/age bucket.
- Reporting segmentation by `fp_active` and `fp_age_bars` buckets.

### Non-usage (disallowed)

- `if fp_active then long/short` directional rules.
- Any direct signal/side generation from `fp_*` columns.
- Mainline threshold retuning.

### Build entrypoint

```bash
python3 project/pipelines/features/build_context_features.py \
  --run_id <run_id> \
  --symbols BTCUSDT,ETHUSDT \
  --timeframe 15m \
  --start 2020-06-01 \
  --end 2025-06-01
```

Default output root:

`data/features/context/funding_persistence/<symbol>/15m.parquet`

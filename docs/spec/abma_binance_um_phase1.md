# ABMA v1 Binance USD-M Input Spec

This locks a reproducible Phase-1 path for ABMA v1 on Binance USD-M Futures.

## Product + source

- Product: `USDT-M Futures`
- Historical source: `data.binance.vision` archives
- Feeds:
- `aggTrades` -> ABMA trades
- `bookTicker` -> ABMA L1 quotes
- Session calendar: UTC daily sessions (`00:00:00` to `23:59:59`)

## Normalized schemas

### Trades

- `ts` (UTC, tz-aware)
- `symbol` (string)
- `price` (float > 0)
- `size` (float > 0)

### Quotes

- `ts` (UTC, tz-aware)
- `symbol` (string)
- `bid_px` (float > 0)
- `ask_px` (float > 0, `ask_px >= bid_px`)
- `bid_sz` (float)
- `ask_sz` (float)

### Session calendar

- `session_date` (UTC date)
- `exchange_tz` (`UTC`)
- `open_ts` (`00:00:00+00:00`)
- `close_ts` (`23:59:59+00:00`)

## Ingest command

```bash
python -m project.pipelines.ingest.ingest_binance_um_abma_l1 \
  --run_id 20260206_abma_um_l1 \
  --symbols BTCUSDT,ETHUSDT \
  --start 2024-01-01 \
  --end 2024-01-07
```

Outputs are written to:

- `data/lake/runs/<run_id>/microstructure/trades`
- `data/lake/runs/<run_id>/microstructure/quotes`
- `data/lake/runs/<run_id>/microstructure/session_calendar`

## ABMA analyzer command

```bash
python -m project.pipelines.research.analyze_abma_v1 \
  --run_id 20260206_abma_um_l1 \
  --symbols BTCUSDT,ETHUSDT
```

Report artifacts:

- `data/reports/abma/<run_id>/summary.json`
- `data/reports/abma/<run_id>/curves.parquet`
- `data/reports/abma/<run_id>/report.md`

## Minimal Phase-1 matrix

- Symbols: `BTCUSDT`, `ETHUSDT`
- Period: `2024-01-01` to `2024-03-31`
- Splits:
- `year`
- `symbol`

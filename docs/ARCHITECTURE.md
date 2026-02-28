# Architecture

Backtest is an event-first, spec-driven quantitative research platform for Binance perpetual and spot markets. The system runs as a sequential pipeline orchestrated by `project/pipelines/run_all.py`, converting raw OHLCV data into statistically validated strategy blueprints.

---

## Pipeline overview

```
Raw data (Binance API)
        │
        ▼
[INGEST] ohlcv + funding + OI + liquidations + spot
        │
        ▼
[CLEAN] build_cleaned_5m → gap masking, vol filter, basis state, ToB snapshots
        │
        ▼
[FEATURES] build_features_v1 → vol regime, carry, microstructure, funding persistence
           build_context_features → cross-symbol correlation, beta, regime context
           build_market_context   → session, news windows, macro flags
        │
        ▼
[EVENT REGISTRY] build_event_registry_<EVENT> (57 event types, run in parallel)
        │
        ▼
[PHASE 2 DISCOVERY] phase2_conditional_hypotheses_<EVENT>
   → calculate_expectancy_stats per symbol × horizon × rule × state
   → hierarchical shrinkage (family → event → state)
   → BH-FDR multiplicity control
   → gate_v1_phase2 promotion/fallback gates
        │
        ▼
[BRIDGE EVAL] bridge_evaluate_phase2 — OOS window separation + cost stress
        │
        ▼
[CANDIDATE PROMOTION] build_strategy_candidates → blueprints.jsonl
        │
        ▼
[BACKTEST] (optional) backtest_strategies → per-strategy PnL via engine/runner.py
        │
        ▼
[WALKFORWARD] (optional) run_walkforward → OOS performance splits
        │
        ▼
[REPORT] make_report → JSON + Markdown summary artifacts
```

---

## Module map

### `project/pipelines/`
The orchestration layer. All stages are plain Python subprocesses managed by `run_all.py`.

| Path | Responsibility |
|------|----------------|
| `run_all.py` | Master orchestrator. Builds stage list, runs them sequentially (analyzers in parallel), writes `run_manifest.json` |
| `pipelines/stages.py` | Stage builder functions: `build_ingest_stages`, `build_core_stages`, `build_research_stages`, `build_evaluation_stages` |
| `pipelines/clean/` | OHLCV cleaning, ToB aggregation, basis state construction |
| `pipelines/features/` | Feature computation: vol regime, microstructure, funding, carry, market context |
| `pipelines/research/` | Phase 1 & 2 analyzers (one script per event family) |
| `pipelines/backtest/` | Strategy simulation against cleaned bars |
| `pipelines/eval/` | Walkforward splits, robustness, microstructure acceptance |
| `pipelines/alpha_bundle/` | Cross-section signals, momentum, regime filter, orthogonalised factor overlay |
| `pipelines/_lib/` | Shared utilities: IO, spec loading, stats, BH-FDR, ontology contracts |

### `project/engine/`
The simulation core, used by `backtest_strategies`.

| File | Responsibility |
|------|----------------|
| `runner.py` | Strategy runner: merges event features, calls risk allocator, computes PnL |
| `risk_allocator.py` | `RiskLimits` + `allocate_position_scales` — deterministic 4-layer cap: strategy → symbol → portfolio → max_new_exposure_per_bar |
| `pnl.py` | `compute_pnl_components` — supports `close` and `next_open` exec modes |
| `execution_model.py` | Dynamic cost model: spread + volatility + liquidity + participation impact |
| `kill_switches.py` | Automated halt triggers: data staleness, drawdown, slippage ratio |
| `monitoring.py` | PSI-based feature drift monitor |

### `project/events/`
- `registry.py` — loads `spec/events/canonical_event_registry.yaml`, exposes `EVENT_REGISTRY_SPECS`

### `project/features/`
Standalone feature computation modules (imported by pipeline scripts):
- `vol_regime.py`, `vol_shock_relaxation.py`, `carry_state.py`, `funding_persistence.py`, `microstructure.py`, `liquidity_vacuum.py`, `context.py`, `context_states.py`, `state_mapping.py`

### `spec/`
Ground truth for all system behaviour. Nothing in `project/` should redefine what is in `spec/`.

| Path | Content |
|------|---------|
| `spec/events/` | One YAML per event type: detection logic, families, prevalence bounds |
| `spec/events/canonical_event_registry.yaml` | Authoritative list of 57 events with canonical families |
| `spec/features/` | Feature definitions: formula, lookback, PIT contracts |
| `spec/states/` | Market state definitions: vol_regime, carry_state, state_families |
| `spec/multiplicity/families.yaml` | Event → canonical_family mapping, allowed rule templates, horizons |
| `spec/multiplicity/taxonomy.yaml` | Taxonomy hierarchy for BH-FDR grouping |
| `spec/gates.yaml` | Phase 2 gate thresholds: `gate_e1`, `gate_v1_phase2`, per-event overrides |
| `spec/global_defaults.yaml` | Default horizons, rule templates, conditioning columns |
| `spec/hypotheses/` | Template verb lexicon, lift-state hypothesis templates |
| `spec/strategies/` | Strategy specs for backtest and smoke testing |

---

## Data lake layout

```
data/
├── lake/
│   ├── raw/          perp/<SYMBOL>/5m/ohlcv/  spot/<SYMBOL>/5m/ohlcv/
│   ├── cleaned/      perp/<SYMBOL>/5m/ohlcv_clean/  spot/<SYMBOL>/5m/...
│   ├── features/     perp/<SYMBOL>/5m/features_v1/
│   └── runs/         <run_id>/features/...  (run-scoped feature partitions)
├── runs/
│   └── <run_id>/     stage manifests (.json), stage logs (.log)
├── reports/
│   ├── phase2/<run_id>/<EVENT>/   phase2 candidates + FDR results
│   ├── strategy_blueprints/<run_id>/blueprints.jsonl
│   └── promotions/   candidate promotion results
└── events/           events.parquet, event_flags.parquet  (canonical registry outputs)
```

All paths are relative to `$BACKTEST_DATA_ROOT` (default: `$(pwd)/data`).

---

## Event family taxonomy

Events are grouped into **canonical families** for BH-FDR multiplicity control and shrinkage:

| Family | Example Events |
|--------|---------------|
| `LIQUIDITY_DISLOCATION` | DEPTH_COLLAPSE, SPREAD_BLOWOUT, ORDERFLOW_IMBALANCE_SHOCK, LIQUIDITY_VACUUM |
| `VOLATILITY_TRANSITION` | VOL_SHOCK, VOL_SPIKE, VOL_RELAXATION_START, VOL_CLUSTER_SHIFT |
| `FUNDING_EPISODE` | FUNDING_EXTREME_ONSET, FUNDING_PERSISTENCE_TRIGGER, FUNDING_NORMALIZATION_TRIGGER, FUNDING_FLIP |
| `POSITIONING_EXTREMES` | OI_SPIKE_POSITIVE, OI_SPIKE_NEGATIVE, OI_FLUSH, DELEVERAGING_WAVE, LIQUIDATION_CASCADE |
| `FORCED_FLOW` | FORCED_FLOW_EXHAUSTION, TREND_EXHAUSTION_TRIGGER, MOMENTUM_DIVERGENCE_TRIGGER |
| `TREND_STRUCTURE` | RANGE_BREAKOUT, FALSE_BREAKOUT, TREND_ACCELERATION, PULLBACK_PIVOT |
| `REGIME_TRANSITION` | VOL_REGIME_SHIFT_EVENT, TREND_TO_CHOP_SHIFT, CORRELATION_BREAKDOWN_EVENT |
| `INFORMATION_DESYNC` | CROSS_VENUE_DESYNC, INDEX_COMPONENT_DIVERGENCE, SPOT_PERP_BASIS_SHOCK |
| `TEMPORAL_STRUCTURE` | SESSION_OPEN_EVENT, SESSION_CLOSE_EVENT, FUNDING_TIMESTAMP_EVENT |
| `EXECUTION_FRICTION` | SPREAD_REGIME_WIDENING_EVENT, SLIPPAGE_SPIKE_EVENT, FEE_REGIME_CHANGE_EVENT |

---

## Phase 2 statistical pipeline detail

```
Raw candidate rows (event × symbol × horizon × rule × state)
    ↓
_apply_hierarchical_shrinkage()
  • Aggregate effect units: global → family → event → state
  • Adaptive lambda estimation (variance decomposition within/between groups)
  • James-Stein partial pooling at each level
  • Build shrunken p-values via scipy.stats.t.sf (vectorized)
    ↓
BH-FDR control (bh_fdr_grouping.py)
  • Grouping by (family, event, template_verb, horizon)
  • q-value threshold: gate_v1_phase2.max_q_value (default 0.05)
    ↓
Gate evaluation (gate_v1_phase2)
  • gate_economic: after_cost_expectancy >= min_after_cost_expectancy_bps
  • gate_economic_conservative: stressed at conservative_cost_multiplier
  • gate_stability: sign agreement first vs. second time half
  • gate_state_information: shrinkage_weight_state >= min_information_weight_state
    ↓
Promotion track: "standard" | "fallback_only"
    ↓
blueprints.jsonl (compile_strategy_blueprints)
```

---

## Orchestrator parallelism

Phase 1 event analyzer stages (`build_event_registry_*`, `phase1_analyze_*`) are grouped and dispatched in parallel via `ThreadPoolExecutor`. Worker count is controlled by `--max_analyzer_workers` (default: `min(cpu_count, 8)`).

All other stages (ingest, clean, features, phase2 discovery, evaluation, backtest) run sequentially to preserve data dependencies.

**Stage output caching:** Enable `BACKTEST_STAGE_CACHE=1` to skip stages whose manifest already exists with a matching `input_hash` (script mtime + args hash).

---

## Key contracts

1. **PIT safety**: features are joined to events using `pd.merge_asof(direction="backward")` — no future data leaks.
2. **Entry lag**: `entry_lag_bars >= 1` is enforced for all close-derived signals.
3. **Missing data**: gap-masked bars are explicitly NaN, never zero-filled.
4. **Ontology hash**: candidate plans carry an `ontology_spec_hash` that must match the current `spec/` hash at phase 2 time.
5. **Registry outputs**: downstream stages must use `events.parquet` / `event_flags.parquet` for event alignment — never re-detect events inline.

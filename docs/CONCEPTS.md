# Concepts

Core concepts, invariants, and statistical machinery in the Backtest system.

---

## Event-first research

The system is **event-first**: all research starts from a discrete market event (a timestamped occurrence of a classified condition), not from a continuous signal. This design choice has several consequences:

- Event prevalence is measurable and bounded (enforced by `gate_e1`)
- Forward returns are indexed from a known point in time — no rolling window ambiguity
- Statistical tests (t-test, BH-FDR) are well-defined over a countable sample
- Entry lag enforcement is trivial: shift the event timestamp by `entry_lag_bars`

---

## Point-in-Time (PIT) safety

Every feature join is performed using `pd.merge_asof(direction="backward")`, which guarantees that only feature bars at or before the event timestamp are used. This is the **only** safe join direction.

**Rules:**
1. `entry_lag_bars >= 1` for all close-derived signals — you cannot trade on a bar's close price at that bar's close
2. Features are computed with strictly causal rolling windows (no `center=True`)
3. Forward returns are computed from the **entry** bar, not the event bar: `close[event_pos + entry_lag_bars + horizon_bars] / close[event_pos + entry_lag_bars] - 1`

A common bug is joining features at the event timestamp directly. This is only valid for order-book features that are available before the bar closes.

---

## Market states

States are discrete regime labels assigned to each bar. They condition event analysis — the same event may have different expectancy in different states.

| State family | Values | Update cadence |
|--------------|--------|----------------|
| `vol_regime` | `low`, `mid`, `high` | 1h |
| `carry_state` | `funding_pos`, `neutral`, `funding_neg` | 8h |

States are derived from continuous features (e.g., `rv_percentile_24h` for vol_regime) and thresholded at spec-defined values. They are **not** re-fitted at analysis time — thresholds come from `spec/states/`.

---

## Hierarchical Bayesian shrinkage

Phase 2 uses empirical-Bayes partial pooling to stabilize estimates across thin-data event/state combinations.

### Structure

```
Global (all events, all families)
  └─ Family level  (e.g., LIQUIDITY_DISLOCATION)
       └─ Event level  (e.g., DEPTH_COLLAPSE)
            └─ State level  (e.g., vol_regime=high)
```

At each level, the James-Stein estimate blends the raw sample mean with the parent-level mean:

```
effect_shrunk = w * effect_level + (1 - w) * effect_parent
w = n / (n + lambda)
```

Where `lambda` controls shrinkage strength. When `adaptive_shrinkage_lambda=1`, lambda is estimated from the within/between group variance decomposition at each run.

### Key parameters

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `lambda_state` | 100 | Shrinkage: state → event |
| `lambda_event` | 300 | Shrinkage: event → family |
| `lambda_family` | 1000 | Shrinkage: family → global |
| `adaptive_lambda_min` | 5 | Lower clamp for adaptive lambda |
| `adaptive_lambda_max` | 5000 | Upper clamp for adaptive lambda |
| `adaptive_lambda_min_total_samples` | 200 | Minimum N before adaptive estimation |

---

## BH-FDR multiplicity control

With 57 event types × ~4 horizons × ~4 templates × ~3 states, the total number of hypotheses tested in a single run is on the order of 3,000–10,000. Without multiplicity correction, a 5% false discovery rate means hundreds of spurious discoveries.

The Benjamini-Hochberg procedure controls the **false discovery rate** (expected fraction of false discoveries among all discoveries) at a specified `max_q_value` (default 0.05).

Hypotheses are grouped by `(canonical_family, canonical_event_type, template_verb, horizon)` for the BH correction — corrections are applied within group, not globally, to avoid being overly conservative for unrelated event families.

---

## Gate system

### gate_e1 (event quality)

Applied before Phase 2. Each event must satisfy:
- `min_prevalence_10k <= count_per_10k_bars <= max_prevalence_10k`
- `join_rate >= min_join_rate` (fraction of events that successfully join to feature bars)
- `max_clustering_5b >= clustering` (event clustering in 5-bar windows — guards against spurious bursts)

### gate_v1_phase2 (promotion gates)

| Gate | Column | Condition |
|------|--------|-----------|
| Economic | `gate_economic` | `after_cost_expectancy >= min_after_cost_expectancy_bps` |
| Conservative economic | `gate_economic_conservative` | stressed at `conservative_cost_multiplier` |
| Sign stability | `gate_stability` | first vs. second half sign agreement |
| State information | `gate_state_information` | `shrinkage_weight_state >= min_information_weight_state` |

The `gate_phase2_final` boolean is the AND of all active gates. Candidates with `gate_phase2_final=False` can still be compiled under the fallback `compile_eligible_phase2_fallback` track if `phase2_quality_score >= quality_floor_fallback`.

### Per-event overrides

Gate thresholds can be overridden per event type in `spec/gates.yaml`:
```yaml
gate_v1_phase2_profiles:
  discovery:
    event_overrides:
      LIQUIDATION_CASCADE:
        max_q_value: 0.13
        min_after_cost_expectancy_bps: -5.0
```

---

## Execution cost model

Transaction costs are modelled dynamically in `engine/execution_model.py`:

```
total_cost = fee_bps_per_side * 2
           + slippage_bps_per_fill * fill_difficulty_factor
```

Where `fill_difficulty_factor` depends on:
- **Spread**: higher spread → harder fill
- **Volatility**: high vol → wider effective spread
- **Liquidity**: low depth → worse fill
- **Participation**: large order relative to volume → market impact

Costs are read from `spec/strategies/<strategy>.yaml` or overridden via `--fees_bps` / `--slippage_bps`. In `tob_regime` mode, costs are calibrated from actual top-of-book snapshots (`data/lake/cleaned/...tob_5m_agg/`).

---

## Strategy blueprint schema

A strategy blueprint is a JSON object in `blueprints.jsonl`:

```json
{
  "event_type": "VOL_SHOCK",
  "canonical_family": "VOLATILITY_TRANSITION",
  "template_verb": "mean_reversion",
  "horizon": "15m",
  "state_id": "vol_regime=high",
  "condition": "vol_regime == 'high'",
  "direction": -1,
  "entry_lag_bars": 1,
  "expectancy": 0.00082,
  "after_cost_expectancy": 0.00055,
  "p_value": 0.021,
  "q_value": 0.038,
  "n_events": 312,
  "promotion_track": "standard",
  "compile_eligible_phase2": true
}
```

The `condition` field is a DSL string that is validated by `strategy_dsl.contract_v1.is_executable_condition` before compilation. Non-executable conditions (analysis-only buckets like `severity_bucket_*`) are blocked from compilation.

---

## Run manifest

Every run produces `data/runs/<run_id>/run_manifest.json`:

```json
{
  "run_id": "discovery_2020_2025",
  "status": "success",
  "started_at": "2025-02-28T04:00:00+00:00",
  "finished_at": "2025-02-28T04:35:12+00:00",
  "git_commit": "abc1234",
  "ontology_spec_hash": "sha256:...",
  "feature_schema_version": "v1",
  "stage_timings_sec": { "build_event_registry_VOL_SHOCK": 23.4, ... },
  "late_artifact_count": 0,
  "symbols": ["BTCUSDT", "ETHUSDT"],
  "discovery_start": "2020-06-01",
  "discovery_end": "2025-07-10"
}
```

The `ontology_spec_hash` ties candidate plans to their generating spec state. Any hash mismatch between a saved plan and the current spec causes Phase 2 to fail unless `--allow_ontology_hash_mismatch 1` is passed.

---

## Time decay weighting

Phase 2 expectancy statistics support exponential time decay, giving more weight to recent events:

```
weight_i = exp(-age_i / tau)
```

Where `age_i` is the age of event `i` relative to the most recent event, and tau is the decay time constant.

**Regime-conditioned decay**: tau is adjusted per `vol_regime` and `liquidity_state` — signals in high-volatility regimes decay faster. Smoothed with EMA (alpha = `regime_tau_smoothing_alpha = 0.15`).

**Directional asymmetric decay**: different tau for long vs. short directions, capturing the empirical finding that upside/downside momentum has different memory lengths.

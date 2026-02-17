# Combined Model 1 + Model 3 Architecture (v1)

AlphaBundle is a parallel research stack combining deterministic infrastructure (Model 3) with multi-signal alpha construction (Model 1).

## Position in this repository
- Mainline and AlphaBundle are treated as **equal research paths**.
- Both must satisfy the same discovery and robustness gates before promotion.
- AlphaBundle is not an automatic replacement for mainline event strategies.

## Included AlphaBundle modules
- `project/pipelines/alpha_bundle/build_universe_snapshot.py`
- `project/pipelines/alpha_bundle/build_cross_section_features.py`
- `project/pipelines/alpha_bundle/build_alpha_signals_v2.py`
- `project/pipelines/alpha_bundle/build_xs_momentum.py`
- `project/pipelines/alpha_bundle/build_onchain_flow_signal.py`
- `project/pipelines/alpha_bundle/build_regime_filter.py`
- `project/pipelines/alpha_bundle/merge_signals.py`
- `project/pipelines/alpha_bundle/fit_orth_and_ridge.py`
- `project/pipelines/alpha_bundle/apply_alpha_bundle.py`

## Promotion policy parity
Use identical gating principles for both tracks:
- adequate effective sample size,
- stable sign/effect across regimes and time splits,
- positive net benefit after friction proxies,
- robustness survivors in expectancy checks.

## When AlphaBundle is recommended
- multi-signal composition is required (time-series + cross-sectional + context),
- robust panel labels exist,
- you need universe-level ranking rather than single-event gating.

## When AlphaBundle is not recommended
- narrow single-event hypothesis where mainline phase1/phase2 is sufficient,
- incomplete feature panel or weak label alignment,
- low-latency iteration needs on one strategy family only.

## Workflow alignment with mainline
1. Run canonical discovery over 2020-2025.
2. Build/validate promoted edge candidates (mainline).
3. Build/validate AlphaBundle scores in parallel when applicable.
4. Convert promoted candidates into strategy builder artifacts.
5. Perform manual backtests from generated strategy candidates.


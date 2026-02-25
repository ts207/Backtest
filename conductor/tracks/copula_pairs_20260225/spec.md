# Specification: Copula Pairs Trading event analyzer

## Overview
Implement a Phase 1 research analyzer to detect statistical mispricing between asset pairs. This uses copula-based conditional probabilities to identify when the price of one asset has deviated significantly from its long-term dependency structure with another cointegrated asset.

## Functional Requirements
- **Marginal Fitting:** Fit empirical or parametric distributions to return series for each asset in the pair.
- **Copula Estimation:** Implement bivariate copula parameter estimation (Gaussian family as baseline).
- **Mispricing Index:** Calculate the conditional probability $P(U_1 \leq u_1 | U_2 = u_2)$ for the pair.
- **Detection Logic:** Generate events when the mispricing index crosses extreme thresholds (e.g., < 0.05 or > 0.95).
- **Event Registration:** Capture `mispricing_index`, `kendalls_tau`, and `cointegration_pval` as event metadata.

## Acceptance Criteria
- Analyzer successfully processes pairs from the feature lake.
- Events align with visual "spread" widenings between cointegrated assets.
- Mathematical properties of the mispricing index (0-1 range) are verified.

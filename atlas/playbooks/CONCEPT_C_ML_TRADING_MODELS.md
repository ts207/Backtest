# Concept Playbook: C_ML_TRADING_MODELS

## Definition
The application of supervised, unsupervised, and reinforcement learning models to event-driven trading. ML models are used either as state classifiers (regime detection) or as "meta-labelers" to filter event triggers based on a large vector of context features. All ML models must adhere to strict PIT constraints.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `feature_vectors` | All PIT features | 1m/15m | 365+ days |
| `forward_labels` | `ret_fwd`, `mae`, `mfe` | Multi-horizon | 365+ days |

## Metrics
*   **`auc_roc`**: Area under the receiver operating characteristic curve; measure of classification performance.
*   **`precision_at_recall_k`**: The accuracy of "Buy" signals at a fixed target recall level.
*   **`feature_importance_stability`**: Consistency of top feature rankings across different training folds.
*   **`model_overfit_ratio`**: Ratio of in-sample Sharpe to out-of-sample Sharpe.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_ML_01` | Leakage Guard | Model performance must be zero when feature timestamps are randomized relative to labels. |
| `T_ML_02` | Economic Edge | ML-filtered events must show a 20%+ improvement in after-cost expectancy vs the raw event trigger. |
| `T_ML_03` | Training Continuity | Models must be retrained using a rolling walk-forward window with no future data in the training set. |

## Artifacts
*   **`project/pipelines/research/phase2_conditional_hypotheses.py`**: Implementation of random forest and linear meta-labelers.
*   **`project/strategies/adapters.py`**: Adapters for translating model outputs into strategy orders.
*   **`data/lake/models/`**: Serialized model weights and training metadata.

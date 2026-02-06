# Phase-1 Interpretation Lock: vol_aftershock_window

## Gate Question
> Is there any window where P(secondary_shock) is consistently lower than controls and placebo,
> with no sign flips across years/sessions,
> and non-increasing hazard shape early?

## Evaluation
Using the baseline Phase-1 summary and the three window-tagged rerun summaries as context, and checking event/control/placebo plus split-consistency diagnostics:

| Window | P(secondary_shock) vs controls | P(secondary_shock) vs placebo | Year sign flips | Session sign flips | Early hazard non-increasing | Pass all conditions? |
|---|---:|---:|---|---|---|---|
| [0,48] | **Higher** (0.054 > 0.036) | Equal (0.054 = 0.054) | No | **Yes** | **No** | **No** |
| [24,96] | Lower (0.072 < 0.117) | **Higher** (0.072 > 0.063) | No | **Yes** | Yes | **No** |
| [48,144] | Lower (0.099 < 0.100) | Lower (0.099 < 0.108) | **Yes** | **Yes** | Yes | **No** |
| [0,96] | **Higher** (0.090 > 0.045) | Lower (0.090 < 0.099) | No | **Yes** | **No** | **No** |

No window satisfies all required conditions simultaneously.

## Binary Outcome Decision
**NO** — freeze `vol_aftershock_window` permanently at Phase-1 interpretation lock.

Implementation consequence: keep `vol_aftershock_window_rerisk_placeholder_v1` in `DRAFT` status and do not promote or design a Phase-2 action for this event.

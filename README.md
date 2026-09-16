# v6.4 PRESSURE + SWITCH RESEARCH

Entry logic is unchanged.

Actual position management:
- SCALP 1/3M, CORE 5/15M, MACRO 1H/4H remain independent.
- Price moving against a position does NOT itself create LOSS or close it.
- State machine: OPEN/HOLD <-> PRESSURE -> EXIT or SWITCH.
- PRESSURE uses opposite 10s/30s aggressive-flow persistence, flow intensity,
  book imbalance, OI context, and price response.
- If pressure fades, state returns to HOLD.
- Strong persistent opposite flow plus price acceptance triggers explicit
  EXIT of the old side and SWITCH into the opposite side.
- Every PRESSURE/HOLD/EXIT/SWITCH is persisted in position_events.
- Silent deletion remains forbidden.
- MFE/MAE continue to accumulate.
- Fixed TP/SL WIN/LOSS remains benchmark-only and never manages actual positions.

Initial research thresholds:
SCALP pressure 55 / switch 80 / persistence 12s
CORE pressure 60 / switch 82 / persistence 25s
MACRO pressure 65 / switch 85 / persistence 60s
These are experimental heuristics, not validated probabilities.


## v6.5 refresh persistence fix
- Fixed deduped browser signals returning before actual position recovery.
- `/api/positions` and `/api/position-audit` self-heal missing mutable position rows
  from the append-only lifecycle event log.
- Latest lifecycle event EXIT => never resurrect.
- Latest lifecycle event OPEN/CONFIRM/HOLD/PRESSURE/SWITCH with no later EXIT
  => active position is restored.
- Recovery preserves original OPEN/SWITCH timestamp and entry.
- Entry signal logic is unchanged.
- Benchmark WIN/LOSS still cannot close an actual position.


## v6.6 signal / actual-position visual split
- Entry detection logic is unchanged.
- Ordinary SCALP / CORE / RETEST / MACRO signals remain directional research signals.
- Ordinary live-signal rows no longer display `진입 $price`; their stored benchmark entry
  remains in SQLite for benchmark research.
- Actual research-position OPEN/SWITCH events get dedicated chart markers:
  `POS L` / `POS S`.
- Actual EXIT remains `X`.
- Position ledger continues to show the real tracked entry/exit/hold/return.
- Additional same-direction signals do not create a second same-engine position;
  they remain confirmation/research observations.
- Benchmark WIN/LOSS is still separate and never closes the actual position.
- Position recovery from v6.5 remains enabled.


## v6.7 position-only visible performance
- Ordinary chart/live signals are direction-only; no entry price is shown for them.
- Only actual `POS L` / `POS S` markers show the tracked entry price.
- POS markers use a distinct high-contrast palette from ordinary directional signals.
- User-facing benchmark WIN/LOSS presentation is removed.
- Actual performance endpoint `/api/position-performance` computes wins/losses/winrate
  only from closed actual research positions: OPEN/SWITCH -> EXIT.
- Open positions are excluded from winrate.
- Benchmark signal outcomes may remain stored internally for historical research, but
  they are not the displayed performance metric and cannot close positions.
- v6.5 refresh recovery and v6.4 HOLD/PRESSURE/SWITCH behavior remain.


## v6.8 position cumulative verification
- Renamed the top `실전 누적 검증` section to `포지션 누적 검증`.
- The visible cumulative performance source is `/api/position-performance`.
- Closed actual positions determine W/L and win rate.
- Open actual positions are shown as holdings and are excluded from win rate.
- Ordinary signal benchmark outcomes are not the cumulative performance metric.


## v6.9 actual position stats wiring fix
- Fixed v6.8: the title changed but `renderForward()` was still counting historical signal benchmark WIN/LOSS.
- Top card is now `포지션 누적 성과` and reads only `/api/position-performance`.
- 1/3M, 5/15M, 1H/4H header stats now also use closed actual positions per engine.
- Open positions do not count as W/L.
- Ordinary signal benchmark statuses no longer feed displayed win rate.

## v6.10 POS marker fix
- Root cause fixed: chart renderer previously drew EXIT and SWITCH only; OPEN was omitted.
- Actual OPEN now renders as `◆ POS L/S $entry`.
- LONG POS is cyan; SHORT POS is yellow; EXIT is neutral X.
- Ordinary signal rows no longer show entry price.
- Engine filtering now includes the matching engine's OPEN/EXIT/SWITCH markers.

## v6.11 position metrics fix
- Verified MFE/MAE formulas were correct, but their persistence depended on the combined
  reversal-manager call that ran after signal evaluation.
- Added `update_position_excursions()` on every OKX trade tick.
- MFE/MAE now persist independently of signal generation and benchmark logic.
- Reversal/pressure manager is also executed before signal evaluation in the public loop.
- Existing position lifecycle, HOLD/PRESSURE/X/SWITCH and entry logic are unchanged.

## v6.12 RETEST color separation
- RETEST L/S markers changed to violet (`#b56cff`).
- Actual POS L remains cyan and POS S remains yellow.
- No signal, position, HOLD/PRESSURE, EXIT or performance logic changed.

## v6.13 MFE/MAE restart-safe rebuild
- Root cause traced: v6.11 tracked excursions only from trade ticks seen after that server
  process/deploy started. An already-open position could therefore show only the post-deploy
  excursion (e.g. +$32.7) and miss an earlier low/high.
- MFE/MAE now rebuild from 1M candle highs/lows from the original `opened_ts` forward.
- Persisted extremes are merged, so values never shrink after a restart.
- Reversal manager refreshes reconstructed excursions before HOLD/PRESSURE/X/SWITCH decisions.
- RETEST color separation from v6.12 is retained.

## v6.14 EXIT / SWITCH split
- Keeps existing pressure scoring and entry logic.
- EXIT-only: SCALP 65/8s/0.22ATR; CORE 70/18s/0.28ATR; MACRO 75/45s/0.38ATR.
- SWITCH remains stronger: SCALP 80/12s/0.38ATR; CORE 82/25s/0.45ATR; MACRO 85/60s/0.55ATR.
- Strong SWITCH is evaluated first. Otherwise EXIT-only writes X and leaves engine flat/WAIT.
- v6.13 restart-safe MFE/MAE reconstruction and v6.12 RETEST color are retained.

## v6.15 PRESSURE ENGINE FIX
- Fixed fatal position-manager bug: `book_imbalance()` did not exist.
- Position manager now reads the same live `book_imb` value used by `/api/live` and the UI.
- Wrapped `manage_position_reversal()` in its own exception boundary so a future position-manager error cannot force the OKX public WebSocket to reconnect.
- v6.14 EXIT-only / SWITCH split remains unchanged:
  - CORE EXIT: pressure >=70, >=18s, adverse >=0.28 ATR -> X and FLAT/WAIT.
  - CORE SWITCH: pressure >=82, >=25s, adverse >=0.45 ATR -> X + opposite position.
- Existing MFE/MAE reconstruction remains unchanged.

## v6.16 STRUCTURE INVALIDATION
- Adds a flow-independent emergency thesis invalidation for research positions.
- SCALP exits at 0.75 ATR adverse from entry.
- CORE exits at 1.00 ATR adverse from entry.
- MACRO exits at 1.35 ATR adverse from entry.
- This produces X -> FLAT/WAIT only; it does NOT automatically reverse.
- Flow EXIT and stronger SWITCH logic from v6.15 remain available before/alongside this backstop.
- Purpose: a position can no longer remain HOLD indefinitely merely because momentary Delta/Book pressure cools after a decisive adverse price expansion.

## v6.17 MULTI ENGINE FIX
Audit result:
- CORE 5M/15M evaluator was alive.
- SCALP 1M/3M evaluator was alive and called continuously; sparse output is condition-driven.
- MACRO 1H/4H had UI/filter/position scaffolding but NO signal evaluator and was never called. It could not generate native MACRO signals.

Fix:
- Added symmetric MACRO evaluator using 4H liquidity + 1H sweep/reclaim + flow flip.
- Added live `evaluate_macro()` call.
- Added MACRO save/reset/cooldown state.
- Added MACRO to `/api/signals?engine=MACRO` filtering and signal stats.
- Existing CORE/SCALP, position EXIT/SWITCH, structure invalidation, MFE/MAE remain.

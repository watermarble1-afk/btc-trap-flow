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

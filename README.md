# v6 FLOW EXIT RESEARCH

Actual research positions:
- SCALP 1/3M, CORE 5/15M, MACRO 1H/4H are independent.
- No TP-based position exit.
- Same-engine same-direction signal = CONFIRM/HOLD.
- Other-engine opposite signals do not close the position.
- Same-engine opposite signal can EXIT/SWITCH that engine.
- Otherwise exit requires: developed favorable excursion + meaningful retrace + opposite 10s/30s order-flow.
- MFE and MAE are persisted while each position is open.
- Every OPEN / CONFIRM / EXIT / SWITCH is persisted in position_events.

Benchmark:
- Legacy TP1 1.5R vs SL WIN/LOSS remains for comparison only.
- Benchmark WIN/LOSS never closes an actual research position.

This is a first research heuristic, not validated trading performance.
No DB reset.

# BTC OKX RADAR v6.44 — CLUSTER TIME/COUNT FIX

Patch scope only: chart signal aggregation display.

- Same-engine counts are now limited to that engine's native candle bucket.
  - SCALP: 3m bucket
  - 5M: 5m bucket
  - 15M: 15m bucket
  - 1H: 1h bucket
  - 4H: 4h bucket
- VWAP / RETEST / normal families are counted separately before cross-engine merging.
- Cross-engine merge uses a tight 75-second actual-signal overlap window plus price proximity.
- A merged marker is placed at the latest constituent signal time (cluster completion), never backdated.
- Filter changes cannot drag a signal backward in time.
- DB, Signal Lab, signal generation, POS logic, v6.41 research, VWAP colors, and SCALP signal-only behavior are unchanged.
- PC and mobile patched.

# BTC Trap Flow v6.26 — FIVE ENGINE

Independent research engines:
- SCALP = 1M execution + 3M structure (paired)
- 5M = independent 5M engine / position
- 15M = independent 15M engine / position
- 1H = independent 1H engine / position
- 4H = independent 4H engine / position

Each engine has its own liquidity/pivots, ATR, signal arm/cooldown, MA/VWAP context, position lifecycle, MFE/MAE and live entry-based return. Other-engine signals cannot close or switch its position.

Legacy CORE history migrates to 5M; legacy MACRO history migrates to 1H so old records are retained. New 15M/4H engines start collecting independently.

UI: separate filters for SCALP, 5M, 15M, 1H, 4H, ALL, plus Signal Hide. PC and mobile included.

Research only; no real orders are placed.


## v6.27 Stability Final
- Throttles WS-triggered SQLite/engine work to prevent HTTP starvation and Railway 499/loading stalls.
- WAL + SQLite busy timeout.
- Frontend server sync is non-overlapping and polls every 10s.
- Loads up to 1000 persisted signals so refresh does not hide older MA EARLY/history merely due to the old 300-row cap.
- Distinct per-engine marker palettes for SCALP, 5M, 15M, 1H, 4H.
- Signal Lab lifecycle tracking is per engine and filters match the five-engine architecture.

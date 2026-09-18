# BTC Trap Flow v6.49 — TURN BIAS RADAR

Purpose: reduce signal noise and make the chart a turning-point radar.

- No new standalone 5M micro-pressure signals are stored. 5M pressure is internal timing evidence only.
- 15M EARLY is tightened: liquidity location + real failed-auction/absorption + another independent clue, score >= 68.
- TURN L/S replaces loose CONF display. TURN requires strict 15M reversal context, 5M execution turn, and higher-timeframe context; threshold >= 88.
- VWAP is evidence only, never a standalone new signal.
- Chart shows only server-confirmed TURN L/S plus the user's manual B/S/X markers.
- Signal Stream shows only useful live context: TURN, strict EARLY, 1H/4H context. Old DB history remains untouched.
- MARKET BIAS shows weighted 4H/1H/15M/5M context. 10s/30s flow is treated as timing evidence, not the big-direction engine.
- Existing SQLite DB / volume / signal research / user trades are preserved.
- Research only; no real orders are placed.

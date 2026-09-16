# v5 Dual BTC Signal Engines

Two independent research signal engines run in parallel:
- SCALP: 3M liquidity/structure + 1M sweep/reclaim + 10s/30s flow flip
- CORE: existing 15M liquidity + 5M sweep/reclaim + flow flip

Database:
- signals now has engine = SCALP or CORE
- existing historical rows are migrated to CORE
- WIN/LOSS benchmark is calculated independently per signal
- /api/stats returns separate totals/wins/losses/open/winrate per engine

BTC chart:
- AUTO: 1M/3M shows SCALP; 5M/15M shows CORE; higher TF shows ALL
- manual SCALP / 5M·15M / ALL buttons
- chart signal text is compact L / S
- existing CORE actual position EXIT remains X
- repeated same-direction signals remain visible by design

Important:
- SCALP thresholds are a first research heuristic, not a validated edge.
- Win rate is benchmark TP1-vs-SL outcome, not real-trading performance.

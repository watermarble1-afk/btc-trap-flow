# BTC OKX RADAR v6.61 RESEARCH FOUNDATION KST

Research-only BTC signal dashboard. It does not place real exchange orders.

## Why v6.61 exists
v6.61 freezes the live 3-SETUP trigger thresholds and turns the project into a measurable research system. The goal is to stop changing conditions by feel and instead compare BASE signals, earlier shadow variants, PREP/ARMED lead time, and the user's own manual entries on the same market data.

## Live 3-SETUP engine (logic frozen from v6.60)
- `PREP` — favorable context and approach to a useful location.
- `ARMED` — price reached/swept/broke the location and is waiting for response.
- `TRIGGER` — price response + 1M micro structure + flow timing. Only TRIGGER is a live 3-SETUP signal.

### PULLBACK
Higher-TF alignment -> 5M SMA20/SMA60/session VWAP location -> touch/probe -> reclaim -> 1M structure -> flow timing.

### REVERSAL
15M liquidity approach -> sweep/absorption -> failed auction -> 5M rejection -> 1M structure -> flow fade/turn.

### RETEST
Compression/range edge -> confirmed 5M displacement break -> first return -> hold/reject -> 1M structure -> resumed flow.

## v6.61 research foundation
- PREP / ARMED / TRIGGER **state transitions are now persisted** in `setup_stage_events` so lead time and conversion rate can be measured.
- Each state row stores market context such as price, ATR, HTF trend, VWAP/SMA, 10s/30s delta, OI, book and flow intensity.
- PULLBACK rows include the actual reference (`SMA20`, `SMA60`, or `VWAP`) instead of treating every pullback as the same setup.
- Two research-only shadow variants are recorded without appearing as trading signals and without opening positions:
  - `PRICE` — price/structure conditions without requiring the BASE flow confirmation.
  - `FAST` — price/structure conditions with an earlier/looser timing confirmation.
- `shadow_research` records MFE/MAE and 5/15/30/60 minute forward prices for those variants.
- `user_trade_research` applies the same MFE/MAE and fixed-horizon measurement to the user's manual B/S entries, independent of when the manual position is later closed.
- Existing BASE `signal_research` remains intact for apples-to-apples comparison.

## Lifecycle fixes
- A fired PULLBACK/REVERSAL/RETEST arm no longer blocks a fresh setup for the full original TTL; after the 2-minute TRIGGER display window it can form a new setup.
- RETEST breakout IDs are consumed when fired, invalidated, or expired so the same old breakout is not repeatedly re-armed.
- Consumed RETEST breakout timestamps are persisted in SQLite so a Railway restart does not resurrect the same used breakout.

## UI / verification fixes
- Legacy browser-side TRAP/RETEST signal arming is retired. The browser is display-only; the server 3-SETUP engine is authoritative.
- SETUP RADAR has its own lightweight ~2 second endpoint instead of waiting for the heavier 10 second full sync.
- Signal markers now use a dedicated overlay: the exact signal candle high/low has a dot, a vertical connector, and a compact PB/RV/RT badge. The badge may stack when multiple signals occur on the same candle, but the connector always points to the exact candle.
- 3-SETUP is labeled as the live 5M signal engine. Legacy 1/3M and 15M buttons are marked as historical DB display; 1H/4H remain separate higher-TF engines.
- A RESEARCH LAB panel exposes counts and JSON/CSV exports.

## Research endpoints
- `/api/setup-watch` — lightweight current PREP/ARMED/TRIGGER state.
- `/api/research/summary` — BASE/shadow/user counts and aggregate MFE/MAE.
- `/api/research/export` — combined JSON export.
- `/api/research/export.csv?kind=signals`
- `/api/research/export.csv?kind=setup`
- `/api/research/export.csv?kind=shadow`
- `/api/research/export.csv?kind=user`

## VWAP standard
- Intraday 1M/3M/5M/15M/1H/4H: session-anchored at 00:00 UTC.
- 1D: anchored to the first UTC day of the month.
- Typical price `(H+L+C)/3` weighted by candle volume.
- Chart VWAP and signal-engine VWAP use the same calculation.

## Validation performed for this build
- Python compile check.
- Desktop and mobile JavaScript syntax checks.
- Synthetic server runtime test covering DB migration, 3-SETUP evaluation, setup transition logging, shadow recording/tracking, user benchmark tracking, summary/export endpoints and CSV output.
- Headless browser mock test covering full page boot, server sync, dedicated SETUP polling, RESEARCH LAB rendering, and the exact-candle signal overlay. Two same-candle signals produced two stacked badges, two connector lines and two anchor dots with zero page errors in the mock run.

Deploy by uploading all extracted files to GitHub and committing. Railway auto-deploys from the repository.

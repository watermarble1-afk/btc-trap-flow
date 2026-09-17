# BTC OKX RADAR v6.39 · 5M/15M PRECISION + LIVE POSITION LAB

## v6.39
- 5M precision: 15M direction is context/veto; weak score (<85) or no 5M VWAP reaction/own structure is not promoted as a new raw 5M signal.
- 15M precision: 15M structure + VWAP location/reaction required; 1H opposite structure vetoes the 15M raw signal; weak score (<85) is rejected.
- When a 5M execution signal aligns with 15M direction, its research name is stored as `5M+15M CONF L/S`.
- 15M qualified raw signal is stored as `15M CONF L/S`.
- Existing strict POS confluence gate remains: raw signal != automatic position. Confluence Score is a condition-alignment score, not a probability.
- Existing re-entry guard / lifecycle / DB history preserved.

## Signal Lab UI
- Added polished `SYSTEM LIVE POSITION` and `MY LIVE POSITION` cards.
- System live card shows engine, side, entry, current price-based return %, live PnL ($, 1 BTC basis), and close control.
- My live card shows side, entry, current price, holding time, live return %, live PnL $, and position close control.
- My trade timing/history table remains separate from system position history.
- System realized TOTAL uses engine position events only. My realized TOTAL uses user position events only. They are not combined.
- Fixed Signal Lab live-price state wiring so the system live PnL card can actually render from `/api/live`.

## Existing v6.38 behavior retained
- Multi-select signal filters.
- Signal count aggregation uses the signal engine's own timeframe bucket, not the currently viewed chart timeframe.
- POS/X/SWITCH and user B/S/X are not aggregated.
- PC + mobile are shipped together.

Research simulator only. No private OKX keys and no real orders.

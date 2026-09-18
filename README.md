# BTC OKX RADAR v6.54 — FINAL UI SYNC

Base: v6.52 LIVE PIPELINE FIX.

Changes only to presentation/visibility (signal generation thresholds are unchanged):
- Chart now shows deduplicated WATCH candidates (EARLY/WATCH) plus confirmed TURN markers.
- Other research signals remain in SIGNAL STREAM / SQLite history; they are not deleted.
- WATCH markers are deduplicated by selected chart timeframe bucket to avoid returning to marker spam.
- Confirmed TURN markers remain prominent.
- Lightweight Charts time-axis/crosshair labels are formatted in Asia/Seoul (KST).
- PC/mobile version labels updated.

Deployment: upload/overwrite all extracted files in GitHub and Commit changes. Railway auto-deploys. Keep the Railway volume/database untouched.


## v6.54
- PC/mobile chart marker policy synchronized: WATCH candidates + confirmed TURN only.
- Mobile header/legend/help text synchronized with PC.
- KST chart time retained on both PC and mobile.
- Signal generation, DB schema, position logic, server pipeline unchanged from v6.53.

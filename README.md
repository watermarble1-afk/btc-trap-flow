# BTC OKX RADAR v6.41 · SIGNAL RESEARCH

Base: v6.40 multi-signal + side-position layout.

## v6.41
- Adds forward-only `signal_research` DB table. Existing signal/position/user-trade ledgers are untouched.
- Every new/recent signal automatically accumulates MFE/MAE and the first observed BTC price after 5/15/30/60 minutes.
- New `/api/signal-research` endpoint for research UI and mobile/server clients.
- SIGNAL LAB adds a 5M/15M post-signal validation table with directional 5/15/30/60m returns plus MFE/MAE.
- This is measurement only: it does not change WIN/LOSS, close positions, or claim a probability/win rate.
- Existing v6.39 5M/15M precision logic and v6.40 multi-select/side-position layout remain intact.
- PC and mobile continue using the same server/DB; mobile behavior is preserved and the research API is available to it without changing its trading controls.

Deploy: overwrite repo files -> Commit changes -> Railway auto-deploy. Keep `/data/trapflow.db`.

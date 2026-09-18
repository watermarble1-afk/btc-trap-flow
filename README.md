# BTC OKX RADAR v6.58 THREE SETUP LEAD KST

Research-only BTC signal dashboard. No real orders are placed.

## v6.58 core redesign
The 3-SETUP engine now separates **early recognition** from the actual signal.

- `PREP` — context is favorable and price is approaching a useful location.
- `ARMED` — price has touched/swept/broken the location and the engine is waiting for response.
- `TRIGGER` — price response + 1M micro structure + flow timing align. Only this stage is persisted as a signal marker.

### PULLBACK
Higher-timeframe trend alignment -> approach 5M SMA20/SMA60/VWAP -> touch/probe -> reclaim -> 1M structure break -> flow turn.

### REVERSAL
Approach 15M liquidity -> sweep or absorption -> failed auction back inside the level -> 5M rejection -> 1M structure turn -> flow fade/turn.

### RETEST
Compression/range-edge warning -> confirmed 5M displacement breakout -> first return to the broken level -> hold/rejection -> 1M structure + resumed flow.

## Important design rules
- 10s/30s delta is **timing evidence only**, never the primary direction source.
- PREP/ARMED states are live diagnostics and are not written to the signal DB.
- Only PULLBACK / REVERSAL / RETEST `TRIGGER` events are saved as new 3-SETUP signals.
- Existing DB/history and forward `signal_research` MFE/MAE/horizon measurement remain intact.
- No automatic real trading is performed.

Deploy by uploading all extracted files to GitHub and committing. Railway auto-deploys from the repository.


## v6.58 VWAP SYNC
- Signal-engine VWAP changed from 240-bar rolling VWAP to the same anchored VWAP shown on the chart.
- Intraday (1M/3M/5M/15M/1H/4H): resets at 00:00 UTC.
- 1D: resets monthly at 00:00 UTC on day 1.
- Price basis: typical price (H+L+C)/3 weighted by candle volume.
- Existing 3-SETUP PREP -> ARMED -> TRIGGER logic remains intact; all VWAP evidence now references this synchronized value.


## v6.60 — SIGNAL CANDLE ANCHOR
- PULLBACK / REVERSAL / RETEST markers anchor to the opening timestamp of the displayed timeframe candle.
- Marker labels include the actual signal HH:mm time.
- Labels shortened to PB / RV / RT to reduce overlap while preserving direction and score.
- Manual OPEN / EXIT markers follow the same candle anchoring rule.
- Signal/VWAP/PREP-ARMED-TRIGGER logic unchanged from v6.59.

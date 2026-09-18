# BTC OKX RADAR v6.47 · DISPLAY-CANDLE SIGNAL FIX

This patch changes chart signal grouping only.

## Fixed
- Signal grouping now follows the **currently displayed chart candle**, not the signal engine's native timeframe bucket.
- On a 1M chart, signals generated on different 1-minute candles are shown separately.
- On a 5M chart, signals that fall inside the same displayed 5-minute candle are grouped and counted together.
- Same principle applies to 15M / 1H / 4H / higher displayed timeframes.
- Toggling 5M / 15M / 1H filters no longer drags older/later signals into one native-engine bucket.
- EARLY / CONF / VWAP / RETEST families remain separate so their counts cannot inflate each other.

## Unchanged
- Signal-generation logic
- EARLY → 5M → CONF logic
- 1H / 4H signal logic
- VWAP visibility toggle
- Position simulator logic
- SQLite / existing DB records
- Signal Lab raw rows

PC and mobile are both patched.


## v6.48 TURN RADAR
- Raw research signals remain in DB and move to SIGNAL STREAM.
- Main chart shows only confluence-based TURN L/S candidates.
- TURN display score is a research heuristic, not a probability or validated edge.
- Manual user B/S/X markers remain on chart.
- No server-side signal generation, DB schema, position logic, or API route was changed.

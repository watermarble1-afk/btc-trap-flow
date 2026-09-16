# v5.6 Trade Table + Candle/Daily Move

Changes:
- Chart markers are clean again: no entry/exit dollar amount in marker text.
- Added right-side position/trade ledger with engine, direction, entry, exit, holding time and return.
- Existing CORE OPEN/EXIT/SWITCH position events feed the ledger; no fake exits are created.
- Restores/extends candle hover information: OHLC + candle % + high/low range % where the existing OHLC legend is used.
- Adds '오늘 · 일봉 기준' card showing current daily candle dollar move and percentage move from daily open.
- Existing signal labels/colors, SCALP/TRAP/MACRO engines and benchmark stats remain.

SCALP and MACRO still do not have independent actual position state machines; their signal entries remain in signal history/benchmark until that is implemented.

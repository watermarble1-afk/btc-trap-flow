# v5.7 HARD FIX

Fixed:
- Server falsely showing OFFLINE even while the direct OKX chart moved.
  Cause: frontend stats code referenced `st.MACRO` instead of `stats.MACRO`, throwing inside syncServer.
- Restored server signal history and position event/X rendering after sync.
- Added the requested right-side position/trade table using the actual page `.box` structure.
- Chart markers no longer show dollar amounts; X is clean.
- Trade table shows entry, exit, holding time and return for real recorded CORE position events.
- Fixed candle hover %: OHLC, previous-candle %, candle body %, high-low range %.
- Fixed today's daily candle dollar move and percentage from daily open.
- AUTO now maps 1H/4H to MACRO.
- Fixed malformed external Lightweight Charts script tag that had swallowed older inline hover code.

No SQLite schema reset or DB deletion is performed.

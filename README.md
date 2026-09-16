# MY MARKET TERMINAL v3.1

Changes
- LONG: blue
- SHORT: red
- RETEST: cyan
- X: actual position-state EXIT marker only
- WIN/LOSS remains only in the separate benchmark/history result column
- Same-direction TRAP keeps/confirms the current position
- Opposite TRAP exits and switches
- Added reversal exit: after >=0.80 ATR favorable excursion, a >=0.45 ATR retrace plus opposite 10s/30s flow confirms EXIT
- Existing /data database is preserved; position_state schema migrates automatically
- btc.html cache-busted via ?v=31 for mobile/browser refresh

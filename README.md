# BTC Trap Flow v6.26 — FIVE ENGINE

Independent research engines:
- SCALP = 1M execution + 3M structure (paired)
- 5M = independent 5M engine / position
- 15M = independent 15M engine / position
- 1H = independent 1H engine / position
- 4H = independent 4H engine / position

Each engine has its own liquidity/pivots, ATR, signal arm/cooldown, MA/VWAP context, position lifecycle, MFE/MAE and live entry-based return. Other-engine signals cannot close or switch its position.

Legacy CORE history migrates to 5M; legacy MACRO history migrates to 1H so old records are retained. New 15M/4H engines start collecting independently.

UI: separate filters for SCALP, 5M, 15M, 1H, 4H, ALL, plus Signal Hide. PC and mobile included.

Research only; no real orders are placed.


## v6.27 Stability Final
- Throttles WS-triggered SQLite/engine work to prevent HTTP starvation and Railway 499/loading stalls.
- WAL + SQLite busy timeout.
- Frontend server sync is non-overlapping and polls every 10s.
- Loads up to 1000 persisted signals so refresh does not hide older MA EARLY/history merely due to the old 300-row cap.
- Distinct per-engine marker palettes for SCALP, 5M, 15M, 1H, 4H.
- Signal Lab lifecycle tracking is per engine and filters match the five-engine architecture.


## v6.28 VWAP CONFLUENCE FINAL
- MA EARLY generation retired; historical rows remain in SQLite for research.
- Historical MA EARLY markers are hidden from BTC chart/live target to reduce clutter.
- Added independent VWAP reclaim/rejection signals for SCALP, 5M, 15M, 1H, 4H.
- Actual research POS now opens only through a strict confluence gate (signal + VWAP + flow + book/activity). The gate score is heuristic, not a probability.
- Opposite signal cannot instantly flip a position; switch requires PRESSURE plus exceptional confluence.
- Position-event API history expanded to 5000 so overnight CONFIRM traffic cannot hide OPEN events from Signal Lab.
- Signal Lab filters now use the five independent engine keys.

## v6.29 CONFIRMED SIGNAL
- Chart-only marker aggregation: same displayed candle + engine + direction + signal name renders once as `(n)`; raw SQLite rows are untouched.
- VWAP L/S markers use a dedicated cyan/orange visual family.
- POS gate now requires >=82 confluence AND at least four independent evidence groups among setup, VWAP location/reaction, flow, order/activity, own-TF structure, higher-TF structure.
- Direct higher-TF conflict vetoes normal POS unless exceptional confluence reaches 92+.
- Opposite signal alone cannot switch; existing position must already be PRESSURE plus confirmed 92+ opposite confluence.
- Browser-persisted signals now use the same strict POS gate; they can no longer bypass it through the legacy immediate position function.
- Confluence score is a condition score, not a calibrated win probability.

## v6.31 Simulator controls
- Signal Lab shows cumulative realized return (simple sum of closed POS return percentages).
- Signal Lab shows cumulative realized P/L in USD on a 1 BTC notional basis; fees and leverage are excluded.
- Each currently open engine position has a manual close button. It records an EXIT with reason MANUAL and closes only that simulator engine position; it never sends an OKX order.

## v6.32 REENTRY GUARD
- Main/mobile trade ledger adds per-trade realized P/L ($, 1 BTC basis; fees/leverage excluded).
- Signal Lab position ledger adds the same per-trade P/L column.
- Automatic FLOW/STRUCTURE exits arm a same-engine/same-side re-entry lock: SCALP 8m, 5M 15m, 15M 30m, 1H 60m, 4H 120m.
- Signals continue to be stored during the lock; only simulator POS re-entry is blocked.
- Fresh positions have a FLOW hold guard: SCALP 2m, 5M 8m, 15M 15m, 1H 30m, 4H 60m. Structure invalidation can still close a clearly broken thesis.
- FLOW pressure must persist longer before ordinary exit. Manual close remains immediate and does not arm the automatic re-entry lock.


## v6.33 SIGNAL COUNT FIX
- Chart-only aggregation hardened: same displayed candle + engine + direction + canonical signal family is one marker with `(n)`, including `(1)`.
- Historical naming variants are normalized for display so TRAP/engine-name variants no longer escape aggregation.
- VWAP, RETEST, SCALP remain separate signal families.
- POS / EXIT / SWITCH markers are never aggregated.
- Raw SQLite signals and Signal Lab rows are unchanged.
- v6.32 re-entry guard, simulator P&L and manual close controls are preserved.


## v6.34 CLEAN CHART + LIVE POS
- ALL chart view is now execution-first: raw SCALP/TRAP/VWAP/RETEST observation markers are hidden. POS / X / SWITCH remain visible.
- Selecting a specific engine (1/3M, 5M, 15M, 1H, 4H) restores that engine's raw signals, still aggregated per bar as `(n)`.
- Signal DB and Signal Lab remain unchanged; this is display-only filtering.
- Signal Lab current simulator positions now show live return % and 1-BTC unrealized P/L, plus a `포지션 종료` button.
- Main BTC/mobile current-position cards retain live return/P&L and manual close controls.
- v6.32 re-entry guard and v6.33 count aggregation remain intact.

## v6.36 UI / USER DESK PATCH
- PC + mobile together.
- Move engine trade ledger out of narrow right sidebar to a wide panel below the chart.
- Add separate personal position/trade panel beside engine ledger on desktop; stacked on mobile.
- Rename manual controls to LONG 진입 / SHORT 진입 to clarify these are new simulated positions at current market price.
- Existing 포지션 종료 button remains the only manual close control; no duplicate liquidation button.
- Personal position stays fully separate from engine positions and has its own live P/L and personal trade ledger.
- Manual user chart entry markers are minimal B / S only; personal exits are kept in the personal ledger rather than adding chart clutter.
- Existing v6.35 HTF 15M/1H stricter confluence logic is preserved.


## v6.37 USER TRADE TRACKING
- Manual user position API now returns current price, live P/L %, and live P/L USD.
- User OPEN trades appear immediately in Signal Lab; they no longer wait for EXIT to become visible.
- User ledger records OPEN and EXIT lifecycle separately from engine positions.
- Chart shows user entry as B/S and user exit as X.
- PC and mobile both updated.

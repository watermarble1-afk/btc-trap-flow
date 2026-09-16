# v6.25 UI + POSITION RETURN PATCH

Base: v6.24 MA/VWAP Context.

Changes only to display/UI behavior:
- Replaces AUTO signal-view button with `신호 제거`.
- `신호 제거` hides all chart markers (SCALP/TRAP/MACRO/MA EARLY/POS/X/SWITCH) without deleting DB/history.
- SCALP/TRAP/MACRO/ALL restores marker views.
- Open engine position card shows live return from that position's own entry price.
- LONG return = (current-entry)/entry; SHORT return = (entry-current)/entry.
- Live position return refreshes every 500ms from current market price.
- Shows both percentage and dollar-per-1-BTC price move: `현재수익률 +0.123% · 현재손익 +$93.4`.
- Desktop and mobile both patched.
- Signal/entry/exit logic unchanged from v6.24.

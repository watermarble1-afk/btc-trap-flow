# v5.5 1H / 4H Macro Trend Engine

Adds a third independent BTC research signal engine:

MACRO (1H/4H)
- 4H EMA 8/21 defines broad trend regime.
- 1H EMA 8/21 confirms trend and supplies pullback/reclaim trigger.
- Requires 1H pullback toward fast EMA followed by close back in trend direction.
- Uses 1H ATR to normalize distance/strength.
- 45 minute signal cooldown.
- Benchmark signals: MACRO L / MACRO S.
- Separate MACRO WIN/LOSS stats.

Display:
- AUTO: 1M/3M => SCALP, 5M/15M => TRAP, 1H/4H => MACRO, higher TF => ALL.
- Manual button: 추세 1H/4H.
- ALL distinguishes all three engines by label, timeframe and color.
- Live history shows [1H/4H] and entry price.

Important:
MACRO is a first research heuristic for broad-trend context, not a validated trading edge.
Like SCALP, MACRO is benchmark-signal only. The existing actual position/X state machine remains CORE-only.

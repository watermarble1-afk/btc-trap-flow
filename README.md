# BTC OKX RADAR v6.63 MTF PRESSURE KST

## Purpose
v6.63 changes the flow architecture instead of adding another signal condition.

The directional pressure input is now built from **1M / 3M / 5M / 15M aggressor-flow windows**. The old 10s / 30s flow is retained only as a final MICRO timing check.

## MTF pressure engine
- Incoming OKX trades are aggregated into persistent 1-minute buy/sell-notional buckets.
- Recent minute buckets are stored in SQLite (`flow_minutes`) and restored after Railway restart/deploy.
- Live pressure is calculated for rolling 1M, 3M, 5M, and 15M windows.
- Each window exposes buy-vs-sell pressure, delta ratio, price movement, flow intensity, data coverage, and a BUY / SELL / BALANCE state.
- FAST pressure = 1M + 3M.
- SLOW pressure = 5M + 15M.
- A FAST-vs-SLOW divergence is tracked as an early TURN indication.
- Pressure values are evidence strength, not win probability.

## Signal system changes
- PULLBACK / RETEST still require their original location + price-response + 1M structure logic.
- Their flow confirmation now requires MTF pressure alignment first; 10s/30s can only confirm the final timing.
- REVERSAL now uses 5M/15M aggression for the sweep/absorption context and FAST-vs-SLOW pressure turn for the reversal transition.
- 1H / 4H legacy tactical triggers also use MTF pressure as the directional flow layer, with micro flow only as the final timing check.
- The engine-position HOLD / PRESSURE / EXIT / SWITCH manager now weights MTF pressure heavily and gives 10s/30s only a small timing weight.

## Decision Layer
The v6.62 conflict resolver remains in place and now also consumes MTF pressure:
- stable bias: LONG / SHORT / NEUTRAL
- action: WAIT / WATCH / READY / TRIGGER / CONFLICT
- LONG <-> SHORT still cannot flip directly; it must pass through NEUTRAL
- a RAW trigger can be held at READY when MTF pressure is materially opposite
- MTF pressure contributes to evidence but is not treated as probability

## UI
A new panel under the price chart shows:
- 1M / 3M / 5M / 15M LONG vs SHORT pressure
- delta, price move, flow intensity, and warm-up coverage
- FAST vs SLOW pressure and early TURN direction
- 10s / 30s remain visible in the right-side MICRO TIMING box

The default chart still shows only Decision Layer execution markers. `RAW 연구신호 보기` restores PB / RV / RT research markers.

## Persistence and warm-up
The first fresh deployment needs a short warm-up before MTF pressure becomes a valid signal input. Once minute-flow data has been collected, it is persisted and restored across Railway restarts. The UI explicitly displays WARM coverage instead of pretending incomplete 15M data is complete.

## Research
Existing research remains intact:
- BASE signals + forward MFE/MAE
- PREP / ARMED / TRIGGER lifecycle events
- SHADOW PRICE / FAST variants
- manual-trade benchmark
- Decision Layer history

Setup lifecycle context now also records the 1M / 3M / 5M / 15M pressure state at the time of the event.

## Important
This is still a research trading system. MTF pressure is intended to reduce micro-flow noise and improve timing context; it is not evidence by itself that a trade is profitable.

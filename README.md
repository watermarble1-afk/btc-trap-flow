# BTC OKX RADAR v6.66 — EXEC + PRECURSOR LAB

This version freezes the current EXEC Decision Layer for validation and removes unrelated live signal generation from the runtime/UI. It adds one independent leading-anomaly research engine (PRECURSOR) without changing or vetoing EXEC.

## Live signal scope

Only two signal layers are exposed on the chart:

- `EXEC L / EXEC S`: the existing Decision Layer trigger. PB / REVERSAL / RETEST remain internal feeder states so EXEC can continue to be tested unchanged.
- `EARLY L / EARLY S`: the new PRECURSOR warning. It is an independent research warning, not an entry signal and it does not affect EXEC.

`X` on the chart is reserved for the user's own manually simulated position exit. Decision invalidation is still recorded internally but is no longer drawn as `X`.

## Removed / frozen signal paths

No new live signals are generated from the following paths:

- independent 1H / 4H signal engines
- legacy SCALP 1M/3M engine
- legacy standalone 15M engine
- standalone VWAP signal generator
- PRICE / FAST shadow-signal variants
- public PB / REVERSAL / RETEST raw signal rows

Historical database rows are not deleted. 1H / 4H market structure is still used as context inside the existing EXEC feeder logic; only their independent signal generation is stopped.

## PRECURSOR engine

PRECURSOR looks for a combination of four leading-anomaly ideas plus location:

1. **Impact decay** — aggressive buying/selling continues but produces less matching price progress than previous minutes.
2. **Absorption** — one-sided aggressor flow persists while price refuses to continue in that direction.
3. **Pressure deceleration** — 1M/3M fast pressure weakens before 5M/15M slow pressure fully turns.
4. **Liquidity sweep / reclaim** — a recent 1M local extreme is swept and quickly reclaimed.
5. **Location** — the anomaly is near 15M liquidity or a recent 5M local extreme.

An `EARLY` chart event requires a high combined score, at least three independent sensor groups, meaningful location/sweep context, and a margin over the opposite side. Repeated events are episode-deduplicated so the chart does not spam the same warning.

## Research / validation

v6.66 records forward research for both layers:

- EXEC: MFE / MAE and 5 / 15 / 30 / 60 minute forward prices.
- PRECURSOR: MFE / MAE and 5 / 15 / 30 / 60 minute forward prices.
- PRECURSOR also records the next same-direction EXEC within 60 minutes and its lead time in seconds.

Exports:

- `/api/research/export.csv?kind=decision` — EXEC events + forward research
- `/api/research/export.csv?kind=precursor` — EARLY events + forward research + lead time to EXEC
- setup lifecycle / historical research exports remain available for audit.

## Runtime/UI changes

- PC and mobile chart signal controls are simplified to EXEC + EARLY only.
- RAW PB/RV/RT toggle and legacy signal-engine selectors are removed.
- Legacy engine position panels are removed from the live UI.
- The MTF 1M / 3M / 5M / 15M pressure panel remains unchanged.
- A dedicated PRECURSOR panel shows LONG/SHORT early scores, sensor groups and reasons.
- Cache key bumped to `v665`.

## Validation performed

- Python compile check.
- PC and mobile JavaScript syntax checks.
- Duplicate HTML ID checks.
- Synthetic LONG and SHORT precursor scenarios.
- Neutral-flow scenario to verify no EARLY event is emitted from location alone.
- PRECURSOR episode de-duplication check.
- EXEC and PRECURSOR research tables / exports tested.
- PRECURSOR → next EXEC lead-time linking tested.
- Confirmed `public_loop` no longer calls the independent 1H/4H signal engines or legacy shadow/signal research hot paths.
- Confirmed PB/RV/RT live evaluator no longer inserts public raw signal rows.

The actual OKX/Railway long-running soak test still has to occur in the deployed environment.


## v6.66 — SIGNAL LAYER TOGGLES
- EXEC confirmed, PRECURSOR EARLY, and stored PB/RV/RT research-result overlays can be shown/hidden independently.
- Research overlay is display-only: it does not restart retired 1H/4H/SCALP/old-15M/VWAP generators and does not alter EXEC or EARLY logic.
- Chart defaults: EXEC ON, EARLY ON, PB/RV/RT research OFF. Choice persists in localStorage.
- User B/S/X markers always remain visible; X remains user-position exit only.
- Base PB/RV/RT records are fetched from /api/signals solely for optional chart review.

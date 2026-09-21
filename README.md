# BTC OKX RADAR v6.69 — VWAP14 ONLY LAB

## 목적
이번 버전은 새로 만든 **VWAP14 + MA KNOT 방향 엔진만 단독 검증**하기 위한 경량 기준판입니다.
기존 신호 데이터는 DB에 보존하지만 신규 생성은 중단합니다.

### 활성 엔진
- VWAP14 DIRECTION: `WATCH → LEAN → RELEASE`
- 기준: 15M Rolling VWAP14 + MA5/8/10/20 knot + failed move + slope inflection
- 내 수동 포지션 B/S/X 기록은 유지

### 전원 OFF
- EXEC Decision
- PULLBACK / REVERSAL / RETEST
- EARLY / PRECURSOR
- EVENT ENTRY
- 1H / 4H 독립 신호
- SCALP / 구 15M / standalone VWAP
- MTF order-flow pressure 수집 및 snapshot writer

### 경량화
- 서버 public WS: trades/books/OI 대신 ticker만 구독
- 서버 business WS: 15M + 1D만 구독
- seed: 15M + 1D만 로드
- VWAP14 엔진 평가 1초 간격, outcome 연구 2초 간격
- PC/모바일은 EXEC/EARLY/EVENT/BASE 신호 API를 더 이상 fetch하지 않음
- 브라우저의 별도 trades/books/OI public WS도 시작하지 않음
- 차트에는 VWAP14 WATCH/LEAN/RELEASE + 내 B/S/X만 표시

### 보존
기존 DB 테이블/과거 신호는 삭제하지 않습니다. 나중에 비교가 필요하면 다시 꺼낼 수 있습니다.

---

# BTC OKX RADAR v6.68 — VWAP14 DIRECTION LAB

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

v6.67 records forward research for both layers:

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


## v6.67 — SIGNAL LAYER TOGGLES
- EXEC confirmed, PRECURSOR EARLY, and stored PB/RV/RT research-result overlays can be shown/hidden independently.
- Research overlay is display-only: it does not restart retired 1H/4H/SCALP/old-15M/VWAP generators and does not alter EXEC or EARLY logic.
- Chart defaults: EXEC ON, EARLY ON, PB/RV/RT research OFF. Choice persists in localStorage.
- User B/S/X markers always remain visible; X remains user-position exit only.
- Base PB/RV/RT records are fetched from /api/signals solely for optional chart review.


## v6.67 — EVENT ENTRY LAB
- EXEC and PRECURSOR/EARLY logic are preserved.
- Adds an independent stateful EVENT ENTRY research engine.
- Sequence: exhaustion/absorption -> liquidity sweep or failed auction -> reclaim -> first 1M structure response -> micro not opposing.
- EVENT does not feed, block, boost, or veto EXEC/EARLY.
- Adds EVENT chart layer toggle, live panel, stream/history entries, MFE/MAE + 5/15/30/60m research, and EXEC lead-time matching.


## v6.67a — EVENT TOGGLE HOTFIX
- EVENT 타점 버튼 ON 상태 CSS가 누락되어 클릭해도 시각적으로 상태가 바뀌지 않던 UI 버그 수정.
- 모든 신호 레이어 버튼에 aria-pressed / ON-OFF title 상태 동기화 추가.
- OFF 버튼은 opacity를 낮춰 ON/OFF 구분을 명확히 함.
- iframe/service-worker cache key를 v668로 갱신.
- 서버/EXEC/EARLY/EVENT 신호 로직은 v6.67과 동일.

## v6.68 — VWAP14 DIRECTION LAB

This build keeps the existing EXEC / EARLY / EVENT engines frozen for side-by-side comparison and adds one independent research engine based on the user's real 15-minute chart process.

### Chart VWAP
- Main chart VWAP is now **rolling Length 14** instead of UTC session-anchored.
- Calculation: last 14 candles, HLC3 = (H+L+C)/3, weighted by candle base volume.
- The visible line is labeled **VWAP14**.
- The frozen legacy EXEC engine still keeps its prior internal anchored VWAP so the EXEC benchmark is not silently changed in the same experiment.

### VWAP14 DIRECTION engine (15M)
The engine watches MA5 / MA8 / MA10 / MA20 as a bundle and records a lifecycle rather than one late confirmation:
1. **WATCH** — price is near VWAP14 and the MA bundle is compressed (MA KNOT).
2. **LEAN** — VWAP14 is being held/rejected, the attempted move is failing/stalling, and short-MA slope starts to inflect.
3. **RELEASE** — price leaves the MA knot on the VWAP-supported side. This is the actual entry-candidate research event.

It also publishes an invalidation level near the opposite side of VWAP14 / the MA bundle. WATCH and LEAN are alerts to inspect the chart, not entry commands.

### Research / UI
- New independent chart toggle: `VWAP14 방향`.
- Chart badges: `VW WATCH`, `VW LEAN`, `VW REL`.
- New side panel: VWAP14 value, MA knot width in ATR, invalidation, reasons.
- RELEASE events receive 5m / 15m / 30m / 60m, MFE and MAE outcome research.
- New export: `/api/research/export.csv?kind=vwapma`.
- Cache version bumped to 668 on PC/mobile.

## v6.70 — VWAP14 ONLY UI CLEANUP
- Removed obsolete/stopped signal panels from PC/mobile UI.
- Top dashboard now shows BTC price, VWAP14, stage, direction, VWAP distance, MA knot width and invalidation.
- Added VWAP14 DIRECTION FLOW panel below the chart with WATCH → LEAN → RELEASE active-stage highlighting.
- Right panel now contains only VWAP14 direction details, signal guide, research summary, manual position and live VWAP14 stream.
- Default chart timeframe is 15M to match the active engine.
- Server signal logic remains v6.69 VWAP14-only; this patch changes UI/observability only.

## v6.71 — LIVE SYNC + TODAY MOVE HOTFIX
- Restored dedicated Today / 1D USD and percent move card.
- Core `/api/live` sync is now independent from optional user-position/event API failures.
- Added VWAP14 engine heartbeat (LIVE / STALE / OFFLINE) with update age.
- 24H SERVER status now reflects public ticker and business candle streams directly.
- No VWAP14 signal-condition changes from v6.70.


## v6.72 — BOOT / LIVE SYNC HOTFIX
- Fixed a frontend boot crash caused by legacy renderContext() referencing the removed #mtf panel.
- Server/VWAP14 sync now starts independently of chart history boot, so a chart-side error cannot block TODAY, VWAP14 state or heartbeat.
- VWAP14 signal logic is unchanged from v6.71.
- Cache/version bumped to v6.72.

## v7.01 MA CYCLE CHART PATCH
- Removed legacy VW WATCH / VW LEAN / VW REL markers from the chart layer.
- MA CYCLE phase transitions are now persisted and drawn on the actual candle where they occur: RELEASE / ALIGN / EXPANSION / MA_HIT / REALIGN / RE_EXPANSION / BREAKDOWN.
- Added always-on SMA 5 / 10 / 20 / 60 / 120 / 240 / 480 to BTC desktop and mobile charts.
- BIAS remains persistent through ordinary pullbacks; chart markers represent PHASE changes, not independent LONG/SHORT calls.

## v7.04 TOP / BOTTOM RADAR
- Existing MA CYCLE engine logic is unchanged.
- Adds independent chart-side TOP/BOTTOM research radar for every displayed timeframe.
- Stages: TOP WATCH → TOP WARNING → EXIT L / BOTTOM WATCH → BOTTOM WARNING → EXIT S.
- TOP/BOTTOM is a reversal-risk / exit-watch module, not an automatic opposite-position entry signal.
- Uses only data available up to each candle: VWAP14 distance, short-MA spread, spread contraction, short-MA slope deceleration/turn, and candle rejection context.
- Adds separate TOP/BOTTOM chart signal ON/OFF toggle; preference is saved in browser localStorage.

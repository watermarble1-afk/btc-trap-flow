# BTC OKX RADAR v6.50 — TURN DIAGNOSTIC RADAR

v6.49 TURN BIAS RADAR 기반 진단 패치.

## 핵심 변경
- 기존 신호/포지션 DB 삭제 없음.
- 기존 TURN 생성 로직은 유지하고, 오른쪽 패널을 TURN 탐지 과정 진단용으로 재구성.
- MARKET REGIME: 4H/1H/15M/5M 구조 기반 큰 방향.
- TURN LOCATION: 15M liquidity 거리(ATR)와 LONG/SHORT 위치 점수.
- REVERSAL PRESSURE: 10s/30s Delta, flow intensity, book을 방향 결정이 아닌 마지막 반전 확인 재료로 표시.
- TURN RADAR: LOCATION / STRUCTURE / MA·VWAP / FLOW REVERSAL을 합류 점수로 표시.
- 점수는 확률이 아니라 heuristic evidence-strength score.
- PC(btc.html)와 모바일(mobile.html) 모두 반영.

## TURN RADAR 상태
- NEUTRAL < 55
- WATCH >= 55
- ARMED >= 70
- CONFIRMED >= 82

## 배포
ZIP을 풀고 GitHub 저장소 루트에 전체 덮어쓰기 후 Commit changes. Railway 자동 배포.

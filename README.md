# BTC OKX RADAR v6.42 · UNIVERSAL SIGNAL CLUSTER

v6.41 기반 차트 표시 전용 패치.

- SCALP(1/3M), 5M, 15M, 1H, 4H 등 선택된 모든 일반 신호를 대상으로 클러스터링
- 같은 방향 + 5분 이내 + 가격 0.22% 이내로 겹치는 신호는 차트에서 하나로 합침
- 예: `[1/3M+5M+15M] S (8)`
- `(8)`은 합쳐진 원본 신호 개수
- 첫 경고 시점에 대표 마커를 표시하여 선행성 확인 가능
- LONG/SHORT 방향이 다르면 절대 합치지 않음
- POS / X / SWITCH / 사용자 B·S·X는 합치지 않음
- SQLite 원본 signals 및 Signal Lab 연구 데이터는 삭제/병합하지 않음
- v6.41 사후검증 기능 유지
- PC + 모바일 동일 적용

# BTC OKX RADAR v6.45 · EARLY → 5M → CONF

이번 버전은 신호 구조를 단순화한 연구 패치입니다.

- 1/3M SCALP: 신규 신호 생성/DB 저장/자동 POS 중단. 과거 기록은 삭제하지 않음.
- VWAP: 독립 VWAP L/S 신호 생성 중단. 15M EARLY와 5M/CONF의 내부 근거로만 사용.
- 15M EARLY: 유동성 위치 + failed auction / absorption / flow fade / VWAP context / OI trap 중 선행 징후를 조합해 미리 경고.
- 5M L/S: 단기 체결 흐름 + 5M 구조/가격/VWAP 반응으로 압력 전환을 표시. 이 단계는 POS를 열지 않음.
- CONF L/S: 최근 15M EARLY + 5M 압력전환 + 상위 TF 비충돌/보조 근거가 합쳐질 때만 생성. 이미 15M ATR 기준 과도하게 진행된 경우 추격 CONF를 억제.
- 자동 시뮬 POS: CONF에서만 5M 실행 POS 후보. EARLY/5M 단계는 포지션을 열지 않음.
- 1H / 4H: 기존 `evaluate_tf_engine()` 로직을 그대로 유지. v6.45 패치에서 해당 엔진 조건은 변경하지 않음.
- Signal Lab: EARLY / 5M / CONF별 사후 MFE/MAE, 5/15/30/60분 결과와 `근거` 문자열 확인 가능.
- PC + 모바일 동시 반영.
- 기존 DB 유지. 기존 SCALP/VWAP 과거 데이터 삭제 없음.

주의: SCORE/CONF는 조건 합치도이며 승률이나 실제 확률이 아닙니다. 실제 주문은 전송하지 않습니다.

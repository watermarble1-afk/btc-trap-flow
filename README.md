# MY MARKET TERMINAL v4.2 — Gaon chart fix

- 가온전선 일봉 데이터: 공개 history JSON을 1순위로 사용
- Yahoo Finance는 fallback
- 주식 지표 설정 초기화가 chart 생성 전에 실행되던 race condition 수정
- 데이터 실패 시 화면에 오류 원인 표시
- BTC/주식 지표 설정 기능 유지
- 기존 BTC DB 및 position state 유지

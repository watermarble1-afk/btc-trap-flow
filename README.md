# BTC Trap Flow Collector

24/7 OKX BTC-USDT-SWAP public-data collector and TRAP signal recorder.

Railway:
- Start command is provided by `Procfile`.
- For persistent SQLite storage, attach a Railway Volume mounted at `/data`.
- Health endpoint: `/`
- Live data: `/api/live`
- Signals: `/api/signals`
- Stats: `/api/stats`

No OKX API key is required because this uses public market data only.

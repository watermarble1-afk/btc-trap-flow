# BTC Trap Flow + MY MARKET TERMINAL v2

Railway deployment bundle.

Upload/replace these in the existing GitHub repository:
- server.py
- requirements.txt
- Procfile
- static/index.html
- static/btc.html

Important:
- Keep the existing Railway volume mounted at /data.
- Existing API endpoints remain under /api/*.
- The old root health page moves to /api/status.
- The public root / now serves MY MARKET TERMINAL.
- No OKX private API key or trading permission is used.

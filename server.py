import asyncio, json, os, sqlite3, time, re
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
import httpx, websockets
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response

INST="BTC-USDT-SWAP"
PUB="wss://ws.okx.com:8443/ws/v5/public"
BIZ="wss://ws.okx.com:8443/ws/v5/business"
DB=os.getenv("DB_PATH","/data/trapflow.db")
if not os.path.isdir(os.path.dirname(DB)):
    DB="trapflow.db"

app=FastAPI(title="BTC MA Cycle Radar v7.0 KST")
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_methods=["*"],allow_headers=["*"])

@app.middleware("http")
async def no_stale_terminal_html(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path.lower()
    if path in ("/", "/mobile") or path.endswith((".html", ".js", ".webmanifest")):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

trades=deque(maxlen=64)
# v6.63: persistent 1-minute aggressor-flow buckets. These power 1M/3M/5M/15M pressure.
flow_minutes=deque(maxlen=16)
oi_hist=deque(maxlen=16)
candles={"1M":deque(maxlen=80),"3M":deque(maxlen=80),"5M":deque(maxlen=80),"15M":deque(maxlen=720),"1H":deque(maxlen=80),"4H":deque(maxlen=80),"1D":deque(maxlen=40)}
book_imb=0.0
current_oi=None
last_price=None
trap_arm=None
last_signal_ts=0
prev_d10=0.0
scalp_arm=None
scalp_last_signal_ts=0
macro_arm=None
macro_prev_d10=0.0
macro_last_signal_ts=0
vwap_last_ts={"SCALP":0,"5M":0,"15M":0,"1H":0,"4H":0}
tf_arms={e:None for e in ("5M","15M","1H","4H")}
tf_prev_d10={e:0.0 for e in ("5M","15M","1H","4H")}
tf_last_signal_ts={e:0 for e in ("5M","15M","1H","4H")}
# v6.45 staged signal pipeline: 15M EARLY -> 5M pressure -> CONF.
# SCALP generation is disabled; 1H/4H engines remain unchanged.
stage_15m_context={"LONG":None,"SHORT":None}
stage_last_bucket={"EARLY_LONG":-1,"EARLY_SHORT":-1,"5M_LONG":-1,"5M_SHORT":-1,"CONF_LONG":-1,"CONF_SHORT":-1,
                   "ENTRY_LONG":-1,"ENTRY_SHORT":-1}
# v6.61: three-setup lead state. PREP/ARMED/TRIGGER transitions are persisted for research; only TRIGGER is a live signal.
setup_arms={f"{setup}_{side}":None for setup in ("PULLBACK","REVERSAL","RETEST") for side in ("LONG","SHORT")}
setup_watch={"updated_ts":0,"best":None,"items":[]}
# v6.62: execution decision layer. Raw PB/RV/RT remain untouched and keep recording.
# This state only resolves conflicts/duplication for the human-facing execution view.
decision_state={
    "updated_ts":0,"bias":"NEUTRAL","action":"WAIT","candidate":"NEUTRAL",
    "long_evidence":0.0,"short_evidence":0.0,"agreement":[],"reasons":[],
    "pending_side":None,"pending_since":0,"bias_since":0,"trigger_setup":None,
    "conflict":False,"note":"decision evidence is heuristic, not probability"
}
decision_last_trigger_bucket={"LONG":-1,"SHORT":-1}
decision_last_event_sig=None
# v6.65: independent precursor research engine. It never feeds or vetoes EXEC.
precursor_state={
    "updated_ts":0,"state":"SCANNING","side":"NONE","score":0.0,
    "long_score":0.0,"short_score":0.0,"groups":[],"reasons":[],
    "active_since":0,"last_event_ts":0,
    "note":"early anomaly warning only; not an entry signal"
}
precursor_runtime={"active_side":None,"active_since":0,"clear_since":0,
                   "last_emit":{"LONG":0,"SHORT":0}}
# v6.67: independent stateful EVENT ENTRY engine. It does not feed/veto EXEC or EARLY.
event_entry_state={
    "updated_ts":0,"state":"SCANNING","side":"NONE","score":0.0,"phase":"NONE",
    "level":None,"started_ts":0,"reasons":[],"last_event_ts":0,
    "note":"stateful exhaustion -> sweep/failure -> reclaim entry research; independent from EXEC"
}
event_entry_runtime={
    "LONG":None,"SHORT":None,"last_emit":{"LONG":0,"SHORT":0},
    "last_stage":{"LONG":None,"SHORT":None}
}
# v6.69: VWAP14 + MA KNOT is the ONLY live signal engine. All legacy signal engines are runtime-disabled.
vwap_ma_state={
    "updated_ts":0,"state":"SCANNING","side":"NONE","stage":"NONE","score":0.0,
    "vwap14":None,"price":None,"distance_atr":None,"knot_atr":None,"invalidation":None,
    "slopes":{},"reasons":[],"active_since":0,
    "note":"OKX-style rolling VWAP14 + MA5/8/10/20 compression; research only"
}
vwap_ma_runtime={
    "side":"NONE","stage":"NONE","started_ts":0,"last_change_ts":0,
    "last_emit":{},"last_release":{"LONG":0,"SHORT":0}
}
setup_flow_prev={"ts":0,"d10":0.0,"d30":0.0}
setup_stage_last={}
shadow_last_bucket={}
LEGACY_SHADOW_ENABLED=False  # v6.65: PRICE/FAST shadow variants frozen; no new shadow signals.
retest_consumed_break={"LONG":0,"SHORT":0}
retest_consumed_loaded=False
scalp_prev_d10=0.0
status={"public":"starting","business":"starting","started":int(time.time()*1000)}
SIGNAL_MODE="MA_CYCLE_V7"
LEGACY_ENGINES_ENABLED=False
# v6.27: throttle CPU/SQLite-heavy research work so high-rate trade WS cannot starve HTTP.
last_heavy_ms={"excursion":0,"manager":0,"evaluate":0,"outcomes":0}

def db():
    c=sqlite3.connect(DB, timeout=5.0)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=5000")
    c.execute("""CREATE TABLE IF NOT EXISTS signals(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, name TEXT, side TEXT,
      entry REAL, sl REAL, tp1 REAL, tp2 REAL, score REAL,
      d10 REAL,d30 REAL,oi60 REAL,flow REAL,book REAL,status TEXT DEFAULT 'OPEN',
      closed_ts INTEGER)""")
    c.execute("""CREATE TABLE IF NOT EXISTS snapshots(
      ts INTEGER PRIMARY KEY, price REAL,d10 REAL,d30 REAL,oi60 REAL,flow REAL,book REAL,oi REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS flow_minutes(
      minute_ts INTEGER PRIMARY KEY, buy REAL DEFAULT 0, sell REAL DEFAULT 0,
      open REAL, high REAL, low REAL, close REAL, trades INTEGER DEFAULT 0, updated_ts INTEGER
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_flow_minutes_ts ON flow_minutes(minute_ts)")
    sigcols={r[1] for r in c.execute("PRAGMA table_info(signals)").fetchall()}
    if "engine" not in sigcols:
        c.execute("ALTER TABLE signals ADD COLUMN engine TEXT DEFAULT 'CORE'")
        c.execute("UPDATE signals SET engine='CORE' WHERE engine IS NULL OR engine=''")
    if "reason" not in sigcols:
        c.execute("ALTER TABLE signals ADD COLUMN reason TEXT DEFAULT ''")
    c.execute("""CREATE TABLE IF NOT EXISTS position_events(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, event TEXT, side TEXT,
      price REAL, signal_id INTEGER, note TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS position_state(
      id INTEGER PRIMARY KEY CHECK(id=1), side TEXT, entry REAL, opened_ts INTEGER,
      signal_id INTEGER, updated_ts INTEGER)""")
    c.execute("""CREATE TABLE IF NOT EXISTS engine_positions(
      engine TEXT PRIMARY KEY, side TEXT, entry REAL, opened_ts INTEGER,
      signal_id INTEGER, updated_ts INTEGER, best_price REAL, atr_open REAL)""")
    epcols={r[1] for r in c.execute("PRAGMA table_info(engine_positions)").fetchall()}
    if "worst_price" not in epcols:c.execute("ALTER TABLE engine_positions ADD COLUMN worst_price REAL")
    if "mfe" not in epcols:c.execute("ALTER TABLE engine_positions ADD COLUMN mfe REAL DEFAULT 0")
    if "mae" not in epcols:c.execute("ALTER TABLE engine_positions ADD COLUMN mae REAL DEFAULT 0")
    if "state" not in epcols:c.execute("ALTER TABLE engine_positions ADD COLUMN state TEXT DEFAULT 'HOLD'")
    if "pressure_since" not in epcols:c.execute("ALTER TABLE engine_positions ADD COLUMN pressure_since INTEGER")
    if "pressure_score" not in epcols:c.execute("ALTER TABLE engine_positions ADD COLUMN pressure_score REAL DEFAULT 0")
    evcols={r[1] for r in c.execute("PRAGMA table_info(position_events)").fetchall()}
    if "engine" not in evcols:c.execute("ALTER TABLE position_events ADD COLUMN engine TEXT DEFAULT 'CORE'")
    c.execute("UPDATE position_events SET engine='CORE' WHERE engine IS NULL OR engine=''")
    # v6.26: split legacy paired engines into independent timeframe engines.
    c.execute("UPDATE signals SET engine='5M' WHERE engine='CORE'")
    c.execute("UPDATE signals SET engine='1H' WHERE engine='MACRO'")
    c.execute("UPDATE position_events SET engine='5M' WHERE engine='CORE'")
    c.execute("UPDATE position_events SET engine='1H' WHERE engine='MACRO'")
    c.execute("""CREATE TABLE IF NOT EXISTS position_reentry_guard(
      engine TEXT PRIMARY KEY, side TEXT, exit_ts INTEGER, exit_price REAL, atr REAL, reason TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS user_positions(
      id INTEGER PRIMARY KEY CHECK(id=1), side TEXT, entry REAL, opened_ts INTEGER, updated_ts INTEGER
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS user_position_events(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, event TEXT, side TEXT, price REAL, note TEXT
    )""")
    c.execute("UPDATE engine_positions SET engine='5M' WHERE engine='CORE' AND NOT EXISTS (SELECT 1 FROM engine_positions x WHERE x.engine='5M')")
    c.execute("DELETE FROM engine_positions WHERE engine='CORE'")
    c.execute("UPDATE engine_positions SET engine='1H' WHERE engine='MACRO' AND NOT EXISTS (SELECT 1 FROM engine_positions x WHERE x.engine='1H')")
    c.execute("DELETE FROM engine_positions WHERE engine='MACRO'")
    # Lightweight schema migration for position-state tracking.
    cols={r[1] for r in c.execute("PRAGMA table_info(position_state)").fetchall()}
    if "best_price" not in cols:c.execute("ALTER TABLE position_state ADD COLUMN best_price REAL")
    if "atr_open" not in cols:c.execute("ALTER TABLE position_state ADD COLUMN atr_open REAL")
    # v6.41: forward signal research. Raw signals stay untouched; this table records
    # post-signal excursion and fixed-horizon prices for later 5M/15M validation.
    c.execute("""CREATE TABLE IF NOT EXISTS signal_research(
      signal_id INTEGER PRIMARY KEY, started_ts INTEGER, last_ts INTEGER,
      mfe_pct REAL DEFAULT 0, mae_pct REAL DEFAULT 0,
      p5 REAL, p15 REAL, p30 REAL, p60 REAL,
      FOREIGN KEY(signal_id) REFERENCES signals(id)
    )""")
    rcols={r[1] for r in c.execute("PRAGMA table_info(signal_research)").fetchall()}
    for col in ("b5","b10","b20","b30"):
        if col not in rcols:c.execute(f"ALTER TABLE signal_research ADD COLUMN {col} REAL")
    # v6.61 research foundation: persist setup lifecycle and shadow comparisons without
    # changing the live signal thresholds. This lets us prove whether PREP/ARMED and
    # flow confirmation actually add edge instead of guessing from screenshots.
    c.execute("""CREATE TABLE IF NOT EXISTS setup_stage_events(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, setup TEXT, side TEXT, stage TEXT,
      score REAL, price REAL, level REAL, ref TEXT, reason TEXT, context_json TEXT
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_setup_stage_ts ON setup_stage_events(ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_setup_stage_key ON setup_stage_events(setup,side,stage,ts)")
    c.execute("""CREATE TABLE IF NOT EXISTS shadow_signals(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, bucket INTEGER, setup TEXT, variant TEXT, side TEXT,
      entry REAL, score REAL, reason TEXT, context_json TEXT,
      UNIQUE(bucket,setup,variant,side)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS shadow_research(
      shadow_id INTEGER PRIMARY KEY, started_ts INTEGER, last_ts INTEGER,
      mfe_pct REAL DEFAULT 0, mae_pct REAL DEFAULT 0, p5 REAL, p15 REAL, p30 REAL, p60 REAL,
      FOREIGN KEY(shadow_id) REFERENCES shadow_signals(id)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS user_trade_research(
      open_event_id INTEGER PRIMARY KEY, started_ts INTEGER, last_ts INTEGER, side TEXT, entry REAL,
      mfe_pct REAL DEFAULT 0, mae_pct REAL DEFAULT 0, p5 REAL, p15 REAL, p30 REAL, p60 REAL,
      FOREIGN KEY(open_event_id) REFERENCES user_position_events(id)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS setup_runtime_state(
      key TEXT PRIMARY KEY, value_int INTEGER, updated_ts INTEGER
    )""")
    # v6.62: append-only execution-decision history. This does not replace raw signal history.
    c.execute("""CREATE TABLE IF NOT EXISTS decision_events(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, event TEXT, side TEXT, action TEXT,
      evidence REAL, opposite_evidence REAL, setups TEXT, reason TEXT, price REAL
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_decision_events_ts ON decision_events(ts)")
    # v6.65: independent leading-anomaly research. Kept separate from EXEC and legacy signals.
    c.execute("""CREATE TABLE IF NOT EXISTS precursor_events(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, bucket INTEGER, side TEXT, score REAL,
      price REAL, groups TEXT, reason TEXT, context_json TEXT,
      UNIQUE(bucket,side)
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_precursor_events_ts ON precursor_events(ts)")
    c.execute("""CREATE TABLE IF NOT EXISTS precursor_research(
      precursor_id INTEGER PRIMARY KEY, started_ts INTEGER, last_ts INTEGER,
      mfe_pct REAL DEFAULT 0, mae_pct REAL DEFAULT 0, p5 REAL, p15 REAL, p30 REAL, p60 REAL,
      next_exec_ts INTEGER, lead_sec REAL,
      FOREIGN KEY(precursor_id) REFERENCES precursor_events(id)
    )""")
    prcols={r[1] for r in c.execute("PRAGMA table_info(precursor_research)").fetchall()}
    if "next_exec_ts" not in prcols:c.execute("ALTER TABLE precursor_research ADD COLUMN next_exec_ts INTEGER")
    if "lead_sec" not in prcols:c.execute("ALTER TABLE precursor_research ADD COLUMN lead_sec REAL")
    # v6.67: EVENT ENTRY is a separate, stateful research signal. No EXEC coupling.
    c.execute("""CREATE TABLE IF NOT EXISTS event_entry_stage_events(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, side TEXT, stage TEXT, score REAL,
      price REAL, level REAL, reason TEXT, context_json TEXT
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_event_entry_stage_ts ON event_entry_stage_events(ts)")
    c.execute("""CREATE TABLE IF NOT EXISTS event_entry_events(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, bucket INTEGER, side TEXT, score REAL,
      price REAL, level REAL, reason TEXT, context_json TEXT, UNIQUE(bucket,side)
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_event_entry_events_ts ON event_entry_events(ts)")
    c.execute("""CREATE TABLE IF NOT EXISTS event_entry_research(
      event_id INTEGER PRIMARY KEY, started_ts INTEGER, last_ts INTEGER,
      mfe_pct REAL DEFAULT 0, mae_pct REAL DEFAULT 0, p5 REAL, p15 REAL, p30 REAL, p60 REAL,
      next_exec_ts INTEGER, lead_sec REAL, FOREIGN KEY(event_id) REFERENCES event_entry_events(id)
    )""")
    # v6.68: VWAP14 + MA KNOT direction lifecycle and RELEASE outcome research.
    c.execute("""CREATE TABLE IF NOT EXISTS vwap_ma_events(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, bucket INTEGER, side TEXT, stage TEXT, score REAL,
      price REAL, vwap14 REAL, knot_atr REAL, invalidation REAL, reason TEXT, context_json TEXT,
      UNIQUE(bucket,side,stage)
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_vwap_ma_events_ts ON vwap_ma_events(ts)")
    c.execute("""CREATE TABLE IF NOT EXISTS vwap_ma_research(
      event_id INTEGER PRIMARY KEY, started_ts INTEGER, last_ts INTEGER,
      mfe_pct REAL DEFAULT 0, mae_pct REAL DEFAULT 0, p5 REAL, p15 REAL, p30 REAL, p60 REAL,
      FOREIGN KEY(event_id) REFERENCES vwap_ma_events(id)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS decision_research(
      decision_id INTEGER PRIMARY KEY, started_ts INTEGER, last_ts INTEGER,
      mfe_pct REAL DEFAULT 0, mae_pct REAL DEFAULT 0, p5 REAL, p15 REAL, p30 REAL, p60 REAL,
      FOREIGN KEY(decision_id) REFERENCES decision_events(id)
    )""")
    c.commit(); return c

def median(xs):
    x=sorted(xs)
    if not x:return 0
    n=len(x);return x[n//2] if n%2 else (x[n//2-1]+x[n//2])/2

def flow(ms):
    now=int(time.time()*1000); b=s=0.0
    for t in reversed(trades):
        if now-t["ts"]>ms: break
        if t["side"]=="buy": b+=t["notional"]
        else:s+=t["notional"]
    total=b+s
    return {"buy":b,"sell":s,"total":total,"delta":b-s,"ratio":(b-s)/total if total else 0}


def add_minute_flow(px, ts, side, notional):
    """Accumulate taker-aggressor flow in 1-minute buckets without SQLite writes per trade."""
    m=int(ts//60000*60000); px=float(px); n=float(notional or 0)
    target=None
    if flow_minutes and int(flow_minutes[-1]['ts'])==m:
        target=flow_minutes[-1]
    elif not flow_minutes or m>int(flow_minutes[-1]['ts']):
        target={'ts':m,'buy':0.0,'sell':0.0,'open':px,'high':px,'low':px,'close':px,'trades':0}
        flow_minutes.append(target)
    else:
        # Rare out-of-order trade: update an existing recent minute if present.
        for z in reversed(flow_minutes):
            if int(z['ts'])==m:
                target=z; break
            if int(z['ts'])<m: break
        if target is None:return
    if str(side).lower()=='buy':target['buy']+=n
    else:target['sell']+=n
    target['high']=max(float(target.get('high') or px),px); target['low']=min(float(target.get('low') or px),px)
    target['close']=px; target['trades']=int(target.get('trades') or 0)+1


def load_minute_flow():
    """Restore recent minute pressure after Railway restart."""
    flow_minutes.clear(); c=db()
    rows=c.execute("SELECT minute_ts,buy,sell,open,high,low,close,trades FROM flow_minutes ORDER BY minute_ts DESC LIMIT 240").fetchall(); c.close()
    for r in reversed(rows):
        flow_minutes.append({'ts':int(r[0]),'buy':float(r[1] or 0),'sell':float(r[2] or 0),'open':float(r[3] or 0),'high':float(r[4] or 0),'low':float(r[5] or 0),'close':float(r[6] or 0),'trades':int(r[7] or 0)})


def persist_minute_flow():
    if not flow_minutes:return
    c=db(); now=int(time.time()*1000)
    for z in list(flow_minutes)[-3:]:
        c.execute("""INSERT INTO flow_minutes(minute_ts,buy,sell,open,high,low,close,trades,updated_ts)
                     VALUES(?,?,?,?,?,?,?,?,?)
                     ON CONFLICT(minute_ts) DO UPDATE SET buy=excluded.buy,sell=excluded.sell,open=excluded.open,
                     high=excluded.high,low=excluded.low,close=excluded.close,trades=excluded.trades,updated_ts=excluded.updated_ts""",
                  (int(z['ts']),float(z.get('buy') or 0),float(z.get('sell') or 0),float(z.get('open') or 0),float(z.get('high') or 0),float(z.get('low') or 0),float(z.get('close') or 0),int(z.get('trades') or 0),now))
    # Keep a week. Small table, but bounded persistence avoids endless growth.
    c.execute("DELETE FROM flow_minutes WHERE minute_ts<?",(now-7*24*60*60*1000,)); c.commit(); c.close()


def _flow_window(minutes, now=None):
    """Approximate exact rolling N-minute aggressor pressure from persistent 1M buckets.

    The oldest boundary minute is overlap-weighted; the live current minute is already partial,
    so its observed trades are used as-is. This avoids a "1M" window accidentally containing
    almost two full minute buckets near a minute boundary.
    """
    now=int(now or time.time()*1000); minutes=max(1,int(minutes)); window_ms=minutes*60000; cutoff=now-window_ms
    rows=[]; current_start=now//60000*60000
    for z in flow_minutes:
        st=int(z['ts']); en=min(st+60000,now)
        overlap=max(0,min(en,now)-max(st,cutoff))
        if overlap<=0:continue
        # Current bucket only contains trades observed up to now; do not down-weight it again.
        weight=1.0 if st==current_start else min(1.0,overlap/60000.0)
        rows.append((z,weight))
    buy=sum(float(z.get('buy') or 0)*w for z,w in rows); sell=sum(float(z.get('sell') or 0)*w for z,w in rows); total=buy+sell
    ratio=(buy-sell)/total if total else 0.0
    if flow_minutes:
        earliest=int(flow_minutes[0]['ts'])
        covered=max(0,now-max(cutoff,earliest)); coverage=min(1.0,covered/float(window_ms))
    else:coverage=0.0
    zs=[z for z,_ in rows]
    op=float(zs[0].get('open') or 0) if zs else 0.0; cl=float(zs[-1].get('close') or 0) if zs else 0.0
    price_pct=((cl/op)-1)*100 if op and cl else 0.0
    full=[float(z.get('buy') or 0)+float(z.get('sell') or 0) for z in list(flow_minutes)[-61:-1] if float(z.get('buy') or 0)+float(z.get('sell') or 0)>0]
    base=median(full) or 1.0; covered_minutes=max(.10,minutes*coverage)
    intensity=(total/covered_minutes)/base if total else 0.0
    long_abs=max(0.0,-ratio) if price_pct>=0 else 0.0
    short_abs=max(0.0,ratio) if price_pct<=0 else 0.0
    long_share=50.0*(1.0+ratio); short_share=100.0-long_share
    state='BUY' if ratio>=.08 else 'SELL' if ratio<=-.08 else 'BALANCE'
    return {'minutes':minutes,'buy':buy,'sell':sell,'total':total,'delta':buy-sell,'ratio':ratio,
            'long':long_share,'short':short_share,'price_pct':price_pct,'intensity':intensity,
            'coverage':coverage,'trades':sum(int(z.get('trades') or 0) for z in zs),
            'absorb_long':long_abs,'absorb_short':short_abs,'state':state}


def mtf_pressure_snapshot(now=None):
    """1M/3M/5M/15M pressure stack used by setups + Decision Layer.

    This is evidence strength, not a probability. 10s/30s are intentionally excluded here.
    """
    now=int(now or time.time()*1000)
    tfs={f'{m}M':_flow_window(m,now) for m in (1,3,5,15)}
    weights={'1M':.34,'3M':.30,'5M':.22,'15M':.14}
    den=sum(weights[k]*max(.15,float(v.get('coverage') or 0)) for k,v in tfs.items()) or 1.0
    weighted=sum(weights[k]*max(.15,float(v.get('coverage') or 0))*float(v.get('ratio') or 0) for k,v in tfs.items())/den
    fast=.58*float(tfs['1M']['ratio'])+.42*float(tfs['3M']['ratio'])
    slow=.60*float(tfs['5M']['ratio'])+.40*float(tfs['15M']['ratio'])
    turn=fast-slow
    state='LONG' if weighted>=.06 else 'SHORT' if weighted<=-.06 else 'BALANCE'
    turn_side='LONG' if turn>=.07 and fast>=-.02 else 'SHORT' if turn<=-.07 and fast<=.02 else 'NONE'
    ready=float(tfs['3M']['coverage'])>=.67
    return {'updated_ts':now,'timeframes':tfs,'weighted_delta':weighted,'fast_delta':fast,'slow_delta':slow,
            'turn_delta':turn,'long':50*(1+weighted),'short':50*(1-weighted),'state':state,
            'turn_side':turn_side,'ready':ready,'note':'1M/3M/5M/15M aggressor pressure; evidence, not probability'}


def _precursor_minute_metrics():
    """Compact per-minute price-impact features from persistent aggressor-flow buckets."""
    out=[]
    for z in list(flow_minutes)[-6:]:
        buy=float(z.get('buy') or 0); sell=float(z.get('sell') or 0); total=buy+sell
        op=float(z.get('open') or 0); cl=float(z.get('close') or 0)
        ratio=(buy-sell)/total if total else 0.0
        pp=((cl/op)-1)*100 if op and cl else 0.0
        out.append({'ts':int(z.get('ts') or 0),'ratio':ratio,'price_pct':pp,'total':total,
                    'buy':buy,'sell':sell})
    return out


def _precursor_side_features(side, now, snap):
    """Return a deliberately small set of *leading-anomaly* sensors.

    The aim is not to predict price from a single metric. We only warn when order-flow
    effort stops producing matching price progress near a meaningful location.
    """
    p=float(last_price or 0); A5=max(atr_tf('5M',24),1.0); A15=max(atr_tf('15M',24),1.0); A1=max(atr_tf('1M',30),1.0)
    t=snap.get('timeframes') or {}; f1=t.get('1M') or {}; f3=t.get('3M') or {}; f5=t.get('5M') or {}; f15=t.get('15M') or {}
    d1=float(f1.get('ratio') or 0); d3=float(f3.get('ratio') or 0); fast=float(snap.get('fast_delta') or 0); slow=float(snap.get('slow_delta') or 0)
    score=0.0; groups=[]; reasons=[]; comp={}
    isL=side=='LONG'; sign=1 if isL else -1

    # 1) LOCATION: anomaly matters more near prior liquidity / local extremes.
    H15,L15=liquidity_tf('15M',100,30)
    confirmed5=[x for x in candles.get('5M',[]) if x.get('confirm')=='1']
    prev5=confirmed5[-24:-1] if len(confirmed5)>=4 else []
    local_low=min((float(x['low']) for x in prev5),default=None); local_high=max((float(x['high']) for x in prev5),default=None)
    level15=float(L15 if isL else H15) if (L15 is not None and H15 is not None) else None
    dist15=((p-level15)/A15 if isL else (level15-p)/A15) if level15 is not None else 99.0
    local_level=local_low if isL else local_high
    dist5=((p-local_level)/A5 if isL else (local_level-p)/A5) if local_level is not None else 99.0
    near15=(-.25<=dist15<=.65); near5=(-.22<=dist5<=.55)
    if near15 or near5:
        pts=10+(6 if near15 else 0)+(4 if near5 else 0); score+=pts; groups.append('LOCATION');
        reasons.append(('15M_LIQ' if near15 else 'LOCAL_EXTREME')+f' {min(abs(dist15),abs(dist5)):.2f}ATR'); comp['location']=pts
    else: comp['location']=0

    # 2) IMPACT DECAY: similar aggression is moving price less than prior minutes.
    mm=_precursor_minute_metrics(); cur=mm[-1] if mm else {'ratio':0,'price_pct':0,'total':0}
    prev=mm[-4:-1] if len(mm)>=4 else mm[:-1]
    def agg(m): return max(0.0,-float(m['ratio'])) if isL else max(0.0,float(m['ratio']))
    def adverse(m): return max(0.0,-float(m['price_pct'])) if isL else max(0.0,float(m['price_pct']))
    prior_eff=[adverse(m)/max(.04,agg(m)) for m in prev if agg(m)>=.04 and m.get('total',0)>0]
    cur_agg=agg(cur); cur_eff=adverse(cur)/max(.04,cur_agg)
    pe=median(prior_eff) if prior_eff else 0.0
    impact=(cur_agg>=.04 and ((pe>=.025 and cur_eff<=pe*.62) or (cur_agg>=.08 and ((float(cur['price_pct'])>=-.012) if isL else (float(cur['price_pct'])<=.012)))))
    if impact:
        score+=25;groups.append('IMPACT_DECAY');reasons.append(f'IMPACT_DECAY {cur_eff:.3f}<{pe:.3f}');comp['impact_decay']=25
    else: comp['impact_decay']=0

    # 3) ABSORPTION: aggressive flow persists but price refuses to progress with it.
    c1=list(candles.get('1M',[]))[-1] if candles.get('1M') else None; sh1=_bar_shape(c1) if c1 else None
    if isL:
        flow_abs=(d1<=-.06 and float(f1.get('price_pct') or 0)>=-.022) or (d3<=-.08 and float(f3.get('price_pct') or 0)>=-.05)
        wick_abs=bool(sh1 and d1<=-.03 and sh1['lower']>=max(sh1['body'],.20*sh1['rng']) and sh1['close_pos']>=.45)
    else:
        flow_abs=(d1>=.06 and float(f1.get('price_pct') or 0)<=.022) or (d3>=.08 and float(f3.get('price_pct') or 0)<=.05)
        wick_abs=bool(sh1 and d1>=.03 and sh1['upper']>=max(sh1['body'],.20*sh1['rng']) and sh1['close_pos']<=.55)
    absorption=flow_abs or wick_abs
    if absorption:
        score+=25;groups.append('ABSORPTION');reasons.append('AGGRESSION_WITHOUT_PROGRESS'+(' + WICK' if wick_abs else ''));comp['absorption']=25
    else: comp['absorption']=0

    # 4) PRESSURE DECELERATION: dominant side has not flipped yet, but fast pressure is fading first.
    if isL:
        decel=(d3<=-.025 and d1-d3>=.035) or (slow<=-.02 and fast-slow>=.05)
    else:
        decel=(d3>=.025 and d1-d3<=-.035) or (slow>=.02 and fast-slow<=-.05)
    if decel:
        score+=20;groups.append('PRESSURE_DECAY');reasons.append(f'FAST-SLOW {(fast-slow)*100:+.1f}%');comp['pressure_decay']=20
    else: comp['pressure_decay']=0

    # 5) LIQUIDITY SWEEP / RECLAIM: optional but strong. This is still earlier than EXEC confirmation.
    confirmed1=[x for x in candles.get('1M',[]) if x.get('confirm')=='1']
    prior1=confirmed1[-13:-1] if len(confirmed1)>=4 else []
    sweep=False; sweep_level=None
    if c1 and prior1:
        if isL:
            sweep_level=min(float(x['low']) for x in prior1)
            sweep=float(c1['low'])<sweep_level-.010*A1 and p>sweep_level+.004*A1
        else:
            sweep_level=max(float(x['high']) for x in prior1)
            sweep=float(c1['high'])>sweep_level+.010*A1 and p<sweep_level-.004*A1
    if sweep:
        score+=15;groups.append('SWEEP');reasons.append('1M_SWEEP_RECLAIM');comp['sweep']=15
    else: comp['sweep']=0

    score=max(0.0,min(100.0,score))
    return {'side':side,'score':round(score,1),'groups':groups,'reasons':reasons,'components':comp,
            'dist15_atr':round(dist15,3),'dist5_atr':round(dist5,3),'d1':round(d1,4),'d3':round(d3,4),
            'fast':round(fast,4),'slow':round(slow,4),'price':p,'coverage':round(float(f3.get('coverage') or 0),3)}


def _save_precursor_event(side, feat, now):
    bucket=int(now//300000); c=db(); ctx=json.dumps(feat,separators=(',',':'),ensure_ascii=False)
    cur=c.execute("""INSERT OR IGNORE INTO precursor_events(ts,bucket,side,score,price,groups,reason,context_json)
      VALUES(?,?,?,?,?,?,?,?)""",(now,bucket,side,float(feat['score']),float(last_price or 0),','.join(feat['groups']),' | '.join(feat['reasons']),ctx))
    inserted=cur.rowcount>0
    if inserted:
        pid=cur.lastrowid
        c.execute("""INSERT OR IGNORE INTO precursor_research(precursor_id,started_ts,last_ts,mfe_pct,mae_pct)
          VALUES(?,?,?,?,?)""",(pid,now,now,0.0,0.0))
    c.commit();c.close();return inserted


def evaluate_precursor():
    """Independent early-warning engine. It does not influence EXEC in v6.65."""
    global precursor_state, precursor_runtime
    if not last_price or len(flow_minutes)<2:return precursor_state
    now=int(time.time()*1000); snap=mtf_pressure_snapshot(now)
    if float((snap.get('timeframes') or {}).get('3M',{}).get('coverage') or 0)<.45:
        precursor_state={**precursor_state,'updated_ts':now,'state':'WARMUP','side':'NONE','score':0.0,'long_score':0.0,'short_score':0.0,'groups':[],'reasons':['3M FLOW WARMUP']}
        return precursor_state
    L=_precursor_side_features('LONG',now,snap); S=_precursor_side_features('SHORT',now,snap)
    best=L if L['score']>=S['score'] else S; other=S if best is L else L
    margin=float(best['score'])-float(other['score']); qualifies=(best['score']>=68 and len(best['groups'])>=3 and ('LOCATION' in best['groups'] or 'SWEEP' in best['groups']) and margin>=8)
    watch=(best['score']>=48 and len(best['groups'])>=2)
    state='EARLY' if qualifies else 'WATCH' if watch else 'SCANNING'; side=best['side'] if state!='SCANNING' else 'NONE'

    active=precursor_runtime.get('active_side'); active_since=int(precursor_runtime.get('active_since') or 0)
    # An episode remains one event. It must genuinely cool off before another same-side warning.
    if active and (now-active_since>25*60*1000):
        precursor_runtime['active_side']=None; precursor_runtime['active_since']=0; precursor_runtime['clear_since']=0; active=None
    if active:
        active_feat=L if active=='LONG' else S
        if float(active_feat['score'])<42:
            if not precursor_runtime.get('clear_since'): precursor_runtime['clear_since']=now
            elif now-int(precursor_runtime['clear_since'])>=75*1000:
                precursor_runtime['active_side']=None; precursor_runtime['active_since']=0; precursor_runtime['clear_since']=0; active=None
        else: precursor_runtime['clear_since']=0
    emitted=False
    if qualifies and (not active or active==best['side']):
        if not active:
            last=int((precursor_runtime.get('last_emit') or {}).get(best['side']) or 0)
            if now-last>=5*60*1000:
                emitted=_save_precursor_event(best['side'],best,now)
                if emitted:
                    precursor_runtime['active_side']=best['side']; precursor_runtime['active_since']=now; precursor_runtime['clear_since']=0
                    precursor_runtime['last_emit'][best['side']]=now; active=best['side']; active_since=now
    elif qualifies and active and active!=best['side']:
        state='WATCH'; side=best['side']; best={**best,'reasons':best['reasons']+['OPPOSITE_EPISODE_STILL_ACTIVE']}

    last_evt=int(precursor_state.get('last_event_ts') or 0)
    if emitted:last_evt=now
    precursor_state={'updated_ts':now,'state':state,'side':side,'score':round(float(best['score']),1),
      'long_score':round(float(L['score']),1),'short_score':round(float(S['score']),1),'groups':list(best['groups']),
      'reasons':list(best['reasons']),'active_side':precursor_runtime.get('active_side'),'active_since':int(precursor_runtime.get('active_since') or 0),
      'last_event_ts':last_evt,'margin':round(margin,1),'components':best.get('components') or {},
      'note':'EARLY = effort/result anomaly warning. Independent from EXEC; not an entry signal.'}
    return precursor_state



def _event_stage_log(side, stage, score, level, reasons, context, now):
    """Persist only state transitions so research can reconstruct the sequence without DB spam."""
    global event_entry_runtime
    sig=f"{stage}|{round(float(level or 0),2)}"
    if (event_entry_runtime.get('last_stage') or {}).get(side)==sig:
        return
    event_entry_runtime['last_stage'][side]=sig
    c=db(); c.execute("""INSERT INTO event_entry_stage_events(ts,side,stage,score,price,level,reason,context_json)
      VALUES(?,?,?,?,?,?,?,?)""",(int(now),side,stage,float(score or 0),float(last_price or 0),float(level or 0),
      ' | '.join(reasons or []),json.dumps(context or {},separators=(',',':'),ensure_ascii=False)))
    c.commit(); c.close()


def _event_reference_level(side):
    """Nearest short-horizon liquidity edge used by the event sequence."""
    a=[x for x in candles.get('1M',[]) if x.get('confirm')=='1']
    prior=a[-13:-1] if len(a)>=4 else a[:-1]
    if not prior:return None
    return min(float(x['low']) for x in prior) if side=='LONG' else max(float(x['high']) for x in prior)


def _event_micro_structure(side, A1):
    """First local structure response. Intentionally much faster than EXEC's MTF confirmation."""
    a=[x for x in candles.get('1M',[]) if x.get('confirm')=='1']
    if len(a)<3 or not last_price:return False, None
    prev=a[-2:]
    if side=='LONG':
        level=max(float(x['high']) for x in prev)
        return float(last_price)>=level+.006*A1, level
    level=min(float(x['low']) for x in prev)
    return float(last_price)<=level-.006*A1, level


def _save_event_entry(side, score, level, reasons, ctx, now):
    bucket=int(now//300000); c=db()
    cur=c.execute("""INSERT OR IGNORE INTO event_entry_events(ts,bucket,side,score,price,level,reason,context_json)
      VALUES(?,?,?,?,?,?,?,?)""",(int(now),bucket,side,float(score),float(last_price or 0),float(level or 0),
      ' | '.join(reasons or []),json.dumps(ctx or {},separators=(',',':'),ensure_ascii=False)))
    inserted=cur.rowcount>0
    if inserted:
        eid=cur.lastrowid
        c.execute("""INSERT OR IGNORE INTO event_entry_research(event_id,started_ts,last_ts,mfe_pct,mae_pct)
          VALUES(?,?,?,?,?)""",(eid,int(now),int(now),0.0,0.0))
    c.commit(); c.close(); return inserted


def evaluate_event_entry():
    """v6.67 stateful EVENT ENTRY research engine.

    Unlike EARLY (anomaly warning), EVENT requires a *sequence*:
      1) exhaustion/absorption while the old side still has pressure,
      2) liquidity sweep or failed-auction response around a local extreme,
      3) reclaim + first 1M structure response,
      4) micro flow must not strongly oppose the entry.

    It deliberately does NOT wait for full 1M/3M/5M/15M alignment, so it can be earlier
    than EXEC. It never feeds, blocks or changes EXEC/EARLY.
    """
    global event_entry_state, event_entry_runtime
    if not last_price or len(flow_minutes)<2:return event_entry_state
    now=int(time.time()*1000); p=float(last_price); snap=mtf_pressure_snapshot(now); t=snap.get('timeframes') or {}
    if float((t.get('3M') or {}).get('coverage') or 0)<.45:
        event_entry_state={**event_entry_state,'updated_ts':now,'state':'WARMUP','side':'NONE','phase':'NONE','score':0.0,'reasons':['3M FLOW WARMUP']}
        return event_entry_state
    A1=max(atr_tf('1M',30),1.0); A5=max(atr_tf('5M',24),1.0)
    d1=float((t.get('1M') or {}).get('ratio') or 0); d3=float((t.get('3M') or {}).get('ratio') or 0)
    fast=float(snap.get('fast_delta') or 0); slow=float(snap.get('slow_delta') or 0)
    micro10=float(flow(10000).get('ratio') or 0); micro30=float(flow(30000).get('ratio') or 0)
    c1=list(candles.get('1M',[]))[-1] if candles.get('1M') else None
    sh1=_bar_shape(c1) if c1 else None
    current=[]

    for side in ('LONG','SHORT'):
        isL=side=='LONG'; sign=1 if isL else -1
        feat=_precursor_side_features(side,now,snap); comp=feat.get('components') or {}
        anomaly=sum(1 for k in ('impact_decay','absorption','pressure_decay') if float(comp.get(k) or 0)>0)
        level=_event_reference_level(side)
        arm=event_entry_runtime.get(side)
        # Arm while the OLD pressure still exists. This is the important difference from a normal confirmation signal.
        old_pressure=(d3<=.045 and (d1<=.09 or slow<=.035)) if isL else (d3>=-.045 and (d1>=-.09 or slow>=-.035))
        near=level is not None and (((p-level)/A5<=.55 and (p-level)/A5>=-.35) if isL else ((level-p)/A5<=.55 and (level-p)/A5>=-.35))
        arm_ok=float(feat.get('score') or 0)>=48 and anomaly>=2 and old_pressure and (near or 'LOCATION' in (feat.get('groups') or []) or 'SWEEP' in (feat.get('groups') or []))
        if arm is None and arm_ok:
            arm={'side':side,'started_ts':now,'origin':p,'level':float(level or p),'extreme':p,'expires_ts':now+12*60*1000,
                 'base_score':float(feat.get('score') or 0),'base_groups':list(feat.get('groups') or []),'swept':('SWEEP' in (feat.get('groups') or [])),
                 'sweep_ts':now if 'SWEEP' in (feat.get('groups') or []) else 0}
            event_entry_runtime[side]=arm
            _event_stage_log(side,'ARMED',feat.get('score'),arm['level'],['EXHAUSTION_ARM']+list(feat.get('groups') or []),{'feat':feat},now)
        if arm is None:continue
        if now>=int(arm.get('expires_ts') or 0):
            _event_stage_log(side,'EXPIRED',arm.get('base_score'),arm.get('level'),['TIMEOUT'],{'arm':arm},now)
            event_entry_runtime[side]=None; event_entry_runtime['last_stage'][side]=None; continue
        arm['extreme']=min(float(arm.get('extreme') or p),p) if isL else max(float(arm.get('extreme') or p),p)
        lvl=float(arm.get('level') or p)
        # New sweep can happen after the initial exhaustion arm.
        if isL and float(arm['extreme'])<lvl-.012*A1: arm['swept']=True; arm['sweep_ts']=arm.get('sweep_ts') or now
        if (not isL) and float(arm['extreme'])>lvl+.012*A1: arm['swept']=True; arm['sweep_ts']=arm.get('sweep_ts') or now
        reclaim=(p>=lvl+.004*A1) if isL else (p<=lvl-.004*A1)
        # Failed auction can qualify even without a clean textbook sweep: aggression persists, but candle rejects the edge.
        if isL:
            reject=bool(sh1 and d1<=.015 and sh1['lower']>=max(sh1['body']*.8,.16*sh1['rng']) and sh1['close_pos']>=.56)
            pressure_improve=(d1-d3>=.025 or fast-slow>=.035)
            micro_ok=(micro10>=-.10 and micro30>=-.08)
        else:
            reject=bool(sh1 and d1>=-.015 and sh1['upper']>=max(sh1['body']*.8,.16*sh1['rng']) and sh1['close_pos']<=.44)
            pressure_improve=(d1-d3<=-.025 or fast-slow<=-.035)
            micro_ok=(micro10<=.10 and micro30<=.08)
        failed_auction=reject and pressure_improve
        struct_ok, struct_level=_event_micro_structure(side,A1)
        # A sweep needs reclaim; a non-sweep path needs a clear failed-auction rejection.
        response_ok=(bool(arm.get('swept')) and reclaim) or failed_auction
        reasons=['EXHAUSTION']
        if arm.get('swept'): reasons.append('SWEEP_RECLAIM' if reclaim else 'SWEEP_WAIT_RECLAIM')
        if failed_auction: reasons.append('FAILED_AUCTION')
        if struct_ok: reasons.append('1M_STRUCTURE_RESPONSE')
        if pressure_improve: reasons.append('PRESSURE_IMPROVING')
        if micro_ok: reasons.append('MICRO_NOT_OPPOSING')
        score=48 + (12 if float(comp.get('impact_decay') or 0)>0 else 0) + (12 if float(comp.get('absorption') or 0)>0 else 0) + (8 if float(comp.get('pressure_decay') or 0)>0 else 0) + (12 if response_ok else 0) + (12 if struct_ok else 0) + (6 if micro_ok else 0)
        score=max(0.0,min(100.0,score))
        phase='RECLAIM' if response_ok else 'ARMED'
        current.append({'side':side,'state':'ARMED','phase':phase,'score':score,'level':lvl,'started_ts':int(arm['started_ts']),'reasons':reasons,'feat':feat})
        _event_stage_log(side,phase,score,lvl,reasons,{'d1':d1,'d3':d3,'fast':fast,'slow':slow,'micro10':micro10,'micro30':micro30,'struct_level':struct_level,'arm':arm},now)
        # Entry is event sequence completion, not a score threshold alone.
        cooldown=now-int((event_entry_runtime.get('last_emit') or {}).get(side) or 0)
        trigger=response_ok and struct_ok and pressure_improve and micro_ok and score>=78 and cooldown>=5*60*1000
        if trigger:
            ctx={'feat':feat,'arm':arm,'d1':round(d1,5),'d3':round(d3,5),'fast':round(fast,5),'slow':round(slow,5),
                 'micro10':round(micro10,5),'micro30':round(micro30,5),'struct_level':struct_level,'pressure_state':snap.get('state')}
            if _save_event_entry(side,score,lvl,reasons,ctx,now):
                event_entry_runtime['last_emit'][side]=now
                _event_stage_log(side,'TRIGGER',score,lvl,reasons,ctx,now)
                event_entry_runtime[side]=None; event_entry_runtime['last_stage'][side]=None
                event_entry_state={'updated_ts':now,'state':'TRIGGER','side':side,'phase':'ENTRY','score':round(score,1),'level':lvl,
                  'started_ts':int(arm['started_ts']),'reasons':reasons,'last_event_ts':now,
                  'note':'EVENT = exhaustion -> liquidity failure/reclaim -> first 1M structure response. Independent from EXEC/EARLY.'}
                return event_entry_state

    if current:
        best=max(current,key=lambda x:x['score'])
        event_entry_state={'updated_ts':now,'state':'ARMED','side':best['side'],'phase':best['phase'],'score':round(best['score'],1),
          'level':best['level'],'started_ts':best['started_ts'],'reasons':best['reasons'],'last_event_ts':int(event_entry_state.get('last_event_ts') or 0),
          'note':'EVENT waits for sequence completion; not connected to EXEC.'}
    else:
        # Keep TRIGGER visible briefly, then return to scan.
        if event_entry_state.get('state')=='TRIGGER' and now-int(event_entry_state.get('last_event_ts') or 0)<120000:
            event_entry_state={**event_entry_state,'updated_ts':now}
        else:
            event_entry_state={'updated_ts':now,'state':'SCANNING','side':'NONE','phase':'NONE','score':0.0,'level':None,'started_ts':0,
              'reasons':['WAIT_EXHAUSTION_SEQUENCE'],'last_event_ts':int(event_entry_state.get('last_event_ts') or 0),
              'note':'EVENT waits for exhaustion -> failure/reclaim -> structure response.'}
    return event_entry_state


def update_event_entry_research():
    if not last_price:return
    now=int(time.time()*1000); px=float(last_price); c=db()
    rows=c.execute("""SELECT e.id,e.ts,e.side,e.price,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60,r.next_exec_ts,r.lead_sec
      FROM event_entry_events e LEFT JOIN event_entry_research r ON r.event_id=e.id
      WHERE e.ts>=? ORDER BY e.ts DESC LIMIT 2000""",(now-4*60*60*1000,)).fetchall()
    for eid,ts,side,entry,mfe,mae,p5,p15,p30,p60,next_exec_ts,lead_sec in rows:
        if not entry:continue
        entry=float(entry); move=(px-entry)/entry*100.0; fav=move if side=='LONG' else -move; adv=-move if side=='LONG' else move
        mfe=max(float(mfe or 0),fav,0.0); mae=max(float(mae or 0),adv,0.0); vals=[p5,p15,p30,p60]
        for i,m in enumerate((5,15,30,60)):
            if vals[i] is None and now-int(ts)>=m*60000: vals[i]=px
        if next_exec_ts is None:
            hit=c.execute("""SELECT ts FROM decision_events WHERE event='TRIGGER' AND side=? AND ts>=? AND ts<=? ORDER BY ts ASC LIMIT 1""",(side,int(ts),int(ts)+60*60*1000)).fetchone()
            if hit:
                next_exec_ts=int(hit[0]); lead_sec=(next_exec_ts-int(ts))/1000.0
        c.execute("""INSERT INTO event_entry_research(event_id,started_ts,last_ts,mfe_pct,mae_pct,p5,p15,p30,p60,next_exec_ts,lead_sec)
          VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET last_ts=excluded.last_ts,mfe_pct=excluded.mfe_pct,mae_pct=excluded.mae_pct,
          p5=COALESCE(event_entry_research.p5,excluded.p5),p15=COALESCE(event_entry_research.p15,excluded.p15),p30=COALESCE(event_entry_research.p30,excluded.p30),p60=COALESCE(event_entry_research.p60,excluded.p60),
          next_exec_ts=COALESCE(event_entry_research.next_exec_ts,excluded.next_exec_ts),lead_sec=COALESCE(event_entry_research.lead_sec,excluded.lead_sec)""",
          (eid,ts,now,mfe,mae,*vals,next_exec_ts,lead_sec))
    c.commit(); c.close()

def update_precursor_research():
    if not last_price:return
    now=int(time.time()*1000); px=float(last_price); c=db()
    rows=c.execute("""SELECT e.id,e.ts,e.side,e.price,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60,r.next_exec_ts,r.lead_sec
      FROM precursor_events e LEFT JOIN precursor_research r ON r.precursor_id=e.id
      WHERE e.ts>=? ORDER BY e.ts DESC LIMIT 2000""",(now-4*60*60*1000,)).fetchall()
    for pid,ts,side,entry,mfe,mae,p5,p15,p30,p60,next_exec_ts,lead_sec in rows:
        if not entry:continue
        entry=float(entry); move=(px-entry)/entry*100.0; fav=move if side=='LONG' else -move; adv=-move if side=='LONG' else move
        mfe=max(float(mfe or 0),fav,0.0); mae=max(float(mae or 0),adv,0.0); vals=[p5,p15,p30,p60]
        for i,m in enumerate((5,15,30,60)):
            if vals[i] is None and now-int(ts)>=m*60000: vals[i]=px
        if next_exec_ts is None:
            hit=c.execute("""SELECT ts FROM decision_events WHERE event='TRIGGER' AND side=? AND ts>=? AND ts<=? ORDER BY ts ASC LIMIT 1""",(side,int(ts),int(ts)+60*60*1000)).fetchone()
            if hit:
                next_exec_ts=int(hit[0]); lead_sec=(next_exec_ts-int(ts))/1000.0
        c.execute("""INSERT INTO precursor_research(precursor_id,started_ts,last_ts,mfe_pct,mae_pct,p5,p15,p30,p60,next_exec_ts,lead_sec)
          VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(precursor_id) DO UPDATE SET last_ts=excluded.last_ts,mfe_pct=excluded.mfe_pct,mae_pct=excluded.mae_pct,
          p5=COALESCE(precursor_research.p5,excluded.p5),p15=COALESCE(precursor_research.p15,excluded.p15),p30=COALESCE(precursor_research.p30,excluded.p30),p60=COALESCE(precursor_research.p60,excluded.p60),
          next_exec_ts=COALESCE(precursor_research.next_exec_ts,excluded.next_exec_ts),lead_sec=COALESCE(precursor_research.lead_sec,excluded.lead_sec)""",
          (pid,ts,now,mfe,mae,*vals,next_exec_ts,lead_sec))
    c.commit();c.close()


def update_decision_research():
    """Forward MFE/MAE + horizons for the *actual EXEC* trigger events."""
    if not last_price:return
    now=int(time.time()*1000); px=float(last_price); c=db()
    rows=c.execute("""SELECT e.id,e.ts,e.side,e.price,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60
      FROM decision_events e LEFT JOIN decision_research r ON r.decision_id=e.id
      WHERE e.event='TRIGGER' AND e.ts>=? ORDER BY e.ts DESC LIMIT 2000""",(now-4*60*60*1000,)).fetchall()
    for did,ts,side,entry,mfe,mae,p5,p15,p30,p60 in rows:
        if not entry:continue
        entry=float(entry); move=(px-entry)/entry*100.0; fav=move if side=='LONG' else -move; adv=-move if side=='LONG' else move
        mfe=max(float(mfe or 0),fav,0.0); mae=max(float(mae or 0),adv,0.0); vals=[p5,p15,p30,p60]
        for i,m in enumerate((5,15,30,60)):
            if vals[i] is None and now-int(ts)>=m*60000: vals[i]=px
        c.execute("""INSERT INTO decision_research(decision_id,started_ts,last_ts,mfe_pct,mae_pct,p5,p15,p30,p60)
          VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(decision_id) DO UPDATE SET last_ts=excluded.last_ts,mfe_pct=excluded.mfe_pct,mae_pct=excluded.mae_pct,
          p5=COALESCE(decision_research.p5,excluded.p5),p15=COALESCE(decision_research.p15,excluded.p15),p30=COALESCE(decision_research.p30,excluded.p30),p60=COALESCE(decision_research.p60,excluded.p60)""",
          (did,ts,now,mfe,mae,*vals))
    c.commit();c.close()


def minute_pressure_gate(side, reversal=False, loose=False, snap=None):
    """Primary flow gate. Minute pressure decides context; micro 10s/30s never decides direction by itself."""
    snap=snap or mtf_pressure_snapshot(); t=snap['timeframes']
    if not snap.get('ready'):return False
    d1=float(t['1M']['ratio']); d3=float(t['3M']['ratio']); fast=float(snap['fast_delta']); slow=float(snap['slow_delta'])
    if side=='LONG':
        if reversal:return fast>=(-.035 if loose else -.01) and (d1-d3>=.035 or fast-slow>=.055 or fast>=.055)
        return fast>=(-.035 if loose else .005) and d1>=-.08 and (d3>=-.06 or d1>d3+.05)
    if reversal:return fast<=(.035 if loose else .01) and (d1-d3<=-.035 or fast-slow<=-.055 or fast<=-.055)
    return fast<=(.035 if loose else -.005) and d1<=.08 and (d3<=.06 or d1<d3-.05)

def flow_intensity():
    now=int(time.time()*1000); vals=[]
    for k in range(1,7):
        lo=now-k*10000; hi=now-(k-1)*10000
        vals.append(sum(t["notional"] for t in trades if lo<=t["ts"]<hi))
    base=median([x for x in vals if x>0]) or 1
    return flow(10000)["total"]/base

def oi_delta(ms=60000):
    if current_oi is None or not oi_hist:return 0.0
    target=int(time.time()*1000)-ms; base=oi_hist[0]
    for x in oi_hist:
        if x["ts"]<=target: base=x
        else: break
    return 100*(current_oi-base["oi"])/base["oi"] if base["oi"] else 0

def atr5():
    a=list(candles["5M"])
    if len(a)<25:return max((last_price or 1)*0.001,1)
    vals=[]
    for i in range(max(1,len(a)-24),len(a)):
        c=a[i];p=a[i-1]["close"]
        vals.append(max(c["high"]-c["low"],abs(c["high"]-p),abs(c["low"]-p)))
    return median(vals) or 1

def pivots(a,r=2):
    H=L=None
    for k in range(r,len(a)-r):
        if all(a[j]["high"]<a[k]["high"] for j in range(k-r,k+r+1) if j!=k): H=a[k]["high"]
        if all(a[j]["low"]>a[k]["low"] for j in range(k-r,k+r+1) if j!=k): L=a[k]["low"]
    return H,L

def liquidity15():
    a=[x for x in candles["15M"] if x.get("confirm")=="1"]
    if len(a)<20:return None,None
    H,L=pivots(a[-80:])
    local=a[-24:]
    return H or max(x["high"] for x in local), L or min(x["low"] for x in local)

def atr_tf(tf,n=24):
    a=list(candles[tf])
    if len(a)<n+1:return max((last_price or 1)*0.001,1)
    vals=[]
    for i in range(max(1,len(a)-n),len(a)):
        c=a[i];p=a[i-1]["close"]
        vals.append(max(c["high"]-c["low"],abs(c["high"]-p),abs(c["low"]-p)))
    return median(vals) or 1

def liquidity_tf(tf,lookback=80,local_n=24):
    a=[x for x in candles[tf] if x.get("confirm")=="1"]
    if len(a)<20:return None,None
    H,L=pivots(a[-lookback:])
    local=a[-local_n:]
    return H or max(x["high"] for x in local), L or min(x["low"] for x in local)

MA_PERIODS=(20,60,120,240,480)

def sma_tf(tf,n):
    a=list(candles.get(tf,[]))
    if len(a)<n:return None
    return sum(float(x["close"]) for x in a[-n:])/n

def vwap_tf(tf,n=240):
    """Session-anchored VWAP used by the signal engine.

    Intraday TFs reset at 00:00 UTC, exactly like the chart VWAP.
    1D resets at the first UTC day of each month.  ``n`` is retained only
    for backwards-compatible callers; it no longer creates a rolling window.
    """
    a=list(candles.get(tf,[]))
    if not a:return None
    latest_ts=int(a[-1].get("ts",0) or 0)
    if latest_ts<=0:return None
    dt=datetime.fromtimestamp(latest_ts/1000.0,tz=timezone.utc)
    if tf=="1D":
        anchor=datetime(dt.year,dt.month,1,tzinfo=timezone.utc)
    else:
        anchor=datetime(dt.year,dt.month,dt.day,tzinfo=timezone.utc)
    anchor_ms=int(anchor.timestamp()*1000)
    session=[x for x in a if int(x.get("ts",0) or 0)>=anchor_ms]
    if not session:return None
    den=sum(float(x.get("volume",0) or 0) for x in session)
    if den<=0:return None
    return sum(((float(x["high"])+float(x["low"])+float(x["close"]))/3.0)*float(x.get("volume",0) or 0) for x in session)/den

def rolling_vwap_tf(tf,n=14,offset=0):
    """Rolling n-candle VWAP for the new v6.68 direction radar.

    Uses HLC3 * base-volume and intentionally does NOT reset at UTC midnight.
    Existing vwap_tf() stays unchanged so the frozen EXEC benchmark is not altered.
    """
    a=list(candles.get(tf,[]))
    end=len(a)-int(offset or 0)
    if end<n or end<=0:return None
    w=a[end-n:end]
    den=sum(float(x.get("volume",0) or 0) for x in w)
    if den<=0:return None
    return sum(((float(x["high"])+float(x["low"])+float(x["close"]))/3.0)*float(x.get("volume",0) or 0) for x in w)/den

def _sma_at(tf,n,offset=0):
    a=list(candles.get(tf,[])); end=len(a)-int(offset or 0)
    if end<n or end<=0:return None
    return sum(float(x["close"]) for x in a[end-n:end])/n

def _ma_bundle(tf='15M',offset=0):
    vals={n:_sma_at(tf,n,offset) for n in (5,8,10,20)}
    if any(v is None for v in vals.values()):return None
    A=max(atr_tf(tf,24),1.0); arr=list(vals.values())
    return {"ma":vals,"low":min(arr),"high":max(arr),"center":sum(arr)/len(arr),"width_atr":(max(arr)-min(arr))/A}

def _ma_slope(tf,n,back=1):
    now=_sma_at(tf,n,0); old=_sma_at(tf,n,back)
    if now is None or old is None:return 0.0
    return (now-old)/max(atr_tf(tf,24),1.0)

def _vwap_ma_features():
    """15M MA-cycle radar matching the user's discretionary process.

    Direction (BIAS) and timing (PHASE) are intentionally separated.
    The engine reads MA alignment/slope/spread + VWAP14 and classifies the
    repeating cycle: COMPRESSION -> RELEASE -> ALIGN -> EXPANSION -> MA_HIT
    -> REALIGN -> RE_EXPANSION.  A counter-trend wiggle is never a new bias
    unless the medium structure actually breaks.
    """
    a=list(candles.get('15M',[]))
    if len(a)<125 or not last_price:return None
    p=float(last_price); A=max(atr_tf('15M',24),1.0)
    lens=(5,10,20,60,120)
    ma={n:_sma_at('15M',n,0) for n in lens}
    prev={n:_sma_at('15M',n,3) for n in lens}
    if any(v is None for v in ma.values()) or any(v is None for v in prev.values()):return None
    vw=rolling_vwap_tf('15M',14,0)
    if vw is None:return None
    slopes={n:(ma[n]-prev[n])/(3*A) for n in lens}
    short=(5,10,20); medium=(20,60,120)
    short_vals=[ma[n] for n in short]; med_vals=[ma[n] for n in medium]
    short_spread=(max(short_vals)-min(short_vals))/A
    med_spread=(max(med_vals)-min(med_vals))/A
    prev_short=[prev[n] for n in short]
    prev_spread=(max(prev_short)-min(prev_short))/A
    spread_delta=short_spread-prev_spread
    long_align=ma[5]>ma[10]>ma[20]>ma[60]>ma[120]
    short_align=ma[5]<ma[10]<ma[20]<ma[60]<ma[120]
    long_core=ma[20]>ma[60]>ma[120] and slopes[20]>-.015 and slopes[60]>-.012
    short_core=ma[20]<ma[60]<ma[120] and slopes[20]<.015 and slopes[60]<.012
    long_slope=sum(slopes[n] for n in short)/3
    short_slope=-long_slope
    vdist=(p-vw)/A
    compression=short_spread<=.20 or (short_spread<=.32 and spread_delta<-.025)
    expanding=spread_delta>.018 and abs(long_slope)>.008
    # 'MA hit': price returns into the fast/medium ribbon while the core trend survives.
    ribbon_lo=min(ma[5],ma[10],ma[20],ma[60]); ribbon_hi=max(ma[5],ma[10],ma[20],ma[60])
    long_hit=long_core and p<=ribbon_hi+.10*A and p>=ma[60]-.28*A
    short_hit=short_core and p>=ribbon_lo-.10*A and p<=ma[60]+.28*A
    long_realign=long_core and ma[5]>ma[10]>ma[20] and slopes[5]>0 and slopes[10]>0 and p>=vw-.12*A
    short_realign=short_core and ma[5]<ma[10]<ma[20] and slopes[5]<0 and slopes[10]<0 and p<=vw+.12*A
    long_break=(p<ma[60]-.35*A and ma[20]<ma[60] and slopes[20]<-.025) or (ma[20]<ma[60]<ma[120] and p<vw-.25*A)
    short_break=(p>ma[60]+.35*A and ma[20]>ma[60] and slopes[20]>.025) or (ma[20]>ma[60]>ma[120] and p>vw+.25*A)
    long_strength=(30 if long_core else 0)+(22 if long_align else 0)+(16 if long_slope>.01 else 0)+(12 if vdist>-.10 else 0)+(12 if spread_delta>.01 else 0)+(8 if slopes[120]>=-.005 else 0)
    short_strength=(30 if short_core else 0)+(22 if short_align else 0)+(16 if short_slope>.01 else 0)+(12 if vdist<.10 else 0)+(12 if spread_delta>.01 else 0)+(8 if slopes[120]<=.005 else 0)
    return {'price':p,'atr':A,'vwap14':vw,'distance_atr':vdist,'ma':ma,'slopes_all':slopes,
            'short_spread':short_spread,'med_spread':med_spread,'spread_delta':spread_delta,
            'compression':compression,'expanding':expanding,'long_align':long_align,'short_align':short_align,
            'long_core':long_core,'short_core':short_core,'long_hit':long_hit,'short_hit':short_hit,
            'long_realign':long_realign,'short_realign':short_realign,'long_break':long_break,'short_break':short_break,
            'LONG':{'score':min(100,long_strength),'invalidation':ma[60]-.35*A},
            'SHORT':{'score':min(100,short_strength),'invalidation':ma[60]+.35*A}}

def _save_vwap_ma_event(side,stage,feat,now):
    """Persist MA-cycle phase transitions for chart markers/research."""
    if side not in ('LONG','SHORT') or stage not in ('RELEASE','ALIGN','EXPANSION','MA_HIT','REALIGN','RE_EXPANSION','BREAKDOWN'):
        return False,[]
    bucket=int(now//900000); d=feat[side]
    reasons=[]
    if feat.get('compression'): reasons.append('COMPRESSION_RELEASE')
    if feat.get('expanding'): reasons.append('MA_SPREAD_EXPANDING')
    if (feat.get('long_hit') if side=='LONG' else feat.get('short_hit')): reasons.append('MA_HIT')
    if (feat.get('long_realign') if side=='LONG' else feat.get('short_realign')): reasons.append('REALIGN')
    reasons.append('VWAP_ABOVE' if feat.get('distance_atr',0)>=0 else 'VWAP_BELOW')
    ctx={'distance_atr':feat['distance_atr'],'short_spread':feat['short_spread'],'med_spread':feat['med_spread'],
         'spread_delta':feat['spread_delta'],'ma':feat['ma'],'slopes':feat['slopes_all']}
    c=db(); cur=c.execute("""INSERT OR IGNORE INTO vwap_ma_events(ts,bucket,side,stage,score,price,vwap14,knot_atr,invalidation,reason,context_json)
      VALUES(?,?,?,?,?,?,?,?,?,?,?)""",(int(now),bucket,side,stage,float(d['score']),float(feat['price']),float(feat['vwap14']),float(feat['short_spread']),float(d['invalidation']),' | '.join(reasons),json.dumps(ctx,separators=(',',':'),ensure_ascii=False)))
    inserted=cur.rowcount>0
    if inserted and stage in ('RE_EXPANSION','EXPANSION'):
        eid=cur.lastrowid; c.execute("INSERT OR IGNORE INTO vwap_ma_research(event_id,started_ts,last_ts,mfe_pct,mae_pct) VALUES(?,?,?,?,?)",(eid,int(now),int(now),0.0,0.0))
    c.commit(); c.close(); return inserted,reasons

def evaluate_vwap_ma():
    """Persistent MA-cycle state machine. Bias changes only on structural failure."""
    global vwap_ma_state,vwap_ma_runtime
    feat=_vwap_ma_features(); now=int(time.time()*1000)
    if not feat:
        vwap_ma_state={**vwap_ma_state,'updated_ts':now,'state':'WARMUP','side':'NONE','stage':'WARMUP','reasons':['15M MA120 WARMUP']}
        return vwap_ma_state
    rt=vwap_ma_runtime
    old=str(rt.get('side') or 'NONE')
    # Hysteresis: preserve trend through ordinary pullbacks/MA hits.
    if old=='LONG' and not feat['long_break']:
        side='LONG'
    elif old=='SHORT' and not feat['short_break']:
        side='SHORT'
    elif feat['LONG']['score']>=58 and feat['LONG']['score']>=feat['SHORT']['score']+12:
        side='LONG'
    elif feat['SHORT']['score']>=58 and feat['SHORT']['score']>=feat['LONG']['score']+12:
        side='SHORT'
    else:
        side='NONE'
    if side!=old:
        rt['side']=side;rt['started_ts']=now;rt['last_change_ts']=now
    isL=side=='LONG'
    if side=='NONE':
        phase='COMPRESSION' if feat['compression'] else 'SCANNING'
    else:
        align=feat['long_align'] if isL else feat['short_align']
        hit=feat['long_hit'] if isL else feat['short_hit']
        realign=feat['long_realign'] if isL else feat['short_realign']
        brk=feat['long_break'] if isL else feat['short_break']
        if brk: phase='BREAKDOWN'
        elif hit and not realign: phase='MA_HIT'
        elif hit and realign: phase='REALIGN'
        elif align and feat['expanding']: phase='RE_EXPANSION' if str(rt.get('stage')) in ('MA_HIT','REALIGN') else 'EXPANSION'
        elif align: phase='ALIGN'
        elif feat['compression']: phase='COMPRESSION'
        else: phase='RELEASE'
    prev_stage=str(rt.get('stage') or 'NONE')
    # Preserve RE_EXPANSION recognition one cycle after a hit/realign.
    if side!='NONE' and phase=='EXPANSION' and prev_stage in ('MA_HIT','REALIGN'): phase='RE_EXPANSION'
    if phase!=prev_stage:
        rt['stage']=phase;rt['last_change_ts']=now
        if side in ('LONG','SHORT'):
            try:_save_vwap_ma_event(side,phase,feat,now)
            except Exception as e: print('ma-cycle event save',e)
    d=feat.get(side) if side in ('LONG','SHORT') else None
    reasons=[]
    if side!='NONE':
        reasons.append('CORE '+('정배열' if isL else '역배열'))
        reasons.append('VWAP '+('상단' if feat['distance_atr']>=0 else '하단'))
        if feat['compression']: reasons.append('이평 수렴')
        if feat['expanding']: reasons.append('이격 발산')
        if (feat['long_hit'] if isL else feat['short_hit']): reasons.append('이평치기')
        if (feat['long_realign'] if isL else feat['short_realign']): reasons.append('재정렬')
    else: reasons.append('방향 확정 전')
    score=float(d['score']) if d else max(float(feat['LONG']['score']),float(feat['SHORT']['score']))
    vwap_ma_state={'updated_ts':now,'state':phase,'side':side,'stage':phase,'score':round(score,1),
      'vwap14':round(float(feat['vwap14']),2),'price':round(float(feat['price']),2),'distance_atr':round(float(feat['distance_atr']),3),
      'knot_atr':round(float(feat['short_spread']),3),'spread_delta':round(float(feat['spread_delta']),4),
      'invalidation':round(float(d['invalidation']),2) if d else None,
      'slopes':{'short':round(sum(feat['slopes_all'][n] for n in (5,10,20))/3,4),'accel':round(float(feat['spread_delta']),4),'ma20':round(float(feat['slopes_all'][20]),4)},
      'ma':{str(k):round(float(v),2) for k,v in feat['ma'].items()},'reasons':reasons,'active_since':int(rt.get('started_ts') or 0),
      'note':'BIAS is persistent. PHASE tracks compression/release/alignment/expansion/MA-hit/realign/re-expansion/breakdown.'}
    return vwap_ma_state

def update_vwap_ma_research():
    if not last_price:return
    now=int(time.time()*1000); px=float(last_price); c=db(); c.row_factory=sqlite3.Row
    rows=c.execute("""SELECT e.*,r.started_ts,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60
      FROM vwap_ma_events e JOIN vwap_ma_research r ON r.event_id=e.id WHERE e.stage='RELEASE' AND (? - e.ts)<=7200000
      ORDER BY e.ts DESC LIMIT 500""",(now,)).fetchall()
    for r in rows:
        side=str(r['side']); entry=float(r['price'] or 0)
        if not entry:continue
        move=(px-entry)/entry*100*(1 if side=='LONG' else -1)
        mfe=max(float(r['mfe_pct'] or 0),move); mae=min(float(r['mae_pct'] or 0),move)
        vals={}; age=now-int(r['ts'])
        for mins,col in ((5,'p5'),(15,'p15'),(30,'p30'),(60,'p60')):
            if r[col] is None and age>=mins*60000: vals[col]=px
        c.execute("""UPDATE vwap_ma_research SET last_ts=?,mfe_pct=?,mae_pct=?,p5=COALESCE(p5,?),p15=COALESCE(p15,?),p30=COALESCE(p30,?),p60=COALESCE(p60,?) WHERE event_id=?""",
          (now,mfe,mae,vals.get('p5'),vals.get('p15'),vals.get('p30'),vals.get('p60'),int(r['id'])))
    c.commit(); c.close()

def vwap_signal_context(tf, atr_value):
    """VWAP-only location context. MA EARLY is retired in v6.29.
    Returns a directional reaction/reclaim score without using moving averages.
    """
    a=list(candles.get(tf,[]))
    vw=vwap_tf(tf,240)
    if len(a)<2 or vw is None or not last_price:
        return {"vwap":vw,"long":0,"short":0,"dist_atr":999.0,"long_reaction":False,"short_reaction":False}
    c=a[-1]; prev=a[-2]; A=max(float(atr_value or 1),1.0)
    dist=abs(float(last_price)-vw)/A
    # Reclaim/support: price probes VWAP then closes back above it.
    long_reaction=(c["low"]<=vw+.10*A and c["close"]>vw and (prev["close"]<=vw or c["close"]>c["open"]))
    # Rejection/resistance: price probes VWAP then closes back below it.
    short_reaction=(c["high"]>=vw-.10*A and c["close"]<vw and (prev["close"]>=vw or c["close"]<c["open"]))
    ls=(35 if long_reaction else 0)+(10 if c["close"]>vw else 0)+(5 if dist<=.35 else 0)
    ss=(35 if short_reaction else 0)+(10 if c["close"]<vw else 0)+(5 if dist<=.35 else 0)
    return {"vwap":vw,"long":ls,"short":ss,"dist_atr":dist,
            "long_reaction":long_reaction,"short_reaction":short_reaction}

def trend_bias_tf(tf, bars=4):
    """Simple confirmed price-structure bias: +1 bull, -1 bear, 0 mixed."""
    a=[x for x in candles.get(tf,[]) if x.get("confirm")=="1"]
    if len(a)<bars:return 0
    z=a[-bars:]
    closes=[float(x["close"]) for x in z]
    highs=[float(x["high"]) for x in z]
    lows=[float(x["low"]) for x in z]
    bull=closes[-1]>closes[0] and highs[-1]>=highs[0] and lows[-1]>=lows[0]
    bear=closes[-1]<closes[0] and highs[-1]<=highs[0] and lows[-1]<=lows[0]
    return 1 if bull else -1 if bear else 0

def higher_tf_for(engine):
    return {"SCALP":"5M","5M":"15M","15M":"1H","1H":"4H","4H":None}.get(engine)

def position_confluence(engine, side, signal_score, atr_value=None):
    """Strict CONF/POS gate. Score = condition confluence, never a probability.
    Requires independent evidence groups so repeated flow readings cannot manufacture a POS.
    """
    tf="3M" if engine=="SCALP" else engine
    A=atr_value or (atr_tf("1M",30) if engine=="SCALP" else atr_tf(tf,24))
    ctx=vwap_signal_context(tf,A)
    w10,w30=flow(10000),flow(30000); inten=flow_intensity()
    ss=float(signal_score or 0); score=0; reasons=[]; groups=set()

    # 1) Original setup quality
    if ss>=95: score+=24; groups.add("setup"); reasons.append("setup95+")
    elif ss>=90: score+=21; groups.add("setup"); reasons.append("setup90+")
    elif ss>=85: score+=17; groups.add("setup"); reasons.append("setup85+")
    elif ss>=80: score+=10

    # 2) Location / price reaction at VWAP
    if side=="LONG":
        if ctx["long_reaction"]: score+=24;groups.add("location");reasons.append("VWAP reclaim")
        elif ctx["vwap"] is not None and last_price>=ctx["vwap"] and ctx["dist_atr"]<=.45:
            score+=12;groups.add("location");reasons.append("VWAP support")
    else:
        if ctx["short_reaction"]: score+=24;groups.add("location");reasons.append("VWAP reject")
        elif ctx["vwap"] is not None and last_price<=ctx["vwap"] and ctx["dist_atr"]<=.45:
            score+=12;groups.add("location");reasons.append("VWAP resistance")

    # 3) Flow confirmation - 10s+30s count as ONE evidence group
    flow_ok=(w10["ratio"]>=.08 and w30["ratio"]>=.02) if side=="LONG" else (w10["ratio"]<=-.08 and w30["ratio"]<=-.02)
    if flow_ok:
        score+=20;groups.add("flow");reasons.append("flow confirm")
    elif (w10["ratio"]>=.06 if side=="LONG" else w10["ratio"]<=-.06):
        score+=8

    # 4) Order-book/activity confirmation
    book_ok=(book_imb>=.10) if side=="LONG" else (book_imb<=-.10)
    if book_ok and inten>=1.05:
        score+=14;groups.add("order");reasons.append("book+activity")
    elif book_ok or inten>=1.15:
        score+=6

    # 5) Own-TF structure and higher-TF veto/confirmation
    own=trend_bias_tf(tf); wanted=1 if side=="LONG" else -1
    if own==wanted:
        score+=12;groups.add("structure");reasons.append("TF structure")
    higher=higher_tf_for(engine)
    hb=trend_bias_tf(higher) if higher else 0
    if hb==wanted:
        score+=10;groups.add("higher");reasons.append("higher TF")
    elif hb==-wanted:
        score-=15;reasons.append("higher TF conflict")

    score=max(0,min(score,100))
    # CONF requires at least four genuinely different evidence groups.
    confirmed=(score>=82 and len(groups)>=4)
    # v6.35: 15M/1H are quality-first engines: require stronger independent confirmation.
    if engine=="15M": confirmed=(score>=88 and len(groups)>=5 and "location" in groups and "structure" in groups)
    elif engine=="1H": confirmed=(score>=90 and len(groups)>=5 and "location" in groups and "structure" in groups)
    # If higher TF directly conflicts, only exceptional confluence can override it.
    if hb==-wanted and score<92: confirmed=False
    return score, reasons, ctx, confirmed, sorted(groups)

def set_reentry_guard(engine, side, exit_price, atr_value, reason):
    c=db(); c.execute("""INSERT INTO position_reentry_guard(engine,side,exit_ts,exit_price,atr,reason)
      VALUES(?,?,?,?,?,?) ON CONFLICT(engine) DO UPDATE SET side=excluded.side,exit_ts=excluded.exit_ts,
      exit_price=excluded.exit_price,atr=excluded.atr,reason=excluded.reason""",
      (engine,side,int(time.time()*1000),float(exit_price),float(atr_value or 0),reason))
    c.commit(); c.close()

def reentry_guard_status(engine, side):
    # Prevent same-engine/same-direction churn after an automatic stop.
    # New signals are still stored; only simulator POS re-entry is locked.
    lock_ms={"SCALP":8*60*1000,"5M":15*60*1000,"15M":30*60*1000,"1H":60*60*1000,"4H":120*60*1000}.get(engine,15*60*1000)
    c=db(); c.row_factory=sqlite3.Row
    r=c.execute("SELECT * FROM position_reentry_guard WHERE engine=?",(engine,)).fetchone(); c.close()
    if not r or str(r["side"])!=side:return False,0,None
    left=lock_ms-(int(time.time()*1000)-int(r["exit_ts"] or 0))
    return left>0,max(0,left),dict(r)

def apply_position_if_strong(engine,side,price,signal_id,signal_score,atr_value=None,source="SIGNAL"):
    """Only CONFIRMED multi-factor setups may create a research POS.

    v6.43: SCALP (1/3M) remains a signal-only research engine. It may keep
    recording SCALP/VWAP observations, but it cannot OPEN/CONFIRM/SWITCH an
    automatic research position. Existing SCALP positions, if any, are left
    to the normal lifecycle manager so they are never silently deleted.
    """
    if engine == "SCALP":
        return 0
    gate,reasons,ctx,confirmed,groups=position_confluence(engine,side,signal_score,atr_value)
    pos=current_position(engine)
    note=f"{source} CONF {gate} groups={'+'.join(groups) or '-'}: "+(",".join(reasons) or "insufficient confluence")
    if not pos:
        locked,left,guard=reentry_guard_status(engine,side)
        if locked:
            # Same-side automatic re-entry is forbidden for a TF-specific reset window.
            # This kills the S->FLOW X->S->FLOW X loop without deleting any signals.
            return gate
        if confirmed:
            set_position(engine,side,price,signal_id,atr_value)
            position_event("OPEN",side,price,signal_id,note,engine)
        return gate
    if pos["side"]==side:
        if confirmed:
            position_event("CONFIRM",side,price,signal_id,note,engine)
        return gate
    # Opposite signal never flips by itself. Existing POS must already be PRESSURE and
    # opposite evidence must be exceptional.
    if str(pos.get("state") or "HOLD").upper()=="PRESSURE" and confirmed and gate>=92:
        position_event("EXIT",pos["side"],price,signal_id,"CONFIRMED SWITCH / "+note,engine)
        clear_position(engine,"CONFIRMED_SWITCH")
        set_position(engine,side,price,signal_id,atr_value)
        position_event("SWITCH",side,price,signal_id,note,engine)
    return gate

def save_vwap_signal(engine,side,score,tf,A,ctx,metrics):
    global vwap_last_ts
    now=int(time.time()*1000); p=float(last_price)
    name=f"VWAP {'L' if side=='LONG' else 'S'}"
    risk=max(.55*A,1.0)
    sl=p-risk if side=="LONG" else p+risk
    tp1=p+1.5*risk if side=="LONG" else p-1.5*risk
    tp2=p+2.3*risk if side=="LONG" else p-2.3*risk
    c=db();cur=c.execute("""INSERT INTO signals(ts,name,side,entry,sl,tp1,tp2,score,d10,d30,oi60,flow,book,status,engine)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',?)""",
      (now,name,side,p,sl,tp1,tp2,score,metrics["d10"],metrics["d30"],metrics["oi60"],metrics["flow"],metrics["book"],engine))
    sid=cur.lastrowid;c.commit();c.close()
    apply_position_if_strong(engine,side,p,sid,score,A,"VWAP")
    vwap_last_ts[engine]=now

def evaluate_vwap_context():
    """Independent VWAP reclaim/rejection signals for SCALP, 5M, 15M, 1H and 4H."""
    if not last_price or len(trades)<20:return
    now=int(time.time()*1000); w10,w30=flow(10000),flow(30000); inten=flow_intensity(); oi60=oi_delta()
    cfg={
      "SCALP":("3M",atr_tf("1M",30),120000),
      "5M":("5M",atr_tf("5M",24),240000),
      "15M":("15M",atr_tf("15M",24),600000),
      "1H":("1H",atr_tf("1H",24),1800000),
      "4H":("4H",atr_tf("4H",24),3600000),
    }
    for engine,(tf,A,cooldown) in cfg.items():
        if now-vwap_last_ts.get(engine,0)<cooldown:continue
        ctx=vwap_signal_context(tf,A)
        metrics={"d10":w10["ratio"],"d30":w30["ratio"],"oi60":oi60,"flow":inten,"book":book_imb}
        long_flow=(w10["ratio"]>.05 and w30["ratio"]>-.08 and w10["ratio"]-w30["ratio"]>.03)
        short_flow=(w10["ratio"]<-.05 and w30["ratio"]<.08 and w30["ratio"]-w10["ratio"]>.03)
        lscore=50+(20 if long_flow else 0)+(10 if book_imb>-.05 else 0)+(10 if inten>1.05 else 0)+(5 if oi60>=-.005 else 0)+(5 if ctx["dist_atr"]<=.30 else 0)
        sscore=50+(20 if short_flow else 0)+(10 if book_imb<.05 else 0)+(10 if inten>1.05 else 0)+(5 if oi60>=-.005 else 0)+(5 if ctx["dist_atr"]<=.30 else 0)
        if ctx["long_reaction"] and long_flow and lscore>=80:
            save_vwap_signal(engine,"LONG",min(lscore,100),tf,A,ctx,metrics)
        elif ctx["short_reaction"] and short_flow and sscore>=80:
            save_vwap_signal(engine,"SHORT",min(sscore,100),tf,A,ctx,metrics)

def position_event(event,side,price,signal_id=None,note="",engine="5M"):
    c=db()
    c.execute("INSERT INTO position_events(ts,event,side,price,signal_id,note,engine) VALUES(?,?,?,?,?,?,?)",
              (int(time.time()*1000),event,side,price,signal_id,note,engine))
    c.commit();c.close()

def current_position(engine="5M"):
    c=db();c.row_factory=sqlite3.Row
    row=c.execute("SELECT * FROM engine_positions WHERE engine=?",(engine,)).fetchone()
    c.close()
    return dict(row) if row else None

def all_positions():
    c=db();c.row_factory=sqlite3.Row
    rows=c.execute("SELECT * FROM engine_positions ORDER BY opened_ts").fetchall()
    out=[dict(x) for x in rows]
    # Backward compatibility: if an old CORE position exists only in legacy table,
    # expose it instead of making the UI look empty.
    if False:  # v6.26 legacy CORE fallback retired
        legacy=c.execute("SELECT * FROM position_state WHERE id=1").fetchone()
        if legacy and legacy["side"]:
            d=dict(legacy);d["engine"]="CORE";d.setdefault("best_price",d.get("entry"))
            d.setdefault("worst_price",d.get("entry"));d.setdefault("mfe",0);d.setdefault("mae",0)
            out.append(d)
    c.close()
    return out

def set_position(engine,side,entry,signal_id,atr_value=None):
    now=int(time.time()*1000);A=atr_value or (atr_tf("1M",30) if engine=="SCALP" else atr_tf(engine,24) if engine in ("5M","15M","1H","4H") else atr5())
    c=db()
    c.execute("""INSERT INTO engine_positions(engine,side,entry,opened_ts,signal_id,updated_ts,best_price,atr_open,worst_price,mfe,mae)
                 VALUES(?,?,?,?,?,?,?,?,?,?,?)
                 ON CONFLICT(engine) DO UPDATE SET side=excluded.side,entry=excluded.entry,
                 opened_ts=excluded.opened_ts,signal_id=excluded.signal_id,updated_ts=excluded.updated_ts,
                 best_price=excluded.best_price,atr_open=excluded.atr_open,worst_price=excluded.worst_price,
                 mfe=0,mae=0,state='HOLD',pressure_since=NULL,pressure_score=0""",
              (engine,side,entry,now,signal_id,now,entry,A,entry,0,0))
    c.commit();c.close()

def clear_position(engine, reason=None):
    # Safety invariant: a live research position may never disappear silently.
    # Every deletion must have an explicit close reason supplied by EXIT/SWITCH logic.
    if not reason:
        raise RuntimeError(f"Refusing silent position delete: {engine}")
    c=db();c.execute("DELETE FROM engine_positions WHERE engine=?",(engine,));c.commit();c.close()

def apply_position_signal(engine,side,price,signal_id,atr_value=None):
    """Each timeframe engine owns its own position. Other-engine signals never kill it."""
    pos=current_position(engine)
    if not pos:
        set_position(engine,side,price,signal_id,atr_value)
        position_event("OPEN",side,price,signal_id,"engine entry",engine)
        return
    if pos["side"]==side:
        position_event("CONFIRM",side,price,signal_id,"same-engine confirmation",engine)
        c=db();c.execute("UPDATE engine_positions SET updated_ts=? WHERE engine=?",
                         (int(time.time()*1000),engine));c.commit();c.close()
        return
    # Only an opposite signal from THE SAME engine can switch this engine.
    position_event("EXIT",pos["side"],price,signal_id,"same-engine opposite signal / switch",engine)
    clear_position(engine,"SWITCH")
    set_position(engine,side,price,signal_id,atr_value)
    position_event("SWITCH",side,price,signal_id,"same-engine opposite signal",engine)

def save_signal(name,side,score,level,ext,metrics,engine="5M",atr_value=None):
    global scalp_last_signal_ts,scalp_arm,tf_last_signal_ts,tf_arms
    p=last_price; A=atr_value or atr5()
    if side=="LONG":
        sl=min(ext,level-.22*A); risk=max(p-sl,.35*A); tp1=p+1.5*risk; tp2=p+2.3*risk
    else:
        sl=max(ext,level+.22*A); risk=max(sl-p,.35*A); tp1=p-1.5*risk; tp2=p-2.3*risk
    if (side=="LONG" and p<=sl) or (side=="SHORT" and p>=sl):
        if engine=="SCALP": scalp_arm=None
        elif engine in tf_arms: tf_arms[engine]=None
        return
    now=int(time.time()*1000)
    c=db();cur=c.execute("""INSERT INTO signals(ts,name,side,entry,sl,tp1,tp2,score,d10,d30,oi60,flow,book,status,engine)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',?)""",
      (now,name,side,p,sl,tp1,tp2,score,metrics["d10"],metrics["d30"],
       metrics["oi60"],metrics["flow"],metrics["book"],engine))
    signal_id=cur.lastrowid;c.commit();c.close()

    # Signal benchmark stays sensitive; actual POS requires strict multi-factor confluence.
    apply_position_if_strong(engine,side,p,signal_id,score,A,"SIGNAL")
    if engine=="SCALP":
        scalp_last_signal_ts=now;scalp_arm=None
    elif engine in tf_arms:
        tf_last_signal_ts[engine]=now;tf_arms[engine]=None

def precision_signal_gate(engine, side, score, A):
    """v6.39: quality gate for raw 5M/15M signals.
    15M defines location/direction; 5M is execution confirmation.
    This is a research confluence filter, not a probability estimate.
    """
    if engine not in ("5M","15M"):
        return True, []
    wanted=1 if side=="LONG" else -1
    own=trend_bias_tf(engine)
    ctx=vwap_signal_context(engine,A)
    reasons=[]
    if engine=="5M":
        h=trend_bias_tf("15M")
        if h==-wanted:
            return False,["15M conflict"]
        reaction=ctx["long_reaction"] if side=="LONG" else ctx["short_reaction"]
        # execution signal needs either a real VWAP response or own-TF structure,
        # and weak setup scores are no longer promoted.
        if float(score or 0)<85:
            return False,["score<85"]
        if not reaction and own!=wanted:
            return False,["no 5M reaction/structure"]
        if h==wanted: reasons.append("15M aligned")
        if reaction: reasons.append("5M VWAP reaction")
        if own==wanted: reasons.append("5M structure")
        return True,reasons
    # 15M is quality-first: location + structure, with 1H acting as a veto.
    h=trend_bias_tf("1H")
    if h==-wanted:
        return False,["1H conflict"]
    reaction=ctx["long_reaction"] if side=="LONG" else ctx["short_reaction"]
    near=ctx.get("vwap") is not None and float(ctx.get("dist_atr") or 99)<=.35
    if float(score or 0)<85:
        return False,["score<85"]
    if own!=wanted:
        return False,["15M structure not confirmed"]
    if not (reaction or near):
        return False,["15M location not confirmed"]
    reasons.append("15M structure")
    reasons.append("15M VWAP reaction" if reaction else "15M VWAP location")
    if h==wanted: reasons.append("1H aligned")
    return True,reasons

def _bar_shape(c):
    if not c:return {"rng":1.0,"body":0.0,"upper":0.0,"lower":0.0,"close_pos":0.5}
    h=float(c["high"]); l=float(c["low"]); o=float(c["open"]); cl=float(c["close"])
    rng=max(h-l,1e-9); body=abs(cl-o)
    return {"rng":rng,"body":body,"upper":h-max(o,cl),"lower":min(o,cl)-l,"close_pos":max(0,min(1,(cl-l)/rng))}

def _save_stage_signal(name, side, score, engine, A, reason, allow_position=False):
    """Persist a staged research signal. EARLY/5M pressure never open a POS.
    Only CONF may hand off to the simulator.
    """
    p=float(last_price or 0); A=max(float(A or 1),1.0); now=int(time.time()*1000)
    risk=max(.55*A,1.0)
    sl=p-risk if side=="LONG" else p+risk
    tp1=p+1.5*risk if side=="LONG" else p-1.5*risk
    tp2=p+2.3*risk if side=="LONG" else p-2.3*risk
    w10,w30=flow(10000),flow(30000)
    c=db(); cur=c.execute("""INSERT INTO signals(ts,name,side,entry,sl,tp1,tp2,score,d10,d30,oi60,flow,book,status,engine,reason)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',?,?)""",
      (now,name,side,p,sl,tp1,tp2,float(score),w10["ratio"],w30["ratio"],oi_delta(),flow_intensity(),book_imb,engine,reason))
    sid=cur.lastrowid; c.commit(); c.close()
    if allow_position:
        apply_pipeline_position(side,p,sid,float(score),A,reason)
    return sid

def apply_pipeline_position(side, price, signal_id, score, A, reason):
    """v6.45 simulator entry: only a completed 5M+15M CONF may open a new 5M execution POS.
    Opposite CONF does not blindly reverse a healthy position.
    """
    engine="5M"
    pos=current_position(engine)
    note=f"PIPELINE CONF {score:.0f}: {reason}"
    if not pos:
        locked,_,_=reentry_guard_status(engine,side)
        if locked:return
        if score>=80:
            set_position(engine,side,price,signal_id,A)
            position_event("OPEN",side,price,signal_id,note,engine)
        return
    if pos["side"]==side:
        # Keep the lifecycle auditable but do not average/re-enter.
        position_event("CONFIRM",side,price,signal_id,note,engine)
        return
    # A contrary CONF is an exit/switch candidate only if the current POS is already pressured.
    if str(pos.get("state") or "HOLD").upper()=="PRESSURE" and score>=92:
        position_event("EXIT",pos["side"],price,signal_id,"PIPELINE CONF SWITCH / "+note,engine)
        clear_position(engine,"PIPELINE_CONF_SWITCH")
        set_position(engine,side,price,signal_id,A)
        position_event("SWITCH",side,price,signal_id,note,engine)

def evaluate_5m15m_pipeline():
    """Primary v6.45 signal hierarchy.

    15M EARLY = strict reversal watch: liquidity location + real failure/absorption + second clue.
    5M pressure = internal execution trigger only; no standalone signal is stored.
    TURN L/S   = strict 15M reversal context + 5M execution turn + higher-TF context.

    VWAP is evidence only; it never emits standalone VWAP L/S signals.
    """
    global stage_15m_context, stage_last_bucket
    if not last_price or len(trades)<20:return
    if len(candles["15M"])<8 or len(candles["5M"])<8:return
    now=int(time.time()*1000); p=float(last_price)
    A15=max(atr_tf("15M",24),1.0); A5=max(atr_tf("5M",24),1.0)
    c15=list(candles["15M"])[-1]; c5=list(candles["5M"])[-1]
    sh15=_bar_shape(c15); sh5=_bar_shape(c5)
    H15,L15=liquidity_tf("15M",100,30)
    w10,w30=flow(10000),flow(30000); oi60=oi_delta(); inten=flow_intensity()
    v15=vwap_signal_context("15M",A15); v5=vwap_signal_context("5M",A5)
    b15=int(now//(15*60*1000)); b5=int(now//(5*60*1000))

    # ----- 15M EARLY: location is mandatory, then require at least one leading failure clue.
    if H15 is not None:
        for side in ("SHORT","LONG"):
            wanted=-1 if side=="SHORT" else 1
            reasons=[]; groups=set(); score=0
            if side=="SHORT":
                near_liq=(float(c15["high"])>=H15-.18*A15 or p>=H15-.12*A15)
                failed=(float(c15["high"])>H15 and float(c15["close"])<H15+.04*A15)
                absorption=(w30["ratio"]>.10 and sh15["upper"]>=max(sh15["body"],.18*sh15["rng"]) and sh15["close_pos"]<.72)
                fade=(w30["ratio"]>.06 and w10["ratio"]<w30["ratio"]-.10)
                vwap_ev=v15["short_reaction"] or (v15.get("vwap") is not None and p>v15["vwap"]+.35*A15 and sh15["close_pos"]<.65)
                oi_trap=(oi60>.004 and (failed or absorption))
            else:
                near_liq=(float(c15["low"])<=L15+.18*A15 or p<=L15+.12*A15)
                failed=(float(c15["low"])<L15 and float(c15["close"])>L15-.04*A15)
                absorption=(w30["ratio"]<-.10 and sh15["lower"]>=max(sh15["body"],.18*sh15["rng"]) and sh15["close_pos"]>.28)
                fade=(w30["ratio"]<-.06 and w10["ratio"]>w30["ratio"]+.10)
                vwap_ev=v15["long_reaction"] or (v15.get("vwap") is not None and p<v15["vwap"]-.35*A15 and sh15["close_pos"]>.35)
                oi_trap=(oi60>.004 and (failed or absorption))
            if near_liq:
                groups.add("LIQ"); reasons.append("LIQ"); score+=30
            if failed:
                groups.add("FAIL"); reasons.append("FAIL"); score+=24
            if absorption:
                groups.add("ABS"); reasons.append("ABS"); score+=22
            if fade:
                groups.add("FADE"); reasons.append("FLOW_FADE"); score+=15
            if vwap_ev:
                groups.add("VWAP"); reasons.append("VWAP_CTX"); score+=12
            if oi_trap:
                groups.add("OI"); reasons.append("OI_TRAP"); score+=8
            key="EARLY_"+("SHORT" if side=="SHORT" else "LONG")
            lead_count=len(groups.intersection({"FAIL","ABS","FADE","VWAP"}))
            hard_failure=("FAIL" in groups or "ABS" in groups)
            # v6.49: EARLY is no longer a loose hint. Keep only liquidity-location setups
            # with a real failure/absorption clue plus a second independent leading clue.
            leading=(lead_count>=2 and hard_failure)
            if near_liq and leading and score>=68 and stage_last_bucket.get(key)!=b15:
                reason="+".join(reasons)
                _save_stage_signal("EARLY S" if side=="SHORT" else "EARLY L",side,min(score,100),"15M",A15,reason,False)
                stage_15m_context[side]={"ts":now,"price":p,"score":score,"reason":reason,"groups":sorted(groups)}
                stage_last_bucket[key]=b15

    # Expire old EARLY context after 60 minutes.
    for side in ("LONG","SHORT"):
        ctx=stage_15m_context.get(side)
        if ctx and now-int(ctx.get("ts",0))>60*60*1000:
            stage_15m_context[side]=None

    # ----- 5M pressure turn: one per direction per native 5M bar.
    for side in ("SHORT","LONG"):
        own=trend_bias_tf("5M"); wanted=-1 if side=="SHORT" else 1
        if side=="SHORT":
            flow_turn=(w10["ratio"]<=-.07 and w30["ratio"]<=-.015)
            candle_turn=(float(c5["close"])<float(c5["open"]) and sh5["close_pos"]<.45)
            vwap_turn=v5["short_reaction"]
        else:
            flow_turn=(w10["ratio"]>=.07 and w30["ratio"]>=.015)
            candle_turn=(float(c5["close"])>float(c5["open"]) and sh5["close_pos"]>.55)
            vwap_turn=v5["long_reaction"]
        struct_turn=(own==wanted)
        pressure_groups=[]; ps=0
        if flow_turn: pressure_groups.append("FLOW"); ps+=38
        if struct_turn: pressure_groups.append("STRUCT"); ps+=28
        if vwap_turn: pressure_groups.append("VWAP"); ps+=18
        if candle_turn: pressure_groups.append("PRICE"); ps+=16
        key="5M_"+("SHORT" if side=="SHORT" else "LONG")
        # v6.49: 5M pressure remains an INTERNAL trigger. It is no longer persisted as
        # a standalone signal because micro-flow/one-candle turns created too much noise.
        pressure_ok=flow_turn and len(pressure_groups)>=3 and ps>=72

        # ----- TURN: strong 15M reversal location + 5M execution turn + higher-TF context.
        ctx15=stage_15m_context.get(side)
        if not pressure_ok or not ctx15: continue
        age=now-int(ctx15.get("ts",0))
        if age>45*60*1000: continue
        # Prevent "already dumped/pumped, now chase" entries.
        favorable=((float(ctx15["price"])-p) if side=="SHORT" else (p-float(ctx15["price"])))
        if favorable>0.75*A15: continue
        h1=trend_bias_tf("1H"); h4=trend_bias_tf("4H")
        if h1==-wanted: continue
        conf_groups={"15M_LOCATION","5M_PRESSURE","FLOW"}
        conf_reasons=["15M:"+str(ctx15.get("reason") or "EARLY"),"5M:"+"+".join(pressure_groups)]
        cs=42 + min(float(ctx15.get("score") or 0)*.30,24) + min(ps*.24,20)
        if vwap_turn or ("VWAP" in (ctx15.get("groups") or [])):
            conf_groups.add("VWAP"); cs+=8
        if h1==wanted:
            conf_groups.add("1H"); conf_reasons.append("1H_ALIGN"); cs+=8
        if h4==wanted:
            conf_groups.add("4H"); conf_reasons.append("4H_ALIGN"); cs+=4
        if inten>=1.05:
            conf_groups.add("ACTIVITY"); conf_reasons.append("ACTIVITY"); cs+=4
        cs=max(0,min(cs,100))
        lead_groups=set(ctx15.get("groups") or [])
        hard_location=("FAIL" in lead_groups or "ABS" in lead_groups)
        dual_failure=("FAIL" in lead_groups and "ABS" in lead_groups)
        # Prefer alignment with the 1H direction. A genuine failed auction + absorption
        # may still flag a counter-trend turn, but weak counter-trend micro-flow cannot.
        bias_ok=(h1==wanted) or dual_failure
        key="TURN_"+("SHORT" if side=="SHORT" else "LONG")
        turn_ok=(cs>=88 and len(conf_groups)>=5 and hard_location and bias_ok)
        if turn_ok and stage_last_bucket.get(key)!=b15:
            bias_tag=("TREND" if h1==wanted else "COUNTER")
            reason=bias_tag+" | "+" | ".join(conf_reasons)
            _save_stage_signal("TURN S" if side=="SHORT" else "TURN L",side,cs,"5M",A5,reason,True)
            stage_last_bucket[key]=b15

def evaluate_tf_engine(engine):
    """Independent timeframe engine: its own candles, liquidity, ATR, arm, signals and position."""
    if engine not in ("5M","15M","1H","4H") or not last_price or len(trades)<20:return
    H,L=liquidity_tf(engine,100 if engine in ("5M","15M") else 80,30 if engine in ("5M","15M") else 20)
    if H is None:return
    A=atr_tf(engine,24); c=list(candles[engine])[-1] if candles[engine] else None
    if not c:return
    w10,w30=flow(10000),flow(30000); oi60=oi_delta(); inten=flow_intensity(); now=int(time.time()*1000)
    pressure=mtf_pressure_snapshot(now); p5=float(pressure['timeframes']['5M']['ratio']); p15=float(pressure['timeframes']['15M']['ratio']); pslow=float(pressure['slow_delta'])
    arm=tf_arms.get(engine); prev=tf_prev_d10.get(engine,0.0)
    slow=engine in ("1H","4H")
    sweep=.035 if slow else .04; closebuf=.10 if slow else .08
    refresh=30*60*1000 if engine=="4H" else 15*60*1000 if engine=="1H" else 180000 if engine=="15M" else 120000
    expiry=60*60*1000 if engine=="4H" else 45*60*1000 if engine=="1H" else 300000 if engine=="15M" else 150000
    cooldown=30*60*1000 if engine=="4H" else 20*60*1000 if engine=="1H" else 180000 if engine=="15M" else 90000
    if c["high"]>H+sweep*A and c["close"]<H+closebuf*A:
        if not arm or arm["dir"]!="S" or now-arm["ts"]>refresh: arm={"dir":"S","level":H,"ext":c["high"],"ts":now,"peak":w30["ratio"]}
    if c["low"]<L-sweep*A and c["close"]>L-closebuf*A:
        if not arm or arm["dir"]!="L" or now-arm["ts"]>refresh: arm={"dir":"L","level":L,"ext":c["low"],"ts":now,"peak":w30["ratio"]}
    if arm and now-arm["ts"]>expiry: arm=None
    tf_arms[engine]=arm
    if arm and now-tf_last_signal_ts.get(engine,0)>cooldown:
        reclaim_buf=.025 if slow else .03
        if arm["dir"]=="L":
            reclaim=last_price>arm["level"]+reclaim_buf*A
            hot=(pslow<-.055 or p5<-.07 or p15<-.05)
            micro=(w10["ratio"]>=(-.015 if slow else .0) and (w10["ratio"]-prev>(.045 if slow else .06) or w10["ratio"]>(.045 if slow else .065)))
            flip=minute_pressure_gate('LONG',reversal=True,loose=slow,snap=pressure) and micro
            score=35+(20 if hot else 0)+(25 if flip else 0)+(10 if oi60<-.010 else 0)+(5 if inten>1.0 else 0)+(5 if book_imb>-.20 else 0)
            if reclaim and hot and flip and score>=80:
                ok,_why=precision_signal_gate(engine,"LONG",score,A)
                if ok:
                    nm=("5M+15M CONF L" if engine=="5M" and "15M aligned" in _why else "15M CONF L" if engine=="15M" else f"{engine} L")
                    save_signal(nm,"LONG",score,arm["level"],arm["ext"],{"d10":w10["ratio"],"d30":w30["ratio"],"oi60":oi60,"flow":inten,"book":book_imb},engine,A)
        else:
            reclaim=last_price<arm["level"]-reclaim_buf*A
            hot=(pslow>.055 or p5>.07 or p15>.05)
            micro=(w10["ratio"]<=(.015 if slow else .0) and (prev-w10["ratio"]>(.045 if slow else .06) or w10["ratio"]<(-.045 if slow else -.065)))
            flip=minute_pressure_gate('SHORT',reversal=True,loose=slow,snap=pressure) and micro
            score=35+(20 if hot else 0)+(25 if flip else 0)+(10 if oi60<-.010 else 0)+(5 if inten>1.0 else 0)+(5 if book_imb<.20 else 0)
            if reclaim and hot and flip and score>=80:
                ok,_why=precision_signal_gate(engine,"SHORT",score,A)
                if ok:
                    nm=("5M+15M CONF S" if engine=="5M" and "15M aligned" in _why else "15M CONF S" if engine=="15M" else f"{engine} S")
                    save_signal(nm,"SHORT",score,arm["level"],arm["ext"],{"d10":w10["ratio"],"d30":w30["ratio"],"oi60":oi60,"flow":inten,"book":book_imb},engine,A)
    tf_prev_d10[engine]=w10["ratio"]

def ensure_retest_consumed_loaded():
    global retest_consumed_loaded, retest_consumed_break
    if retest_consumed_loaded:return
    c=db(); rows=c.execute("SELECT key,value_int FROM setup_runtime_state WHERE key IN ('RETEST_CONSUMED_LONG','RETEST_CONSUMED_SHORT')").fetchall(); c.close()
    for k,v in rows:
        side='LONG' if str(k).endswith('LONG') else 'SHORT'; retest_consumed_break[side]=max(int(retest_consumed_break.get(side,0)),int(v or 0))
    retest_consumed_loaded=True

def mark_retest_consumed(side, break_ts):
    ts=int(break_ts or 0)
    if ts<=int(retest_consumed_break.get(side,0)):return
    retest_consumed_break[side]=ts; now=int(time.time()*1000); c=db()
    c.execute("""INSERT INTO setup_runtime_state(key,value_int,updated_ts) VALUES(?,?,?)
      ON CONFLICT(key) DO UPDATE SET value_int=MAX(setup_runtime_state.value_int,excluded.value_int),updated_ts=excluded.updated_ts""",(f'RETEST_CONSUMED_{side}',ts,now))
    c.commit(); c.close()

def _research_context(**extra):
    """Compact snapshot used by setup-stage and shadow research rows."""
    ctx={
      "price":float(last_price or 0),"d10":float(flow(10000)["ratio"]),"d30":float(flow(30000)["ratio"]),
      "oi60":float(oi_delta()),"flow":float(flow_intensity()),"book":float(book_imb),
      "bias":market_bias_snapshot().get("overall"),
      "t5":trend_bias_tf("5M"),"t15":trend_bias_tf("15M"),"t1":trend_bias_tf("1H"),"t4":trend_bias_tf("4H"),
      "vwap5":vwap_tf("5M"),"sma20":sma_tf("5M",20),"sma60":sma_tf("5M",60),
      "atr5":atr_tf("5M",24),"atr15":atr_tf("15M",24)
    }
    ctx.update(extra)
    return ctx

def sync_setup_stage_events(items, context):
    """Persist meaningful lifecycle transitions without 350 ms stage flapping.

    If a setup jumps from IDLE straight to ARMED/TRIGGER in one evaluation, PREP
    (and ARMED before TRIGGER) are still written once so lead-time research remains possible.
    """
    global setup_stage_last
    now=int(time.time()*1000); rank={"IDLE":0,"PREP":1,"ARMED":2,"TRIGGER":3}
    grouped={}
    for x in items:
        grouped.setdefault((x.get("setup"),x.get("side")),[]).append(x)
    rows=[]
    for setup in ("PULLBACK","REVERSAL","RETEST"):
        for side in ("LONG","SHORT"):
            key=(setup,side); seq=grouped.get(key,[])
            # Final displayed state is the most advanced state in this evaluation.
            final=max(seq,key=lambda x:(rank.get(str(x.get("stage")),0),float(x.get("score") or 0))) if seq else None
            final_stage=str(final.get("stage") if final else "IDLE")
            old=str(setup_stage_last.get(key,"IDLE"))
            to_write=[]
            if final_stage=="IDLE":
                if old!="IDLE": to_write=[None]
            elif rank.get(final_stage,0)>rank.get(old,0):
                # Preserve skipped intermediate stages only when advancing from a lower state.
                available={str(x.get("stage")):x for x in seq}
                for st in ("PREP","ARMED","TRIGGER"):
                    if rank[st]>rank.get(old,0) and rank[st]<=rank[final_stage] and st in available:
                        to_write.append(available[st])
            elif final_stage!=old:
                # A real de-escalation (e.g. TRIGGER display expired -> PREP/ARMED) is one transition.
                to_write=[final]
            # Same final stage => no write, even if PREP is also emitted internally this cycle.
            for item in to_write:
                stage=str(item.get("stage") if item else "IDLE")
                score=float(item.get("score") or 0) if item else 0.0
                level=float(item.get("level") or 0) if item and item.get("level") is not None else None
                meta=(item.get("meta") or {}) if item else {}
                ref=str(meta.get("ref") or "")
                reason=" | ".join(item.get("reasons") or []) if item else "NO_ACTIVE_SETUP"
                ctx=dict(context); ctx.update({"setup_meta":meta,"expires_ts":item.get("expires_ts") if item else None})
                rows.append((now,setup,side,stage,score,float(last_price or 0),level,ref,reason,json.dumps(ctx,separators=(",",":"),ensure_ascii=False)))
                old=stage
            setup_stage_last[key]=final_stage
    if rows:
        c=db(); c.executemany("""INSERT INTO setup_stage_events(ts,setup,side,stage,score,price,level,ref,reason,context_json)
          VALUES(?,?,?,?,?,?,?,?,?,?)""",rows); c.commit(); c.close()

def record_shadow(setup, variant, side, score, reasons, bucket, context):
    """Legacy PRICE/FAST comparison frozen in v6.65. Historical rows are preserved."""
    global shadow_last_bucket
    if not LEGACY_SHADOW_ENABLED:return False
    key=(setup,variant,side)
    if shadow_last_bucket.get(key)==bucket:return False
    now=int(time.time()*1000); px=float(last_price or 0)
    ctx=json.dumps(context,separators=(",",":"),ensure_ascii=False)
    c=db()
    try:
        c.execute("""INSERT OR IGNORE INTO shadow_signals(ts,bucket,setup,variant,side,entry,score,reason,context_json)
          VALUES(?,?,?,?,?,?,?,?,?)""",(now,int(bucket),setup,variant,side,px,float(score)," | ".join(reasons),ctx))
        c.commit(); shadow_last_bucket[key]=bucket; return True
    finally:c.close()

def update_shadow_research():
    if not last_price:return
    now=int(time.time()*1000); px=float(last_price); c=db()
    rows=c.execute("""SELECT s.id,s.ts,s.side,s.entry,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60
      FROM shadow_signals s LEFT JOIN shadow_research r ON r.shadow_id=s.id
      WHERE s.ts>=? ORDER BY s.ts DESC LIMIT 3000""",(now-3*60*60*1000,)).fetchall()
    for sid,ts,side,entry,mfe,mae,p5,p15,p30,p60 in rows:
        if not entry:continue
        entry=float(entry); move=(px-entry)/entry*100.0; fav=move if side=="LONG" else -move; adv=-move if side=="LONG" else move
        mfe=max(float(mfe or 0),fav,0.0); mae=max(float(mae or 0),adv,0.0); vals=[p5,p15,p30,p60]
        for i,m in enumerate((5,15,30,60)):
            if vals[i] is None and now-int(ts)>=m*60000: vals[i]=px
        c.execute("""INSERT INTO shadow_research(shadow_id,started_ts,last_ts,mfe_pct,mae_pct,p5,p15,p30,p60)
          VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(shadow_id) DO UPDATE SET last_ts=excluded.last_ts,mfe_pct=excluded.mfe_pct,mae_pct=excluded.mae_pct,
          p5=COALESCE(shadow_research.p5,excluded.p5),p15=COALESCE(shadow_research.p15,excluded.p15),p30=COALESCE(shadow_research.p30,excluded.p30),p60=COALESCE(shadow_research.p60,excluded.p60)""",
          (sid,ts,now,mfe,mae,*vals))
    c.commit(); c.close()

def update_user_trade_research():
    """Fixed-horizon benchmark for the user's own B/S entries, independent of when they exit."""
    if not last_price:return
    now=int(time.time()*1000); px=float(last_price); c=db()
    rows=c.execute("""SELECT e.id,e.ts,e.side,e.price,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60
      FROM user_position_events e LEFT JOIN user_trade_research r ON r.open_event_id=e.id
      WHERE e.event='OPEN' AND e.ts>=? ORDER BY e.ts DESC LIMIT 2000""",(now-3*60*60*1000,)).fetchall()
    for eid,ts,side,entry,mfe,mae,p5,p15,p30,p60 in rows:
        if not entry:continue
        entry=float(entry); move=(px-entry)/entry*100.0; fav=move if side=="LONG" else -move; adv=-move if side=="LONG" else move
        mfe=max(float(mfe or 0),fav,0.0); mae=max(float(mae or 0),adv,0.0); vals=[p5,p15,p30,p60]
        for i,m in enumerate((5,15,30,60)):
            if vals[i] is None and now-int(ts)>=m*60000: vals[i]=px
        c.execute("""INSERT INTO user_trade_research(open_event_id,started_ts,last_ts,side,entry,mfe_pct,mae_pct,p5,p15,p30,p60)
          VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(open_event_id) DO UPDATE SET last_ts=excluded.last_ts,mfe_pct=excluded.mfe_pct,mae_pct=excluded.mae_pct,
          p5=COALESCE(user_trade_research.p5,excluded.p5),p15=COALESCE(user_trade_research.p15,excluded.p15),p30=COALESCE(user_trade_research.p30,excluded.p30),p60=COALESCE(user_trade_research.p60,excluded.p60)""",
          (eid,ts,now,side,entry,mfe,mae,*vals))
    c.commit(); c.close()

def evaluate_setup_signals():
    """v6.57 THREE SETUP LEAD research engine.

    The engine separates early recognition from the actual timing signal:
      PREP   = context is favorable and price is approaching a useful location.
      ARMED  = price has touched/swept/broken the location; wait for response.
      TRIGGER= price response + micro structure + flow confirmation. Only this is stored.

    Direction is never decided by 10s/30s flow alone. Flow is the final timing layer.
    """
    global stage_last_bucket, setup_watch, setup_flow_prev, retest_consumed_break
    ensure_retest_consumed_loaded()
    if not last_price or len(trades)<20 or len(candles['5M'])<25 or len(candles['15M'])<12:return
    now=int(time.time()*1000); p=float(last_price)
    A5=max(atr_tf('5M',24),1.0); A15=max(atr_tf('15M',24),1.0)
    a5=list(candles['5M']); c5=a5[-1]; sh5=_bar_shape(c5)
    a15=list(candles['15M']); c15=a15[-1]; sh15=_bar_shape(c15)
    w10,w30=flow(10000),flow(30000); inten=flow_intensity(); oi60=oi_delta()
    d10=float(w10['ratio']); d30=float(w30['ratio'])
    pressure=mtf_pressure_snapshot(now)
    prev10=float(setup_flow_prev.get('d10') or 0.0)
    flow_accel=d10-prev10
    bias=market_bias_snapshot(); overall=str(bias.get('overall') or 'MIXED')
    t5,t15,t1,t4=(trend_bias_tf(x) for x in ('5M','15M','1H','4H'))
    b5=int(now//300000); b15=int(now//900000)
    items=[]

    # Update the comparison sample only every ~5s so acceleration is not packet noise.
    if now-int(setup_flow_prev.get('ts') or 0)>=5000:
        setup_flow_prev={'ts':now,'d10':d10,'d30':d30}

    def micro_break(side):
        a=[x for x in candles.get('1M',[]) if x.get('confirm')=='1']
        if len(a)<4:return False
        z=a[-4:-1]
        return p>max(float(x['high']) for x in z) if side=='LONG' else p<min(float(x['low']) for x in z)

    def micro_timing(side, loose=False):
        # 10s/30s only answer 'now?', never the directional thesis.
        if side=='LONG':
            base=(d10>=(-.018 if loose else .005) and d10>=d30-.055)
            turn=(flow_accel>=.025 or d10>=.07 or (d30<-.06 and d10>d30+.065))
        else:
            base=(d10<=(.018 if loose else -.005) and d10<=d30+.055)
            turn=(flow_accel<=-.025 or d10<=-.07 or (d30>.06 and d10<d30-.065))
        return base and turn

    def flow_timing(side, loose=False, reversal=False):
        return minute_pressure_gate(side,reversal=reversal,loose=loose,snap=pressure) and micro_timing(side,loose=loose)

    def fast_flow_timing(side, reversal=False):
        # Research comparator: minute pressure is mandatory, micro confirmation is deliberately faster.
        if not minute_pressure_gate(side,reversal=reversal,loose=True,snap=pressure):return False
        if side=='LONG':return d10>=-.035 and (flow_accel>=.012 or d10>=d30+.028)
        return d10<=.035 and (flow_accel<=-.012 or d10<=d30-.028)

    def put(setup,side,stage,score,reasons,level=None,expires=None,meta=None):
        item={'setup':setup,'side':side,'stage':stage,'score':round(max(0,min(100,float(score))),1),
              'reasons':list(reasons),'level':level,'expires_ts':expires,'meta':meta or {}}
        items.append(item)
        return item

    def arm_key(setup,side): return f'{setup}_{side}'

    def set_arm(setup,side,level,extreme,ttl_ms,meta=None):
        k=arm_key(setup,side); old=setup_arms.get(k)
        # A fired setup is displayed for ~2 minutes, then it must be allowed to form a fresh arm.
        if old and old.get('fired') and now-int(old.get('fired_ts') or now)>120000:
            if setup=='RETEST': mark_retest_consumed(side,int((old.get('meta') or {}).get('break_ts') or 0))
            setup_arms[k]=None; old=None
        # Keep an existing arm unless the level materially changed or it expired.
        if old and now<int(old.get('expires_ts') or 0) and abs(float(old.get('level') or level)-level)<=.18*A5:
            if side=='LONG': old['extreme']=min(float(old.get('extreme') or extreme),float(extreme))
            else: old['extreme']=max(float(old.get('extreme') or extreme),float(extreme))
            old['meta']={**(old.get('meta') or {}),**(meta or {})}
            return old
        setup_arms[k]={'ts':now,'level':float(level),'extreme':float(extreme),'expires_ts':now+ttl_ms,
                       'fired':False,'meta':meta or {}}
        return setup_arms[k]

    def valid_arm(setup,side):
        a=setup_arms.get(arm_key(setup,side))
        if a and a.get('fired') and now-int(a.get('fired_ts') or now)>120000:
            if setup=='RETEST': mark_retest_consumed(side,int((a.get('meta') or {}).get('break_ts') or 0))
            setup_arms[arm_key(setup,side)]=None; return None
        if a and now>=int(a.get('expires_ts') or 0):
            if setup=='RETEST': mark_retest_consumed(side,int((a.get('meta') or {}).get('break_ts') or 0))
            setup_arms[arm_key(setup,side)]=None; return None
        return a

    def clear_arm(setup,side):
        a=setup_arms.get(arm_key(setup,side))
        if setup=='RETEST' and a: mark_retest_consumed(side,int((a.get('meta') or {}).get('break_ts') or 0))
        setup_arms[arm_key(setup,side)]=None

    def emit(setup,side,score,reasons,bucket,level=None):
        key=f'SETUP_{setup}_{side}'
        if stage_last_bucket.get(key)==bucket:return False
        # v6.65: PB/RV/RT are internal EXEC feeder states only. Their lifecycle is still
        # persisted in setup_stage_events/shadow tables, but no public raw signal row is created.
        stage_last_bucket[key]=bucket
        a=setup_arms.get(arm_key(setup,side))
        if a:
            a['fired']=True; a['fired_ts']=now; a['trigger_score']=float(score); a['trigger_reasons']=list(reasons)
        if setup=='RETEST' and a: mark_retest_consumed(side,int((a.get('meta') or {}).get('break_ts') or 0))
        put(setup,side,'TRIGGER',score,reasons,level,a.get('expires_ts') if a else None,(a.get('meta') if a else {}))
        return True

    # ------------------------------------------------------------------
    # 1) PULLBACK: higher-TF trend -> approach MA/VWAP -> probe/reclaim -> micro break + flow.
    refs=[('SMA20',sma_tf('5M',20)),('SMA60',sma_tf('5M',60)),('VWAP',vwap_tf('5M',240))]
    refs=[(n,float(v)) for n,v in refs if v]
    for side in ('LONG','SHORT'):
        wanted=1 if side=='LONG' else -1
        slow_votes=sum(x==wanted for x in (t15,t1,t4)); opp_votes=sum(x==-wanted for x in (t15,t1,t4))
        hard_conflict=(t1==-wanted and t4==-wanted)
        if not refs or slow_votes<2 or hard_conflict:
            clear_arm('PULLBACK',side); continue
        ranked=sorted(refs,key=lambda z:abs(p-z[1]))
        near_name,ref=ranked[0]; dist=abs(p-ref)/A5
        confluence=sum(abs(ref-v)<=.22*A5 for _,v in refs)
        approaching=(p>=ref-.12*A5 and p<=ref+.72*A5) if side=='LONG' else (p<=ref+.12*A5 and p>=ref-.72*A5)
        touched=(float(c5['low'])<=ref+.16*A5 and p>=ref-.20*A5) if side=='LONG' else (float(c5['high'])>=ref-.16*A5 and p<=ref+.20*A5)
        context_score=28+slow_votes*11+(6 if overall.endswith('LONG' if side=='LONG' else 'SHORT') else 0)+(4*max(0,confluence-1))
        if approaching and dist<=.72:
            put('PULLBACK',side,'PREP',context_score+max(0,16-dist*20),[f'HTF_ALIGN={slow_votes}',f'APPROACH_{near_name}',f'CONFLUENCE={confluence}'],ref,meta={'ref':near_name,'dist_atr':round(dist,4),'confluence':confluence,'slow_votes':slow_votes})
        if touched:
            arm=set_arm('PULLBACK',side,ref,float(c5['low'] if side=='LONG' else c5['high']),22*60*1000,
                        {'ref':near_name,'slow_votes':slow_votes,'confluence':confluence})
        else: arm=valid_arm('PULLBACK',side)
        if not arm: continue
        if arm.get('fired'):
            if now-int(arm.get('fired_ts') or now)<=120000:
                put('PULLBACK',side,'TRIGGER',arm.get('trigger_score',90),arm.get('trigger_reasons') or ['TRIGGERED'],arm.get('level'),arm.get('expires_ts'))
            continue
        ref=float(arm['level'])
        # If the pullback slices too deeply through the location, wait for a fresh setup.
        invalid=(p<ref-.48*A5) if side=='LONG' else (p>ref+.48*A5)
        if invalid: clear_arm('PULLBACK',side); continue
        response=(p>=ref+.025*A5 and sh5['close_pos']>=.54) if side=='LONG' else (p<=ref-.025*A5 and sh5['close_pos']<=.46)
        candle_ok=(float(c5['close'])>=float(c5['open'])) if side=='LONG' else (float(c5['close'])<=float(c5['open']))
        micro=micro_break(side); ft=flow_timing(side,loose=True)
        not_chasing=(p-ref<=.52*A5) if side=='LONG' else (ref-p<=.52*A5)
        score=context_score+18+ (14 if response else 0)+(10 if micro else 0)+(11 if ft else 0)+(5 if candle_ok else 0)+(4 if inten>=.9 else 0)
        put('PULLBACK',side,'ARMED',score,[f'{arm.get("meta",{}).get("ref",near_name)}_TOUCH','WAIT_RECLAIM','WAIT_MTF_PRESSURE+MICRO'],ref,arm['expires_ts'],arm.get('meta'))
        price_ready=response and candle_ok and micro and not_chasing
        fast_flow=fast_flow_timing(side,reversal=False)
        shadow_ctx=_research_context(setup='PULLBACK',ref=arm.get('meta',{}).get('ref',near_name),level=ref,price_ready=price_ready,base_flow=ft,fast_flow=fast_flow,score=score)
        if price_ready: record_shadow('PULLBACK','PRICE',side,score,['PRICE_RESPONSE','1M_STRUCTURE','NO_FLOW_REQUIREMENT'],b5,shadow_ctx)
        if price_ready and fast_flow: record_shadow('PULLBACK','FAST',side,score,['PRICE_RESPONSE','1M_STRUCTURE','FAST_FLOW'],b5,shadow_ctx)
        if response and candle_ok and micro and ft and not_chasing and score>=80 and not arm.get('fired'):
            emit('PULLBACK',side,score,[f'HTF_ALIGN={slow_votes}',f'{arm.get("meta",{}).get("ref",near_name)}_RECLAIM','1M_STRUCTURE','MTF_PRESSURE+MICRO'],b5,ref)

    # ------------------------------------------------------------------
    # 2) REVERSAL: approach 15M liquidity -> sweep/absorption -> failed auction -> 5M/1M turn.
    H15,L15=liquidity_tf('15M',100,30)
    p5r=float(pressure['timeframes']['5M']['ratio']); p15r=float(pressure['timeframes']['15M']['ratio'])
    pfast=float(pressure['fast_delta']); pslow=float(pressure['slow_delta'])
    if H15 is not None and L15 is not None:
        for side in ('LONG','SHORT'):
            wanted=1 if side=='LONG' else -1; level=float(L15 if side=='LONG' else H15)
            loc_dist=((p-level)/A15) if side=='LONG' else ((level-p)/A15)
            near=(-.20<=loc_dist<=.42)
            if near:
                put('REVERSAL',side,'PREP',52+max(0,18-abs(loc_dist)*28),['15M_LIQ_APPROACH',f'DIST={loc_dist:.2f}ATR'],level,meta={'dist_atr':round(loc_dist,4)})
            if side=='SHORT':
                swept=float(c15['high'])>level+.015*A15
                wick=sh15['upper']>=max(sh15['body'],.18*sh15['rng'])
                aggression=(p5r>.055 or p15r>.045)
                failed=p<level-.015*A15
                reject5=sh5['close_pos']<.48 and float(c5['close'])<=float(c5['open'])
            else:
                swept=float(c15['low'])<level-.015*A15
                wick=sh15['lower']>=max(sh15['body'],.18*sh15['rng'])
                aggression=(p5r<-.055 or p15r<-.045)
                failed=p>level+.015*A15
                reject5=sh5['close_pos']>.52 and float(c5['close'])>=float(c5['open'])
            absorption=near and wick and aggression
            if swept or absorption:
                ext=float(c15['low'] if side=='LONG' else c15['high'])
                arm=set_arm('REVERSAL',side,level,ext,50*60*1000,{'swept':swept,'absorption':absorption})
            else: arm=valid_arm('REVERSAL',side)
            if not arm: continue
            if arm.get('fired'):
                if now-int(arm.get('fired_ts') or now)<=120000:
                    put('REVERSAL',side,'TRIGGER',arm.get('trigger_score',90),arm.get('trigger_reasons') or ['TRIGGERED'],arm.get('level'),arm.get('expires_ts'))
                continue
            level=float(arm['level'])
            # A true reversal must get back inside liquidity; if auction accepts far beyond, invalidate.
            accepted=(p<level-.55*A15) if side=='LONG' else (p>level+.55*A15)
            if accepted: clear_arm('REVERSAL',side); continue
            micro=micro_break(side); ft=flow_timing(side,loose=True,reversal=True)
            fade=((pfast-pslow)>=.055) if side=='LONG' else ((pfast-pslow)<=-.055)
            ctx_align=sum(x==wanted for x in (t1,t4))
            score=46+(16 if arm.get('meta',{}).get('swept') else 0)+(12 if arm.get('meta',{}).get('absorption') else 0)+(12 if failed else 0)+(8 if reject5 else 0)+(8 if micro else 0)+(8 if (ft or fade) else 0)+(4 if oi60>.003 else 0)+(4*ctx_align)
            put('REVERSAL',side,'ARMED',score,['LIQ_TOUCHED','WAIT_FAILED_AUCTION','WAIT_PRICE_MTF_TURN+MICRO'],level,arm['expires_ts'],arm.get('meta'))
            strong_price=failed and reject5 and micro
            timing=(ft or (fade and minute_pressure_gate(side,reversal=True,loose=True,snap=pressure) and micro_timing(side,loose=True)))
            fast_timing=fast_flow_timing(side,reversal=True)
            shadow_ctx=_research_context(setup='REVERSAL',level=level,strong_price=strong_price,base_flow=timing,fast_flow=fast_timing,swept=bool(arm.get('meta',{}).get('swept')),absorption=bool(arm.get('meta',{}).get('absorption')),score=score)
            if strong_price: record_shadow('REVERSAL','PRICE',side,score,['FAILED_AUCTION','5M_REJECT','1M_STRUCTURE','NO_FLOW_REQUIREMENT'],b15,shadow_ctx)
            if strong_price and fast_timing: record_shadow('REVERSAL','FAST',side,score,['FAILED_AUCTION','5M_REJECT','1M_STRUCTURE','FAST_FLOW'],b15,shadow_ctx)
            if strong_price and timing and score>=82 and not arm.get('fired'):
                reasons=['15M_LIQ','SWEEP' if arm.get('meta',{}).get('swept') else 'ABSORB','FAILED_AUCTION','5M_REJECT','1M_STRUCTURE','MTF_PRESSURE_TURN+MICRO']
                emit('REVERSAL',side,score,reasons,b15,level)

    # ------------------------------------------------------------------
    # 3) RETEST: warn near range edge -> arm only after confirmed displacement break -> first hold/reject.
    confirmed5=[x for x in a5 if x.get('confirm')=='1']
    if len(confirmed5)>=24:
        base=confirmed5[-21:]
        hi=max(float(x['high']) for x in base[:-1]); lo=min(float(x['low']) for x in base[:-1])
        width=(hi-lo)/A5
        for side,level in (('LONG',hi),('SHORT',lo)):
            wanted=1 if side=='LONG' else -1
            slow_ok=(t15==wanted or t1==wanted) and not (t15==-wanted and t1==-wanted)
            dist=((level-p)/A5) if side=='LONG' else ((p-level)/A5)
            compression=width<=4.2
            if slow_ok and compression and -.12<=dist<=.38:
                put('RETEST',side,'PREP',54+(10 if t15==wanted else 0)+(8 if t1==wanted else 0)+(6 if width<=3.0 else 0),['RANGE_EDGE','COMPRESSION','WAIT_BREAK'],level,meta={'range_width_atr':round(width,4)})

        # Scan the last 4 confirmed bars so a restart shortly after the break can still recover the arm.
        for idx in range(max(20,len(confirmed5)-4),len(confirmed5)):
            br=confirmed5[idx]; prior=confirmed5[max(0,idx-20):idx]
            if len(prior)<12: continue
            hi0=max(float(x['high']) for x in prior); lo0=min(float(x['low']) for x in prior)
            sh=_bar_shape(br); body_ratio=sh['body']/max(sh['rng'],1e-9)
            if float(br['close'])>hi0+.055*A5 and body_ratio>=.48 and sh['close_pos']>=.66 and int(br['ts'])>int(retest_consumed_break.get('LONG',0)):
                set_arm('RETEST','LONG',hi0,float(br['low']),32*60*1000,{'break_ts':br['ts'],'break_px':br['close']})
            if float(br['close'])<lo0-.055*A5 and body_ratio>=.48 and sh['close_pos']<=.34 and int(br['ts'])>int(retest_consumed_break.get('SHORT',0)):
                set_arm('RETEST','SHORT',lo0,float(br['high']),32*60*1000,{'break_ts':br['ts'],'break_px':br['close']})

        for side in ('LONG','SHORT'):
            arm=valid_arm('RETEST',side)
            if not arm: continue
            if arm.get('fired'):
                if now-int(arm.get('fired_ts') or now)<=120000:
                    put('RETEST',side,'TRIGGER',arm.get('trigger_score',90),arm.get('trigger_reasons') or ['TRIGGERED'],arm.get('level'),arm.get('expires_ts'))
                continue
            wanted=1 if side=='LONG' else -1; level=float(arm['level'])
            # Do not call the breakout itself a retest. Wait at least ~1 minute after break close.
            age=now-int(arm.get('meta',{}).get('break_ts') or arm['ts'])
            if age<60*1000: continue
            deep=(p<level-.28*A5) if side=='LONG' else (p>level+.28*A5)
            if deep: clear_arm('RETEST',side); continue
            in_zone=(level-.12*A5<=p<=level+.25*A5) if side=='LONG' else (level-.25*A5<=p<=level+.12*A5)
            touch=(float(c5['low'])<=level+.14*A5) if side=='LONG' else (float(c5['high'])>=level-.14*A5)
            hold=(p>=level and sh5['close_pos']>=.54) if side=='LONG' else (p<=level and sh5['close_pos']<=.46)
            micro=micro_break(side); ft=flow_timing(side,loose=True)
            slow_ok=(t15==wanted or t1==wanted) and not (t15==-wanted and t1==-wanted)
            score=48+(12 if slow_ok else 0)+(16 if in_zone and touch else 0)+(12 if hold else 0)+(8 if micro else 0)+(8 if ft else 0)+(4 if inten>=.9 else 0)
            put('RETEST',side,'ARMED',score,['BREAK_CONFIRMED','WAIT_LEVEL_RETEST','WAIT_HOLD_MTF+MICRO'],level,arm['expires_ts'],arm.get('meta'))
            price_ready=slow_ok and in_zone and touch and hold and micro
            fast_flow=fast_flow_timing(side,reversal=False)
            shadow_ctx=_research_context(setup='RETEST',level=level,break_ts=arm.get('meta',{}).get('break_ts'),price_ready=price_ready,base_flow=ft,fast_flow=fast_flow,score=score)
            if price_ready: record_shadow('RETEST','PRICE',side,score,['FIRST_RETEST','LEVEL_HOLD','1M_STRUCTURE','NO_FLOW_REQUIREMENT'],b5,shadow_ctx)
            if price_ready and fast_flow: record_shadow('RETEST','FAST',side,score,['FIRST_RETEST','LEVEL_HOLD','1M_STRUCTURE','FAST_FLOW'],b5,shadow_ctx)
            if slow_ok and in_zone and touch and hold and micro and ft and score>=82:
                emit('RETEST',side,score,['STRUCT_BREAK','FIRST_RETEST','LEVEL_HOLD','1M_STRUCTURE','MTF_PRESSURE_RESUME+MICRO'],b5,level)

    # Keep one record per setup/side, preferring the most advanced state.
    rank={'IDLE':0,'PREP':1,'ARMED':2,'TRIGGER':3}
    dedup={}
    for x in items:
        k=(x['setup'],x['side']); old=dedup.get(k)
        if old is None or (rank.get(x['stage'],0),x['score'])>(rank.get(old['stage'],0),old['score']):dedup[k]=x
    out=list(dedup.values())
    out.sort(key=lambda x:(rank.get(x['stage'],0),x['score']),reverse=True)
    setup_watch={'updated_ts':now,'best':out[0] if out else None,'items':out,
                 'pressure':pressure,
                 'flow':{'d10':round(d10,4),'d30':round(d30,4),'accel':round(flow_accel,4),'intensity':round(float(inten),2),
                         'role':'MICRO_TIMING_ONLY'}}
    sync_setup_stage_events(items,_research_context(flow_accel=round(flow_accel,6),overall=overall,A5=A5,A15=A15,mtf_pressure={k:round(float(v.get('ratio') or 0),4) for k,v in pressure['timeframes'].items()},pressure_turn=pressure.get('turn_side'),pressure_ready=pressure.get('ready')))
    update_decision_layer(out, now)

def _save_decision_event(event, side, action, evidence, opposite, setups, reasons, now=None):
    """Append a human-facing decision transition without mutating raw signal history."""
    global decision_last_event_sig
    now=int(now or time.time()*1000)
    sig=(str(event),str(side),str(action),str(setups),int(now//300000))
    # BIAS/INVALIDATE transitions are unique by state; TRIGGER is unique per 5M decision bucket.
    if decision_last_event_sig==sig:return False
    c=db();
    lo=(now//300000)*300000; hi=lo+300000
    exists=c.execute("SELECT id FROM decision_events WHERE event=? AND side=? AND ts>=? AND ts<? LIMIT 1",(event,side,lo,hi)).fetchone()
    if exists:
        c.close(); decision_last_event_sig=sig; return False
    c.execute("""INSERT INTO decision_events(ts,event,side,action,evidence,opposite_evidence,setups,reason,price)
      VALUES(?,?,?,?,?,?,?,?,?)""",(now,event,side,action,float(evidence),float(opposite),str(setups or ''),' | '.join(reasons or []),float(last_price or 0)))
    c.commit(); c.close(); decision_last_event_sig=sig; return True


def update_decision_layer(items, now=None):
    """Resolve raw 3-SETUP observations into one stable human-facing state.

    Design goals:
      * raw PULLBACK / REVERSAL / RETEST continue to record unchanged;
      * repeated same-side setups become one decision, not many chart labels;
      * opposite evidence produces WAIT/CONFLICT instead of instant LONG<->SHORT flipping;
      * LONG -> NEUTRAL -> SHORT (and reverse) is mandatory unless the state starts neutral;
      * raw setup score is only a small input because it is not calibrated probability.
    """
    global decision_state, decision_last_trigger_bucket
    now=int(now or time.time()*1000)
    items=list(items or [])
    bias=market_bias_snapshot(); bscore=float(bias.get('score') or 0)
    pressure=mtf_pressure_snapshot(now); pdelta=float(pressure.get('weighted_delta') or 0); pfast=float(pressure.get('fast_delta') or 0)
    stage_rank={'PREP':1,'ARMED':2,'TRIGGER':3}
    # Keep the most advanced observation for each setup/side.
    best={}
    for x in items:
        setup=str(x.get('setup') or '').upper(); side=str(x.get('side') or '').upper(); stage=str(x.get('stage') or '').upper()
        if setup not in ('PULLBACK','REVERSAL','RETEST') or side not in ('LONG','SHORT') or stage not in stage_rank:continue
        k=(setup,side); old=best.get(k)
        if old is None or (stage_rank[stage],float(x.get('score') or 0))>(stage_rank.get(str(old.get('stage') or ''),0),float(old.get('score') or 0)):best[k]=x

    def side_info(side):
        xs=[x for (setup,s),x in best.items() if s==side]
        stages=[str(x.get('stage') or '').upper() for x in xs]
        active=[x for x in xs if str(x.get('stage') or '').upper() in ('ARMED','TRIGGER')]
        triggers=[x for x in xs if str(x.get('stage') or '').upper()=='TRIGGER']
        # Stage/consensus dominate. The old 80-100 raw score contributes <= 8 points only.
        ev=0.0
        for x in xs:
            st=str(x.get('stage') or '').upper(); raw=max(0.0,min(100.0,float(x.get('score') or 0)))
            ev += {'PREP':11.0,'ARMED':25.0,'TRIGGER':39.0}[st] + max(0.0,min(8.0,(raw-55.0)*0.18))
        if len(active)>=2: ev+=13.0
        if len(active)>=3: ev+=7.0
        wanted=1 if side=='LONG' else -1
        if bscore*wanted>0: ev+=min(18.0,abs(bscore)*2.2)
        elif bscore*wanted<0: ev-=min(13.0,abs(bscore)*1.7)
        # v6.63: multi-minute pressure participates in direction; micro 10s/30s does not.
        if pressure.get('ready'):
            align=pdelta*wanted; fast_align=pfast*wanted
            ev += max(-12.0,min(14.0,align*52.0))
            ev += max(-6.0,min(7.0,fast_align*24.0))
            if pressure.get('turn_side')==side:ev+=6.0
            elif pressure.get('turn_side') in ('LONG','SHORT'):ev-=4.0
        ev=max(0.0,min(100.0,ev))
        return {'evidence':ev,'xs':xs,'active':active,'triggers':triggers,'stages':stages,
                'setups':[str(x.get('setup')) for x in active or xs]}

    L,S=side_info('LONG'),side_info('SHORT')
    le,se=L['evidence'],S['evidence']; margin=abs(le-se)
    preferred='LONG' if le>se else 'SHORT' if se>le else 'NEUTRAL'
    top=max(le,se)
    candidate=preferred if top>=56 and margin>=12 else 'NEUTRAL'
    conflict=(le>=46 and se>=46 and margin<18) or (bool(L['triggers']) and bool(S['triggers']))
    if conflict:candidate='NEUTRAL'

    cur=str(decision_state.get('bias') or 'NEUTRAL')
    pending=decision_state.get('pending_side'); psince=int(decision_state.get('pending_since') or 0)
    changed=False; invalidated=False

    def begin_pending(side):
        nonlocal pending,psince
        if pending!=side: pending=side; psince=now

    if cur=='NEUTRAL':
        if candidate in ('LONG','SHORT') and not conflict:
            begin_pending(candidate)
            if now-psince>=12000:
                cur=candidate; changed=True; pending=None; psince=0
        else:
            pending=None; psince=0
    else:
        opp='SHORT' if cur=='LONG' else 'LONG'
        cur_ev=le if cur=='LONG' else se; opp_ev=se if cur=='LONG' else le
        opp_trig=bool((S if opp=='SHORT' else L)['triggers'])
        structure_invalid=(bscore<=-2 if cur=='LONG' else bscore>=2)
        hard_opp=(opp_ev>=74 and opp_ev-cur_ev>=16) or (opp_trig and opp_ev>=66 and opp_ev-cur_ev>=12)
        if candidate==cur and not conflict:
            pending=None; psince=0
        elif structure_invalid or hard_opp or conflict:
            begin_pending('NEUTRAL')
            # Conflict neutralizes faster; a full opposite reversal still cannot skip NEUTRAL.
            wait_ms=8000 if conflict else 12000
            if now-psince>=wait_ms:
                old=cur; cur='NEUTRAL'; changed=True; invalidated=True; pending=None; psince=0
                _save_decision_event('INVALIDATE',old,'WAIT',cur_ev,opp_ev,'',
                    ['STRUCTURE_INVALID' if structure_invalid else 'OPPOSITE_PRESSURE','NEUTRAL_BEFORE_SWITCH'],now)
        else:
            pending=None; psince=0

    # Human-facing action is only executable when it agrees with the stable bias.
    chosen=L if cur=='LONG' else S if cur=='SHORT' else None
    other=S if cur=='LONG' else L if cur=='SHORT' else None
    action='WAIT'; trigger_setup=None; agreement=[]; reasons=[]
    if conflict:
        action='CONFLICT'; reasons=['BOTH_SIDES_ACTIVE','WAIT_FOR_RESOLUTION']
    elif chosen:
        agreement=list(dict.fromkeys(chosen['setups']))
        trig=chosen['triggers']; armed=[x for x in chosen['xs'] if str(x.get('stage') or '').upper()=='ARMED']; prep=[x for x in chosen['xs'] if str(x.get('stage') or '').upper()=='PREP']
        ce=float(chosen['evidence']); oe=float(other['evidence'] if other else 0)
        pressure_exec_ok=(not pressure.get('ready')) or ((pdelta*(1 if cur=='LONG' else -1))>=-.08 and (pfast*(1 if cur=='LONG' else -1))>=-.07)
        if trig and ce>=62 and ce-oe>=12 and pressure_exec_ok:
            action='TRIGGER'; trigger_setup='+'.join(dict.fromkeys(str(x.get('setup')) for x in trig))
            reasons=['STABLE_'+cur,'MTF_PRESSURE_OK','TRIGGER_'+trigger_setup]
            if len(chosen['active'])>=2:reasons.append('SETUP_AGREEMENT')
        elif trig and not pressure_exec_ok:
            action='READY'; reasons=['STABLE_'+cur,'RAW_TRIGGER','WAIT_MTF_PRESSURE']
        elif armed and ce>=54 and ce-oe>=8:
            action='READY'; reasons=['STABLE_'+cur,'ARMED_'+('+'.join(dict.fromkeys(str(x.get('setup')) for x in armed)))]
            if len(chosen['active'])>=2:reasons.append('SETUP_AGREEMENT')
        elif prep:
            action='WATCH'; reasons=['STABLE_'+cur,'PREP_'+('+'.join(dict.fromkeys(str(x.get('setup')) for x in prep)))]
        else:
            action='WAIT'; reasons=['STABLE_'+cur,'NO_ACTIVE_SETUP']
    else:
        if candidate in ('LONG','SHORT'):
            action='WATCH'; reasons=['CANDIDATE_'+candidate,'HYSTERESIS_WAIT']
        else: reasons=['NO_STABLE_DIRECTION']

    if changed and not invalidated and cur in ('LONG','SHORT'):
        ce=le if cur=='LONG' else se; oe=se if cur=='LONG' else le
        _save_decision_event('BIAS',cur,'WATCH',ce,oe,'+'.join((L if cur=='LONG' else S)['setups']),['BIAS_ESTABLISHED'],now)

    # One execution marker per side per native 5M bucket. Raw signals can fire freely underneath.
    if action=='TRIGGER' and cur in ('LONG','SHORT'):
        b=int(now//300000)
        if decision_last_trigger_bucket.get(cur)!=b:
            decision_last_trigger_bucket[cur]=b
            ce=le if cur=='LONG' else se; oe=se if cur=='LONG' else le
            _save_decision_event('TRIGGER',cur,action,ce,oe,trigger_setup or '+'.join(agreement),reasons,now)

    decision_state={
      'updated_ts':now,'bias':cur,'action':action,'candidate':candidate,
      'long_evidence':round(le,1),'short_evidence':round(se,1),'margin':round(margin,1),
      'agreement':agreement,'reasons':reasons,'pending_side':pending,'pending_since':psince,
      'bias_since':(now if changed and cur!='NEUTRAL' else int(decision_state.get('bias_since') or 0)),
      'trigger_setup':trigger_setup,'conflict':bool(conflict),
      'market_bias':bias,'pressure':{'state':pressure.get('state'),'turn_side':pressure.get('turn_side'),'weighted_delta':round(pdelta,4),'fast_delta':round(pfast,4),'slow_delta':round(float(pressure.get('slow_delta') or 0),4),'ready':bool(pressure.get('ready'))},
      'note':'evidence strength / MTF pressure conflict resolver; not win probability'
    }
    return decision_state


def evaluate():
    # v6.57: old WATCH/EARLY/TURN pipeline is retained in source for audit/history,
    # but new live 5M signal generation is the three-setup engine only.
    evaluate_setup_signals()

def evaluate_scalp():
    """SCALP engine: 3M liquidity/structure + 1M sweep/reclaim + micro-flow flip."""
    global scalp_arm,scalp_prev_d10
    if not last_price or len(trades)<20:return
    H,L=liquidity_tf("3M",100,30)
    if H is None:return
    A=atr_tf("1M",30); c=list(candles["1M"])[-1] if candles["1M"] else None
    if not c:return
    w10,w30=flow(10000),flow(30000);oi60=oi_delta();inten=flow_intensity();now=int(time.time()*1000)
    # tighter sweep thresholds than CORE, shorter arm/cooldown
    if c["high"]>H+.025*A and c["close"]<H+.06*A:
        if not scalp_arm or scalp_arm["dir"]!="S" or now-scalp_arm["ts"]>60000:
            scalp_arm={"dir":"S","level":H,"ext":c["high"],"ts":now,"peak":w30["ratio"]}
    if c["low"]<L-.025*A and c["close"]>L-.06*A:
        if not scalp_arm or scalp_arm["dir"]!="L" or now-scalp_arm["ts"]>60000:
            scalp_arm={"dir":"L","level":L,"ext":c["low"],"ts":now,"peak":w30["ratio"]}
    if scalp_arm and now-scalp_arm["ts"]>90000:scalp_arm=None
    if scalp_arm and now-scalp_last_signal_ts>45000:
        if scalp_arm["dir"]=="L":
            reclaim=last_price>scalp_arm["level"]+.015*A
            hot=scalp_arm["peak"]<-.10 or w30["ratio"]<-.13 or scalp_prev_d10<-.15
            flip=w10["ratio"]>.06 and w10["ratio"]-scalp_prev_d10>.11
            score=35+(20 if hot else 0)+(25 if flip else 0)+(10 if oi60<-.012 else 0)+(5 if inten>1.03 else 0)+(5 if book_imb>-.20 else 0)
            if reclaim and hot and flip and score>=80:
                save_signal("SCALP L","LONG",score,scalp_arm["level"],scalp_arm["ext"],
                  {"d10":w10["ratio"],"d30":w30["ratio"],"oi60":oi60,"flow":inten,"book":book_imb},"SCALP",A)
        else:
            reclaim=last_price<scalp_arm["level"]-.015*A
            hot=scalp_arm["peak"]>.10 or w30["ratio"]>.13 or scalp_prev_d10>.15
            flip=w10["ratio"]<-.06 and scalp_prev_d10-w10["ratio"]>.11
            score=35+(20 if hot else 0)+(25 if flip else 0)+(10 if oi60<-.012 else 0)+(5 if inten>1.03 else 0)+(5 if book_imb<.20 else 0)
            if reclaim and hot and flip and score>=80:
                save_signal("SCALP S","SHORT",score,scalp_arm["level"],scalp_arm["ext"],
                  {"d10":w10["ratio"],"d30":w30["ratio"],"oi60":oi60,"flow":inten,"book":book_imb},"SCALP",A)
    scalp_prev_d10=w10["ratio"]




def evaluate_macro():
    # v6.45: preserve 1H / 4H signal engines exactly as before.
    evaluate_tf_engine("1H")
    evaluate_tf_engine("4H")


def position_path_extremes(pos):
    """Rebuild price extremes from confirmed/live 1M candles since the position opened.
    This makes MFE/MAE survive Railway deploys/restarts instead of only tracking ticks
    observed by the current server process.
    """
    entry=float(pos["entry"])
    opened=int(pos.get("opened_ts") or 0)
    highs=[]; lows=[]
    for bar in candles.get("1M",[]):
        try:
            if int(bar.get("ts",0)) >= opened:
                highs.append(float(bar["high"]))
                lows.append(float(bar["low"]))
        except Exception:
            pass
    if last_price:
        highs.append(float(last_price)); lows.append(float(last_price))
    # Include persisted values too, so rebuilding can never shrink an existing excursion.
    bp=pos.get("best_price"); wp=pos.get("worst_price")
    if bp is not None:
        highs.append(float(bp)); lows.append(float(bp))
    if wp is not None:
        highs.append(float(wp)); lows.append(float(wp))
    highs.append(entry); lows.append(entry)
    return max(highs), min(lows)

def update_position_excursions():
    """Persist MFE/MAE and reconstruct the full path after restart/deploy."""
    if not last_price:
        return
    now=int(time.time()*1000)
    for pos in all_positions():
        engine=pos["engine"]; side=pos["side"]; entry=float(pos["entry"])
        path_high,path_low=position_path_extremes(pos)
        if side=="LONG":
            best=path_high; worst=path_low
            mfe=max(0.0,best-entry); mae=max(0.0,entry-worst)
        else:
            best=path_low; worst=path_high
            mfe=max(0.0,entry-best); mae=max(0.0,worst-entry)
        c=db()
        c.execute("""UPDATE engine_positions
                     SET best_price=?,worst_price=?,mfe=?,mae=?,updated_ts=?
                     WHERE engine=?""",
                  (best,worst,mfe,mae,now,engine))
        c.commit(); c.close()

def manage_position_reversal():
    """Research position state machine:
    OPEN/HOLD <-> PRESSURE -> EXIT or SWITCH.
    Price moving against entry alone never closes a position.
    Decisions combine 1M/3M/5M/15M pressure, price acceptance/retrace and context.
    10s/30s are a small final timing input only. Engines remain independent.
    """
    if not last_price:return
    d10=flow(10000)["ratio"]; d30=flow(30000)["ratio"]
    intensity=flow_intensity(); oi=oi_delta(60000); book=book_imb
    now=int(time.time()*1000); pressure=mtf_pressure_snapshot(now)
    pd=float(pressure.get('weighted_delta') or 0); pf=float(pressure.get('fast_delta') or 0); ps=float(pressure.get('slow_delta') or 0)
    # Ensure reversal logic uses restart-safe, reconstructed MFE/MAE.
    update_position_excursions()

    for pos in all_positions():
        engine=pos["engine"]; side=pos["side"]; entry=float(pos["entry"])
        A=max(float(pos.get("atr_open") or 1.0),1e-9)
        best=float(pos.get("best_price") or entry); worst=float(pos.get("worst_price") or entry)
        old_state=pos.get("state") or "HOLD"; psince=pos.get("pressure_since")

        if side=="LONG":
            best=max(best,last_price); worst=min(worst,last_price)
            mfe=max(0,best-entry); mae=max(0,entry-worst)
            adverse=(entry-last_price)/A
            retrace=(best-last_price)/A
            # Opposite (sell) pressure: MTF flow dominates; micro flow cannot flip the position by itself.
            pscore=0
            if pressure.get('ready') and pd<=-.08: pscore+=30
            if pressure.get('ready') and pf<=-.10: pscore+=22
            if pressure.get('ready') and ps<=-.06: pscore+=15
            if pressure.get('turn_side')=='SHORT': pscore+=8
            if d10<=-.08: pscore+=5
            if d30<=-.05: pscore+=5
            if intensity>=0.80: pscore+=7
            if book<=-.20: pscore+=4
            if oi>0 and pd<0: pscore+=4
            if adverse>=0.25 or retrace>=0.32: pscore+=10
            opposite_side="SHORT"
        else:
            best=min(best,last_price); worst=max(worst,last_price)
            mfe=max(0,entry-best); mae=max(0,worst-entry)
            adverse=(last_price-entry)/A
            retrace=(last_price-best)/A
            # Opposite (buy) pressure: MTF flow dominates; micro flow cannot flip the position by itself.
            pscore=0
            if pressure.get('ready') and pd>=.08: pscore+=30
            if pressure.get('ready') and pf>=.10: pscore+=22
            if pressure.get('ready') and ps>=.06: pscore+=15
            if pressure.get('turn_side')=='LONG': pscore+=8
            if d10>=.08: pscore+=5
            if d30>=.05: pscore+=5
            if intensity>=0.80: pscore+=7
            if book>=.20: pscore+=4
            if oi>0 and pd>0: pscore+=4
            if adverse>=0.25 or retrace>=0.32: pscore+=10
            opposite_side="LONG"

        # Larger timeframe needs more persistence before switching.
        pressure_threshold=55 if engine=="SCALP" else 60 if engine in ("5M","15M") else 65
        switch_threshold=80 if engine=="SCALP" else 82 if engine in ("5M","15M") else 85
        min_pressure_ms=12000 if engine=="SCALP" else 25000 if engine=="5M" else 35000 if engine=="15M" else 60000 if engine=="1H" else 90000

        new_state=old_state
        new_psince=psince
        if pscore>=pressure_threshold:
            if old_state!="PRESSURE":
                new_state="PRESSURE"; new_psince=now
                position_event("PRESSURE",side,last_price,pos["signal_id"],
                               f"opposite pressure score={pscore}",engine)
        else:
            if old_state=="PRESSURE":
                new_state="HOLD"; new_psince=None
                position_event("HOLD",side,last_price,pos["signal_id"],
                               f"pressure released score={pscore}",engine)

        c=db()
        c.execute("""UPDATE engine_positions SET best_price=?,worst_price=?,mfe=?,mae=?,
                     state=?,pressure_since=?,pressure_score=?,updated_ts=? WHERE engine=?""",
                  (best,worst,mfe,mae,new_state,new_psince,pscore,now,engine))
        c.commit();c.close()

        # FLOW HOLD GUARD: do not let micro-flow churn a freshly opened research POS.
        # Structure invalidation remains available, so a clearly broken thesis can still exit.
        hold_ms=max(0, now-int(pos.get("opened_ts") or now))
        min_hold_ms={"SCALP":2*60*1000,"5M":8*60*1000,"15M":15*60*1000,
                     "1H":30*60*1000,"4H":60*60*1000}.get(engine,8*60*1000)
        flow_guard = hold_ms < min_hold_ms
        scalp_guard = engine=="SCALP" and hold_ms < 45000

        # STRUCTURE INVALIDATION: EXIT-only; never forces an opposite entry.
        # During the SCALP guard, only the wider emergency threshold is active.
        structure_exit_atr = 0.85 if engine=="SCALP" else 1.00 if engine in ("5M","15M") else 1.35
        structure_blocked = scalp_guard and adverse < 1.25
        if adverse >= structure_exit_atr and not structure_blocked:
            old_signal=pos["signal_id"]
            note=(f"STRUCTURE INVALIDATION adverse={adverse:.2f}ATR score={pscore} "
                  f"mtf={pd:.3f} fast={pf:.3f} slow={ps:.3f} d10={d10:.3f} d30={d30:.3f} flow={intensity:.2f} "
                  f"oi60={oi:.4f} book={book:.3f} mfe={mfe:.1f} mae={mae:.1f}")
            position_event("EXIT",side,last_price,old_signal,note,engine)
            set_reentry_guard(engine,side,last_price,A,"STRUCTURE_INVALIDATION")
            clear_position(engine,"STRUCTURE_INVALIDATION")
            continue

        persisted = new_state=="PRESSURE" and new_psince and now-int(new_psince)>=min_pressure_ms

        # Strong reversal: EXIT old side + SWITCH to opposite side.
        switch_accept = adverse>=0.38 if engine=="SCALP" else adverse>=0.45 if engine in ("5M","15M") else adverse>=0.55
        if persisted and pscore>=switch_threshold and switch_accept and not flow_guard:
            old_signal=pos["signal_id"]
            note=(f"FLOW SWITCH score={pscore} mtf={pd:.3f} fast={pf:.3f} slow={ps:.3f} d10={d10:.3f} d30={d30:.3f} "
                  f"flow={intensity:.2f} oi60={oi:.4f} book={book:.3f} mfe={mfe:.1f} mae={mae:.1f}")
            position_event("EXIT",side,last_price,old_signal,note,engine)
            set_reentry_guard(engine,side,last_price,A,"FLOW_SWITCH")
            clear_position(engine,"FLOW_SWITCH")
            set_position(engine,opposite_side,last_price,None,A)
            position_event("SWITCH",opposite_side,last_price,None,note,engine)
            continue

        # EXIT-only: current thesis is invalid enough to stop holding, but opposite side
        # is not strong enough for an immediate reverse position.
        exit_threshold=65 if engine=="SCALP" else 70 if engine in ("5M","15M") else 75
        exit_ms=30000 if engine=="SCALP" else 45000 if engine=="5M" else 60000 if engine=="15M" else 90000 if engine=="1H" else 120000
        exit_accept=adverse>=0.22 if engine=="SCALP" else adverse>=0.28 if engine in ("5M","15M") else adverse>=0.38
        exit_persisted = new_state=="PRESSURE" and new_psince and now-int(new_psince)>=exit_ms
        if exit_persisted and pscore>=exit_threshold and exit_accept and not flow_guard:
            old_signal=pos["signal_id"]
            note=(f"FLOW EXIT score={pscore} mtf={pd:.3f} fast={pf:.3f} slow={ps:.3f} d10={d10:.3f} d30={d30:.3f} "
                  f"flow={intensity:.2f} oi60={oi:.4f} book={book:.3f} mfe={mfe:.1f} mae={mae:.1f}")
            position_event("EXIT",side,last_price,old_signal,note,engine)
            set_reentry_guard(engine,side,last_price,A,"FLOW_EXIT")
            clear_position(engine,"FLOW_EXIT")
            continue

def update_signal_research():
    """Forward-only research tracker. Records MFE/MAE and first observed price
    after 5/15/30/60 minutes. This is measurement, not a trading signal.
    """
    if not last_price:return
    now=int(time.time()*1000); px=float(last_price); c=db()
    # Only recent signals need live excursion updates. Older rows remain available.
    rows=c.execute("""SELECT s.id,s.ts,s.side,s.entry,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60,r.b5,r.b10,r.b20,r.b30
      FROM signals s LEFT JOIN signal_research r ON r.signal_id=s.id
      WHERE s.ts>=? ORDER BY s.ts DESC LIMIT 2000""",(now-3*60*60*1000,)).fetchall()
    for sid,ts,side,entry,mfe,mae,p5,p15,p30,p60,b5,b10,b20,b30 in rows:
        if not entry: continue
        entry=float(entry); move=(px-entry)/entry*100.0
        fav=move if side=='LONG' else -move
        adv=-move if side=='LONG' else move
        mfe=max(float(mfe or 0),fav,0.0); mae=max(float(mae or 0),adv,0.0)
        vals=[p5,p15,p30,p60]; horizons=[5,15,30,60]
        for i,m in enumerate(horizons):
            if vals[i] is None and now-int(ts)>=m*60000: vals[i]=px
        bars=[b5,b10,b20,b30]; bar_h=[5,10,20,30]
        for i,nbar in enumerate(bar_h):
            if bars[i] is None and now-int(ts)>=nbar*5*60000: bars[i]=px
        c.execute("""INSERT INTO signal_research(signal_id,started_ts,last_ts,mfe_pct,mae_pct,p5,p15,p30,p60,b5,b10,b20,b30)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(signal_id) DO UPDATE SET
          last_ts=excluded.last_ts,mfe_pct=excluded.mfe_pct,mae_pct=excluded.mae_pct,
          p5=COALESCE(signal_research.p5,excluded.p5),p15=COALESCE(signal_research.p15,excluded.p15),
          p30=COALESCE(signal_research.p30,excluded.p30),p60=COALESCE(signal_research.p60,excluded.p60),
          b5=COALESCE(signal_research.b5,excluded.b5),b10=COALESCE(signal_research.b10,excluded.b10),
          b20=COALESCE(signal_research.b20,excluded.b20),b30=COALESCE(signal_research.b30,excluded.b30)""",
          (sid,ts,now,mfe,mae,*vals,*bars))
    c.commit();c.close()

def update_outcomes():
    """LEGACY BENCHMARK ONLY: fixed TP1-vs-SL. Never closes/removes a research position."""
    if not last_price:return
    now=int(time.time()*1000);c=db()
    rows=c.execute("SELECT id,ts,side,sl,tp1 FROM signals WHERE status='OPEN'").fetchall()
    for i,created_ts,side,sl,tp1 in rows:
        if now<=created_ts:continue
        st=None
        if side=="LONG":
            if last_price<=sl:st="LOSS"
            elif last_price>=tp1:st="WIN"
        else:
            if last_price>=sl:st="LOSS"
            elif last_price<=tp1:st="WIN"
        if st:c.execute("UPDATE signals SET status=?,closed_ts=? WHERE id=?",(st,now,i))
    c.commit();c.close()

async def seed():
    async with httpx.AsyncClient(timeout=15) as h:
        for label,bar in [("15M","15m"),("1D","1D")]:
            try:
                # v6.69 ONLY-LAB: seed only the data required by VWAP14 Direction + daily move.
                raw=[]; after=None
                pages=3 if label=="15M" else 1
                for page in range(pages):
                    params={"instId":INST,"bar":bar,"limit":300 if label=="15M" else 40}
                    if after is not None: params["after"]=str(after)
                    endpoint="https://www.okx.com/api/v5/market/history-candles" if page>0 else "https://www.okx.com/api/v5/market/candles"
                    r=(await h.get(endpoint,params=params)).json()
                    batch=r.get("data",[])
                    if not batch: break
                    raw.extend(batch)
                    after=min(int(x[0]) for x in batch)
                uniq={int(x[0]):x for x in raw}
                arr=[]
                for _,x in sorted(uniq.items()):
                    arr.append({"ts":int(x[0]),"open":float(x[1]),"high":float(x[2]),"low":float(x[3]),
                                "close":float(x[4]),"volume":float(x[5]),"confirm":x[8]})
                candles[label].extend(arr)
            except Exception as e: print("seed",label,e)

async def public_loop():
    """v6.69 ONLY-LAB: lightweight last-price feed + VWAP14 Direction evaluator only.

    EXEC / PB-RV-RT / EARLY / EVENT / MTF pressure / position-manager are deliberately
    not called here. Historical DB rows remain untouched for later audit.
    """
    global last_price
    while True:
        try:
            async with websockets.connect(PUB,ping_interval=20,ping_timeout=20) as ws:
                status["public"]="live"
                await ws.send(json.dumps({"op":"subscribe","args":[{"channel":"tickers","instId":INST}]}))
                async for raw in ws:
                    m=json.loads(raw)
                    if m.get("arg",{}).get("channel")!="tickers":
                        continue
                    for d in m.get("data",[]):
                        if d.get("last") is not None:
                            last_price=float(d["last"])
                    now_ms=int(time.time()*1000)
                    if now_ms-last_heavy_ms["evaluate"]>=1000:
                        evaluate_vwap_ma()
                        last_heavy_ms["evaluate"]=now_ms
                    if now_ms-last_heavy_ms["outcomes"]>=2000:
                        update_vwap_ma_research()
                        last_heavy_ms["outcomes"]=now_ms
        except Exception as e:
            status["public"]="reconnecting";print("public",e);await asyncio.sleep(2)

async def business_loop():
    mapping={"candle15m":"15M","candle1D":"1D"}
    while True:
        try:
            async with websockets.connect(BIZ,ping_interval=20,ping_timeout=20) as ws:
                status["business"]="live"
                await ws.send(json.dumps({"op":"subscribe","args":[{"channel":k,"instId":INST} for k in mapping]}))
                async for raw in ws:
                    m=json.loads(raw);ch=m.get("arg",{}).get("channel")
                    if ch not in mapping:continue
                    label=mapping[ch]
                    for x in m.get("data",[]):
                        c={"ts":int(x[0]),"open":float(x[1]),"high":float(x[2]),"low":float(x[3]),
                           "close":float(x[4]),"volume":float(x[5]),"confirm":x[8]}
                        if candles[label] and candles[label][-1]["ts"]==c["ts"]:candles[label][-1]=c
                        else:candles[label].append(c)
        except Exception as e:
            status["business"]="reconnecting";print("business",e);await asyncio.sleep(2)

async def snapshot_loop():
    while True:
        await asyncio.sleep(10)
        if last_price:
            a,b=flow(10000),flow(30000)
            c=db();c.execute("INSERT OR REPLACE INTO snapshots VALUES(?,?,?,?,?,?,?,?)",
                (int(time.time()*1000)//10000*10000,last_price,a["ratio"],b["ratio"],oi_delta(),flow_intensity(),book_imb,current_oi))
            c.commit();c.close(); persist_minute_flow()

@app.on_event("startup")
async def startup():
    c=db();c.close()
    await seed()
    # v6.69 ONLY-LAB: no minute-flow restore and no snapshot writer.
    asyncio.create_task(public_loop());asyncio.create_task(business_loop())

@app.get("/api/status")
def home():
    return {"service":"BTC MA Cycle Radar v7.0 KST","ok":True,"status":status,"signal_mode":SIGNAL_MODE,"legacy_engines_enabled":False}

def market_bias_snapshot():
    vals={tf:trend_bias_tf(tf) for tf in ("5M","15M","1H","4H")}
    # Higher timeframes carry more weight; this is context, not a trade signal.
    weights={"5M":1,"15M":2,"1H":3,"4H":4}
    score=sum(vals[tf]*weights[tf] for tf in vals)
    if score>=5: overall="STRONG LONG"
    elif score>=2: overall="LONG"
    elif score<=-5: overall="STRONG SHORT"
    elif score<=-2: overall="SHORT"
    else: overall="MIXED"
    return {"overall":overall,"score":score,**{tf:("BULL" if v>0 else "BEAR" if v<0 else "MIXED") for tf,v in vals.items()}}



def _clamp100(x):
    return max(0.0,min(100.0,float(x)))

def _sma_slope(tf,n,back=3):
    a=list(candles.get(tf,[]))
    if len(a)<n+back:return 0.0
    now=sum(float(x["close"]) for x in a[-n:])/n
    old=sum(float(x["close"]) for x in a[-n-back:-back])/n
    A=max(atr_tf(tf),1.0)
    return (now-old)/A

def turn_radar_snapshot():
    """Live diagnostic radar. Scores are heuristic evidence-strength scores, not probabilities."""
    p=float(last_price or 0); A=max(atr_tf("5M"),1.0)
    H,L=liquidity15(); b10=flow(10000); b30=flow(30000); inten=flow_intensity(); oi=oi_delta(); mtfp=mtf_pressure_snapshot()
    bias=market_bias_snapshot(); bscore=float(bias.get("score",0))
    # Regime: higher-TF structure, deliberately slow.
    regime_side="LONG" if bscore>=2 else "SHORT" if bscore<=-2 else "MIXED"
    regime_strength=_clamp100(abs(bscore)/10*100)
    # Location: closeness to 15M liquidity plus MA/VWAP confluence.
    dH=(H-p)/A if H is not None and p else 99; dL=(p-L)/A if L is not None and p else 99
    locS=_clamp100(100-max(0,dH)*38) if dH>=-.35 else 15
    locL=_clamp100(100-max(0,dL)*38) if dL>=-.35 else 15
    ma20=sma_tf("5M",20); ma60=sma_tf("5M",60); ma120=sma_tf("5M",120); vw=vwap_tf("5M",240)
    refs=[x for x in (ma20,ma60,ma120,vw) if x]
    prox=min([abs(p-x)/A for x in refs],default=9)
    ma_prox=_clamp100(100-prox*55)
    slope20=_sma_slope("5M",20); slope60=_sma_slope("5M",60)
    # Structure: 5M + 15M agreement, with MA slope as secondary context.
    t5=trend_bias_tf("5M"); t15=trend_bias_tf("15M")
    structL=_clamp100(35+25*(t5==1)+25*(t15==1)+15*(slope20>0))
    structS=_clamp100(35+25*(t5==-1)+25*(t15==-1)+15*(slope20<0))
    # v6.63: direction/pressure comes from 1M/3M/5M/15M. 10s/30s + book are shown as micro timing only.
    r10=float(b10["ratio"]); r30=float(b30["ratio"]); bk=float(book_imb or 0)
    pL=float(mtfp.get('long') or 50); pS=float(mtfp.get('short') or 50)
    turn=str(mtfp.get('turn_side') or 'NONE')
    flowL=_clamp100(pL + (8 if turn=='LONG' else 0) + 5*float(mtfp['timeframes']['1M'].get('absorb_long') or 0))
    flowS=_clamp100(pS + (8 if turn=='SHORT' else 0) + 5*float(mtfp['timeframes']['1M'].get('absorb_short') or 0))
    # MA/VWAP context: proximity + side/reclaim context, never a standalone trigger.
    ctxL=ma_prox; ctxS=ma_prox
    if vw:
        if p>=vw: ctxL=_clamp100(ctxL+12)
        else: ctxS=_clamp100(ctxS+12)
    if ma20:
        if p>=ma20: ctxL=_clamp100(ctxL+8)
        else: ctxS=_clamp100(ctxS+8)
    # Counter-trend turns require stronger evidence; regime is context, not a hard veto.
    def total(side):
        loc=locL if side=="LONG" else locS; st=structL if side=="LONG" else structS; fc=flowL if side=="LONG" else flowS; mc=ctxL if side=="LONG" else ctxS
        raw=.32*loc+.25*st+.23*fc+.20*mc
        if regime_side not in ("MIXED",side): raw-=8
        return _clamp100(raw)
    ltot,stot=total("LONG"),total("SHORT")
    side="LONG" if ltot>=stot else "SHORT"; score=max(ltot,stot)
    state="CONFIRMED" if score>=82 else "ARMED" if score>=70 else "WATCH" if score>=55 else "NEUTRAL"
    return {
      "side":side,"state":state,"score":round(score,1),"long_score":round(ltot,1),"short_score":round(stot,1),
      "regime":{"side":regime_side,"strength":round(regime_strength,1),"bias":bias},
      "location":{"long":round(locL,1),"short":round(locS,1),"d_high_atr":round(dH,2),"d_low_atr":round(dL,2),"zone":"UPPER" if locS>=70 else "LOWER" if locL>=70 else "MID"},
      "structure":{"long":round(structL,1),"short":round(structS,1),"5M":("BULL" if t5>0 else "BEAR" if t5<0 else "MIXED"),"15M":("BULL" if t15>0 else "BEAR" if t15<0 else "MIXED")},
      "ma_vwap":{"long":round(ctxL,1),"short":round(ctxS,1),"proximity":round(ma_prox,1),"sma20":ma20,"sma60":ma60,"sma120":ma120,"vwap":vw,"slope20":round(slope20,3),"slope60":round(slope60,3)},
      "flow_reversal":{"long":round(flowL,1),"short":round(flowS,1),"d10":r10,"d30":r30,"book":bk,"intensity":round(inten,2),"oi60":round(oi,4),"mtf":mtfp},
      "note":"scores are heuristic evidence strength, not probability"}

def daily_move_snapshot():
    a=list(candles.get("1D",[]))
    if not a:
        return {"open":None,"price":last_price,"usd":None,"pct":None}
    d=a[-1]; o=float(d.get("open") or 0); p=float(last_price or d.get("close") or 0)
    if not o or not p:
        return {"open":o or None,"price":p or None,"usd":None,"pct":None}
    diff=p-o
    return {"open":o,"price":p,"usd":diff,"pct":diff/o*100,"ts":d.get("ts")}

@app.get("/api/live")
def live():
    return {"price":last_price,"status":status,"vwap_ma":vwap_ma_state,"daily":daily_move_snapshot(),
            "signal_mode":SIGNAL_MODE,"legacy_engines_enabled":False}

@app.get("/api/setup-watch")
def api_setup_watch():
    # Compatibility endpoint kept for old clients; no legacy setup engine runs in v6.69.
    return {"disabled":True,"signal_mode":SIGNAL_MODE,"vwap_ma":vwap_ma_state}

@app.get("/api/pressure")
def api_pressure():
    return {"disabled":True,"state":"OFF","reason":"VWAP14_ONLY"}

@app.get("/api/decision")
def api_decision():
    return {"disabled":True,"action":"OFF","bias":"NEUTRAL"}

@app.get("/api/precursor")
def api_precursor():
    return {"disabled":True,"state":"OFF","side":"NONE"}

@app.get("/api/event-entry")
def api_event_entry():
    return {"disabled":True,"state":"OFF","side":"NONE"}

@app.get("/api/event-entry-events")
def api_event_entry_events(limit:int=500):
    c=db(); c.row_factory=sqlite3.Row
    rows=[dict(x) for x in c.execute("SELECT * FROM event_entry_events ORDER BY ts DESC LIMIT ?",(min(max(int(limit),1),2000),)).fetchall()]
    c.close(); return rows

@app.get("/api/vwap-ma")
def api_vwap_ma():
    return vwap_ma_state

@app.get("/api/vwap-ma-events")
def api_vwap_ma_events(limit:int=1000):
    c=db(); c.row_factory=sqlite3.Row
    rows=[dict(x) for x in c.execute("SELECT * FROM vwap_ma_events ORDER BY ts DESC LIMIT ?",(min(max(int(limit),1),3000),)).fetchall()]
    c.close(); return rows

@app.get("/api/precursor-events")
def api_precursor_events(limit:int=500):
    c=db(); c.row_factory=sqlite3.Row
    rows=[dict(x) for x in c.execute("SELECT * FROM precursor_events ORDER BY ts DESC LIMIT ?",(min(max(int(limit),1),2000),)).fetchall()]
    c.close(); return rows

@app.get("/api/decision-events")
def api_decision_events(limit:int=500):
    c=db(); c.row_factory=sqlite3.Row
    rows=[dict(x) for x in c.execute("SELECT * FROM decision_events ORDER BY ts DESC LIMIT ?",(min(max(int(limit),1),2000),)).fetchall()]
    c.close(); return rows

@app.get("/api/research/summary")
def api_research_summary():
    # v6.69 ONLY-LAB: do not scan retired research tables on every UI refresh.
    c=db(); c.row_factory=sqlite3.Row
    vwap_ma=[dict(x) for x in c.execute("""SELECT e.side,e.stage,COUNT(*) n,AVG(r.mfe_pct) avg_mfe,AVG(r.mae_pct) avg_mae
      FROM vwap_ma_events e LEFT JOIN vwap_ma_research r ON r.event_id=e.id GROUP BY e.side,e.stage ORDER BY e.side,e.stage""").fetchall()]
    c.close(); return {"mode":SIGNAL_MODE,"vwap_ma":vwap_ma}

@app.get("/api/research/export")
def api_research_export(limit:int=5000):
    lim=min(max(int(limit),1),20000); c=db(); c.row_factory=sqlite3.Row
    out={
      "generated_ts":int(time.time()*1000),
      "signals":[dict(x) for x in c.execute("""SELECT s.*,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60 FROM signals s LEFT JOIN signal_research r ON r.signal_id=s.id ORDER BY s.ts DESC LIMIT ?""",(lim,)).fetchall()],
      "setup_events":[dict(x) for x in c.execute("SELECT * FROM setup_stage_events ORDER BY ts DESC LIMIT ?",(lim,)).fetchall()],
      "shadow":[dict(x) for x in c.execute("""SELECT s.*,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60 FROM shadow_signals s LEFT JOIN shadow_research r ON r.shadow_id=s.id ORDER BY s.ts DESC LIMIT ?""",(lim,)).fetchall()],
      "user":[dict(x) for x in c.execute("SELECT * FROM user_trade_research ORDER BY started_ts DESC LIMIT ?",(lim,)).fetchall()],
      "decision":[dict(x) for x in c.execute("""SELECT e.*,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60 FROM decision_events e LEFT JOIN decision_research r ON r.decision_id=e.id ORDER BY e.ts DESC LIMIT ?""",(lim,)).fetchall()],
      "precursor":[dict(x) for x in c.execute("""SELECT e.*,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60,r.next_exec_ts,r.lead_sec FROM precursor_events e LEFT JOIN precursor_research r ON r.precursor_id=e.id ORDER BY e.ts DESC LIMIT ?""",(lim,)).fetchall()],
      "event_entry":[dict(x) for x in c.execute("""SELECT e.*,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60,r.next_exec_ts,r.lead_sec FROM event_entry_events e LEFT JOIN event_entry_research r ON r.event_id=e.id ORDER BY e.ts DESC LIMIT ?""",(lim,)).fetchall()],
      "vwap_ma":[dict(x) for x in c.execute("""SELECT e.*,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60 FROM vwap_ma_events e LEFT JOIN vwap_ma_research r ON r.event_id=e.id ORDER BY e.ts DESC LIMIT ?""",(lim,)).fetchall()]
    }
    c.close(); return out

@app.get("/api/research/export.csv")
def api_research_export_csv(kind:str="signals", limit:int=10000):
    import csv, io
    lim=min(max(int(limit),1),30000); k=kind.lower(); c=db(); c.row_factory=sqlite3.Row
    qs={
      "signals":"SELECT s.*,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60 FROM signals s LEFT JOIN signal_research r ON r.signal_id=s.id ORDER BY s.ts DESC LIMIT ?",
      "setup":"SELECT * FROM setup_stage_events ORDER BY ts DESC LIMIT ?",
      "shadow":"SELECT s.*,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60 FROM shadow_signals s LEFT JOIN shadow_research r ON r.shadow_id=s.id ORDER BY s.ts DESC LIMIT ?",
      "user":"SELECT * FROM user_trade_research ORDER BY started_ts DESC LIMIT ?",
      "decision":"SELECT e.*,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60 FROM decision_events e LEFT JOIN decision_research r ON r.decision_id=e.id ORDER BY e.ts DESC LIMIT ?",
      "precursor":"SELECT e.*,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60,r.next_exec_ts,r.lead_sec FROM precursor_events e LEFT JOIN precursor_research r ON r.precursor_id=e.id ORDER BY e.ts DESC LIMIT ?",
      "event":"SELECT e.*,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60,r.next_exec_ts,r.lead_sec FROM event_entry_events e LEFT JOIN event_entry_research r ON r.event_id=e.id ORDER BY e.ts DESC LIMIT ?",
      "vwapma":"SELECT e.*,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60 FROM vwap_ma_events e LEFT JOIN vwap_ma_research r ON r.event_id=e.id ORDER BY e.ts DESC LIMIT ?"
    }
    if k not in qs: c.close(); return Response("kind must be signals, setup, shadow, user, decision, precursor, event, or vwapma",status_code=400,media_type="text/plain")
    rows=[dict(x) for x in c.execute(qs[k],(lim,)).fetchall()]; c.close(); buf=io.StringIO()
    if rows:
        w=csv.DictWriter(buf,fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
    return Response(buf.getvalue(),media_type="text/csv; charset=utf-8",headers={"Content-Disposition":f"attachment; filename=btc_research_{k}.csv"})

@app.get("/api/signals")
def signals(limit:int=100, engine:str="ALL"):
    c=db();c.row_factory=sqlite3.Row
    if engine.upper() in ("SCALP","5M","15M","1H","4H"):
        rows=[dict(x) for x in c.execute("SELECT * FROM signals WHERE engine=? ORDER BY ts DESC LIMIT ?",(engine.upper(),min(limit,1000))).fetchall()]
    else:
        rows=[dict(x) for x in c.execute("SELECT * FROM signals ORDER BY ts DESC LIMIT ?",(min(limit,1000),)).fetchall()]
    c.close();return rows

@app.get("/api/signal-research")
def signal_research(limit:int=500, engine:str="ALL"):
    c=db();c.row_factory=sqlite3.Row
    where=""; args=[]
    if engine.upper() in ("SCALP","5M","15M","1H","4H"):
        where="WHERE s.engine=?";args.append(engine.upper())
    q=f"""SELECT s.id,s.ts,s.engine,s.name,s.side,s.entry,s.score,s.reason,r.mfe_pct,r.mae_pct,r.p5,r.p15,r.p30,r.p60,r.b5,r.b10,r.b20,r.b30
      FROM signals s LEFT JOIN signal_research r ON r.signal_id=s.id {where}
      ORDER BY s.ts DESC LIMIT ?"""
    args.append(min(max(int(limit),1),1500))
    rows=[dict(x) for x in c.execute(q,args).fetchall()];c.close();return rows

@app.get("/api/stats")
def stats():
    c=db();c.row_factory=sqlite3.Row
    out={}
    for eng in ("SCALP","5M","15M","1H","4H"):
        r=c.execute("""SELECT COUNT(*) total,
          SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END) wins,
          SUM(CASE WHEN status='LOSS' THEN 1 ELSE 0 END) losses,
          SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END) open
          FROM signals WHERE engine=?""",(eng,)).fetchone()
        d=dict(r);closed=(d["wins"] or 0)+(d["losses"] or 0)
        d["winrate"]=round(100*(d["wins"] or 0)/closed,1) if closed else None
        out[eng]=d
    c.close();return out



@app.get("/api/position")
def position():
    return current_position() or {"side":"FLAT"}

@app.post("/api/signals/record")
async def api_record_signal(request: Request):
    """Persist a browser-detected signal so refresh cannot erase it.
    Dedupes near-identical client submissions. Actual engine position is also
    opened/confirmed independently from the benchmark outcome.
    """
    x=await request.json()
    name=str(x.get("name") or "").strip()
    side=str(x.get("side") or x.get("dir") or "").upper()
    engine=str(x.get("engine") or "5M").upper()
    if side not in ("LONG","SHORT") or engine not in ("SCALP","5M","15M","1H","4H") or not name:
        return {"ok":False,"error":"invalid signal"}
    if engine=="SCALP":
        return {"ok":False,"error":"SCALP disabled in v6.45"}
    ts=int(x.get("ts") or time.time()*1000)
    entry=float(x.get("entry") or last_price or 0)
    sl=float(x.get("sl") or entry)
    tp1=float(x.get("tp1") or entry)
    tp2=float(x.get("tp2") or entry)
    score=float(x.get("score") or 0)
    m=x.get("metrics") or {}
    d10=float(m.get("d10") or 0); d30=float(m.get("d30") or 0)
    oi60=float(m.get("oi60") or 0); flowv=float(m.get("int") or m.get("flow") or 0)
    bookv=float(m.get("book") or 0)

    c=db()
    dup=c.execute("""SELECT id FROM signals
                     WHERE engine=? AND name=? AND side=? AND ABS(ts-?)<5000
                     ORDER BY id DESC LIMIT 1""",(engine,name,side,ts)).fetchone()
    if dup:
        sid=dup[0]
        c.close()
        # A saved signal and an actual research position are separate records.
        # If the signal already exists but its engine position is missing,
        # rebuild the position instead of returning early and leaving the UI FLAT.
        if not current_position(engine):
            A=atr_tf("1M",30) if engine=="SCALP" else atr_tf(engine,24) if engine in ("5M","15M","1H","4H") else atr5()
            apply_position_if_strong(engine,side,entry,sid,score,A,"BROWSER")
        return {"ok":True,"id":sid,"deduped":True,"position_recovered":bool(current_position(engine))}
    cur=c.execute("""INSERT INTO signals(ts,name,side,entry,sl,tp1,tp2,score,d10,d30,oi60,flow,book,status,engine)
                     VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',?)""",
                  (ts,name,side,entry,sl,tp1,tp2,score,d10,d30,oi60,flowv,bookv,engine))
    sid=cur.lastrowid;c.commit();c.close()
    A=atr_tf("1M",30) if engine=="SCALP" else atr_tf(engine,24) if engine in ("5M","15M","1H","4H") else atr5()
    apply_position_if_strong(engine,side,entry,sid,score,A,"BROWSER")
    return {"ok":True,"id":sid,"deduped":False}


def recover_missing_positions():
    """Self-heal engine_positions from the append-only lifecycle log.
    If an engine has no mutable active row but its latest lifecycle event is
    OPEN/CONFIRM/HOLD/PRESSURE/SWITCH (not EXIT), recreate the active row.
    Never resurrect an engine whose latest lifecycle event is EXIT.
    """
    c=db(); c.row_factory=sqlite3.Row
    for engine in ("SCALP","5M","15M","1H","4H"):
        if current_position(engine):
            continue
        ev=c.execute("""SELECT * FROM position_events WHERE engine=?
                        ORDER BY ts DESC,id DESC LIMIT 1""",(engine,)).fetchone()
        if not ev or str(ev["event"]).upper()=="EXIT":
            continue
        if str(ev["event"]).upper() not in ("OPEN","CONFIRM","HOLD","PRESSURE","SWITCH"):
            continue

        # Prefer the original OPEN/SWITCH for entry/side; fall back to latest event.
        anchor=c.execute("""SELECT * FROM position_events
                            WHERE engine=? AND event IN ('OPEN','SWITCH')
                            AND ts<=? ORDER BY ts DESC,id DESC LIMIT 1""",
                         (engine,ev["ts"])).fetchone()
        a=anchor or ev
        entry=float(a["price"] or ev["price"] or last_price or 0)
        side=str(a["side"] or ev["side"])
        sid=a["signal_id"]
        A=atr_tf("1M",30) if engine=="SCALP" else atr_tf(engine,24) if engine in ("5M","15M","1H","4H") else atr5()
        set_position(engine,side,entry,sid,A)
        # Restore original open timestamp rather than pretending recovery is a new trade.
        c2=db()
        c2.execute("""UPDATE engine_positions SET opened_ts=?,updated_ts=?,
                      state='HOLD' WHERE engine=?""",
                   (int(a["ts"]),int(ev["ts"]),engine))
        c2.commit(); c2.close()
    c.close()


@app.get("/api/position-performance")
def api_position_performance():
    """Only CLOSED actual research positions determine W/L and win rate."""
    c=db(); c.row_factory=sqlite3.Row
    exits=c.execute("""SELECT * FROM position_events WHERE event='EXIT'
                       ORDER BY ts DESC,id DESC LIMIT 2000""").fetchall()
    rows=[]
    buckets={e:{"closed":0,"wins":0,"losses":0} for e in ("SCALP","5M","15M","1H","4H")}
    wins=losses=0
    for ex in exits:
        engine=ex["engine"] or "5M"
        op=c.execute("""SELECT * FROM position_events
                        WHERE engine=? AND event IN ('OPEN','SWITCH') AND ts<=?
                        ORDER BY ts DESC,id DESC LIMIT 1""",(engine,ex["ts"])).fetchone()
        if not op or not op["price"] or not ex["price"]: continue
        side=op["side"]; entry=float(op["price"]); exitp=float(ex["price"])
        ret=((exitp-entry)/entry*100.0) * (1 if side=="LONG" else -1)
        result="BE"
        if ret>0: wins+=1; result="WIN"
        elif ret<0: losses+=1; result="LOSS"
        b=buckets.setdefault(engine,{"closed":0,"wins":0,"losses":0})
        b["closed"]+=1
        if result=="WIN": b["wins"]+=1
        elif result=="LOSS": b["losses"]+=1
        rows.append({"engine":engine,"side":side,"opened_ts":op["ts"],"closed_ts":ex["ts"],
                     "entry":entry,"exit":exitp,"return_pct":ret,"result":result})
    for b in buckets.values():
        d=b["wins"]+b["losses"]
        b["winrate"]=(b["wins"]/d*100.0 if d else None)
    decided=wins+losses
    c.close()
    return {"closed":len(rows),"wins":wins,"losses":losses,
            "winrate":(wins/decided*100.0 if decided else None),
            "engines":buckets,"trades":rows[:500]}

@app.get("/api/position-audit")
def api_position_audit():
    recover_missing_positions()
    c=db();c.row_factory=sqlite3.Row
    ev=[dict(x) for x in c.execute("SELECT * FROM position_events ORDER BY ts DESC LIMIT 50").fetchall()]
    c.close()
    return {"positions":all_positions(),"events":ev}

@app.get("/api/positions")
def api_positions():
    recover_missing_positions()
    return all_positions()

@app.get("/api/position-events")
def position_events(limit:int=300):
    c=db();c.row_factory=sqlite3.Row
    rows=[dict(x) for x in c.execute("SELECT * FROM position_events ORDER BY ts DESC LIMIT ?",(min(limit,5000),)).fetchall()]
    c.close();return rows

@app.post("/api/positions/{engine}/close")
def manual_close_position(engine: str):
    """Close one research/simulator position only. Never sends an exchange order."""
    engine=engine.upper()
    if engine not in ("SCALP","5M","15M","1H","4H"):
        return {"ok":False,"error":"invalid engine"}
    pos=current_position(engine)
    if not pos:
        return {"ok":False,"error":"no open position","engine":engine}
    px=float(last_price or pos.get("entry") or 0)
    if px<=0:
        return {"ok":False,"error":"live price unavailable","engine":engine}
    position_event("EXIT",pos["side"],px,pos.get("signal_id"),"MANUAL simulator close",engine)
    clear_position(engine,"MANUAL")
    return {"ok":True,"engine":engine,"side":pos["side"],"entry":pos["entry"],"exit":px,"reason":"MANUAL"}


@app.get("/api/user-position")
def api_user_position():
    c=db(); c.row_factory=sqlite3.Row
    r=c.execute("SELECT * FROM user_positions WHERE id=1").fetchone(); c.close()
    if not r:
        return {"side":"FLAT","current_price":float(last_price or 0),"live_pct":0.0,"live_usd":0.0}
    out=dict(r)
    px=float(last_price or out.get("entry") or 0); ent=float(out.get("entry") or 0)
    usd=(px-ent) if out.get("side")=="LONG" else (ent-px)
    out.update({"current_price":px,"live_usd":usd,"live_pct":usd/ent*100 if ent else 0.0})
    return out

@app.post("/api/user-position/open")
async def api_user_position_open(request: Request):
    body=await request.json(); side=str(body.get("side","")).upper()
    if side not in ("LONG","SHORT"): return {"ok":False,"error":"side must be LONG or SHORT"}
    px=float(last_price or 0)
    if px<=0:return {"ok":False,"error":"live price unavailable"}
    now=int(time.time()*1000); c=db(); c.row_factory=sqlite3.Row
    old=c.execute("SELECT * FROM user_positions WHERE id=1").fetchone()
    if old:return {"ok":False,"error":"user position already open"}
    c.execute("INSERT INTO user_positions(id,side,entry,opened_ts,updated_ts) VALUES(1,?,?,?,?)",(side,px,now,now))
    c.execute("INSERT INTO user_position_events(ts,event,side,price,note) VALUES(?,?,?,?,?)",(now,"OPEN",side,px,"USER"))
    c.commit(); c.close(); return {"ok":True,"side":side,"entry":px,"opened_ts":now}

@app.post("/api/user-position/close")
def api_user_position_close():
    px=float(last_price or 0); c=db(); c.row_factory=sqlite3.Row
    r=c.execute("SELECT * FROM user_positions WHERE id=1").fetchone()
    if not r: c.close(); return {"ok":False,"error":"no user position"}
    if px<=0: c.close(); return {"ok":False,"error":"live price unavailable"}
    now=int(time.time()*1000); side=r["side"]
    c.execute("INSERT INTO user_position_events(ts,event,side,price,note) VALUES(?,?,?,?,?)",(now,"EXIT",side,px,"USER MANUAL"))
    c.execute("DELETE FROM user_positions WHERE id=1"); c.commit(); c.close()
    return {"ok":True,"side":side,"entry":r["entry"],"exit":px}

@app.get("/api/user-position-events")
def api_user_position_events():
    c=db(); c.row_factory=sqlite3.Row
    rows=[dict(x) for x in c.execute("SELECT * FROM user_position_events ORDER BY ts DESC,id DESC LIMIT 1000").fetchall()]
    c.close(); return rows

@app.get("/api/user-position-performance")
def api_user_position_performance():
    c=db(); c.row_factory=sqlite3.Row
    ev=c.execute("SELECT * FROM user_position_events ORDER BY ts,id").fetchall(); c.close()
    op=None; rows=[]; total_pct=0.0; total_usd=0.0
    for e in ev:
        if e["event"]=="OPEN": op=e
        elif e["event"]=="EXIT" and op:
            entry=float(op["price"]); exitp=float(e["price"]); side=op["side"]
            usd=(exitp-entry) if side=="LONG" else (entry-exitp); pct=usd/entry*100 if entry else 0
            rows.append({"opened_ts":op["ts"],"closed_ts":e["ts"],"side":side,"entry":entry,"exit":exitp,"return_pct":pct,"pnl_usd":usd})
            total_pct+=pct; total_usd+=usd; op=None
    trades=list(reversed(rows))
    if op:
        entry=float(op["price"]); side=op["side"]; px=float(last_price or entry)
        usd=(px-entry) if side=="LONG" else (entry-px); pct=usd/entry*100 if entry else 0
        trades.insert(0,{"opened_ts":op["ts"],"closed_ts":None,"side":side,"entry":entry,"exit":None,"return_pct":pct,"pnl_usd":usd,"status":"OPEN","current_price":px})
    return {"closed":len(rows),"total_pct":total_pct,"total_usd":total_usd,"trades":trades[:500]}


@app.get("/api/stock/gaon")
async def stock_gaon(tf: str="1D"):
    """Gaon Cable 000500 OHLCV. Yahoo primary, Naver daily fallback."""
    import datetime as _dt
    tf=tf.upper()
    headers={"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
             "Accept":"application/json,text/plain,*/*"}
    errors=[]
    ymap={
      "1M":("1m","7d"),"5M":("5m","60d"),"15M":("15m","60d"),
      "30M":("30m","60d"),"60M":("60m","730d"),"1H":("60m","730d"),
      "1D":("1d","5y"),"1W":("1wk","10y"),"1MO":("1mo","max")
    }
    interval,rg=ymap.get(tf,("1d","5y"))
    try:
        url="https://query1.finance.yahoo.com/v8/finance/chart/000500.KS"
        params={"range":rg,"interval":interval,"events":"history","includeAdjustedClose":"true","includePrePost":"false"}
        async with httpx.AsyncClient(timeout=20.0,headers=headers,follow_redirects=True) as client:
            r=await client.get(url,params=params)
            if r.status_code!=200: raise RuntimeError(f"Yahoo HTTP {r.status_code}")
            j=r.json()
        result=(j.get("chart",{}).get("result") or [None])[0]
        if not result: raise RuntimeError(str(j.get("chart",{}).get("error") or "Yahoo result empty"))
        q=result["indicators"]["quote"][0];ts=result.get("timestamp") or []
        rows=[]
        for i,t in enumerate(ts):
            try:
                vals=(q["open"][i],q["high"][i],q["low"][i],q["close"][i])
                if any(v is None for v in vals): continue
                rows.append({"time":int(t),"open":float(vals[0]),"high":float(vals[1]),"low":float(vals[2]),
                             "close":float(vals[3]),"volume":float(q["volume"][i] or 0)})
            except Exception: continue
        if rows:
            return {"ok":True,"symbol":"000500.KS","name":"가온전선","currency":"KRW","source":"YAHOO","tf":tf,"rows":rows}
        raise RuntimeError("Yahoo rows=0")
    except Exception as e:
        errors.append("Yahoo "+repr(e))

    # Daily fallback: parse Naver text manually; do not use ast.literal_eval.
    if tf in ("1D","1W","1MO"):
        try:
            endd=_dt.datetime.now().strftime("%Y%m%d")
            startd=(_dt.datetime.now()-_dt.timedelta(days=3650)).strftime("%Y%m%d")
            url="https://api.finance.naver.com/siseJson.naver"
            params={"symbol":"000500","requestType":"1","startTime":startd,"endTime":endd,"timeframe":"day"}
            nh={"User-Agent":headers["User-Agent"],"Referer":"https://finance.naver.com/"}
            async with httpx.AsyncClient(timeout=20.0,headers=nh,follow_redirects=True) as client:
                r=await client.get(url,params=params)
                if r.status_code!=200: raise RuntimeError(f"Naver HTTP {r.status_code}")
                txt=r.text
            rows=[]
            # Extract rows like ["20260916", 123, 130, 120, 127, 123456, ...]
            for m in re.finditer(r'\[\s*["\']?(\d{8})["\']?\s*,\s*([0-9.]+)\s*,\s*([0-9.]+)\s*,\s*([0-9.]+)\s*,\s*([0-9.]+)\s*,\s*([0-9.]+)',txt):
                ds,o,h,l,c,v=m.groups()
                t=int(_dt.datetime.strptime(ds,"%Y%m%d").replace(tzinfo=_dt.timezone.utc).timestamp())
                rows.append({"time":t,"open":float(o),"high":float(h),"low":float(l),"close":float(c),"volume":float(v)})
            if rows:
                return {"ok":True,"symbol":"000500","name":"가온전선","currency":"KRW","source":"NAVER","tf":"1D","rows":rows}
            raise RuntimeError("Naver rows=0")
        except Exception as e:
            errors.append("Naver "+repr(e))
    return {"ok":False,"symbol":"000500","name":"가온전선","currency":"KRW","tf":tf,"rows":[],"error":" | ".join(errors)}

# Web terminal. Keep this mount at the end so /api/* routes take priority.
STATIC_DIR = Path(__file__).resolve().parent / "static"
if STATIC_DIR.exists():
    @app.get("/mobile", include_in_schema=False)
    async def mobile_app():
        return FileResponse(STATIC_DIR / "mobile.html", headers={"Cache-Control":"no-store, no-cache, must-revalidate, max-age=0"})

    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="terminal")


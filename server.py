import asyncio, json, os, sqlite3, time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
import httpx, websockets
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

INST="BTC-USDT-SWAP"
PUB="wss://ws.okx.com:8443/ws/v5/public"
BIZ="wss://ws.okx.com:8443/ws/v5/business"
DB=os.getenv("DB_PATH","/data/trapflow.db")
if not os.path.isdir(os.path.dirname(DB)):
    DB="trapflow.db"

app=FastAPI(title="BTC Trap Flow Collector")
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_methods=["*"],allow_headers=["*"])

trades=deque(maxlen=12000)
oi_hist=deque(maxlen=2000)
candles={"1M":deque(maxlen=800),"3M":deque(maxlen=600),"5M":deque(maxlen=400),"15M":deque(maxlen=400),"1H":deque(maxlen=300),"4H":deque(maxlen=300)}
book_imb=0.0
current_oi=None
last_price=None
trap_arm=None
last_signal_ts=0
prev_d10=0.0
scalp_arm=None
scalp_last_signal_ts=0
scalp_prev_d10=0.0
status={"public":"starting","business":"starting","started":int(time.time()*1000)}

def db():
    c=sqlite3.connect(DB)
    c.execute("""CREATE TABLE IF NOT EXISTS signals(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, name TEXT, side TEXT,
      entry REAL, sl REAL, tp1 REAL, tp2 REAL, score REAL,
      d10 REAL,d30 REAL,oi60 REAL,flow REAL,book REAL,status TEXT DEFAULT 'OPEN',
      closed_ts INTEGER)""")
    c.execute("""CREATE TABLE IF NOT EXISTS snapshots(
      ts INTEGER PRIMARY KEY, price REAL,d10 REAL,d30 REAL,oi60 REAL,flow REAL,book REAL,oi REAL)""")
    sigcols={r[1] for r in c.execute("PRAGMA table_info(signals)").fetchall()}
    if "engine" not in sigcols:
        c.execute("ALTER TABLE signals ADD COLUMN engine TEXT DEFAULT 'CORE'")
        c.execute("UPDATE signals SET engine='CORE' WHERE engine IS NULL OR engine=''")
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
    # Lightweight schema migration for position-state tracking.
    cols={r[1] for r in c.execute("PRAGMA table_info(position_state)").fetchall()}
    if "best_price" not in cols:c.execute("ALTER TABLE position_state ADD COLUMN best_price REAL")
    if "atr_open" not in cols:c.execute("ALTER TABLE position_state ADD COLUMN atr_open REAL")
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

def position_event(event,side,price,signal_id=None,note="",engine="CORE"):
    c=db()
    c.execute("INSERT INTO position_events(ts,event,side,price,signal_id,note,engine) VALUES(?,?,?,?,?,?,?)",
              (int(time.time()*1000),event,side,price,signal_id,note,engine))
    c.commit();c.close()

def current_position(engine="CORE"):
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
    if not any(x.get("engine")=="CORE" for x in out):
        legacy=c.execute("SELECT * FROM position_state WHERE id=1").fetchone()
        if legacy and legacy["side"]:
            d=dict(legacy);d["engine"]="CORE";d.setdefault("best_price",d.get("entry"))
            d.setdefault("worst_price",d.get("entry"));d.setdefault("mfe",0);d.setdefault("mae",0)
            out.append(d)
    c.close()
    return out

def set_position(engine,side,entry,signal_id,atr_value=None):
    now=int(time.time()*1000);A=atr_value or (atr_tf("1M",30) if engine=="SCALP" else atr_tf("1H",24) if engine=="MACRO" else atr5())
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

def save_signal(name,side,score,level,ext,metrics,engine="CORE",atr_value=None):
    global last_signal_ts,trap_arm,scalp_last_signal_ts,scalp_arm
    p=last_price; A=atr_value or atr5()
    if side=="LONG":
        sl=min(ext,level-.22*A); risk=max(p-sl,.35*A); tp1=p+1.5*risk; tp2=p+2.3*risk
    else:
        sl=max(ext,level+.22*A); risk=max(sl-p,.35*A); tp1=p-1.5*risk; tp2=p-2.3*risk
    if (side=="LONG" and p<=sl) or (side=="SHORT" and p>=sl):
        if engine=="SCALP": scalp_arm=None
        elif engine=="CORE": trap_arm=None
        return
    now=int(time.time()*1000)
    c=db();cur=c.execute("""INSERT INTO signals(ts,name,side,entry,sl,tp1,tp2,score,d10,d30,oi60,flow,book,status,engine)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',?)""",
      (now,name,side,p,sl,tp1,tp2,score,metrics["d10"],metrics["d30"],
       metrics["oi60"],metrics["flow"],metrics["book"],engine))
    signal_id=cur.lastrowid;c.commit();c.close()

    # IMPORTANT: signal benchmark and actual research position are separate.
    apply_position_signal(engine,side,p,signal_id,A)
    if engine=="CORE":
        last_signal_ts=now;trap_arm=None
    elif engine=="SCALP":
        scalp_last_signal_ts=now;scalp_arm=None

def evaluate():
    global trap_arm,prev_d10
    if not last_price or len(trades)<20:return
    H,L=liquidity15()
    if H is None:return
    A=atr5(); c=list(candles["5M"])[-1] if candles["5M"] else None
    w10,w30=flow(10000),flow(30000); oi60=oi_delta(); inten=flow_intensity(); now=int(time.time()*1000)

    if c and c["high"]>H+.04*A and c["close"]<H+.08*A:
        if not trap_arm or trap_arm["dir"]!="S" or now-trap_arm["ts"]>120000:
            trap_arm={"dir":"S","level":H,"ext":c["high"],"ts":now,"peak":w30["ratio"]}
    if c and c["low"]<L-.04*A and c["close"]>L-.08*A:
        if not trap_arm or trap_arm["dir"]!="L" or now-trap_arm["ts"]>120000:
            trap_arm={"dir":"L","level":L,"ext":c["low"],"ts":now,"peak":w30["ratio"]}
    if trap_arm and now-trap_arm["ts"]>150000:trap_arm=None

    if trap_arm and now-last_signal_ts>90000:
        if trap_arm["dir"]=="L":
            reclaim=last_price>trap_arm["level"]+.03*A
            hot=trap_arm["peak"]<-.12 or w30["ratio"]<-.16 or prev_d10<-.18
            flip=w10["ratio"]>.08 and w10["ratio"]-prev_d10>.14
            score=35+(20 if hot else 0)+(25 if flip else 0)+(10 if oi60<-.015 else 0)+(5 if inten>1.05 else 0)+(5 if book_imb>-.18 else 0)
            if reclaim and hot and flip and score>=80:
                save_signal("TRAP L","LONG",score,trap_arm["level"],trap_arm["ext"],
                            {"d10":w10["ratio"],"d30":w30["ratio"],"oi60":oi60,"flow":inten,"book":book_imb})
        else:
            reclaim=last_price<trap_arm["level"]-.03*A
            hot=trap_arm["peak"]>.12 or w30["ratio"]>.16 or prev_d10>.18
            flip=w10["ratio"]<-.08 and prev_d10-w10["ratio"]>.14
            score=35+(20 if hot else 0)+(25 if flip else 0)+(10 if oi60<-.015 else 0)+(5 if inten>1.05 else 0)+(5 if book_imb<.18 else 0)
            if reclaim and hot and flip and score>=80:
                save_signal("TRAP S","SHORT",score,trap_arm["level"],trap_arm["ext"],
                            {"d10":w10["ratio"],"d30":w30["ratio"],"oi60":oi60,"flow":inten,"book":book_imb})
    prev_d10=w10["ratio"]

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
    Decisions combine 10s/30s aggressive flow, intensity, OI/book context,
    and price acceptance/retrace. Engines remain independent.
    """
    if not last_price:return
    d10=flow(10000)["ratio"]; d30=flow(30000)["ratio"]
    intensity=flow_intensity(); oi=oi_delta(60000); book=book_imb
    now=int(time.time()*1000)
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
            # Opposite (sell) pressure. Price alone is insufficient.
            pscore=0
            if d10<=-0.12: pscore+=30
            if d30<=-0.05: pscore+=25
            if intensity>=0.80: pscore+=15
            if book<=-0.20: pscore+=10
            if oi>0 and d30<0: pscore+=10
            if adverse>=0.25 or retrace>=0.32: pscore+=10
            opposite_side="SHORT"
        else:
            best=min(best,last_price); worst=max(worst,last_price)
            mfe=max(0,entry-best); mae=max(0,worst-entry)
            adverse=(last_price-entry)/A
            retrace=(last_price-best)/A
            # Opposite (buy) pressure.
            pscore=0
            if d10>=0.12: pscore+=30
            if d30>=0.05: pscore+=25
            if intensity>=0.80: pscore+=15
            if book>=0.20: pscore+=10
            if oi>0 and d30>0: pscore+=10
            if adverse>=0.25 or retrace>=0.32: pscore+=10
            opposite_side="LONG"

        # Larger timeframe needs more persistence before switching.
        pressure_threshold=55 if engine=="SCALP" else 60 if engine=="CORE" else 65
        switch_threshold=80 if engine=="SCALP" else 82 if engine=="CORE" else 85
        min_pressure_ms=12000 if engine=="SCALP" else 25000 if engine=="CORE" else 60000

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

        # STRUCTURE INVALIDATION: do not let short-lived flow cooling keep a broken
        # position alive. This is EXIT-only; it never forces an opposite entry.
        # Uses the position's original ATR so the rule is stable across restarts.
        structure_exit_atr = 0.75 if engine=="SCALP" else 1.00 if engine=="CORE" else 1.35
        if adverse >= structure_exit_atr:
            old_signal=pos["signal_id"]
            note=(f"STRUCTURE INVALIDATION adverse={adverse:.2f}ATR score={pscore} "
                  f"d10={d10:.3f} d30={d30:.3f} flow={intensity:.2f} "
                  f"oi60={oi:.4f} book={book:.3f} mfe={mfe:.1f} mae={mae:.1f}")
            position_event("EXIT",side,last_price,old_signal,note,engine)
            clear_position(engine,"STRUCTURE_INVALIDATION")
            continue

        persisted = new_state=="PRESSURE" and new_psince and now-int(new_psince)>=min_pressure_ms

        # Strong reversal: EXIT old side + SWITCH to opposite side.
        switch_accept = adverse>=0.38 if engine=="SCALP" else adverse>=0.45 if engine=="CORE" else adverse>=0.55
        if persisted and pscore>=switch_threshold and switch_accept:
            old_signal=pos["signal_id"]
            note=(f"FLOW SWITCH score={pscore} d10={d10:.3f} d30={d30:.3f} "
                  f"flow={intensity:.2f} oi60={oi:.4f} book={book:.3f} mfe={mfe:.1f} mae={mae:.1f}")
            position_event("EXIT",side,last_price,old_signal,note,engine)
            clear_position(engine,"FLOW_SWITCH")
            set_position(engine,opposite_side,last_price,None,A)
            position_event("SWITCH",opposite_side,last_price,None,note,engine)
            continue

        # EXIT-only: current thesis is invalid enough to stop holding, but opposite side
        # is not strong enough for an immediate reverse position.
        exit_threshold=65 if engine=="SCALP" else 70 if engine=="CORE" else 75
        exit_ms=8000 if engine=="SCALP" else 18000 if engine=="CORE" else 45000
        exit_accept=adverse>=0.22 if engine=="SCALP" else adverse>=0.28 if engine=="CORE" else adverse>=0.38
        exit_persisted = new_state=="PRESSURE" and new_psince and now-int(new_psince)>=exit_ms
        if exit_persisted and pscore>=exit_threshold and exit_accept:
            old_signal=pos["signal_id"]
            note=(f"FLOW EXIT score={pscore} d10={d10:.3f} d30={d30:.3f} "
                  f"flow={intensity:.2f} oi60={oi:.4f} book={book:.3f} mfe={mfe:.1f} mae={mae:.1f}")
            position_event("EXIT",side,last_price,old_signal,note,engine)
            clear_position(engine,"FLOW_EXIT")
            continue

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
        for label,bar in [("1M","1m"),("3M","3m"),("5M","5m"),("15M","15m"),("1H","1H"),("4H","4H")]:
            try:
                r=(await h.get("https://www.okx.com/api/v5/market/candles",params={"instId":INST,"bar":bar,"limit":300})).json()
                arr=[]
                for x in reversed(r.get("data",[])):
                    arr.append({"ts":int(x[0]),"open":float(x[1]),"high":float(x[2]),"low":float(x[3]),
                                "close":float(x[4]),"volume":float(x[5]),"confirm":x[8]})
                candles[label].extend(arr)
            except Exception as e: print("seed",label,e)

async def public_loop():
    global current_oi,book_imb,last_price
    while True:
        try:
            async with websockets.connect(PUB,ping_interval=20,ping_timeout=20) as ws:
                status["public"]="live"
                await ws.send(json.dumps({"op":"subscribe","args":[
                    {"channel":"trades","instId":INST},{"channel":"books5","instId":INST},{"channel":"open-interest","instId":INST}]}))
                async for raw in ws:
                    m=json.loads(raw)
                    ch=m.get("arg",{}).get("channel")
                    for d in m.get("data",[]):
                        if ch=="trades":
                            px=float(d["px"]); sz=float(d["sz"]); ts=int(d["ts"]);last_price=px
                            trades.append({"px":px,"ts":ts,"side":d["side"],"notional":px*sz})
                            update_position_excursions()
                        elif ch=="open-interest":
                            current_oi=float(d["oi"]);oi_hist.append({"ts":int(d["ts"]),"oi":current_oi})
                        elif ch=="books5":
                            b=sum(float(x[1]) for x in d.get("bids",[]));a=sum(float(x[1]) for x in d.get("asks",[]))
                            book_imb=(b-a)/(b+a) if b+a else 0
                    try:
                        manage_position_reversal()
                    except Exception as e:
                        print("position_manager",repr(e))
                    evaluate();evaluate_scalp();update_outcomes()
        except Exception as e:
            status["public"]="reconnecting";print("public",e);await asyncio.sleep(2)

async def business_loop():
    mapping={"candle1m":"1M","candle3m":"3M","candle5m":"5M","candle15m":"15M","candle1H":"1H","candle4H":"4H"}
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
            c.commit();c.close()

@app.on_event("startup")
async def startup():
    c=db();c.close()
    await seed()
    asyncio.create_task(public_loop());asyncio.create_task(business_loop());asyncio.create_task(snapshot_loop())

@app.get("/api/status")
def home():
    return {"service":"BTC Trap Flow Collector","ok":True,"status":status}

@app.get("/api/live")
def live():
    a,b=flow(10000),flow(30000);H,L=liquidity15()
    return {"price":last_price,"d10":a["ratio"],"d30":b["ratio"],"oi":current_oi,"oi60":oi_delta(),
            "flow":flow_intensity(),"book":book_imb,"liqH":H,"liqL":L,"armed":trap_arm,"status":status}

@app.get("/api/signals")
def signals(limit:int=100, engine:str="ALL"):
    c=db();c.row_factory=sqlite3.Row
    if engine.upper() in ("CORE","SCALP"):
        rows=[dict(x) for x in c.execute("SELECT * FROM signals WHERE engine=? ORDER BY ts DESC LIMIT ?",(engine.upper(),min(limit,1000))).fetchall()]
    else:
        rows=[dict(x) for x in c.execute("SELECT * FROM signals ORDER BY ts DESC LIMIT ?",(min(limit,1000),)).fetchall()]
    c.close();return rows

@app.get("/api/stats")
def stats():
    c=db();c.row_factory=sqlite3.Row
    out={}
    for eng in ("SCALP","CORE"):
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
    engine=str(x.get("engine") or "CORE").upper()
    if side not in ("LONG","SHORT") or engine not in ("SCALP","CORE","MACRO") or not name:
        return {"ok":False,"error":"invalid signal"}
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
            A=atr_tf("1M",30) if engine=="SCALP" else atr_tf("1H",24) if engine=="MACRO" else atr5()
            apply_position_signal(engine,side,entry,sid,A)
        return {"ok":True,"id":sid,"deduped":True,"position_recovered":bool(current_position(engine))}
    cur=c.execute("""INSERT INTO signals(ts,name,side,entry,sl,tp1,tp2,score,d10,d30,oi60,flow,book,status,engine)
                     VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',?)""",
                  (ts,name,side,entry,sl,tp1,tp2,score,d10,d30,oi60,flowv,bookv,engine))
    sid=cur.lastrowid;c.commit();c.close()
    A=atr_tf("1M",30) if engine=="SCALP" else atr_tf("1H",24) if engine=="MACRO" else atr5()
    apply_position_signal(engine,side,entry,sid,A)
    return {"ok":True,"id":sid,"deduped":False}


def recover_missing_positions():
    """Self-heal engine_positions from the append-only lifecycle log.
    If an engine has no mutable active row but its latest lifecycle event is
    OPEN/CONFIRM/HOLD/PRESSURE/SWITCH (not EXIT), recreate the active row.
    Never resurrect an engine whose latest lifecycle event is EXIT.
    """
    c=db(); c.row_factory=sqlite3.Row
    for engine in ("SCALP","CORE","MACRO"):
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
        A=atr_tf("1M",30) if engine=="SCALP" else atr_tf("1H",24) if engine=="MACRO" else atr5()
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
    buckets={e:{"closed":0,"wins":0,"losses":0} for e in ("SCALP","CORE","MACRO")}
    wins=losses=0
    for ex in exits:
        engine=ex["engine"] or "CORE"
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
    rows=[dict(x) for x in c.execute("SELECT * FROM position_events ORDER BY ts DESC LIMIT ?",(min(limit,500),)).fetchall()]
    c.close();return rows


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
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="terminal")


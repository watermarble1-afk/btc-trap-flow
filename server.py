import asyncio, json, os, sqlite3, time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
import httpx, websockets
from fastapi import FastAPI
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
candles={"5M":deque(maxlen=400),"15M":deque(maxlen=400),"1H":deque(maxlen=300),"4H":deque(maxlen=300)}
book_imb=0.0
current_oi=None
last_price=None
trap_arm=None
last_signal_ts=0
prev_d10=0.0
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

def save_signal(name,side,score,level,ext,metrics):
    global last_signal_ts,trap_arm
    p=last_price; A=atr5()
    if side=="LONG":
        sl=min(ext,level-.22*A); risk=max(p-sl,.35*A); tp1=p+1.5*risk; tp2=p+2.3*risk
    else:
        sl=max(ext,level+.22*A); risk=max(sl-p,.35*A); tp1=p-1.5*risk; tp2=p-2.3*risk
    c=db();c.execute("""INSERT INTO signals(ts,name,side,entry,sl,tp1,tp2,score,d10,d30,oi60,flow,book,status)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN')""",
      (int(time.time()*1000),name,side,p,sl,tp1,tp2,score,metrics["d10"],metrics["d30"],
       metrics["oi60"],metrics["flow"],metrics["book"]))
    c.commit();c.close();last_signal_ts=int(time.time()*1000);trap_arm=None

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

def update_outcomes():
    if not last_price:return
    c=db(); rows=c.execute("SELECT id,side,sl,tp1 FROM signals WHERE status='OPEN'").fetchall()
    now=int(time.time()*1000)
    for i,side,sl,tp1 in rows:
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
        for label,bar in [("5M","5m"),("15M","15m"),("1H","1H"),("4H","4H")]:
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
                        elif ch=="open-interest":
                            current_oi=float(d["oi"]);oi_hist.append({"ts":int(d["ts"]),"oi":current_oi})
                        elif ch=="books5":
                            b=sum(float(x[1]) for x in d.get("bids",[]));a=sum(float(x[1]) for x in d.get("asks",[]))
                            book_imb=(b-a)/(b+a) if b+a else 0
                    evaluate();update_outcomes()
        except Exception as e:
            status["public"]="reconnecting";print("public",e);await asyncio.sleep(2)

async def business_loop():
    mapping={"candle5m":"5M","candle15m":"15M","candle1H":"1H","candle4H":"4H"}
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

@app.get("/")
def home():
    return {"service":"BTC Trap Flow Collector","ok":True,"status":status}

@app.get("/api/live")
def live():
    a,b=flow(10000),flow(30000);H,L=liquidity15()
    return {"price":last_price,"d10":a["ratio"],"d30":b["ratio"],"oi":current_oi,"oi60":oi_delta(),
            "flow":flow_intensity(),"book":book_imb,"liqH":H,"liqL":L,"armed":trap_arm,"status":status}

@app.get("/api/signals")
def signals(limit:int=100):
    c=db();c.row_factory=sqlite3.Row
    rows=[dict(x) for x in c.execute("SELECT * FROM signals ORDER BY ts DESC LIMIT ?",(min(limit,500),)).fetchall()]
    c.close();return rows

@app.get("/api/stats")
def stats():
    c=db()
    rows=c.execute("SELECT status,COUNT(*) FROM signals GROUP BY status").fetchall();c.close()
    return {k:v for k,v in rows}


# Web terminal. Keep this mount at the end so /api/* routes take priority.
STATIC_DIR = Path(__file__).resolve().parent / "static"
if STATIC_DIR.exists():
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="terminal")


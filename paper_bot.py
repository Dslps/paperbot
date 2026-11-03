#!/usr/bin/env python3
"""
Paper-trading bot for three strategies (see research/REPORT.md). No API keys, no real orders:
only public Binance USDT-M futures data, virtual positions. Safe to stop and restart any time.

C  LONG a fresh spike on a young coin
   coin listed < 30 days ago, 24h volume >= $5M
   price jumps >= +10% above the previous 15m close (inside the current 15m bar)
   the coin did NOT rise more than 2.7% in the hour before (spike "out of nowhere")
   entry when price passes +10.5% (stop-buy with slippage), exit 60 min after the spike bar closes,
   emergency stop -12%. One spike event per coin per 4 hours.

B  SHORT a pump on a very young coin
   coin listed < 14 days ago, 24h volume >= $5M, +20% over 24h (hourly closes)
   last settled funding >= -0.05%; short, stop +20%, exit after 24h

E  SHORT a fading pump
   coin 14..180 days old, 24h volume >= $5M, +20% over 24h, price already >= 3% below the 24h high
   last settled funding >= -0.05%; short, stop +30%, exit after 24h
   (B and E share one pump event per coin per 72 hours, as in the backtest)

Common: 5% of equity per trade, max 12 positions, 1x, one position per coin.
Costs as in the backtest: 0.3% per round trip (+0.5% extra when a stop is hit) + real funding.

Storage: local folder paper_bot/ (state.json, trades.csv, status.txt, log.txt),
or Upstash Redis if UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN are set.
If PORT is set, a status page is served on it (/, /trades.csv).

Usage:
  python paper_bot.py          run forever
  python paper_bot.py --once   one look at the market now, no trades
"""
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

# ---------------- settings (from the backtest) ----------------
START_EQUITY = 300.0
POS_SIZE = 0.05
MAX_POS = 12
MIN_VOL_24H = 5_000_000
MIN_FUNDING = -0.0005
COST = 0.003
STOP_SLIP = 0.005

PUMP = 0.20
PUMP_COOLDOWN_H = 72
B_MAX_AGE, B_STOP, B_HOLD_H = 14, 0.20, 24
E_MIN_AGE, E_MAX_AGE, E_OFF_HIGH, E_STOP, E_HOLD_H = 14, 180, -0.03, 0.30, 24

C_MAX_AGE = 30
C_SPIKE = 0.10
C_ENTRY_SLIP = 0.005
C_PRE1H_MAX = 0.027
C_EXIT_AFTER_BAR_MIN = 60          # exit = spike bar close + 60 min
C_STOP = 0.12
C_COOLDOWN_BARS = 16               # 4 hours of 15m bars

POLL_SEC = 10
SCAN_WINDOW_MIN = 30               # hourly pump scan may run up to 30 min late (VPN drops)
EXPECT = {"C": 2.2, "B": 2.6, "E": 1.6}   # backtest average net % per trade
# ----------------------------------------------------------------

# BINANCE_API lets you route requests through your own relay (e.g. a Cloudflare Worker) if the server IP is banned
API = (os.environ.get("BINANCE_API") or "https://fapi.binance.com").rstrip("/")
HOUR, MIN, BAR = 3_600_000, 60_000, 900_000
DAY = 86_400_000
DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_bot")
STATE = os.path.join(DIR, "state.json")
TRADES = os.path.join(DIR, "trades.csv")
STATUS = os.path.join(DIR, "status.txt")
LOG = os.path.join(DIR, "log.txt")

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "").rstrip("/")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
CLOUD = bool(UPSTASH_URL and UPSTASH_TOKEN)
TRADE_COLS = ["strategy", "side", "symbol", "entry_time", "exit_time", "entry", "exit", "reason", "notional_$",
              "price_%", "funding_%", "costs_%", "net_%", "pnl_$", "equity_after_$"]
_saved = {"state": None}
CURRENT = {"st": None}


# ---------------- storage ----------------
def kv(*cmd):
    req = urllib.request.Request(UPSTASH_URL, data=json.dumps([str(c) for c in cmd]).encode(),
                                 headers={"Authorization": f"Bearer {UPSTASH_TOKEN}", "Content-Type": "application/json"})
    for a in range(5):
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read()).get("result")
        except Exception:
            if a == 4:
                raise
            time.sleep(2 * (a + 1))


def now_ms():
    return int(time.time() * 1000)


def fmt(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def log(msg):
    line = f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC] {msg}"
    print(line, flush=True)
    try:
        if CLOUD:
            kv("LPUSH", "pb:log", line)
            kv("LTRIM", "pb:log", 0, 1999)
        else:
            with open(LOG, "a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception as e:
        print(f"(log storage failed: {e})", flush=True)


def recent_logs(n=100):
    if CLOUD:
        return list(reversed(kv("LRANGE", "pb:log", 0, n - 1) or []))
    if not os.path.exists(LOG):
        return []
    with open(LOG, encoding="utf-8") as f:
        return f.read().splitlines()[-n:]


def load_state():
    st = None
    if CLOUD:
        v = kv("GET", "pb3:state")
        st = json.loads(v) if v else None
    elif os.path.exists(STATE):
        with open(STATE, encoding="utf-8") as f:
            st = json.load(f)
    if not st or st.get("version") != 3:
        if st:
            log("old state from the previous bot version found: starting a fresh $300 paper account")
        st = {"version": 3, "equity": START_EQUITY, "open": [], "pump_cd": {}, "spike_cd": {},
              "started": now_ms(), "last_scan_hour": 0}
    return st


def save_state(st):
    if CLOUD:
        txt = json.dumps(st)
        if txt != _saved["state"]:
            kv("SET", "pb3:state", txt)
            _saved["state"] = txt
        return
    tmp = STATE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, indent=1)
    os.replace(tmp, STATE)


def write_trade(t):
    row = [t["strategy"], "LONG" if t["dir"] > 0 else "SHORT", t["symbol"], fmt(t["entry_ts"]), fmt(t["exit_ts"]),
           t["entry"], t["exit"], t["reason"], round(t["notional"], 2), round(100 * t["price_ret"], 2),
           round(100 * t["funding"], 3), round(100 * t["costs"], 2), round(100 * t["net"], 2),
           round(t["pnl"], 2), round(t["equity_after"], 2)]
    if CLOUD:
        kv("RPUSH", "pb3:trades", json.dumps(row))
        return
    new = not os.path.exists(TRADES)
    if not new:                                   # old file from the previous version has other columns
        with open(TRADES, encoding="utf-8") as f:
            if not f.readline().startswith("strategy,"):
                os.replace(TRADES, TRADES.replace(".csv", "_old_version.csv"))
                new = True
    with open(TRADES, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(TRADE_COLS)
        w.writerow(row)


def read_trades():
    if CLOUD:
        return [dict(zip(TRADE_COLS, json.loads(x))) for x in (kv("LRANGE", "pb3:trades", 0, -1) or [])]
    if not os.path.exists(TRADES):
        return []
    with open(TRADES, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return [r for r in rows if "strategy" in r]


# ---------------- market data ----------------
class RateLimited(Exception):
    pass


BAN_UNTIL = [0.0]          # unix time until which Binance asked us to stop sending requests


def ban_left():
    return max(0.0, BAN_UNTIL[0] - time.time())


def get(path, params=None, tries=4):
    if ban_left() > 0:
        raise RateLimited(f"Binance pause, {int(ban_left())}s left")
    url = API + path
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
    for a in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (418, 429):
                # 429 = too many requests, 418 = IP temporarily banned. Retry-After says for how long.
                ra = e.headers.get("Retry-After") if e.headers else None
                wait = int(ra) if ra and ra.strip().isdigit() else (300 if e.code == 418 else 60)
                wait = max(wait, 30)
                BAN_UNTIL[0] = time.time() + wait
                log(f"Binance rate limit HTTP {e.code} (Retry-After={ra}): pausing all requests for {wait}s"
                    + (" - this server's IP is temporarily banned by Binance" if e.code == 418 else ""))
                raise RateLimited(f"HTTP {e.code}")
            elif a == tries - 1:
                raise
            else:
                time.sleep(2 * (a + 1))
        except Exception:
            if a == tries - 1:
                raise
            time.sleep(2 * (a + 1))


def listings():
    """{symbol: age_days} for all TRADING USDT perpetuals."""
    info = get("/fapi/v1/exchangeInfo")
    out = {}
    for s in info["symbols"]:
        if (s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT"
                and s.get("status") == "TRADING" and s.get("onboardDate")):
            out[s["symbol"]] = (now_ms() - int(s["onboardDate"])) / DAY
    return out


def last_settled_funding(sym):
    r = get("/fapi/v1/fundingRate", {"symbol": sym, "limit": 1})
    return float(r[-1]["fundingRate"]) if r else 0.0


def funding_between(sym, t0, t1):
    r = get("/fapi/v1/fundingRate", {"symbol": sym, "startTime": t0 + 1, "endTime": t1, "limit": 1000})
    return sum(float(x["fundingRate"]) for x in r)


def all_prices():
    return {x["symbol"]: float(x["price"]) for x in get("/fapi/v1/ticker/price")}


# ---------------- positions ----------------
def open_position(st, strategy, sym, d, entry, stop_pct, exit_due, info):
    notional = st["equity"] * POS_SIZE
    t_in = now_ms()
    p = {"strategy": strategy, "symbol": sym, "dir": d, "entry": entry, "entry_ts": t_in, "notional": notional,
         "stop": entry * (1 - stop_pct) if d > 0 else entry * (1 + stop_pct), "exit_due": exit_due,
         "checked_until": t_in // MIN * MIN, **info}
    st["open"].append(p)
    log(f"OPEN {strategy} {'LONG' if d > 0 else 'SHORT'} {sym} @ {entry:.6g} (${notional:.2f}) | stop {p['stop']:.6g} | "
        f"exit {fmt(exit_due)} | " + ", ".join(f"{k} {v}" for k, v in info.items()))


def close_position(st, p, exit_price, ts, reason):
    try:
        rates = funding_between(p["symbol"], p["entry_ts"], ts)
    except Exception as e:
        log(f"{p['symbol']}: funding fetch failed ({e}), using 0")
        rates = 0.0
    fund = -p["dir"] * rates                      # long pays +rate, short receives it
    price_ret = p["dir"] * (exit_price / p["entry"] - 1)
    costs = COST + (STOP_SLIP if reason == "stop" else 0)
    net = price_ret - costs + fund
    pnl = p["notional"] * net
    st["equity"] += pnl
    write_trade(dict(p, exit=exit_price, exit_ts=ts, reason=reason, price_ret=price_ret, funding=fund,
                     costs=costs, net=net, pnl=pnl, equity_after=st["equity"]))
    log(f"CLOSE {p['strategy']} {p['symbol']} ({reason}) {p['entry']:.6g} -> {exit_price:.6g} | price {100*price_ret:+.2f}% "
        f"funding {100*fund:+.3f}% net {100*net:+.2f}% = ${pnl:+.2f} | equity ${st['equity']:.2f}")


def check_open(st):
    """Stops and time exits from closed 1m candles since the last check (catches up after downtime)."""
    still = []
    for p in st["open"]:
        k = []
        try:
            start = p["checked_until"]
            while True:
                part = get("/fapi/v1/klines", {"symbol": p["symbol"], "interval": "1m", "startTime": start, "limit": 1000})
                k += part
                if len(part) < 1000:
                    break
                start = int(part[-1][0]) + MIN
        except Exception as e:
            log(f"{p['symbol']}: price check failed ({e}), will retry")
            still.append(p)
            continue
        closed = False
        for x in k:
            o, h, l, c, ct = float(x[1]), float(x[2]), float(x[3]), float(x[4]), int(x[6])
            if ct >= now_ms():
                break
            if p["dir"] > 0 and l <= p["stop"]:
                close_position(st, p, min(p["stop"], o), ct, "stop")
                closed = True
                break
            if p["dir"] < 0 and h >= p["stop"]:
                close_position(st, p, max(p["stop"], o), ct, "stop")
                closed = True
                break
            if ct + 1 >= p["exit_due"]:
                close_position(st, p, c, ct, "time")
                closed = True
                break
            p["checked_until"] = ct + 1
        if not closed:
            still.append(p)
    st["open"] = still


def can_open(st, sym):
    if len(st["open"]) >= MAX_POS:
        return "max positions"
    if any(p["symbol"] == sym for p in st["open"]):
        return "position already open"
    return None


# ---------------- strategy C: fresh spike on a young coin ----------------
def refresh_watch(watch, ages):
    """Called once per 15m bar: previous close, 1h move before, 24h volume for coins < C_MAX_AGE days."""
    bar = now_ms() // BAR * BAR
    out = {}
    for sym, age in ages.items():
        if age >= C_MAX_AGE:
            continue
        try:
            k = get("/fapi/v1/klines", {"symbol": sym, "interval": "15m", "limit": 99, "endTime": bar - 1})
        except RateLimited:
            raise
        except Exception as e:
            log(f"{sym}: 15m klines failed ({e})")
            continue
        k = [x for x in k if int(x[6]) < bar]
        if len(k) < 6:
            continue
        prev_close = float(k[-1][4])
        out[sym] = {"bar": bar, "prev_close": prev_close, "pre1h": prev_close / float(k[-5][4]) - 1,
                    "vol24": sum(float(x[7]) for x in k[-96:]), "age": age, "event": False, "done": False}
    watch.clear()
    watch.update(out)


def check_spikes(st, watch, prices):
    for sym, w in watch.items():
        p = prices.get(sym)
        if p is None or w["done"]:
            continue
        trig = w["prev_close"] * (1 + C_SPIKE)
        if p < trig:
            continue
        bar_idx = w["bar"] // BAR
        if not w["event"]:
            last = st["spike_cd"].get(sym)
            if last is not None and bar_idx - last < C_COOLDOWN_BARS:
                w["done"] = True
                continue
            st["spike_cd"][sym] = bar_idx            # spike event registered (consumes the cooldown)
            w["event"] = True
            reasons = []
            if w["vol24"] < MIN_VOL_24H:
                reasons.append(f"volume ${w['vol24']/1e6:.1f}M < $5M")
            if w["pre1h"] >= C_PRE1H_MAX:
                reasons.append(f"already ran {100*w['pre1h']:+.1f}% in the hour before")
            r = can_open(st, sym)
            if r:
                reasons.append(r)
            if reasons:
                log(f"SKIP C {sym}: spike +10% (age {w['age']:.1f}d) -> {', '.join(reasons)}")
                w["done"] = True
                continue
        if p >= trig * (1 + C_ENTRY_SLIP):
            r = can_open(st, sym)
            if r:
                log(f"SKIP C {sym}: {r}")
                w["done"] = True
                continue
            exit_due = w["bar"] + BAR + C_EXIT_AFTER_BAR_MIN * MIN
            open_position(st, "C", sym, 1, p, C_STOP, exit_due,
                          {"age_d": round(w["age"], 1), "move_vs_prev15m_%": round(100 * (p / w["prev_close"] - 1), 1),
                           "pre1h_%": round(100 * w["pre1h"], 1)})
            w["done"] = True


# ---------------- strategies B and E: pumps over 24h ----------------
def pump_scan(st, ages, dry=False):
    hour_start = now_ms() // HOUR * HOUR
    hour_idx = hour_start // HOUR
    found = []
    for sym, age in sorted(ages.items(), key=lambda x: x[1]):
        if age >= E_MAX_AGE:
            continue
        try:
            k = get("/fapi/v1/klines", {"symbol": sym, "interval": "1h", "limit": 27, "endTime": hour_start - 1})
        except RateLimited:
            raise
        except Exception as e:
            log(f"{sym}: 1h klines failed ({e})")
            continue
        k = [x for x in k if int(x[6]) < hour_start]
        if len(k) < 25:
            continue
        c_now = float(k[-1][4])
        r24 = c_now / float(k[-25][4]) - 1
        vol24 = sum(float(x[7]) for x in k[-24:])
        off_high = c_now / max(float(x[2]) for x in k[-24:]) - 1
        if r24 < PUMP or vol24 < MIN_VOL_24H:
            continue
        last = st["pump_cd"].get(sym)
        if last is not None and hour_idx - last < PUMP_COOLDOWN_H:
            continue
        if not dry:
            st["pump_cd"][sym] = hour_idx            # pump event registered (B and E share it)
        strat = "B" if age < B_MAX_AGE else "E"
        reasons = []
        if strat == "E" and off_high > E_OFF_HIGH:
            reasons.append(f"still near the 24h high ({100*off_high:+.1f}%)")
        fr = None
        if not reasons:
            try:
                fr = last_settled_funding(sym)
            except Exception as e:
                reasons.append(f"funding unavailable ({e})")
            if fr is not None and fr < MIN_FUNDING:
                reasons.append(f"funding {100*fr:.3f}% < -0.05%")
        r = can_open(st, sym)
        if r:
            reasons.append(r)
        info = {"age_d": round(age, 1), "24h_%": round(100 * r24, 1), "off_24h_high_%": round(100 * off_high, 1),
                "vol24_$M": round(vol24 / 1e6, 1), "funding_%": None if fr is None else round(100 * fr, 4)}
        found.append((strat, sym, info, reasons))
        if dry:
            continue
        if reasons:
            log(f"SKIP {strat} {sym}: pump {info['24h_%']:+.1f}% -> {', '.join(reasons)}")
            continue
        try:
            entry = float(get("/fapi/v1/ticker/price", {"symbol": sym})["price"])
        except Exception as e:
            log(f"{sym}: price failed ({e}), skip")
            continue
        stop, hold = (B_STOP, B_HOLD_H) if strat == "B" else (E_STOP, E_HOLD_H)
        open_position(st, strat, sym, -1, entry, stop, now_ms() + hold * HOUR, info)
    return found


# ---------------- status ----------------
def status_text(st):
    rows = read_trades()
    days = max((now_ms() - st["started"]) / DAY, 1e-9)
    lines = [f"Paper bot (C + B + E) status, {fmt(now_ms())}",
             f"Running {days:.1f} days | equity ${st['equity']:.2f} (start ${START_EQUITY:.0f}, "
             f"{100*(st['equity']/START_EQUITY-1):+.1f}%) | position size {int(POS_SIZE*100)}%, max {MAX_POS}",
             "Closed trades by strategy (live vs backtest average):"]
    for s in ["C", "B", "E"]:
        nets = [float(r["net_%"]) for r in rows if r["strategy"] == s]
        if nets:
            lines.append(f"  {s}: {len(nets)} trades | win {100*sum(x>0 for x in nets)/len(nets):.0f}% | "
                         f"avg {sum(nets)/len(nets):+.2f}% (backtest {EXPECT[s]:+.1f}%)")
        else:
            lines.append(f"  {s}: no trades yet (backtest {EXPECT[s]:+.1f}%)")
    if rows:
        allnet = [float(r["net_%"]) for r in rows]
        lines.append(f"  all: {len(allnet)} trades | avg {sum(allnet)/len(allnet):+.2f}%")
    lines.append(f"Open positions ({len(st['open'])}/{MAX_POS}):")
    for p in st["open"]:
        lines.append(f"  {p['strategy']} {'LONG ' if p['dir'] > 0 else 'SHORT'} {p['symbol']:16s} entry {p['entry']:.6g} "
                     f"stop {p['stop']:.6g} exit {fmt(p['exit_due'])}")
    return "\n".join(lines) + "\n"


def write_status(st):
    if not CLOUD:
        with open(STATUS, "w", encoding="utf-8") as f:
            f.write(status_text(st))


def start_web():
    port = int(os.environ.get("PORT", "0") or 0)
    if not port:
        return
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        def _send(self, body, ctype="text/plain; charset=utf-8"):
            b = body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)

        def do_GET(self):
            try:
                if self.path.startswith("/ping"):
                    self._send("ok")
                elif self.path.startswith("/trades.csv"):
                    rows = read_trades()
                    self._send(",".join(TRADE_COLS) + "\n" + "\n".join(",".join(str(r[c]) for c in TRADE_COLS) for r in rows),
                               "text/csv; charset=utf-8")
                else:
                    st = CURRENT["st"] or load_state()
                    self._send(status_text(st) + "\nAll trades: /trades.csv\n\n--- last 100 log lines ---\n"
                               + "\n".join(recent_logs(100)))
            except Exception as e:
                self._send(f"error: {e}")

        def do_HEAD(self):
            self.send_response(200)
            self.end_headers()

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("SELF_URL")
    if url:
        def pinger():
            while True:
                time.sleep(600)
                try:
                    urllib.request.urlopen(url.rstrip("/") + "/ping", timeout=30).read()
                except Exception:
                    pass
        threading.Thread(target=pinger, daemon=True).start()
    print(f"status page on port {port}", flush=True)


# ---------------- main loop ----------------
def once():
    print("Connecting to Binance...")
    ages = listings()
    young = {s: a for s, a in ages.items() if a < C_MAX_AGE}
    print(f"OK: {len(ages)} USDT perpetuals, {len(young)} younger than {C_MAX_AGE} days, "
          f"{sum(a < E_MAX_AGE for a in ages.values())} younger than {E_MAX_AGE} days\n")
    watch = {}
    refresh_watch(watch, ages)
    prices = all_prices()
    print("Strategy C watch list (young coins, trigger = +10% vs previous 15m close):")
    for sym, w in sorted(watch.items(), key=lambda x: x[1]["age"]):
        p = prices.get(sym, float("nan"))
        print(f"  {sym:16s} age {w['age']:5.1f}d  vol24 ${w['vol24']/1e6:7.1f}M  price {p:<12.6g} trigger "
              f"{w['prev_close']*1.10:<12.6g} ({100*(p/w['prev_close']-1):+.1f}% now)  1h before {100*w['pre1h']:+.1f}%")
    print("\nPumps right now (strategies B / E), last closed hour:")
    found = pump_scan(load_state(), ages, dry=True)
    if not found:
        print("  none (no coin up 20%+ over 24h with $5M+ volume)")
    for strat, sym, info, reasons in found:
        print(f"  {strat} {sym}: {info} -> {'WOULD SHORT' if not reasons else 'skip: ' + ', '.join(reasons)}")


def main():
    if not CLOUD:
        os.makedirs(DIR, exist_ok=True)
    if "--once" in sys.argv:
        once()
        return
    start_web()
    st = load_state()
    CURRENT["st"] = st
    ages, ages_at = {}, 0
    watch, watch_bar = {}, 0
    last_min = 0
    log(f"Bot started{' (cloud storage)' if CLOUD else ''}: strategies C + B + E, equity ${st['equity']:.2f}, "
        f"open {len(st['open'])}, size {int(POS_SIZE*100)}%, max {MAX_POS}")
    while True:
        if ban_left() > 0:                        # Binance asked us to pause: don't spam requests or the log
            time.sleep(min(ban_left(), 60) + 1)
            continue
        t = now_ms()
        try:
            if t - ages_at > HOUR or not ages:
                ages = listings()
                ages_at = t
                log(f"Binance OK: {len(ages)} perpetuals, {sum(a < C_MAX_AGE for a in ages.values())} younger than {C_MAX_AGE}d")
        except urllib.error.HTTPError as e:
            log(f"BINANCE NOT REACHABLE (HTTP {e.code})" + (" - Binance blocks this server's country" if e.code in (451, 403) else ""))
        except Exception as e:
            log(f"Binance unreachable: {e} (retrying)")
        if ages:
            bar = t // BAR * BAR
            try:
                if bar != watch_bar and t - bar >= 3_000:
                    refresh_watch(watch, ages)
                    watch_bar = bar
                if watch and watch_bar == bar:
                    check_spikes(st, watch, all_prices())
            except Exception as e:
                log(f"spike check error: {e}")
            try:
                if t // MIN != last_min:
                    last_min = t // MIN
                    check_open(st)
            except Exception as e:
                log(f"position check error: {e}")
            try:
                hour = t // HOUR
                if hour > st["last_scan_hour"] and t % HOUR >= 20_000:
                    if t % HOUR <= SCAN_WINDOW_MIN * MIN:
                        pump_scan(st, ages)
                    else:
                        log(f"pump scan skipped this hour (no connection in the first {SCAN_WINDOW_MIN} min)")
                    st["last_scan_hour"] = hour
                    write_status(st)
            except Exception as e:
                log(f"pump scan error: {e} (will retry)")
        try:
            save_state(st)
        except Exception as e:
            log(f"state save failed: {e}")
        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()

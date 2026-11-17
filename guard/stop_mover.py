#!/usr/bin/env python3
"""stop_mover.py - FREE stop-loss mover (no AI, no tokens). v9, 17 Nov 2026 (Rasni asked in chat).

Runs inside the 1-minute watcher (binance_live.py watch-once calls run() every loop, about every minute;
every 15 s while a real entry waits). It follows EXACTLY the routine's step 7 thresholds:
  progress = how far the BEST price since entry went toward the ORIGINAL tp (0 = entry, 1 = tp)
  now      = the same with the price now (last 1m close)
  Step 1 no-loss : MAIN progress >= 0.6 and now >= 0.3; all others progress >= 0.5 and now >= 0.25
                   -> sl = entry +0.2% into profit, stopStep "noloss", changes kind "noloss"
  Step 2 keep half: (after step 1) now >= 0.8 -> sl = halfway entry..original tp, stopStep "half", kind "halfprofit"
  One step per trade per run, each step once. The sl is NEVER moved farther from the entry, the tp is never changed.
  "best" uses 1m candle highs/lows (nothing between two loops is missed).
PRACTICE trades (risky / slow / trend): also closes them in ledger.json when a 1m candle touches sl or tp
  (both in one candle -> sl, the careful choice), closedReason tp|sl|breakeven|locked, pnlUsdt after 0.05% fee
  per side, r, result, afterClose, and updates accounts[strategy] + stats.by_strategy (+ by_risky_type) once
  (the trade gets accounted: true, autoClosed: true).
REAL trades (main / second / daily): only the ledger sl/stopStep is changed here; the engine's manage() step that
  runs right after this in the SAME loop replaces the real Binance stop (gtb_ orders only, new stop first).
  Closing real trades stays with Binance's own SL/TP orders + the engine.
WAITING LIMIT IDEAS (v9.1, Rasni asked in chat): every loop, for ideas still new/waiting/paused (not filled), the
  1m candles since the idea was sent (or since its entry/sl/tp last changed) are checked. If the price reached the
  TAKE PROFIT or the STOP LOSS before it reached the entry, the idea is cancelled (status "cancelled", closedAt,
  cancelReason "... (free watcher)", autoClosed + accounted true; nothing is counted in accounts). For main/second/
  daily the engine's manage() then cancels the real gtb_ entry on Binance in the SAME loop; if Binance says it had
  just filled, the trade is put back to "running" (undo_cancel) and protect() puts SL + TP on it.
  Practice ideas (risky/slow/trend) whose entry price is reached are marked "running" (filledAt, fillPrice); if the
  same candle also reaches the stop loss it is closed as "sl" (careful choice). Real ideas are never marked filled
  here: Binance decides that. A candle that reaches the entry AND the target is counted as a fill, not a cancel.
SAFE WRITES: ledger.json is changed under a lock (ledger.json.lock), re-read just before writing, only the fields
  touched here are merged, then an atomic replace. If the AI routine writes an older copy over a change, the
  record in the state file puts the change back on the next loop (never looser, never twice into the accounts).
"""
import json, os, time, fcntl, datetime

LK = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
FEE = 0.0005
PRACTICE = ("risky", "slow", "trend")
RANK = {"none": 0, "noloss": 1, "half": 2}
TAG = "(free watcher)"

def iso(dt=None): return (dt or datetime.datetime.now(LK)).isoformat(timespec="seconds")
def ms_iso(ms): return datetime.datetime.fromtimestamp(ms / 1000, LK).isoformat(timespec="seconds")
def iso_ms(s):
    try: return int(datetime.datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp() * 1000)
    except Exception: return None

def _default_klines(symbol, start_ms):
    import binance_live as B
    out = []; s = int(start_ms)
    for _ in range(3):                                   # up to 3000 minutes (a daily holds max 24h)
        k = B.pub("/fapi/v1/klines", {"symbol": symbol, "interval": "1m", "startTime": s, "limit": 1000}) or []
        out += k
        if len(k) < 1000: break
        s = int(k[-1][0]) + 60000
    return out
KLINES = _default_klines                                 # tests replace this

# ------------------------------------------------------------------ pure rules
def thresholds(strategy): return (0.6, 0.3) if strategy == "main" else (0.5, 0.25)
def prog(side, entry, tp, price):
    d = tp - entry
    return (price - entry) / d if d else 0.0
def tighter(side, new, old, eps=1e-12):
    """True if `new` is a stop CLOSER to (or past) the entry in the profit direction than `old`."""
    if old is None: return True
    return new > old + eps if side == "LONG" else new < old - eps
def rnd(x, tick, up):
    if not tick: return float(f"{x:.8g}")
    from decimal import Decimal, ROUND_UP, ROUND_DOWN
    q = (Decimal(str(x)) / Decimal(str(tick))).to_integral_value(rounding=ROUND_UP if up else ROUND_DOWN) * Decimal(str(tick))
    return float(q)

def decide(t, best, now, tick=None):
    """Next step for a running trade, or None. t: side, entry, sl, tp, orig, strategy, stopStep."""
    side = t["side"]; entry = float(t["entry"]); sl = float(t["sl"])
    tp0 = float((t.get("orig") or {}).get("tp") or t["tp"])
    step = t.get("stopStep") or "none"
    if (side == "LONG" and tp0 <= entry) or (side == "SHORT" and tp0 >= entry): return None
    p, n = prog(side, entry, tp0, best), prog(side, entry, tp0, now)
    a, b = thresholds(t.get("strategy"))
    long_ = side == "LONG"
    if step == "none" and p >= a and n >= b:
        lvl = rnd(entry * (1.002 if long_ else 0.998), tick, up=long_)         # rounded into profit
        return {"step": "noloss", "kind": "noloss", "old": sl, "new": lvl, "move": tighter(side, lvl, sl), "progress": round(p, 3), "now": round(n, 3)}
    if step == "noloss" and n >= 0.8:
        lvl = rnd((entry + tp0) / 2, tick, up=not long_)                       # rounded toward the entry (a hair safer)
        return {"step": "half", "kind": "halfprofit", "old": sl, "new": lvl, "move": tighter(side, lvl, sl), "progress": round(p, 3), "now": round(n, 3)}
    return None

def close_numbers(t, exitp, reason_when_sl):
    side = t["side"]; d = 1 if side == "LONG" else -1
    entry = float(t.get("fillPrice") or t["entry"]); qty = float(t.get("qty") or 0)
    sl0 = float((t.get("orig") or {}).get("sl") or t["sl"])
    gross = d * (exitp - entry) * qty; fees = FEE * qty * (entry + exitp); net = gross - fees
    risk = abs(entry - sl0)
    return {"pnlUsdt": round(net, 2), "feesUsdt": round(fees, 4), "r": round(d * (exitp - entry) / risk, 2) if risk else 0.0,
            "result": "win" if net > 0.005 else ("loss" if net < -0.005 else "even")}

def best_money(t):
    """(USDT, % of margin) at the trade's best price, after 0.05% fee per side. qty from the trade (else margin x lev / entry)."""
    try:
        entry = float(t["entry"]); best = float(t["best"]); m = float(t.get("margin") or 10); lev = float(t.get("leverage") or 10)
        qty = float(t.get("qty") or 0) or m * lev / entry
        d = 1 if t.get("side") == "LONG" else -1
        net = d * (best - entry) * qty - FEE * qty * (entry + best)
        return round(net, 2), round(net / m * 100, 2)
    except Exception: return None
def sl_reason(step): return {"none": "sl", "noloss": "breakeven", "half": "locked"}.get(step or "none", "sl")

# ------------------------------------------------------------------ ledger io
class Locked:
    def __init__(self, path): self.f = open(path + ".lock", "w")
    def __enter__(self): fcntl.flock(self.f, fcntl.LOCK_EX); return self
    def __exit__(self, *a): fcntl.flock(self.f, fcntl.LOCK_UN); self.f.close()
def _load(p):
    with open(p) as fh: return json.load(fh)
def _save(obj, p):
    tmp = p + ".mover.tmp"
    with open(tmp, "w") as fh: json.dump(obj, fh, indent=1, ensure_ascii=False)
    os.replace(tmp, p)

def _add_account(L, t, nums):
    s = t.get("strategy"); res = nums["result"]; u = nums["pnlUsdt"]
    acc = L.setdefault("accounts", {}).setdefault(s, {"start": 1000, "balance": 1000, "wins": 0, "losses": 0, "even": 0, "closed": 0, "total_usdt": 0})
    acc["balance"] = round(float(acc.get("balance", 1000)) + u, 4); acc["closed"] = acc.get("closed", 0) + 1
    acc["total_usdt"] = round(float(acc.get("total_usdt", 0)) + u, 4)
    key = {"win": "wins", "loss": "losses", "even": "even"}[res]; acc[key] = acc.get(key, 0) + 1
    st = L.setdefault("stats", {})
    buckets = [st.setdefault("by_strategy", {}).setdefault(s, {"sent": 0, "closed": 0, "wins": 0, "losses": 0, "total_R": 0, "total_usdt": 0})]
    if s == "risky" and t.get("riskyType"):
        buckets.append(st.setdefault("by_risky_type", {}).setdefault(t["riskyType"], {"sent": 0, "closed": 0, "wins": 0, "losses": 0, "total_R": 0, "total_usdt": 0}))
    for b in buckets:
        b["closed"] = b.get("closed", 0) + 1
        if res == "win": b["wins"] = b.get("wins", 0) + 1
        elif res == "loss": b["losses"] = b.get("losses", 0) + 1
        b["total_R"] = round(float(b.get("total_R", 0)) + nums["r"], 3); b["total_usdt"] = round(float(b.get("total_usdt", 0)) + u, 4)

def _levels(t):
    try: return [float(t["entry"]), float(t["sl"]), float(t["tp"])]
    except Exception: return None

def after_close(at_ms):
    """THE one place that makes the 'After the close: still watching' fields (every close path uses it: stop mover, watcher, cloud guard copy)."""
    return {"watching": True, "until": ms_iso(int(at_ms) + 7 * 86400000)}

def ensure_after_close(ledger_path, now_ms=None):
    """Safety net (Rasni 18 Nov): any CLOSED idea with no afterClose (e.g. closed by the AI check) whose 7-day watch window is still
    open gets the standard fields from its close time. Read first, lock + save only when something is missing."""
    now_ms = int(now_ms or time.time() * 1000)
    def missing(L): return [t for t in L.get("trades", []) if t.get("status") == "closed" and not t.get("afterClose") and iso_ms(t.get("closedAt"))
                            and iso_ms(t["closedAt"]) + 7 * 86400000 > now_ms]
    try:
        if not missing(_load(ledger_path)): return []
        with Locked(ledger_path):
            L = _load(ledger_path); fixed = []
            for t in missing(L): t["afterClose"] = after_close(iso_ms(t["closedAt"])); fixed.append(t.get("id"))
            if fixed: L["autoMoverAt"] = iso(); _save(L, ledger_path)
        return [{"what": "afterClose added", "trade": k} for k in fixed]
    except Exception as e: return [{"what": "afterClose check failed", "err": str(e)[:120]}]

def _close_practice(L, t, ts, lvl, kind, o, now_ms, notes, k):
    reason = "tp" if kind == "tp" else sl_reason(t.get("stopStep"))
    nums = close_numbers(t, lvl, reason); at = ms_iso(min(o + 30000, now_ms))
    words = {"tp": "take profit hit", "sl": "stop loss hit", "breakeven": "no-loss stop hit", "locked": "keep-half stop hit"}[reason]
    fields = {"status": "closed", "closedAt": at, "exitPrice": lvl, "closedReason": reason, "result": nums["result"],
              "pnlUsdt": nums["pnlUsdt"], "r": nums["r"], "pnlNowUsdt": nums["pnlUsdt"], "autoClosed": True, "accounted": True,
              "afterClose": after_close(o)}
    change = {"at": at, "kind": "other", "old": "running", "new": "closed",
              "reason": f"auto: {words} at {lvl} {TAG}: {nums['pnlUsdt']:+.2f} USDT after fees"}
    t.update(fields); t.setdefault("changes", []).append(change); _add_account(L, t, nums)
    ts["closed"] = {"fields": fields, "change": change, "nums": nums}
    notes.append({"what": "practice trade closed", "trade": k, "symbol": t.get("coin"), "reason": reason, "exit": lvl, "pnlUsdt": nums["pnlUsdt"]})

WAIT = ("new", "waiting", "paused")
def _wait_start(t):
    """first candle (open ms) that counts for a waiting idea: the minute after it was sent, or after its last entry/sl/tp change."""
    xs = [iso_ms(t.get("sent") or t.get("time")) or 0] + [iso_ms(c.get("at")) or 0 for c in (t.get("changes") or []) if c.get("kind") in ("entry", "sl", "tp")]
    return max(xs) // 60000 * 60000 + 60000

def undo_cancel(ledger_path, state_path, k, rec):
    """The real entry filled just before our cancel reached Binance: the idea is running after all."""
    with Locked(ledger_path):
        L = _load(ledger_path); t = next((x for x in L.get("trades", []) if x.get("id") == k), None)
        if not t or t.get("status") != "cancelled" or not t.get("autoClosed"): return False
        at = rec.get("openedAt") or iso()
        for f in ("closedAt", "cancelReason", "autoClosed", "accounted"): t.pop(f, None)
        t.update({"status": "running", "filledAt": at, "fillTime": at, "fillPrice": rec.get("fillPrice") or t["entry"]})
        t.setdefault("changes", []).append({"at": iso(), "kind": "other", "old": "cancelled", "new": "running",
                                           "reason": f"auto: the real entry filled just before the cancel, so the trade is running with its stop loss and take profit {TAG}"})
        L["autoMoverAt"] = iso(); _save(L, ledger_path)
    try:
        with open(state_path) as fh: S = json.load(fh)
        S.get("trades", {}).get(k, {}).pop("cancelled", None); S.get("trades", {}).get(k, {})["noWait"] = True; _save_state(S, state_path)
    except Exception: pass
    return True

def _has_change(t, kind, new):
    return any(c.get("kind") == kind and TAG in str(c.get("reason", "")) and c.get("new") == new for c in (t.get("changes") or []))

# ------------------------------------------------------------------ one loop
def run(ledger_path, state_path, real_open=None, info=None, now_ms=None, quiet=True):
    """Returns a list of notes (dicts) for what changed. real_open: {tradeId: engine record with status open}."""
    real_open = real_open or {}; info = info or {}
    now_ms = int(now_ms or time.time() * 1000)
    S = {}
    try:
        with open(state_path) as fh: S = json.load(fh)
    except Exception: S = {}
    S.setdefault("trades", {})
    notes = ensure_after_close(ledger_path, now_ms)
    try: L0 = _load(ledger_path)
    except Exception as e: return [{"what": "ledger not readable", "err": str(e)}]
    # ---- 1. read candles OUTSIDE the lock (network), decide per trade
    plans = {}; wplans = {}
    for t in L0.get("trades", []):
        k = t.get("id"); sym = t.get("coin") or t.get("symbol"); strat = t.get("strategy")
        rec = real_open.get(k)
        if (k and sym and rec is None and t.get("status") in WAIT and t.get("side") in ("LONG", "SHORT") and _levels(t)
                and str(t.get("orderType") or t.get("entryType") or "limit").lower() != "market"):
            ts = S["trades"].setdefault(k, {})
            w0 = _wait_start(t)
            if ts.get("wFrom") != w0 or ts.get("wLevels") != _levels(t): ts.update({"wFrom": w0, "wLevels": _levels(t), "wLast": None, "entryTouched": False})
            if ts.get("entryTouched") or ts.get("noWait"): continue
            start = int(ts.get("wLast") or w0)
            if start > now_ms: continue
            try: K = KLINES(sym, start)
            except Exception as e: notes.append({"what": "candles failed", "symbol": sym, "err": str(e)[:120]}); continue
            exp = iso_ms(t.get("expiresAt"))
            K = [x for x in (K or []) if int(x[0]) >= start and (exp is None or int(x[0]) < exp)]
            if K: wplans[k] = (t, ts, K)
            continue
        running = t.get("status") == "running" or (rec is not None and t.get("status") in ("new", "waiting", "running"))
        if not k or not sym or not running or t.get("side") not in ("LONG", "SHORT"): continue
        if not all(isinstance(t.get(x), (int, float)) for x in ("entry", "sl", "tp")): continue
        ts = S["trades"].setdefault(k, {})
        fill_ms = iso_ms(rec.get("openedAt")) if rec else None
        fill_ms = fill_ms or iso_ms(t.get("filledAt") or t.get("fillTime")) or now_ms - 60000
        start = int(ts.get("lastMs") or (fill_ms // 60000 * 60000))
        try: K = KLINES(sym, start)
        except Exception as e: notes.append({"what": "candles failed", "symbol": sym, "err": str(e)[:120]}); continue
        K = [x for x in (K or []) if int(x[0]) >= start]
        if not K: continue
        plans[k] = (t, ts, K, fill_ms, rec)
    if not plans and not wplans and not any(v.get("closed") or v.get("applied") or v.get("cancelled") or v.get("filled") for v in S["trades"].values()):
        _save_state(S, state_path); return notes
    # ---- 2. apply under the lock on a FRESH copy (only the fields touched here)
    with Locked(ledger_path):
        L = _load(ledger_path); by_id = {t.get("id"): t for t in L.get("trades", [])}; dirty = False
        # 2a. put back our own earlier changes if an older copy was written over them
        for k, ts in S["trades"].items():
            t = by_id.get(k)
            if not t or t.get("status") not in ("new", "waiting", "paused", "running"): continue
            if t.get("status") in WAIT and _levels(t) == ts.get("wLevels"):
                if ts.get("cancelled"):
                    c = ts["cancelled"]; t.update(c["fields"]); t.setdefault("changes", []).append(c["change"])
                    dirty = True; notes.append({"what": "re-applied cancel (ledger was overwritten)", "trade": k}); continue
                if ts.get("filled"):
                    c = ts["filled"]; t.update(c["fields"]); t.setdefault("changes", []).append(c["change"])
                    dirty = True; notes.append({"what": "re-applied practice fill (ledger was overwritten)", "trade": k})
            if t.get("status") not in ("new", "waiting", "running"): continue
            if ts.get("closed") and t.get("status") == "running" and not t.get("accounted"):
                c = ts["closed"]; t.update(c["fields"]); t.setdefault("changes", []).append(c["change"]); _add_account(L, t, c["nums"])
                dirty = True; notes.append({"what": "re-applied close (ledger was overwritten)", "trade": k})
                continue
            ap = ts.get("applied")
            if ap and RANK.get(t.get("stopStep") or "none", 0) < RANK[ap["step"]] and tighter(t["side"], ap["sl"], float(t["sl"])):
                t["sl"] = ap["sl"]; t["stopStep"] = ap["step"]
                if not _has_change(t, ap["kind"], ap["sl"]): t.setdefault("changes", []).append(ap["change"])
                dirty = True; notes.append({"what": "re-applied " + ap["step"] + " (ledger was overwritten)", "trade": k, "sl": ap["sl"]})
        # 2w. waiting limit ideas: target / stop loss reached BEFORE the entry -> cancel; practice entry reached -> running
        for k, (t_old, ts, K) in wplans.items():
            t = by_id.get(k)
            if not t or t.get("status") not in WAIT or _levels(t) != _levels(t_old) or ts.get("cancelled"): continue
            side = t["side"]; long_ = side == "LONG"; entry, sl, tp = _levels(t); strat = t.get("strategy")
            practice = strat in PRACTICE; paused = t.get("status") == "paused"
            ev = None
            for x in K:
                o, hi, lo = int(x[0]), float(x[2]), float(x[3])
                touch_e = (not paused) and lo <= entry <= hi
                sl_hit = lo <= sl if long_ else hi >= sl; tp_hit = hi >= tp if long_ else lo <= tp
                if touch_e: ev = ("fill", o, sl_hit); break
                if tp_hit or sl_hit: ev = ("tp" if tp_hit else "sl", o, None); break
            ts["wLast"] = int(K[-1][0])
            if not ev: continue
            kind, o, sl_too = ev; at = ms_iso(min(o + 30000, now_ms))
            if kind == "fill":
                if not practice:                          # real ideas: Binance decides the fill, stop checking this idea
                    ts["entryTouched"] = True; continue
                fields = {"status": "running", "filledAt": at, "fillTime": at, "fillPrice": entry}
                change = {"at": at, "kind": "other", "old": t.get("status"), "new": "running",
                          "reason": f"auto: the price reached the entry {entry}, so the practice trade started {TAG}"}
                t.update(fields); t.setdefault("changes", []).append(change)
                ts["filled"] = {"fields": fields, "change": change}
                ts.update({"lastMs": o + 60000, "slSeen": sl, "hitsFrom": o + 60000, "best": None, "worst": None})
                notes.append({"what": "practice entry filled", "trade": k, "symbol": t.get("coin"), "entry": entry}); dirty = True
                if sl_too: _close_practice(L, t, ts, sl, "sl", o, now_ms, notes, k)
                continue
            words = "the target (take profit " + str(tp) + ")" if kind == "tp" else "the stop loss (" + str(sl) + ")"
            fields = {"status": "cancelled", "closedAt": at, "autoClosed": True, "accounted": True,
                      "cancelReason": f"Price reached {words} before the entry {entry}, so the idea was cancelled {TAG}"}
            change = {"at": at, "kind": "other", "old": t.get("status"), "new": "cancelled", "reason": "auto: " + fields["cancelReason"]}
            t.update(fields); t.setdefault("changes", []).append(change)
            ts["cancelled"] = {"fields": fields, "change": change}; dirty = True
            notes.append({"what": "waiting idea cancelled", "trade": k, "symbol": t.get("coin"), "touched": kind, "real": strat not in PRACTICE})
        # 2b. this minute
        for k, (t_old, ts, K, fill_ms, rec) in plans.items():
            t = by_id.get(k)
            if not t or t.get("status") not in ("new", "waiting", "running"): continue
            if t.get("stopStep") != t_old.get("stopStep") or t.get("sl") != t_old.get("sl"): ts.pop("hitsFrom", None)
            side = t["side"]; long_ = side == "LONG"; sl = float(t["sl"]); tp = float(t["tp"]); strat = t.get("strategy")
            practice = strat in PRACTICE and rec is None
            # hits are only checked from the moment we know the CURRENT sl (never on old candles with a newer sl)
            if ts.get("slSeen") != sl:
                if ts.get("lastMs") is None:      # first look: the AI check already watched the trade up to its last run / sl change
                    seen = [iso_ms(c.get("at")) or 0 for c in (t.get("changes") or []) if c.get("kind") in ("sl", "noloss", "halfprofit")]
                    ts["hitsFrom"] = max([fill_ms // 60000 * 60000 + 60000, (iso_ms(L.get("updated")) or 0) // 60000 * 60000] + [x // 60000 * 60000 + 60000 for x in seen])
                else: ts["hitsFrom"] = int(K[0][0])
                ts["slSeen"] = sl
            hits_from = int(ts.get("hitsFrom") or K[0][0])
            best = ts.get("best"); worst = ts.get("worst"); hit = None
            for x in K:
                o, hi, lo, cl = int(x[0]), float(x[2]), float(x[3]), float(x[4])
                if practice and o >= hits_from and hit is None:
                    sl_hit = lo <= sl if long_ else hi >= sl; tp_hit = hi >= tp if long_ else lo <= tp
                    if sl_hit: hit = ("sl", sl, o)
                    elif tp_hit: hit = ("tp", tp, o)
                if hit: break
                if o >= fill_ms // 60000 * 60000 + 60000 or ts.get("lastMs"):
                    b = hi if long_ else lo; w = lo if long_ else hi
                    best = b if best is None else (max(best, b) if long_ else min(best, b))
                    worst = w if worst is None else (min(worst, w) if long_ else max(worst, w))
            now_px = float(K[-1][4]); last_open = int(K[-1][0])
            ts["lastMs"] = last_open                                     # the forming candle is read again next loop
            if best is not None: ts["best"] = best
            if worst is not None: ts["worst"] = worst
            tb = t.get("best"); tw = t.get("worst")
            if best is not None and (tb is None or (best > tb if long_ else best < tb)): t["best"] = best; dirty = True
            if worst is not None and (tw is None or (worst < tw if long_ else worst > tw)): t["worst"] = worst; dirty = True
            mp = best_money(t)                                           # v9.2: maxPnlUsdt/Pct always match "best" (qty, after fees)
            if mp and (t.get("maxPnlUsdt") != mp[0] or t.get("maxPnlPct") != mp[1]): t["maxPnlUsdt"], t["maxPnlPct"] = mp; dirty = True
            if hit:
                kind, lvl, o = hit
                _close_practice(L, t, ts, lvl, kind, o, now_ms, notes, k); dirty = True
                continue
            if best is None: continue
            tick = (info.get(t.get("coin") or t.get("symbol")) or {}).get("tick")
            d = decide(t, best, now_px, float(tick) if tick else None)
            if not d: continue
            at = iso(); words = "no-loss: stop moved to entry +0.2%" if d["step"] == "noloss" else "keep half: stop moved halfway to the target"
            if d["move"]:
                change = {"at": at, "kind": d["kind"], "old": d["old"], "new": d["new"],
                          "reason": f"auto: {words} {TAG} (best {d['progress']:.0%} of the way, now {d['now']:.0%})"}
                t["sl"] = d["new"]; t.setdefault("changes", []).append(change)
                ts["applied"] = {"step": d["step"], "kind": d["kind"], "sl": d["new"], "change": change}
                notes.append({"what": "stop moved", "trade": k, "symbol": t.get("coin"), "step": d["step"], "old": d["old"], "new": d["new"], "real": rec is not None})
            else:
                notes.append({"what": "step marked (stop was already tighter)", "trade": k, "step": d["step"], "sl": d["old"]})
            t["stopStep"] = d["step"]; ts["slSeen"] = float(t["sl"]); ts["hitsFrom"] = last_open + 60000; dirty = True   # new stop counts from the next candle
        if dirty:
            L["autoMoverAt"] = iso()
            _save(L, ledger_path)
    # forget finished trades after a day
    for k in list(S["trades"]):
        t = None
        for x in L.get("trades", []):
            if x.get("id") == k: t = x; break
        if t is None or (t.get("status") in ("closed", "cancelled", "expired") and now_ms - (iso_ms(t.get("closedAt")) or now_ms) > 86400000):
            S["trades"].pop(k, None)
    _save_state(S, state_path)
    return notes

def _save_state(S, p):
    S["at"] = iso(); tmp = p + ".tmp"
    with open(tmp, "w") as fh: json.dump(S, fh, indent=1)
    os.replace(tmp, p)

#!/usr/bin/env python3
"""cloud_guard.py - the FREE always-on cloud guard (GitHub Actions, every 5 min). No AI, no tokens.

Why: the box (where the 1-minute watcher runs) is paused by the app when idle. While it sleeps nobody moves practice
stops, closes practice trades at SL/TP or cancels waiting ideas. This script does that job in the cloud, with the SAME
rules (it runs the box's own stop_mover.py, copied next to it), on PRACTICE trades only.

  Never touches Binance accounts: no API key, only PUBLIC 1m candles. Trades the box's real-money engine has an
  order/position for (realIds) are left out completely (Binance holds their SL/TP; the box handles them on wake).

Files on branch 'data' of the desk repo (all AES-256-GCM encrypted, same key as the desk = PBKDF2(PIN, salt)):
  guard_in.enc.json   written by the BOX (guard_box.py): active trades, the mover's state, realIds, ticks, gen, at
  guard_out.enc.json  written HERE: the cloud's copy of the trades + mover state + the list of changes it made
  data.enc.json       the desk's data. Patched HERE only while the box is asleep, so the desk shows cloud closes.
The box is the MASTER: on wake it merges guard_out's changes into ledger.json (guard_box.merge_before_mover) only for
candles the box itself has not checked yet, each change once (key = trade id + kind). Then it publishes again.

Price source: Binance futures 1m candles. GitHub's computers are in the US and Binance futures answers them
"451 restricted location", so it then uses Binance SPOT 1m candles (data-api.binance.vision, public). Spot is very
close to the futures price but not identical; every change made with spot says so ("source": "spot").

env: GUARD_KEY (base64 32 bytes, GitHub secret), GITHUB_TOKEN (the Action's own token), GITHUB_REPOSITORY.
Secrets are never printed.
"""
import os, sys, json, time, base64, zlib, copy, re, tempfile, datetime, urllib.request, urllib.error, urllib.parse, secrets as _sec

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import stop_mover as M                                   # the box's own rules (copy kept in sync by the box)

REPO = os.environ.get("GITHUB_REPOSITORY", "bm26200967/gtb-desk")
BRANCH = "data"; IN_PATH = "guard_in.enc.json"; OUT_PATH = "guard_out.enc.json"; DESK_PATH = "data.enc.json"
AAD_GUARD = "gtb-guard-v1"; AAD_DESK = "gtb-desk-v1"
ASLEEP_S = 420               # box counts as asleep if its guard_in is older than 7 min (it pings every 5 min)
DESK_REFRESH_S = 900        # while the box sleeps, refresh the desk cloud status at least every 15 min
KEEPALIVE_S = 6 * 86400      # re-enable the workflow about weekly (public repos: schedules stop after 60 quiet days)
WAIT = ("new", "waiting", "paused")
LK = M.LK

def iso(dt=None): return (dt or datetime.datetime.now(LK)).isoformat(timespec="seconds")
def log(msg):
    msg = str(msg)
    for name in ("GUARD_KEY", "GITHUB_TOKEN", "GITHUB_DESK_TOKEN", "LIVEDESK_PIN"):
        v = os.environ.get(name)
        if v and len(v) > 3: msg = msg.replace(v, "***")
    print(f"{iso()} {msg}", flush=True)

# ------------------------------------------------------------------ crypto (same envelope as livedesk/publish.py)
def enc(obj, key, salt_b64, iters, aad):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    raw = zlib.compress(json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8"), 9)
    iv = _sec.token_bytes(12)
    ct = AESGCM(key).encrypt(iv, raw, aad.encode())
    b = lambda x: base64.b64encode(x).decode()
    return {"v": 1, "kdf": "PBKDF2-SHA256", "iter": iters, "salt": salt_b64, "iv": b(iv), "aad": aad, "zip": "deflate",
            "ct": b(ct), "at": iso()}
def dec(env, key):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    raw = AESGCM(key).decrypt(base64.b64decode(env["iv"]), base64.b64decode(env["ct"]), env["aad"].encode())
    return json.loads(zlib.decompress(raw))

# ------------------------------------------------------------------ candles (public, no key)
FAPI = "https://fapi.binance.com/fapi/v1/klines"
SPOT = "https://data-api.binance.vision/api/v3/klines"
STATE = {"fapi": None, "source": None}
def _get_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "gtb-cloud-guard"})
    with urllib.request.urlopen(req, timeout=timeout) as r: return json.loads(r.read())
def _pages(base, symbol, start_ms):
    out = []; s = int(start_ms)
    for _ in range(3):
        k = _get_json(base + "?" + urllib.parse.urlencode({"symbol": symbol, "interval": "1m", "startTime": s, "limit": 1000})) or []
        out += k
        if len(k) < 1000: break
        s = int(k[-1][0]) + 60000
    return out
def spot_symbol(sym):
    """Futures 1000PEPEUSDT = 1000 x spot PEPEUSDT."""
    m = re.match(r"^(1000000|1000)([A-Z0-9]+)$", sym)
    return (m.group(2), int(m.group(1))) if m else (sym, 1)
def klines(symbol, start_ms):
    if STATE["fapi"] is not False:
        try:
            k = _pages(FAPI, symbol, start_ms); STATE["fapi"] = True; STATE["source"] = STATE["source"] or "futures"; return k
        except urllib.error.HTTPError as e:
            if e.code in (451, 403): STATE["fapi"] = False; log(f"futures candles blocked here ({e.code}), using spot candles")
            else: raise
        except Exception:
            STATE["fapi"] = False
    base, mult = spot_symbol(symbol)
    k = _pages(SPOT, base, start_ms)
    STATE["source"] = "spot"
    if mult != 1:
        k = [[r[0]] + [str(float(x) * mult) for x in r[1:5]] + list(r[5:]) for r in k]
    return k

# ------------------------------------------------------------------ the pure part (tests call this)
RANK = M.RANK
def _kind_at(t, field):
    ms = M.iso_ms(t.get(field)); return (ms // 60000 * 60000) if ms else None

def process(gin, prev, now_ms=None, klines_fn=None):
    """One cloud run. gin = decrypted guard_in, prev = decrypted guard_out (or None). Returns (gout, new_changes)."""
    now_ms = int(now_ms or time.time() * 1000)
    if prev and prev.get("baseGen") == gin.get("gen"):
        L = copy.deepcopy(prev["ledger"]); S = copy.deepcopy(prev.get("state") or {"trades": {}}); changes = list(prev.get("changes") or [])
        extremes = dict(prev.get("extremes") or {})
    else:                                                # the box published a newer picture: start from it
        L = copy.deepcopy(gin["ledger"]); S = copy.deepcopy(gin.get("state") or {"trades": {}}); changes = []; extremes = {}
    S.setdefault("trades", {})
    real = set(gin.get("realIds") or [])
    keep_real = [t for t in L.get("trades", []) if t.get("id") in real]
    work = dict(L); work["trades"] = [t for t in L.get("trades", []) if t.get("id") not in real]
    before = {t.get("id"): copy.deepcopy(t) for t in work["trades"]}
    old_fn, old_tag = M.KLINES, M.TAG; M.KLINES = klines_fn or klines; M.TAG = "(free watcher) (cloud guard)"
    d = tempfile.mkdtemp(prefix="guard_")
    lp = os.path.join(d, "ledger.json"); sp = os.path.join(d, "state.json")
    try:
        with open(lp, "w") as fh: json.dump(work, fh)
        with open(sp, "w") as fh: json.dump(S, fh)
        notes = M.run(lp, sp, real_open={}, info=gin.get("info") or {}, now_ms=now_ms)
        with open(lp) as fh: L2 = json.load(fh)
        with open(sp) as fh: S2 = json.load(fh)
    finally:
        M.KLINES = old_fn; M.TAG = old_tag
    src = STATE["source"] or ("test" if klines_fn else None)
    have = {c["key"] for c in changes}; new = []
    for t in L2.get("trades", []):
        k = t.get("id"); b = before.get(k)
        if not b: continue
        ts = copy.deepcopy(S2["trades"].get(k) or {})
        lv = [b.get("entry"), b.get("sl"), b.get("tp")]
        prevp = {"status": b.get("status"), "sl": b.get("sl"), "stopStep": b.get("stopStep") or "none", "levels": lv}
        def add(kind, at_ms, rec):
            key = f"{k}:{kind}"
            if key in have: return
            have.add(key)
            c = {"key": key, "id": k, "kind": kind, "atMs": at_ms, "at": M.ms_iso(at_ms) if at_ms else iso(), "prev": prevp,
                 "rec": rec, "trade": t, "state": ts, "source": src}
            changes.append(c); new.append(c)
        if b.get("status") in WAIT and t.get("filledAt") and not b.get("filledAt"):
            add("fill", _kind_at(t, "filledAt"), ts.get("filled"))
        if b.get("status") in WAIT and t.get("status") == "cancelled":
            add("cancel", _kind_at(t, "closedAt"), ts.get("cancelled"))
        if RANK.get(t.get("stopStep") or "none", 0) > RANK.get(b.get("stopStep") or "none", 0):
            for step in ("noloss", "half"):
                if RANK[step] > RANK.get(b.get("stopStep") or "none", 0) and RANK[step] <= RANK[t.get("stopStep")]:
                    add(step, int(ts.get("lastMs") or now_ms // 60000 * 60000), ts.get("applied") or {"step": step, "sl": t.get("sl")})
        if b.get("status") in WAIT + ("running",) and t.get("status") == "closed":
            add("close", _kind_at(t, "closedAt"), ts.get("closed"))
        if t.get("best") != b.get("best") or t.get("worst") != b.get("worst"):
            extremes[k] = {"best": t.get("best"), "worst": t.get("worst"), "maxPnlUsdt": t.get("maxPnlUsdt"),
                           "maxPnlPct": t.get("maxPnlPct"), "atMs": int(ts.get("lastMs") or 0), "source": src}
    L2["trades"] = L2.get("trades", []) + keep_real
    gout = {"v": 1, "baseGen": gin.get("gen"), "at": iso(), "atMs": now_ms, "ledger": L2, "state": S2, "changes": changes,
            "extremes": extremes, "source": src, "runs": int((prev or {}).get("runs", 0)) + 1,
            "keepaliveAt": (prev or {}).get("keepaliveAt"),
            "deskPatchedAt": (prev or {}).get("deskPatchedAt") if (prev or {}).get("baseGen") == gin.get("gen") else None,
            "lastNotes": [n for n in notes if n.get("what")][-20:]}
    return gout, new

def box_asleep(gin, now_ms=None):
    at = M.iso_ms(gin.get("at")); now_ms = int(now_ms or time.time() * 1000)
    return at is None or now_ms - at > ASLEEP_S * 1000

def patch_desk(d, gout, touched_ids, now_iso=None):
    """Put the cloud's versions of the touched trades into the desk data + a small cloudGuard status block."""
    by = {t.get("id"): t for t in gout["ledger"].get("trades", [])}
    n = 0
    for part in ("desk", "ledger"):
        trades = (d.get(part) or {}).get("trades")
        if not isinstance(trades, list): continue
        for i, t in enumerate(trades):
            k = t.get("id")
            if k in touched_ids and k in by: trades[i] = by[k]; n += 1
    d["cloudGuard"] = {"at": now_iso or iso(), "every": "5 min", "boxAsleep": True, "source": gout.get("source"),
                       "changes": len(gout.get("changes") or []), "patched": n}
    return n

# ------------------------------------------------------------------ GitHub contents API (the Action's own token)
def gh(method, path, body=None, token=None):
    token = token or os.environ.get("GITHUB_TOKEN", "")
    req = urllib.request.Request("https://api.github.com" + path, data=json.dumps(body).encode() if body is not None else None,
                                 method=method, headers={"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
                                 "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "gtb-cloud-guard"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            t = r.read(); return r.status, (json.loads(t) if t else {})
    except urllib.error.HTTPError as e:
        t = e.read()
        try: return e.code, json.loads(t)
        except Exception: return e.code, {"message": t[:200].decode("utf-8", "replace")}
    except Exception as e:
        return 0, {"message": "network: " + type(e).__name__}

def get_file(path, token=None):
    c, j = gh("GET", f"/repos/{REPO}/contents/{path}?ref={BRANCH}", token=token)
    if c != 200: return None, None
    raw = base64.b64decode(j.get("content", "")) if j.get("content") else None
    if raw is None and j.get("download_url"):            # files over 1 MB come without content
        c2, j2 = gh("GET", f"/repos/{REPO}/git/blobs/{j['sha']}", token=token); raw = base64.b64decode(j2.get("content", ""))
    return json.loads(raw), j.get("sha")

def put_file(path, obj, sha, msg, token=None):
    body = {"message": msg, "content": base64.b64encode((json.dumps(obj, separators=(",", ":")) + "\n").encode()).decode(), "branch": BRANCH}
    if sha: body["sha"] = sha
    return gh("PUT", f"/repos/{REPO}/contents/{path}", body, token=token)

# ------------------------------------------------------------------ one Action run
def main():
    kb = os.environ.get("GUARD_KEY", "").strip()
    if not kb: log("GUARD_KEY secret missing: nothing to do"); return 2
    key = base64.b64decode(kb)
    env_in, _ = get_file(IN_PATH)
    if not env_in: log("no guard_in yet (the box has not published): nothing to do"); return 0
    gin = dec(env_in, key)
    env_out, out_sha = get_file(OUT_PATH)
    prev = None
    if env_out:
        try: prev = dec(env_out, key)
        except Exception as e: log(f"old guard_out unreadable ({type(e).__name__}): starting fresh")
    now_ms = int(time.time() * 1000)
    asleep = box_asleep(gin, now_ms)
    if os.environ.get("GUARD_TEST"):                     # test run: read + decrypt + candles + rules, write NOTHING
        gout, new = process(gin, prev, now_ms)
        log(f"TEST OK: box {'asleep' if asleep else 'awake'} (last sign {gin.get('at')}), {len(gin['ledger'].get('trades', []))} active trades, "
            f"{len(gin.get('realIds') or [])} real left out, would change {[c['key'] for c in new]}, price source {gout['source']}")
        env_d, _ = get_file(DESK_PATH)
        if env_d: dec(env_d, key); log("TEST OK: desk data decrypts with the key")
        return 0
    if not asleep:                                       # the box's own 1-minute watcher is on duty: never compete with it
        log(f"box awake (last sign {gin.get('at')}): nothing to do")
        if now_ms - (M.iso_ms((prev or {}).get("keepaliveAt")) or 0) > KEEPALIVE_S * 1000 and prev is not None:
            c, _ = gh("PUT", f"/repos/{REPO}/actions/workflows/{os.environ.get('GUARD_WORKFLOW', 'cloud-guard.yml')}/enable")
            log(f"keepalive (re-enable workflow) -> {c}")
            prev["keepaliveAt"] = iso(); put_file(OUT_PATH, enc(prev, key, gin.get("salt"), gin.get("iter"), AAD_GUARD), out_sha, "cloud guard keepalive")
        return 0
    gout, new = process(gin, prev, now_ms)
    gout["boxAsleep"] = asleep; gout["boxAt"] = gin.get("at")
    ext_changed = (prev or {}).get("extremes") != gout["extremes"]
    log(f"box {'ASLEEP' if asleep else 'awake'} (last sign {gin.get('at')}), {len(gin['ledger'].get('trades', []))} active trades, "
        f"{len(gin.get('realIds') or [])} real left out, new changes {len(new)}: {[c['key'] for c in new]}, source {gout['source']}")
    if now_ms - (M.iso_ms((prev or {}).get("keepaliveAt")) or 0) > KEEPALIVE_S * 1000:
        wf = os.environ.get("GUARD_WORKFLOW", "cloud-guard.yml")
        c, _ = gh("PUT", f"/repos/{REPO}/actions/workflows/{wf}/enable")
        log(f"keepalive (re-enable workflow) -> {c}"); gout["keepaliveAt"] = iso()
    salt, iters = gin.get("salt"), gin.get("iter")
    stale = now_ms - (M.iso_ms(gout.get("deskPatchedAt")) or 0) > DESK_REFRESH_S * 1000
    if asleep and (new or ext_changed or stale):
        env_d, dsha = get_file(DESK_PATH)
        if env_d:
            d = dec(env_d, key)
            touched = {c["id"] for c in gout["changes"]} | set(gout["extremes"])
            n = patch_desk(d, gout, touched)
            c, j = put_file(DESK_PATH, enc(d, key, env_d.get("salt"), env_d.get("iter"), AAD_DESK), dsha, "data (cloud guard)")
            log(f"desk patch ({n} trades) -> {c}" + (" (the box pushed meanwhile: skipped)" if c in (409, 422) else ""))
            if c in (200, 201): gout["deskPatchedAt"] = iso()
    if asleep or new or not prev or prev.get("baseGen") != gout.get("baseGen") or gout.get("keepaliveAt") != (prev or {}).get("keepaliveAt"):
        c, j = put_file(OUT_PATH, enc(gout, key, salt, iters, AAD_GUARD), out_sha, "cloud guard")
        log(f"guard_out push -> {c}")
        if c not in (200, 201): return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())

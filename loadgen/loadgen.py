import argparse
import asyncio
import csv
import json
import math
import multiprocessing as mp
import os
import queue
import random
import statistics
import sys
import time
import urllib.parse
import urllib.request
from array import array
from collections import Counter, defaultdict

IS_WIN = sys.platform.startswith("win")
MEASURE_SKIP = 5.0
NONE = float("inf")
IDLE_MAX = 3.0


def build_profile(a):
    steps = []
    t = 0.0
    if a.profile == "step":
        if a.warmup > 0:
            steps.append({"idx": 0, "kind": "warmup", "rate": a.warmup_rate,
                          "t0": 0.0, "t1": a.warmup})
            t = a.warmup
        k = 1
        rate = a.start_rate
        while rate <= a.max_rate and k <= a.max_steps:
            steps.append({"idx": k, "kind": "step", "rate": rate,
                          "t0": t, "t1": t + a.step_duration})
            t += a.step_duration
            rate += a.step_inc
            k += 1
    else:
        if a.warmup > 0:
            steps.append({"idx": 0, "kind": "warmup", "rate": a.rate, "t0": 0.0, "t1": a.warmup})
            t = a.warmup
        steps.append({"idx": 1, "kind": "const", "rate": a.rate, "t0": t, "t1": t + a.duration})
    return steps


def rate_at(steps, t):
    for s in steps:
        if s["t0"] <= t < s["t1"]:
            return s["rate"]
    return 0.0


class Http:
    def __init__(self, host, port, max_conns):
        self.host, self.port = host, port
        self.host_hdr = f"{host}:{port}".encode()
        self.idle = []
        self.sem = asyncio.Semaphore(max_conns)
        self.opened = 0

    async def _open(self):
        r, w = await asyncio.open_connection(self.host, self.port)
        self.opened += 1
        return r, w

    @staticmethod
    def _close(conn):
        try:
            conn[1].close()
        except Exception:
            pass

    @staticmethod
    async def _read_response(reader):
        head = await reader.readuntil(b"\r\n\r\n")
        lines = head.split(b"\r\n")
        status = int(lines[0].split(b" ", 2)[1])
        headers = {}
        for ln in lines[1:]:
            if ln:
                k, _, v = ln.partition(b":")
                headers[k.strip().lower()] = v.strip()
        keep = headers.get(b"connection", b"").lower() != b"close"
        if 100 <= status < 200 or status in (204, 304):
            return status, headers, keep
        cl = headers.get(b"content-length")
        if cl is not None:
            n = int(cl)
            if n:
                await reader.readexactly(n)
        elif headers.get(b"transfer-encoding", b"").lower() == b"chunked":
            while True:
                size = int((await reader.readuntil(b"\r\n")).split(b";")[0], 16)
                await reader.readexactly(size + 2)
                if size == 0:
                    break
        else:
            await reader.read()
            keep = False
        return status, headers, keep

    async def get(self, path, on_sent):
        req = b"GET " + path.encode() + b" HTTP/1.1\r\nHost: " + self.host_hdr + b"\r\n\r\n"
        await self.sem.acquire()
        try:
            for attempt in (1, 2):
                conn = None
                if attempt == 1:
                    while self.idle:
                        c, last = self.idle.pop()
                        if time.monotonic() - last < IDLE_MAX:
                            conn = c
                            break
                        self._close(c)
                reused = conn is not None
                if conn is None:
                    conn = await self._open()
                keep = False
                try:
                    conn[1].write(req)
                    if attempt == 1:
                        on_sent()
                    status, headers, keep = await self._read_response(conn[0])
                    return status, headers.get(b"x-pod", b"-").decode()
                except (ConnectionError, asyncio.IncompleteReadError):
                    keep = False
                    if attempt == 1 and reused:
                        continue
                    raise
                finally:
                    if keep:
                        self.idle.append((conn, time.monotonic()))
                    else:
                        self._close(conn)
        finally:
            self.sem.release()


class Stats:
    def __init__(self):
        self.sent = Counter()
        self.ok = Counter()
        self.non2xx = Counter()
        self.conn_err = Counter()
        self.timeouts = Counter()
        self.sched_ok = Counter()
        self.sched_bad = Counter()
        self.lat = defaultdict(lambda: array("f"))
        self.pods = Counter()
        self.codes = Counter()
        self.cpu = {}
        self.lag = array("f")


async def worker_async(idx, nproc, cfg, steps, ids, start_val, stop_val, report_q):
    loop = asyncio.get_running_loop()
    if hasattr(loop, "_clock_resolution"):
        loop._clock_resolution = 0.0002
    clock = time.perf_counter
    start = clock() + (start_val.value - time.time())
    now = lambda: clock() - start
    http = Http(cfg["host"], cfg["port"], cfg["max_conns"])
    st = Stats()
    rnd = random.Random(idx * 7919 + int(time.time()))
    timeout = cfg["timeout"]
    tasks = set()
    has_timeout_cm = hasattr(asyncio, "timeout")

    def pick():
        p = cfg["path"]
        return p.replace("{id}", str(rnd.choice(ids))) if ids else p

    async def one(t_sched, path):
        def on_sent():
            st.sent[int(now())] += 1
        remaining = t_sched + timeout - now()
        kind, status, pod = "conn", 0, "-"
        try:
            if remaining <= 0:
                raise asyncio.TimeoutError
            if has_timeout_cm:
                async with asyncio.timeout(remaining):
                    status, pod = await http.get(path, on_sent)
            else:
                status, pod = await asyncio.wait_for(http.get(path, on_sent), remaining)
            kind = "ok" if 200 <= status < 300 else "non2xx"
        except (asyncio.TimeoutError, TimeoutError):
            kind = "timeout"
        except (OSError, ConnectionError, asyncio.IncompleteReadError, ValueError, IndexError):
            kind = "conn"
        t_end = now()
        se, ss = int(t_end), int(t_sched)
        if kind == "ok":
            st.ok[se] += 1
            st.sched_ok[ss] += 1
            st.pods[pod] += 1
        else:
            st.sched_bad[ss] += 1
            if kind == "non2xx":
                st.non2xx[se] += 1
                st.pods[pod] += 1
            elif kind == "timeout":
                st.timeouts[se] += 1
                st.non2xx[se] += 1
            else:
                st.conn_err[se] += 1
        st.codes[str(status) if kind in ("ok", "non2xx") else kind] += 1
        if kind == "timeout":
            st.lat[ss].append(timeout * 1000.0)
        elif kind != "conn":
            st.lat[ss].append((t_end - t_sched) * 1000.0)

    rstate = {"reported": 0}

    def report(final):
        r0 = rstate["reported"]
        if final > r0:
            report_q.put(("ok", idx, r0, final, [st.ok.get(s, 0) for s in range(r0, final)]))
            rstate["reported"] = final

    async def reporter():
        while True:
            await asyncio.sleep(0.25)
            cur = now()
            st.cpu[int(cur)] = time.process_time()
            report(int(cur - 0.05))

    rep = asyncio.create_task(reporter())
    while now() < -0.001:
        await asyncio.sleep(min(0.05, -now()))

    for s in steps:
        if s["rate"] <= 0:
            continue
        rp = s["rate"] / nproc
        i = 0
        while True:
            t = s["t0"] + (i + idx / nproc) / rp
            if t >= s["t1"] or t >= stop_val.value:
                break
            while True:
                d = t - now()
                if d <= 0:
                    break
                await asyncio.sleep(d)
            st.lag.append(max(0.0, now() - t) * 1000.0)
            task = loop.create_task(one(t, pick()))
            tasks.add(task)
            task.add_done_callback(tasks.discard)
            i += 1
        if s["t1"] >= stop_val.value:
            break
    if tasks:
        await asyncio.gather(*list(tasks), return_exceptions=True)
    st.cpu[int(now()) + 1] = time.process_time()
    rep.cancel()
    report(int(math.ceil(max(now(), steps[-1]["t1"] if stop_val.value == NONE
                             else min(stop_val.value, steps[-1]["t1"])))) + 1)
    for c, _ in http.idle:
        Http._close(c)
    lag_sorted = sorted(st.lag)
    return {
        "idx": idx,
        "sent": dict(st.sent), "ok": dict(st.ok), "non2xx": dict(st.non2xx),
        "conn_err": dict(st.conn_err), "timeouts": dict(st.timeouts),
        "sched_ok": dict(st.sched_ok), "sched_bad": dict(st.sched_bad),
        "lat": {s: a.tobytes() for s, a in st.lat.items()},
        "pods": dict(st.pods), "codes": dict(st.codes), "cpu": st.cpu,
        "opened": http.opened,
        "lag_p99_ms": lag_sorted[int(0.99 * (len(lag_sorted) - 1))] if lag_sorted else 0.0,
        "lag_max_ms": lag_sorted[-1] if lag_sorted else 0.0,
    }


def worker_main(idx, nproc, cfg, steps, ids, start_val, stop_val, ready_q, go_evt, report_q, out_q):
    if IS_WIN:
        try:
            import ctypes
            ctypes.windll.winmm.timeBeginPeriod(1)
        except Exception:
            pass
    ready_q.put(idx)
    go_evt.wait()
    res = asyncio.run(worker_async(idx, nproc, cfg, steps, ids, start_val, stop_val, report_q))
    out_q.put(res)


def pct(vals, p):
    if not vals:
        return float("nan")
    k = (len(vals) - 1) * p / 100.0
    lo = int(k)
    hi = min(lo + 1, len(vals) - 1)
    return vals[lo] + (vals[hi] - vals[lo]) * (k - lo)


def merge(parts):
    keys = ["sent", "ok", "non2xx", "conn_err", "timeouts", "sched_ok", "sched_bad"]
    m = {k: Counter() for k in keys}
    lat = defaultdict(lambda: array("f"))
    pods, codes = Counter(), Counter()
    cpu_by_worker = []
    for p in parts:
        for k in keys:
            m[k].update({int(s): v for s, v in p[k].items()})
        for s, b in p["lat"].items():
            a = array("f")
            a.frombytes(b)
            lat[int(s)].extend(a)
        pods.update(p["pods"])
        codes.update(p["codes"])
        cpu_by_worker.append({int(s): v for s, v in p["cpu"].items()})
    return m, lat, pods, codes, cpu_by_worker


def cpu_between(cpu_by_worker, a, b):
    total = 0.0
    for c in cpu_by_worker:
        if not c:
            continue
        keys = sorted(c)
        ka = max([k for k in keys if k <= a], default=keys[0])
        kb = max([k for k in keys if k <= b], default=keys[-1])
        if kb > ka:
            total += (c[kb] - c[ka]) / (kb - ka)
    return total


def analyze(cfg, steps, stop_at, start_epoch, parts):
    m, lat, pods, codes, cpu_w = merge(parts)
    end = int(math.ceil(min(stop_at, steps[-1]["t1"])))
    rows = []
    for s in range(0, end):
        ls = sorted(lat.get(s, ()))
        rows.append({
            "t": s, "epoch": round(start_epoch + s, 3),
            "target_rps": float(rate_at(steps, s + 0.5)),
            "sent_rps": m["sent"].get(s, 0),
            "achieved_rps": m["ok"].get(s, 0),
            "ok_2xx": m["ok"].get(s, 0),
            "non_2xx": m["non2xx"].get(s, 0),
            "conn_errors": m["conn_err"].get(s, 0),
            "timeouts": m["timeouts"].get(s, 0),
            "p50_ms": round(pct(ls, 50), 3), "p95_ms": round(pct(ls, 95), 3),
            "p99_ms": round(pct(ls, 99), 3),
        })
    out_steps = []
    for st in steps:
        if st["t0"] >= stop_at:
            break
        a, b = int(st["t0"] + MEASURE_SKIP), int(st["t1"])
        if st["kind"] != "step":
            a = int(st["t0"])
        secs = range(a, b)
        ach = [m["ok"].get(s, 0) for s in secs]
        sent = [m["sent"].get(s, 0) for s in secs]
        ls = sorted(x for s in secs for x in lat.get(s, ()))
        okc = sum(m["sched_ok"].get(s, 0) for s in secs)
        badc = sum(m["sched_bad"].get(s, 0) for s in secs)
        out_steps.append({
            "idx": st["idx"], "kind": st["kind"], "target_rps": float(st["rate"]),
            "t0": st["t0"], "t1": st["t1"], "win_t0": a, "win_t1": b,
            "epoch_t0": start_epoch + st["t0"], "epoch_t1": start_epoch + st["t1"],
            "win_epoch_t0": start_epoch + a, "win_epoch_t1": start_epoch + b,
            "achieved_mean": statistics.fmean(ach) if ach else 0.0,
            "achieved_std": statistics.stdev(ach) if len(ach) > 1 else 0.0,
            "sent_mean": statistics.fmean(sent) if sent else 0.0,
            "p50_ms": pct(ls, 50), "p95_ms": pct(ls, 95), "p99_ms": pct(ls, 99),
            "non_2xx_share": badc / (okc + badc) if (okc + badc) else float("nan"),
            "conn_errors": sum(m["conn_err"].get(s, 0) for s in secs),
            "timeouts": sum(m["timeouts"].get(s, 0) for s in secs),
            "gen_cpu_cores": cpu_between(cpu_w, a, b),
        })
    measured = [s for s in out_steps if s["kind"] != "warmup"]
    best = max(measured, key=lambda s: s["achieved_mean"]) if measured else None
    return {
        "config": cfg, "start_epoch": start_epoch, "stop_at": stop_at,
        "start_iso": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(start_epoch)),
        "steps": out_steps, "per_second": rows,
        "X": best["achieved_mean"] if best else None,
        "X_std": best["achieved_std"] if best else None,
        "X_step": best["idx"] if best else None,
        "X_target": best["target_rps"] if best else None,
        "pods": dict(pods), "codes": dict(codes),
        "connections_opened": sum(p["opened"] for p in parts),
        "gen_lag_p99_ms": max(p["lag_p99_ms"] for p in parts),
        "gen_lag_max_ms": max(p["lag_max_ms"] for p in parts),
    }


def fetch_ids(base, prefix, n):
    with urllib.request.urlopen(f"{base}{prefix}?limit={n}&offset=0", timeout=10) as r:
        body = json.loads(r.read())
    ids = [it["id"] for it in body.get("items", [])]
    if not ids:
        sys.exit("У базі немає записів. Спершу запустіть scripts/seed.py")
    return ids


def main():
    ap = argparse.ArgumentParser(description="Генератор навантаження ЛР2 (відкритий контур)")
    ap.add_argument("--url", default="http://127.0.0.1:8080")
    ap.add_argument("--path", default="/consultations/{id}")
    ap.add_argument("--profile", choices=["step", "const"], default="step")
    ap.add_argument("--warmup", type=float, default=10, help="прогрів, с")
    ap.add_argument("--warmup-rate", type=float, default=200)
    ap.add_argument("--start-rate", type=float, default=200)
    ap.add_argument("--step-inc", type=float, default=200)
    ap.add_argument("--step-duration", type=float, default=20)
    ap.add_argument("--max-rate", type=float, default=20000, help="запобіжник для профілю step")
    ap.add_argument("--max-steps", type=int, default=200)
    ap.add_argument("--gen-limit", type=float, default=0,
                    help="межа генератора G; прогін зупиняється після сходинки з заданою >= G")
    ap.add_argument("--rate", type=float, default=300, help="інтенсивність для профілю const")
    ap.add_argument("--duration", type=float, default=30, help="тривалість для профілю const")
    ap.add_argument("--procs", type=int, default=min(4, os.cpu_count() or 1))
    ap.add_argument("--max-conns", type=int, default=2500,
                    help="найбільша кількість одночасних з'єднань одного процесу")
    ap.add_argument("--timeout", type=float, default=5.0)
    ap.add_argument("--ids", type=int, default=100)
    ap.add_argument("--label", default="")
    ap.add_argument("--out", default="")
    ap.add_argument("--csv", default="", help="посекундний CSV (за замовчуванням поруч з --out)")
    a = ap.parse_args()

    u = urllib.parse.urlparse(a.url)
    host, port = u.hostname, u.port or 80
    base = f"{u.scheme}://{host}:{port}"
    ids = None
    if "{id}" in a.path:
        ids = fetch_ids(base, a.path.split("/{id}")[0], a.ids)

    steps = build_profile(a)
    procs = max(1, a.procs)
    cfg = {"host": host, "port": port, "path": a.path, "profile": a.profile,
           "procs": procs, "max_conns": a.max_conns, "timeout": a.timeout,
           "warmup": a.warmup, "warmup_rate": a.warmup_rate, "start_rate": a.start_rate,
           "step_inc": a.step_inc, "step_duration": a.step_duration,
           "gen_limit": a.gen_limit, "label": a.label, "ids": len(ids or [])}

    ctx = mp.get_context("spawn")
    ready_q, report_q, out_q = ctx.Queue(), ctx.Queue(), ctx.Queue()
    go_evt = ctx.Event()
    start_val = ctx.RawValue("d", 0.0)
    stop_val = ctx.RawValue("d", NONE)
    ws = [ctx.Process(target=worker_main,
                      args=(i, procs, cfg, steps, ids, start_val, stop_val,
                            ready_q, go_evt, report_q, out_q))
          for i in range(procs)]
    for w in ws:
        w.start()
    for _ in ws:
        ready_q.get()
    start_epoch = time.time() + 1.0
    start_val.value = start_epoch
    go_evt.set()
    print(f"[loadgen] profile={a.profile} procs={procs} path={a.path} "
          f"start={time.strftime('%H:%M:%S', time.localtime(start_epoch))}", flush=True)

    ok = Counter()
    upto = {i: 0 for i in range(procs)}
    below = 0
    stop_at = steps[-1]["t1"]
    for st in steps:
        need = int(st["t1"])
        while min(upto.values()) < need:
            try:
                msg = report_q.get(timeout=1.0)
            except queue.Empty:
                if not any(w.is_alive() for w in ws):
                    break
                continue
            _, i, r0, r1, vals = msg
            for k, v in enumerate(vals):
                ok[r0 + k] += v
            upto[i] = max(upto[i], r1)
        a0 = int(st["t0"] + (MEASURE_SKIP if st["kind"] == "step" else 0))
        win = [ok.get(s, 0) for s in range(a0, need)]
        mean = statistics.fmean(win) if win else 0.0
        print(f"[loadgen] {st['kind']:6s} {st['idx']:3d} target={st['rate']:7.0f} "
              f"achieved(last {len(win)} s)={mean:8.1f}", flush=True)
        if st["kind"] != "step":
            continue
        below = below + 1 if mean < 0.9 * st["rate"] else 0
        reason = None
        if below >= 2:
            reason = "дві сходинки поспіль досягнута < 0.9 заданої"
        elif a.gen_limit and st["rate"] >= a.gen_limit:
            reason = f"досягнуто межі генератора G={a.gen_limit:.0f}"
        if reason:
            stop_at = st["t1"]
            stop_val.value = stop_at
            print(f"[loadgen] зупинка після сходинки {st['idx']}: {reason}", flush=True)
            break

    def drain():
        while True:
            try:
                report_q.get_nowait()
            except queue.Empty:
                return

    parts = []
    while len(parts) < len(ws):
        drain()
        try:
            parts.append(out_q.get(timeout=0.2))
        except queue.Empty:
            pass
    for w in ws:
        while w.is_alive():
            drain()
            w.join(timeout=0.2)

    res = analyze(cfg, steps, stop_at, start_epoch, parts)
    print("\n target  sent    achieved ± std   p50,ms   p95,ms   p99,ms  non2xx  conn_err")
    for s in res["steps"]:
        print(f"{s['target_rps']:7.0f} {s['sent_mean']:7.1f} {s['achieved_mean']:9.1f} ± "
              f"{s['achieved_std']:5.1f} {s['p50_ms']:8.2f} {s['p95_ms']:8.2f} {s['p99_ms']:8.2f} "
              f"{s['non_2xx_share'] * 100:6.2f}% {s['conn_errors']:6d}  {s['kind']}")
    print(f"\nX = {res['X']:.1f} ± {res['X_std']:.1f} зап/с на сходинці {res['X_step']} "
          f"(задана {res['X_target']:.0f}); екземпляри: {res['pods']}")
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)) or ".", exist_ok=True)
        with open(a.out, "w", encoding="utf-8") as f:
            json.dump(res, f, ensure_ascii=False, indent=1)
        csv_path = a.csv or os.path.splitext(a.out)[0] + "_per_second.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(res["per_second"][0].keys()))
            w.writeheader()
            w.writerows(res["per_second"])
        print(f"[loadgen] saved -> {a.out}, {csv_path}")


if __name__ == "__main__":
    main()

import argparse
import json
import os
import platform
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT = os.path.join(ROOT, "results", "lab2")
LOADGEN = os.path.join(ROOT, "loadgen", "loadgen.py")
LB = os.getenv("LB_URL", "http://127.0.0.1:8080")
PROM = os.getenv("PROM_URL", "http://127.0.0.1:9090")


def sh(args, check=True, capture=True, quiet=False):
    if not quiet:
        print(">>>", " ".join(args), flush=True)
    r = subprocess.run(args, cwd=ROOT, text=True, encoding="utf-8", errors="replace",
                       capture_output=capture)
    if check and r.returncode != 0:
        print(r.stdout or "", r.stderr or "", file=sys.stderr)
        sys.exit(f"команда завершилась з кодом {r.returncode}: {' '.join(args)}")
    return r


def http_get(url, timeout=5):
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def save_json(name, obj):
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)
    print(f"[lab2] saved -> {os.path.relpath(path, ROOT)}")
    return path


def web_containers():
    ids = sh(["docker", "compose", "ps", "-q", "web"], quiet=True).stdout.split()
    res = []
    for cid in ids:
        fmt = ("{{.Name}}|{{.Id}}|{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}|"
               "{{.HostConfig.NanoCpus}}|{{.HostConfig.Memory}}|{{.Config.Hostname}}|{{.State.Status}}")
        line = sh(["docker", "inspect", "--format", fmt, cid], quiet=True).stdout.strip()
        name, full, ip, ncpu, mem, hostname, status = line.split("|")
        if status != "running":
            continue
        res.append({"name": name.lstrip("/"), "id": full[:12], "ip": ip,
                    "cpus": int(ncpu) / 1e9 if ncpu.isdigit() else None,
                    "mem_bytes": int(mem) if mem.isdigit() else None, "hostname": hostname})
    return sorted(res, key=lambda c: c["name"])


def prom_targets():
    st, _, body = http_get(f"{PROM}/api/v1/targets?state=active")
    if st != 200:
        return []
    return [t for t in json.loads(body)["data"]["activeTargets"] if t["labels"].get("job") == "web"]


def cmd_info(_):
    fmt = "{{json .}}"
    info = json.loads(sh(["docker", "info", "--format", fmt]).stdout)
    data = {
        "docker_ncpu": info.get("NCPU"),
        "docker_mem_bytes": info.get("MemTotal"),
        "docker_mem_gib": round(info.get("MemTotal", 0) / 2 ** 30, 2),
        "docker_server_version": info.get("ServerVersion"),
        "docker_os": info.get("OperatingSystem"),
        "docker_kernel": info.get("KernelVersion"),
        "cgroup_version": info.get("CgroupVersion"),
        "host_platform": platform.platform(),
        "host_cpu_logical": os.cpu_count(),
        "host_processor": platform.processor(),
        "python": sys.version.split()[0],
        "web_cpus_env": os.getenv("WEB_CPUS", "1.0 (за замовчуванням)"),
    }
    print(json.dumps(data, ensure_ascii=False, indent=1))
    save_json("docker_info.json", data)


def wait_until(pred, timeout, what):
    t_end = time.time() + timeout
    while time.time() < t_end:
        try:
            if pred():
                return True
        except Exception:
            pass
        time.sleep(1)
    sys.exit(f"не дочекались: {what}")


def wait_drain(timeout=300):
    url = f"{PROM}/api/v1/query?query=" + urllib.parse.quote('sum(http_requests_in_progress{job="web"})')
    t_end = time.time() + timeout
    shown = False
    while time.time() < t_end:
        try:
            st, _, body = http_get(url)
            res = json.loads(body)["data"]["result"]
            v = float(res[0]["value"][1]) if res else 0.0
        except Exception:
            return
        if v < 1:
            if shown:
                print("[lab2] черга в сервісі спорожніла", flush=True)
            return
        if not shown:
            print(f"[lab2] у сервісі ще {v:.0f} запитів в обробці, чекаю...", flush=True)
            shown = True
        time.sleep(5)
    print("[lab2] !!! черга не спорожніла за відведений час", flush=True)


def cmd_up(a):
    n = a.n
    wait_drain()
    args = ["docker", "compose", "up", "-d", "--scale", f"web={n}"]
    if getattr(a, "build", False):
        args.insert(3, "--build")
    sh(args, capture=False)
    sh(["docker", "compose", "restart", "lb"], capture=False)
    wait_until(lambda: http_get(f"{LB}/healthz")[0] == 200, 90, "lb /healthz = 200")
    wait_until(lambda: len(web_containers()) == n, 60, f"{n} запущених контейнерів web")

    def pods_ok():
        seen = set()
        for _ in range(8 * n):
            st, h, _ = http_get(f"{LB}/livez")
            seen.add({k.lower(): v for k, v in h.items()}.get("x-pod"))
        return len(seen - {None}) == n
    wait_until(pods_ok, 60, f"відповіді від {n} різних екземплярів через lb")

    def targets_ok():
        t = prom_targets()
        return len(t) == n and all(x["health"] == "up" for x in t)
    wait_until(targets_ok, 120, f"{n} цілей web у стані UP у Prometheus")
    st, _, body = http_get(f"{LB}/consultations?limit=1")
    total = json.loads(body).get("total", 0) if st == 200 else 0
    if total < 100:
        sh([sys.executable, os.path.join(ROOT, "scripts", "seed.py"), "--url", LB,
            "--n", str(100 - total)], capture=False)
    print(f"[lab2] стенд готовий: web={n}, цілі Prometheus: "
          f"{[(t['labels']['instance'], t['health']) for t in prom_targets()]}")


def cmd_check(_):
    lines = []
    p = lines.append
    p("$ docker compose ps")
    p(sh(["docker", "compose", "ps", "--format",
          "table {{.Name}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"], quiet=True).stdout.rstrip())
    p("")
    p(f"$ 8 запитів GET {LB}/consultations/1 -> код і заголовок X-Pod")
    for _ in range(8):
        st, h, _ = http_get(f"{LB}/consultations/1")
        h = {k.lower(): v for k, v in h.items()}
        p(f"{st}  X-Pod: {h.get('x-pod')}  X-Upstream: {h.get('x-upstream')}")
    p("")
    st, h, body = http_get(f"{LB}/livez")
    p(f"$ GET /livez -> {st} {body.decode()}")
    st, h, body = http_get(f"{LB}/noop")
    p(f"$ GET /noop -> {st} ")
    p("")
    st, h, body = http_get(f"{LB}/metrics")
    p(f"$ GET /metrics -> {st}, Content-Type: {h.get('Content-Type')}; фрагмент:")
    keep = ("http_requests_total{", "http_request_duration_seconds_bucket{le=\"0.005\",method=\"GET\",path=\"/consultations/{id}\"",
            "http_request_duration_seconds_bucket{le=\"+Inf\",method=\"GET\",path=\"/consultations/{id}\"",
            "http_request_duration_seconds_count{method=\"GET\",path=\"/consultations/{id}\"",
            "http_requests_in_progress ", "db_pool_connections_busy ", "db_pool_requests_waiting ",
            "db_pool_size ", "db_pool_wait_seconds_bucket{le=\"0.001\"}", "db_pool_wait_seconds_count",
            "process_cpu_seconds_total ", "app_instance_info")
    for ln in body.decode().splitlines():
        if ln.startswith(keep):
            p("  " + ln)
    p("")
    p("$ цілі Prometheus (job=web)")
    for t in prom_targets():
        p(f"  {t['labels']['instance']:22s} {t['health']:5s} lastScrape={t.get('lastScrape', '')[:19]} "
          f"duration={float(t.get('lastScrapeDuration', 0)) * 1000:.1f} ms")
    p("")
    p("$ ліміти контейнерів web")
    for c in web_containers():
        mem = f"{c['mem_bytes'] / 2 ** 20:.0f} MiB" if c["mem_bytes"] else "-"
        p(f"  {c['name']}  hostname={c['hostname']}  ip={c['ip']}  cpus={c['cpus']}  mem={mem}")
    text = "\n".join(lines)
    print(text)
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, "check.txt"), "w", encoding="utf-8") as f:
        f.write(text + "\n")
    print("[lab2] saved -> results/lab2/check.txt")


class StatsSampler(threading.Thread):
    def __init__(self, path):
        super().__init__(daemon=True)
        self.path, self.stop_evt = path, threading.Event()

    def run(self):
        with open(self.path, "w", encoding="utf-8") as f:
            while not self.stop_evt.is_set():
                t0 = time.time()
                r = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}"],
                                   cwd=ROOT, text=True, encoding="utf-8", errors="replace",
                                   capture_output=True)
                t1 = time.time()
                rows = []
                for ln in r.stdout.splitlines():
                    try:
                        rows.append(json.loads(ln))
                    except ValueError:
                        pass
                f.write(json.dumps({"t": (t0 + t1) / 2, "rows": rows}, ensure_ascii=False) + "\n")
                f.flush()
                self.stop_evt.wait(max(0.0, 2.0 - (time.time() - t0)))

    def stop(self):
        self.stop_evt.set()
        self.join(timeout=15)


def gen_limit():
    path = os.path.join(OUT, "noop.json")
    if not os.path.exists(path):
        sys.exit("Спершу визначте межу генератора: python scripts/lab2.py noop")
    return json.load(open(path, encoding="utf-8"))["X"]


def loadgen(extra, out):
    cmd = [sys.executable, LOADGEN, "--url", LB, "--out", out] + extra
    print(">>>", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=ROOT, check=True)


def cmd_noop(a):
    os.makedirs(OUT, exist_ok=True)
    sampler = StatsSampler(os.path.join(OUT, "stats_noop.jsonl"))
    sampler.start()
    try:
        loadgen(["--profile", "step", "--path", "/noop", "--label", "noop",
                 "--procs", str(a.procs)], os.path.join(OUT, "noop.json"))
    finally:
        sampler.stop()
    print(f"[lab2] межа генератора G = {gen_limit():.1f} зап/с")


def cmd_run(a):
    n = a.n
    if not a.no_up:
        cmd_up(a)
    G = a.gen_limit or gen_limit()
    meta = {"n": n, "gen_limit": G, "containers": web_containers(),
            "web_cpus_env": os.getenv("WEB_CPUS", ""), "started": time.time()}
    save_json(f"run_N{n}_meta.json", meta)
    time.sleep(5)
    sampler = StatsSampler(os.path.join(OUT, f"stats_N{n}.jsonl"))
    sampler.start()
    try:
        loadgen(["--profile", "step", "--path", "/consultations/{id}", "--label", f"N{n}",
                 "--gen-limit", f"{G:.1f}", "--procs", str(a.procs)],
                os.path.join(OUT, f"run_N{n}.json"))
    finally:
        sampler.stop()
    time.sleep(6)
    wait_drain()


def cmd_graceful(a):
    a.n = 2
    cmd_up(a)
    cs = web_containers()
    victim = cs[-1]
    out = os.path.join(OUT, "graceful_load.json")
    cmd = [sys.executable, LOADGEN, "--url", LB, "--out", out, "--profile", "const",
           "--rate", str(a.rate), "--duration", "40", "--warmup", "0", "--label", "graceful",
           "--procs", "2"]
    print(">>>", " ".join(cmd), flush=True)
    proc = subprocess.Popen(cmd, cwd=ROOT)
    time.sleep(15)
    t0 = time.time()
    sh(["docker", "stop", victim["id"]], capture=False)
    t1 = time.time()
    code = sh(["docker", "inspect", "--format", "{{.State.ExitCode}}", victim["id"]]).stdout.strip()
    logs = sh(["docker", "logs", "--tail", "12", victim["id"]], check=False)
    proc.wait()
    res = json.load(open(out, encoding="utf-8"))
    rows = res["per_second"]
    win = [r for r in rows if t0 - 5 <= r["epoch"] + 0.5 <= t0 + 5]
    summary = {
        "victim": victim, "rate": a.rate,
        "docker_stop_started": t0, "docker_stop_finished": t1,
        "docker_stop_seconds": round(t1 - t0, 3),
        "exit_code": int(code) if code.lstrip("-").isdigit() else code,
        "window": [t0 - 5, t0 + 5],
        "window_seconds": len(win),
        "window_ok_2xx": sum(r["ok_2xx"] for r in win),
        "window_non_2xx": sum(r["non_2xx"] for r in win),
        "window_conn_errors": sum(r["conn_errors"] for r in win),
        "window_max_p99_ms": max((r["p99_ms"] for r in win), default=None),
        "run_ok_2xx": sum(r["ok_2xx"] for r in rows),
        "run_non_2xx": sum(r["non_2xx"] for r in rows),
        "run_conn_errors": sum(r["conn_errors"] for r in rows),
        "pods": res["pods"],
        "logs_tail": (logs.stdout + logs.stderr).strip().splitlines(),
        "per_second_window": win,
    }
    save_json("graceful.json", summary)
    print(f"\n[graceful] зупинено {victim['name']} за {summary['docker_stop_seconds']} с, "
          f"ExitCode={summary['exit_code']}")
    print(f"[graceful] 10 с навколо зупинки: 2xx={summary['window_ok_2xx']}, "
          f"не 2xx={summary['window_non_2xx']}, помилок з'єднання={summary['window_conn_errors']}")
    ok = summary["exit_code"] == 0 and summary["window_non_2xx"] == 0 and summary["window_conn_errors"] == 0
    print("[graceful] РЕЗУЛЬТАТ:", "OK" if ok else "НЕ ВИКОНАНО")


def cmd_all(a):
    cmd_info(a)
    cmd_graceful(a)
    a.n = 1
    cmd_up(a)
    cmd_noop(a)
    for n in (1, 2, 3, 4):
        a.n = n
        cmd_run(a)


def main():
    ap = argparse.ArgumentParser(description="Прогони ЛР2")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info")
    s = sub.add_parser("up")
    s.add_argument("--n", type=int, default=1)
    s.add_argument("--build", action="store_true")
    sub.add_parser("check")
    s = sub.add_parser("graceful")
    s.add_argument("--rate", type=float, default=300)
    s = sub.add_parser("noop")
    s.add_argument("--procs", type=int, default=4)
    s = sub.add_parser("run")
    s.add_argument("--n", type=int, required=True)
    s.add_argument("--gen-limit", type=float, default=0, help="G, за замовчуванням з noop.json")
    s.add_argument("--procs", type=int, default=4)
    s.add_argument("--no-up", action="store_true", help="не перепіднімати стенд")
    s = sub.add_parser("all")
    s.add_argument("--rate", type=float, default=300)
    s.add_argument("--procs", type=int, default=4)
    s.add_argument("--gen-limit", type=float, default=0)
    s.add_argument("--no-up", action="store_true")
    a = ap.parse_args()
    {"info": cmd_info, "up": cmd_up, "check": cmd_check, "graceful": cmd_graceful,
     "noop": cmd_noop, "run": cmd_run, "all": cmd_all}[a.cmd](a)


if __name__ == "__main__":
    main()

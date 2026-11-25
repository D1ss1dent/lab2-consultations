import argparse
import glob
import json
import math
import os
import re
import statistics
import sys
import urllib.parse
import urllib.request

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT = os.path.join(ROOT, "results", "lab2")
DOCS = os.path.join(ROOT, "docs", "lab2")
PROM = os.getenv("PROM_URL", "http://127.0.0.1:9090")

PATH_SEL = 'job="web",method="GET",path="/consultations/{id}"'
POOL_SIZE = 15
WIN = 15


def prom(api, **params):
    url = f"{PROM}/api/v1/{api}?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=30) as r:
        body = json.loads(r.read())
    if body.get("status") != "success":
        raise RuntimeError(body)
    return body["data"]["result"]


def q_range(expr, start, end, step=5):
    return prom("query_range", query=expr, start=f"{start:.3f}", end=f"{end:.3f}", step=step)


def q_inst(expr, t):
    return prom("query", query=expr, time=f"{t:.3f}")


def fetch_run(run):
    t_start = run["start_epoch"] - 5
    t_end = run["start_epoch"] + run["stop_at"] + 10
    series = {
        "rps": f'sum by (instance) (rate(http_requests_total{{{PATH_SEL}}}[15s]))',
        "cpu": 'rate(process_cpu_seconds_total{job="web"}[15s])',
        "inflight": 'http_requests_in_progress{job="web"}',
        "server_p99": f'histogram_quantile(0.99, sum by (le) (increase(http_request_duration_seconds_bucket{{{PATH_SEL}}}[15s])))',
        "pool_busy": 'db_pool_connections_busy{job="web"}',
        "pool_wait_p99": 'histogram_quantile(0.99, sum by (le) (increase(db_pool_wait_seconds_bucket{job="web"}[15s])))',
        "info": 'app_instance_info{job="web"}',
    }
    data = {"range": {k: q_range(v, t_start, t_end) for k, v in series.items()}, "steps": []}
    for st in run["steps"]:
        b = st["win_epoch_t1"]
        w = int(round(st["win_epoch_t1"] - st["win_epoch_t0"]))
        rec = {"idx": st["idx"], "t": b, "window_s": w}
        q = {
            "buckets": f'sum by (le) (increase(http_request_duration_seconds_bucket{{{PATH_SEL}}}[{w}s]))',
            "pool_wait_buckets": f'sum by (le) (increase(db_pool_wait_seconds_bucket{{job="web"}}[{w}s]))',
            "cpu_per_inst": f'rate(process_cpu_seconds_total{{job="web"}}[{w}s])',
            "rps_per_inst": f'sum by (instance) (rate(http_requests_total{{{PATH_SEL}}}[{w}s]))',
            "inflight_per_inst": f'avg_over_time(http_requests_in_progress{{job="web"}}[{w}s])',
            "pool_busy_avg": f'avg_over_time(db_pool_connections_busy{{job="web"}}[{w}s])',
            "pool_busy_max": f'max_over_time(db_pool_connections_busy{{job="web"}}[{w}s])',
            "pool_waiting_avg": f'avg_over_time(db_pool_requests_waiting{{job="web"}}[{w}s])',
            "mem_per_inst": f'avg_over_time(process_resident_memory_bytes{{job="web"}}[{w}s])',
        }
        for k, expr in q.items():
            rec[k] = q_inst(expr, b)
        data["steps"].append(rec)
    return data


def fnum(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return float("nan")
    return v


def buckets_from(result):
    out = []
    for r in result:
        le = r["metric"].get("le")
        if le is None:
            continue
        out.append((math.inf if le == "+Inf" else float(le), fnum(r["value"][1])))
    out.sort()
    return out


def quantile_from_buckets(bk, q=0.99):
    if not bk:
        return float("nan"), None, None, False
    n = bk[-1][1]
    if not n or n <= 0 or math.isnan(n):
        return float("nan"), None, None, False
    r = q * n
    prev_b, prev_c = 0.0, 0.0
    for b, c in bk:
        if c >= r and c > prev_c:
            if math.isinf(b):
                return prev_b, prev_b, math.inf, True
            return prev_b + (b - prev_b) * (r - prev_c) / (c - prev_c), prev_b, b, False
        if not math.isinf(b):
            prev_b, prev_c = b, c
    return prev_b, prev_b, math.inf, True


def per_inst(result):
    return {r["metric"].get("instance", "?"): fnum(r["value"][1]) for r in result}


def mean(vals):
    vals = [v for v in vals if not math.isnan(v)]
    return statistics.fmean(vals) if vals else float("nan")


def step_metrics(run, prom_run):
    by_idx = {s["idx"]: s for s in (prom_run or {}).get("steps", [])}
    out = []
    for st in run["steps"]:
        row = dict(st)
        p = by_idx.get(st["idx"])
        if p:
            q, lo, hi, beyond = quantile_from_buckets(buckets_from(p["buckets"]))
            row.update({
                "server_p99_ms": q * 1000, "server_bucket": (lo, hi), "server_beyond": beyond,
                "server_count": (buckets_from(p["buckets"]) or [(0, 0)])[-1][1],
            })
            wq, wlo, whi, wbeyond = quantile_from_buckets(buckets_from(p["pool_wait_buckets"]))
            row.update({"pool_wait_p99_ms": wq * 1000, "pool_wait_beyond": wbeyond})
            cpu = per_inst(p["cpu_per_inst"])
            infl = per_inst(p["inflight_per_inst"])
            busy = per_inst(p["pool_busy_avg"])
            row.update({
                "cpu_per_inst": cpu, "cpu_avg": mean(cpu.values()),
                "rps_per_inst": per_inst(p["rps_per_inst"]),
                "inflight_per_inst": infl, "inflight_avg": mean(infl.values()),
                "pool_busy_per_inst": busy, "pool_busy_avg": mean(busy.values()),
                "pool_busy_max": max(per_inst(p["pool_busy_max"]).values(), default=float("nan")),
                "pool_waiting_avg": mean(per_inst(p["pool_waiting_avg"]).values()),
                "mem_avg_mib": mean(per_inst(p["mem_per_inst"]).values()) / 2 ** 20,
            })
        out.append(row)
    return out


def generator_limited(run, G):
    meas = [s for s in run["steps"] if s["kind"] == "step"]
    if not meas:
        return True, "немає сходинок"
    last = meas[-1]
    reasons = []
    if G and run["X"] >= 0.9 * G:
        reasons.append(f"X(N) = {run['X']:.1f} >= 0.9·G = {0.9 * G:.1f}")
    if last["achieved_mean"] >= 0.9 * last["target_rps"]:
        reasons.append(f"на останній сходинці ({last['target_rps']:.0f}) досягнута "
                       f"{last['achieved_mean']:.1f} >= 0.9 заданої")
    return bool(reasons), "; ".join(reasons)


def usl_fit(points):
    u = [n - 1 for n, _ in points]
    v = [n * (n - 1) for n, _ in points]
    y = [n / c - 1 for n, c in points]
    Suu = sum(x * x for x in u)
    Svv = sum(x * x for x in v)
    Suv = sum(a * b for a, b in zip(u, v))
    Suy = sum(a * b for a, b in zip(u, y))
    Svy = sum(a * b for a, b in zip(v, y))
    sums = {"Σu²": Suu, "Σv²": Svv, "Σuv": Suv, "Σuy": Suy, "Σvy": Svy}
    note = ""
    if len(points) >= 2:
        den = Suu * Svv - Suv ** 2
        sigma = (Svv * Suy - Suv * Svy) / den
        kappa = (Suu * Svy - Suv * Suy) / den
        if sigma < 0:
            sigma, kappa = 0.0, Svy / Svv
            note = "оцінка σ вийшла від'ємною, прийнято σ = 0, κ = Σvy / Σv²"
        elif kappa < 0:
            sigma, kappa = Suy / Suu, 0.0
            note = "оцінка κ вийшла від'ємною, прийнято κ = 0, σ = Σuy / Σu²"
    elif len(points) == 1:
        sigma, kappa = Suy / Suu, 0.0
        note = "лише одна точка з N > 1: двопараметричний підбір неможливий, оцінено тільки σ при κ = 0"
    else:
        return None, None, sums, "немає точок для підбору"
    return sigma, kappa, sums, note


def usl_x(lam, sigma, kappa, n):
    return lam * n / (1 + sigma * (n - 1) + kappa * n * (n - 1))


def parse_mem(s):
    m = re.match(r"([\d.]+)\s*([KMGT]?i?B)", s.strip())
    if not m:
        return float("nan")
    v, u = float(m.group(1)), m.group(2)
    mult = {"B": 1 / 2 ** 20, "KiB": 1 / 1024, "KB": 1 / 1024, "MiB": 1, "MB": 1,
            "GiB": 1024, "GB": 1024, "TiB": 2 ** 20}
    return v * mult.get(u, 1)


def stats_in_window(path, t0, t1):
    if not os.path.exists(path):
        return {}, 0
    samples = [json.loads(ln) for ln in open(path, encoding="utf-8") if ln.strip()]
    inside = [s for s in samples if t0 <= s["t"] <= t1]
    if not inside and samples:
        mid = (t0 + t1) / 2
        inside = [min(samples, key=lambda s: abs(s["t"] - mid))]
    acc = {}
    for s in inside:
        for r in s["rows"]:
            name = r.get("Name", "?")
            d = acc.setdefault(name, {"cpu": [], "mem": [], "lim": r.get("MemUsage", "/").split("/")[-1].strip()})
            d["cpu"].append(fnum(r.get("CPUPerc", "nan").rstrip("%")))
            d["mem"].append(parse_mem(r.get("MemUsage", "0B / 0B").split("/")[0]))
    return {k: {"cpu": mean(v["cpu"]), "mem_mib": mean(v["mem"]), "limit": v["lim"]}
            for k, v in sorted(acc.items())}, len(inside)


def f1(x, nd=1):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "-"
    return f"{x:.{nd}f}"


def bucket_str(row):
    lo, hi = row.get("server_bucket", (None, None))
    if lo is None:
        return "-", "-"
    if row.get("server_beyond") or (hi is not None and math.isinf(hi)):
        return f"> {lo * 1000:.0f} мс", "не обмежена зверху"
    return f"({lo * 1000:.0f}; {hi * 1000:.0f}] мс", f"{(hi - lo) * 1000:.0f} мс"


def server_p99_str(row):
    if "server_p99_ms" not in row:
        return "-"
    if row.get("server_beyond"):
        return f"> {row['server_p99_ms']:.0f}"
    return f1(row["server_p99_ms"], 2)


def inst_names(prom_run, meta):
    names = {}
    by_host = {}
    for c in (meta or {}).get("containers", []):
        by_host[c["hostname"]] = c["name"]
        names[f"{c['ip']}:8000"] = c["name"]
    for r in (prom_run or {}).get("range", {}).get("info", []):
        inst, pod = r["metric"].get("instance"), r["metric"].get("pod")
        if inst and inst not in names:
            names[inst] = by_host.get(pod, pod)
    short = {}
    for k, v in names.items():
        m = re.search(r"web-(\d+)$", v or "")
        short[k] = f"web-{m.group(1)}" if m else (v or k)
    return short


def plt_mod():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({"figure.dpi": 110, "font.size": 9, "axes.grid": True,
                         "grid.alpha": 0.3, "axes.spines.top": False, "axes.spines.right": False})
    return plt


def plot_timeseries(n, run, prom_run, meta, path):
    plt = plt_mod()
    t0 = run["start_epoch"]
    rows = run["per_second"]
    names = inst_names(prom_run, meta)
    fig, ax = plt.subplots(5, 1, figsize=(11, 14), sharex=True)
    ts = [r["t"] for r in rows]
    ax[0].step(ts, [r["target_rps"] for r in rows], where="post", label="задана", color="#888", lw=1.5)
    ax[0].plot(ts, [r["achieved_rps"] for r in rows], label="досягнута (2xx/с)", color="#1f77b4", lw=1)
    ax[0].plot(ts, [r["sent_rps"] for r in rows], label="фактично надіслано", color="#2ca02c", lw=0.7, alpha=0.6)
    ax[0].set_ylabel("запитів/с")
    ax[0].set_title(f"N = {n}: задана і досягнута інтенсивність (генератор, посекундно)")
    ax[0].legend(loc="upper left")

    ax[1].plot(ts, [r["p99_ms"] for r in rows], label="p99 клієнта (посекундно)", color="#d62728", lw=1)
    sp = (prom_run or {}).get("range", {}).get("server_p99", [])
    if sp:
        xs = [float(v[0]) - t0 for v in sp[0]["values"]]
        ys = [fnum(v[1]) * 1000 for v in sp[0]["values"]]
        ax[1].plot(xs, ys, label="p99 сервера (Prometheus, вікно 15 с, крок 5 с)",
                   color="#9467bd", lw=1.5, marker=".", ms=4)
    ax[1].set_yscale("log")
    ax[1].axhline(1000, color="#9467bd", ls=":", lw=0.8)
    ax[1].text(ts[0] if ts else 0, 1050, "остання скінченна межа бакета 1000 мс", fontsize=7, color="#9467bd")
    ax[1].set_ylabel("мс (лог.)")
    ax[1].set_title("p99 клієнта і p99 сервера")
    ax[1].legend(loc="upper left")

    def per_instance(axis, key, title, ylabel, scale=1.0):
        for r in sorted((prom_run or {}).get("range", {}).get(key, []),
                        key=lambda r: names.get(r["metric"].get("instance"), "")):
            inst = r["metric"].get("instance", "?")
            axis.plot([float(v[0]) - t0 for v in r["values"]], [fnum(v[1]) * scale for v in r["values"]],
                      label=names.get(inst, inst), lw=1.2, marker=".", ms=3)
        axis.set_title(title)
        axis.set_ylabel(ylabel)
        if axis.get_legend_handles_labels()[0]:
            axis.legend(loc="upper left", ncol=4)

    per_instance(ax[2], "rps", "Запитів за секунду на кожному екземплярі (rate за 15 с)", "запитів/с")
    per_instance(ax[3], "cpu", "Використання CPU кожним екземпляром (rate process_cpu_seconds_total)", "ядер")
    per_instance(ax[4], "inflight", "Запитів в обробці на кожному екземплярі", "запитів")
    for a_ in ax:
        for st in run["steps"]:
            a_.axvline(st["t0"], color="#bbb", lw=0.5, ls="--")
    ax[-1].set_xlabel(f"час від початку прогону, с (старт {run['start_iso']})")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def plot_usl(xs, lam, sigma, kappa, path):
    plt = plt_mod()
    fig, axis = plt.subplots(figsize=(8, 5))
    Ns = sorted(xs)
    nmax = max(Ns + [8])
    grid = [i / 10 for i in range(10, nmax * 10 + 1)]
    axis.plot(grid, [lam * g for g in grid], "--", color="#888", label="λN, лінійне масштабування")
    if sigma is not None:
        axis.plot(grid, [usl_x(lam, sigma, kappa, g) for g in grid], color="#d62728",
                  label=f"USL: σ = {sigma:.4f}, κ = {kappa:.5f}")
    for n in Ns:
        x, std, lim = xs[n]
        axis.errorbar([n], [x], yerr=[std], fmt="o", color="#7f7f7f" if lim else "#1f77b4",
                      capsize=4, ms=7)
        axis.annotate(f"{x:.0f}{' (обм. генератором)' if lim else ''}", (n, x),
                      textcoords="offset points", xytext=(6, -12), fontsize=8)
    axis.set_xlabel("N, екземплярів web")
    axis.set_ylabel("X(N), запитів/с")
    axis.set_title("Виміряна пропускна здатність X(N) і універсальний закон масштабованості")
    axis.set_xlim(0.5, nmax + 0.5)
    axis.set_ylim(0, None)
    axis.legend()
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def plot_m1(steps_by_n, path):
    plt = plt_mod()
    fig, ax = plt.subplots(1, 2, figsize=(12, 4.5))
    colors = {1: "#1f77b4", 2: "#ff7f0e", 3: "#2ca02c", 4: "#d62728"}
    for n, steps in sorted(steps_by_n.items()):
        st = [s for s in steps if s["kind"] == "step" and "pool_busy_avg" in s]
        if not st:
            continue
        x = [s["target_rps"] for s in st]
        ax[0].plot(x, [s["pool_busy_avg"] for s in st], "o-", color=colors.get(n), label=f"N = {n}, середнє")
        ax[0].plot(x, [s["pool_busy_max"] for s in st], "^:", color=colors.get(n), alpha=0.6,
                   label=f"N = {n}, максимум")
        ax[1].plot(x, [s["pool_wait_p99_ms"] for s in st], "o-", color=colors.get(n), label=f"N = {n}")
    ax[0].axhline(POOL_SIZE, color="#888", ls="--", lw=1)
    ax[0].text(ax[0].get_xlim()[0], POOL_SIZE + 0.3, f"розмір пулу = {POOL_SIZE}", fontsize=8, color="#666")
    ax[0].set_title("М1: зайняті з'єднання пулу на один екземпляр")
    ax[0].set_xlabel("задана інтенсивність, запитів/с")
    ax[0].set_ylabel("з'єднань")
    ax[0].legend(fontsize=8)
    ax[1].set_yscale("symlog", linthresh=1)
    ax[1].set_title("М1: p99 часу очікування з'єднання з пулу")
    ax[1].set_xlabel("задана інтенсивність, запитів/с")
    ax[1].set_ylabel("мс")
    ax[1].legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def plot_p99_compare(steps, path):
    plt = plt_mod()
    st = [s for s in steps if s["kind"] == "step"]
    fig, axis = plt.subplots(figsize=(8, 4.5))
    x = [s["target_rps"] for s in st]
    axis.plot(x, [s["p99_ms"] for s in st], "o-", label="p99 клієнта")
    axis.plot(x, [s.get("server_p99_ms", float("nan")) for s in st], "s-", label="p99 сервера (гістограма)")
    for b in (5, 10, 25, 50, 100, 250, 500, 1000):
        axis.axhline(b, color="#ccc", lw=0.6, ls=":")
    axis.set_yscale("log")
    axis.set_xlabel("задана інтенсивність, запитів/с")
    axis.set_ylabel("мс (лог.), пунктир - межі бакетів Г1")
    axis.set_title("N = 1: p99 клієнта і p99 сервера на кожній сходинці")
    axis.legend()
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def md_table(head, rows):
    out = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def load_json(name):
    p = os.path.join(OUT, name)
    return json.load(open(p, encoding="utf-8")) if os.path.exists(p) else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--offline", action="store_true", help="не звертатись до Prometheus")
    ap.add_argument("--refetch", action="store_true", help="перезабрати дані навіть якщо є кеш")
    a = ap.parse_args()
    os.makedirs(DOCS, exist_ok=True)
    blocks = {}

    info = load_json("docker_info.json")
    if info:
        blocks["docker_info"] = md_table(["Параметр", "Значення"], [
            ["CPU, виділені Docker (docker info NCPU)", info["docker_ncpu"]],
            ["Пам'ять, виділена Docker (docker info MemTotal)", f"{info['docker_mem_gib']} ГіБ"],
            ["Docker Engine", info["docker_server_version"]],
            ["ОС віртуальної машини Docker", info["docker_os"]],
            ["Ядро", info["docker_kernel"]],
            ["Версія cgroup", info.get("cgroup_version")],
            ["Хост", info["host_platform"]],
            ["Логічних процесорів хоста", info["host_cpu_logical"]],
            ["Python генератора", info["python"]],
        ])

    check = os.path.join(OUT, "check.txt")
    if os.path.exists(check):
        blocks["check"] = "```\n" + open(check, encoding="utf-8").read().rstrip() + "\n```"

    gr = load_json("graceful.json")
    if gr:
        ok = gr["exit_code"] == 0 and gr["window_non_2xx"] == 0 and gr["window_conn_errors"] == 0
        rows = [[f"{r['t']}", f1(r["target_rps"], 0), r["sent_rps"], r["ok_2xx"], r["non_2xx"],
                 r["conn_errors"], f1(r["p99_ms"], 2)] for r in gr["per_second_window"]]
        blocks["graceful"] = "\n".join([
            f"Зупинено контейнер `{gr['victim']['name']}` (hostname `{gr['victim']['hostname']}`) під "
            f"навантаженням {gr['rate']:.0f} зап/с на двох екземплярах. `docker stop` тривав "
            f"{gr['docker_stop_seconds']} с.", "",
            md_table(["Перевірка", "Результат"], [
                ["Код завершення контейнера (`docker inspect --format '{{.State.ExitCode}}'`)", gr["exit_code"]],
                ["Відповіді 2xx за 10 с навколо зупинки", gr["window_ok_2xx"]],
                ["Відповіді з кодом не 2xx за 10 с навколо зупинки", gr["window_non_2xx"]],
                ["Помилки з'єднання за 10 с навколо зупинки", gr["window_conn_errors"]],
                ["Найбільший посекундний p99 клієнта у вікні, мс", f1(gr["window_max_p99_ms"], 2)],
                ["Не 2xx / помилки з'єднання за весь прогін (40 с)", f"{gr['run_non_2xx']} / {gr['run_conn_errors']}"],
                ["Висновок", "коректна зупинка працює" if ok else "умову НЕ виконано"],
            ]), "",
            "Посекундні дані генератора у вікні ±5 с навколо `docker stop`:", "",
            md_table(["с", "задана", "надіслано", "2xx", "не 2xx", "помилки з'єднання", "p99, мс"], rows), "",
            "Останні рядки журналу зупиненого контейнера:", "",
            "```", *gr["logs_tail"], "```",
        ])

    noop = load_json("noop.json")
    G = noop["X"] if noop else None
    if noop:
        rows = [[f1(s["target_rps"], 0), f1(s["sent_mean"]), f"{f1(s['achieved_mean'])} ± {f1(s['achieved_std'])}",
                 f1(s["p50_ms"], 2), f1(s["p99_ms"], 2), f"{s['non_2xx_share'] * 100:.2f}%"]
                for s in noop["steps"] if s["kind"] == "step"]
        blocks["noop"] = "\n".join([
            md_table(["Задана, зап/с", "Надіслано, зап/с", "Досягнута ± σ, зап/с", "p50, мс", "p99, мс", "Не 2xx"], rows),
            "", f"Межа генератора G = {G:.1f} зап/с, сходинка {noop['X_target']:.0f} зап/с, "
                f"розкид ± {noop['X_std']:.1f}.",
        ])

    runs, metas, proms, steps_by_n = {}, {}, {}, {}
    for f in sorted(glob.glob(os.path.join(OUT, "run_N*.json"))):
        m = re.search(r"run_N(\d+)\.json$", f)
        if not m:
            continue
        n = int(m.group(1))
        runs[n] = json.load(open(f, encoding="utf-8"))
        metas[n] = load_json(f"run_N{n}_meta.json")
        cache = os.path.join(OUT, f"prom_N{n}.json")
        if os.path.exists(cache) and not a.refetch:
            proms[n] = json.load(open(cache, encoding="utf-8"))
        elif not a.offline:
            try:
                proms[n] = fetch_run(runs[n])
                with open(cache, "w", encoding="utf-8") as fh:
                    json.dump(proms[n], fh)
                print(f"[analyze] Prometheus -> {os.path.relpath(cache, ROOT)}")
            except Exception as exc:
                print(f"[analyze] !!! Prometheus недоступний для N={n}: {exc}")
        steps_by_n[n] = step_metrics(runs[n], proms.get(n))

    if not runs:
        print("[analyze] немає results/lab2/run_N*.json")
    summary_lines = []

    xs = {}
    rows = []
    for n in sorted(runs):
        run = runs[n]
        st = next(s for s in steps_by_n[n] if s["idx"] == run["X_step"])
        lim, why = generator_limited(run, G)
        xs[n] = (run["X"], run["X_std"], lim)
        rows.append([n, f1(run["X"]), f1(run["X_std"]), f"{run['X_target']:.0f} (№{run['X_step']})",
                     f1(st["p99_ms"], 2), server_p99_str(st), f"{st['non_2xx_share'] * 100:.2f}%",
                     f1(st.get("cpu_avg", float("nan")), 3), f1(st.get("inflight_avg", float("nan")), 2),
                     "так" if lim else "ні"])
    if rows:
        cpus = {n: (metas[n] or {}).get("containers", [{}])[0].get("cpus") for n in runs if metas.get(n)}
        blocks["xn"] = "\n".join([
            md_table(["N", "X(N), зап/с", "Розкид σ, зап/с", "Сходинка X(N), зап/с", "p99 клієнта, мс",
                      "p99 сервера, мс", "Частка не 2xx", "CPU одного екземпляра, ядер",
                      "Запитів в обробці на екземпляр", "Обмежена генератором"], rows),
            "", f"Межа генератора G = {f1(G)} зап/с, поріг 0.9·G = {f1(G * 0.9 if G else None)} зап/с. "
                f"Ліміт CPU контейнера web під час серії: {', '.join(f'{v:g}' for v in sorted(set(v for v in cpus.values() if v)))} CPU.",
        ])
        notes = []
        for n in sorted(runs):
            lim, why = generator_limited(runs[n], G)
            if lim:
                notes.append(f"- N = {n} обмежена генератором: {why}.")
        if notes:
            blocks["xn"] += "\n\n" + "\n".join(notes)

    for n in sorted(runs):
        trs = []
        for s in steps_by_n[n]:
            if s["kind"] != "step":
                continue
            trs.append([f1(s["target_rps"], 0), f1(s["sent_mean"]), f"{f1(s['achieved_mean'])} ± {f1(s['achieved_std'])}",
                        f1(s["p50_ms"], 1), f1(s["p95_ms"], 1), f1(s["p99_ms"], 1), server_p99_str(s),
                        f"{s['non_2xx_share'] * 100:.2f}%", s["conn_errors"],
                        f1(s.get("cpu_avg", float("nan")), 3), f1(s.get("inflight_avg", float("nan")), 1),
                        f1(s.get("pool_busy_avg", float("nan")), 1), f1(s.get("pool_wait_p99_ms", float("nan")), 2),
                        f1(s["gen_cpu_cores"], 2)])
        blocks[f"steps_N{n}"] = md_table(
            ["Задана", "Надіслано", "Досягнута ± σ", "p50 кл.", "p95 кл.", "p99 кл.", "p99 серв.",
             "Не 2xx", "Пом. з'єдн.", "CPU/екз., ядер", "В обробці/екз.", "Пул зайнято/екз.",
             "p99 очік. пулу, мс", "CPU генер., ядер"], trs)
        summary_lines.append(f"## Сходинки, N = {n}\n\n" + blocks[f"steps_N{n}"] + "\n")

    for n in (1, 4):
        if n in runs:
            p = os.path.join(DOCS, f"timeseries_N{n}.png")
            plot_timeseries(n, runs[n], proms.get(n), metas.get(n), p)
            print(f"[analyze] {os.path.relpath(p, ROOT)}")

    if 1 in runs:
        trs = []
        for s in steps_by_n[1]:
            if s["kind"] != "step":
                continue
            bstr, err = bucket_str(s)
            trs.append([f1(s["target_rps"], 0), f1(s["p99_ms"], 2), server_p99_str(s), bstr, err,
                        f1(s["p99_ms"] - s["server_p99_ms"], 2) if "server_p99_ms" in s and not s.get("server_beyond") else "-"])
        blocks["p99_compare"] = md_table(
            ["Задана, зап/с", "p99 клієнта, мс", "p99 сервера, мс", "Бакет b(i-1); b(i)",
             "Макс. похибка оцінки", "Різниця клієнт - сервер, мс"], trs)
        p = os.path.join(DOCS, "p99_client_vs_server_N1.png")
        plot_p99_compare(steps_by_n[1], p)

    if 1 in xs and not xs[1][2]:
        lam = xs[1][0]
        pts = [(n, xs[n][0] / lam) for n in sorted(xs) if n > 1 and not xs[n][2]]
        sigma, kappa, sums, note = usl_fit(pts)
        trs = []
        for n in sorted(xs):
            c = xs[n][0] / lam
            row = [n, f1(xs[n][0]), f1(xs[n][1]), f"{c:.3f}", f"{n / c - 1:.4f}",
                   "ні (обмежена генератором)" if xs[n][2] else ("так" if n > 1 else "λ = X(1)")]
            if sigma is not None:
                row.append(f1(usl_x(lam, sigma, kappa, n)))
            trs.append(row)
        head = ["N", "X(N)", "σ X(N)", "C(N) = X(N)/X(1)", "y = N/C - 1", "У підборі"]
        if sigma is not None:
            head.append("X(N) за законом")
        lines = [md_table(head, trs), ""]
        lines.append("Суми для методу найменших квадратів: " +
                     ", ".join(f"{k} = {v:.4f}" for k, v in sums.items()) + ".")
        if sigma is not None:
            lines.append("")
            lines.append(f"σ = {sigma:.5f}, κ = {kappa:.6f}" + (f" ({note})" if note else ""))
            if kappa > 0 and sigma < 1:
                ns = math.sqrt((1 - sigma) / kappa)
                lines.append("")
                lines.append(f"N* = √((1 − σ) / κ) = √((1 − {sigma:.5f}) / {kappa:.6f}) = {ns:.2f}, "
                             f"X(N*) = {usl_x(lam, sigma, kappa, ns):.1f} зап/с, "
                             f"X(round N*) = X({max(1, round(ns))}) = {usl_x(lam, sigma, kappa, max(1, round(ns))):.1f} зап/с.")
            elif kappa == 0 and sigma > 0:
                lines.append("")
                lines.append(f"κ = 0, тому максимуму немає, X(N) наближається до λ/σ = {lam / sigma:.1f} зап/с.")
        else:
            lines.append("")
            lines.append(f"Підбір неможливий: {note}. За методичкою треба зменшити ліміт CPU web удвічі "
                         f"(WEB_CPUS=0.5 у .env) і повторити серію.")
        blocks["usl"] = "\n".join(lines)
        plot_usl(xs, lam, sigma, kappa, os.path.join(DOCS, "usl.png"))
        save = {"lambda": lam, "sigma": sigma, "kappa": kappa, "points": pts, "note": note, "sums": sums}
        with open(os.path.join(OUT, "usl.json"), "w", encoding="utf-8") as fh:
            json.dump(save, fh, ensure_ascii=False, indent=1)
    elif 1 in xs:
        blocks["usl"] = ("Точка N = 1 обмежена генератором, тому за методичкою треба зменшити ліміт CPU "
                         "контейнера web удвічі (WEB_CPUS=0.5 у .env) і повторити серію.")

    nmax = 4 if 4 in runs else (max(runs) if runs else None)
    if nmax:
        run = runs[nmax]
        st = next(s for s in steps_by_n[nmax] if s["idx"] == run["X_step"])
        stats, k = stats_in_window(os.path.join(OUT, f"stats_N{nmax}.jsonl"),
                                   st["win_epoch_t0"], st["win_epoch_t1"])
        trs = [[name, f1(v["cpu"], 1), f1(v["mem_mib"], 1), v["limit"]] for name, v in stats.items()]
        trs.append(["генератор", f1(st["gen_cpu_cores"] * 100, 1), "-", "-"])
        total = sum(v["cpu"] for v in stats.values() if not math.isnan(v["cpu"])) + st["gen_cpu_cores"] * 100
        ncpu = (info or {}).get("docker_ncpu")
        lines = [f"N = {nmax}, сходинка {st['target_rps']:.0f} зап/с (X({nmax}) = {run['X']:.1f}), "
                 f"середнє з {k} замірів `docker stats --no-stream` за 15 с вимірювання.", "",
                 md_table(["Контейнер / процес", "CPU, %", "Пам'ять, МіБ", "Ліміт пам'яті"], trs), "",
                 f"Разом контейнери і генератор: {total:.0f}% "
                 + (f"з {ncpu * 100}% доступних Docker ({ncpu} CPU), тобто {total / ncpu:.0f}% процесора." if ncpu else "")]
        if "pool_busy_avg" in st:
            lines += ["", md_table(["Метрика Prometheus на цій сходинці", "Значення"], [
                ["CPU одного екземпляра web (середнє), ядер", f1(st["cpu_avg"], 3)],
                ["CPU кожного екземпляра, ядер", ", ".join(f1(v, 3) for v in st["cpu_per_inst"].values())],
                ["Запитів/с на кожен екземпляр", ", ".join(f1(v, 1) for v in st["rps_per_inst"].values())],
                ["Запитів в обробці на екземпляр", f1(st["inflight_avg"], 2)],
                ["Зайнятих з'єднань пулу на екземпляр (середнє / максимум)",
                 f"{f1(st['pool_busy_avg'], 2)} / {f1(st['pool_busy_max'], 0)} з {POOL_SIZE}"],
                ["Потоків, що чекають на з'єднання, на екземпляр", f1(st["pool_waiting_avg"], 2)],
                ["p99 очікування з'єднання з пулу, мс", f1(st["pool_wait_p99_ms"], 2)],
                ["Пам'ять процесу web (RSS), МіБ", f1(st["mem_avg_mib"], 1)],
            ])]
        blocks["resources"] = "\n".join(lines)

        trs = []
        for n in sorted(steps_by_n):
            for s in steps_by_n[n]:
                if s["kind"] == "step" and "pool_busy_avg" in s:
                    trs.append([n, f1(s["target_rps"], 0), f1(s["pool_busy_avg"], 2), f1(s["pool_busy_max"], 0),
                                f1(s["pool_waiting_avg"], 2), f1(s["pool_wait_p99_ms"], 2)])
        if trs:
            blocks["m1"] = md_table(["N", "Задана, зап/с", "Пул зайнято/екз. (середнє)", "Пул зайнято (макс.)",
                                     "Чекають на з'єднання/екз.", "p99 очікування з'єднання, мс"], trs)
            plot_m1({n: steps_by_n[n] for n in steps_by_n if n in (1, 4)}, os.path.join(DOCS, "m1_pool.png"))

    order = ["docker_info", "check", "graceful", "noop", "xn", "p99_compare", "usl", "resources", "m1"]
    titles = {"docker_info": "Ресурси Docker", "check": "Перевірка стенду", "graceful": "Коректна зупинка",
              "noop": "Межа генератора (/noop)", "xn": "X(N)", "p99_compare": "p99 клієнта і сервера, N = 1",
              "usl": "Універсальний закон масштабованості", "resources": "Ресурси на сходинці X(4)",
              "m1": "М1: пул з'єднань"}
    text = "# Зведення ЛР2\n\n" + "\n".join(f"## {titles[k]}\n\n{blocks[k]}\n" for k in order if k in blocks)
    text += "\n" + "\n".join(summary_lines)
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, "summary.md"), "w", encoding="utf-8") as fh:
        fh.write(text)
    print("[analyze] results/lab2/summary.md")


if __name__ == "__main__":
    sys.exit(main())

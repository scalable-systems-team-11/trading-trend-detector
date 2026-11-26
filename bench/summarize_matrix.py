#!/usr/bin/env python3
"""Summarize the runs of bench/run_matrix.sh into one table (mean +- sample SD per config).

    python3 bench/summarize_matrix.py [prefix]        # default prefix: clean

Reads results/benchmarks/*-<prefix>-*/ (result.json, docker-stats.txt), finds each run's
window output in output/ (the newest file of the run's kind that started before the run
finished) and compares it with the reference run of its group: the first TCP run of the
throughput runs, the TCP run of the same speed for latency runs. Writes
results/benchmarks/summary-<prefix>.md and .csv and prints the Markdown.
"""

import csv
import glob
import hashlib
import json
import os
import re
import statistics
import sys
from collections import defaultdict

RESULTS = "results/benchmarks"
OUTPUT = "output"
PREFIX = sys.argv[1] if len(sys.argv) > 1 else "clean"
OUT_PREFIX = {"receiver": "windows-", "kafka": "kafka-windows-", "flink": "flink-windows-"}
STAMP = re.compile(r"(\d{4}-\d\d-\d\dT\d\d-\d\d-\d\dZ)")


def stamp_of(path):
    m = STAMP.search(os.path.basename(path.rstrip("/")))
    return m.group(1) if m else None


def output_groups():
    """kind -> sorted [(start stamp, [files])] of window outputs."""
    groups = defaultdict(lambda: defaultdict(list))
    for path in glob.glob(os.path.join(OUTPUT, "*.jsonl")):
        name, stamp = os.path.basename(path), stamp_of(path)
        if not stamp:
            continue
        for kind, prefix in OUT_PREFIX.items():
            if name.startswith(prefix):
                groups[kind][stamp].append(path)
    return {k: sorted(v.items()) for k, v in groups.items()}


def find_output(kind, finished, groups):
    before = [files for stamp, files in groups.get(kind, []) if stamp <= finished]
    return sorted(before[-1]) if before else []


def load_rows(files):
    """(symbol, window_start) -> hash of the row without run_id (last run in each file)."""
    rows = {}
    for path in files:
        run, part = None, {}
        with open(path, encoding="utf-8") as f:
            for line in f:
                row = json.loads(line)
                run_id = row.pop("run_id")
                if run_id != run:  # a file appended to by several runs: keep the last
                    run, part = run_id, {}
                key = (row["symbol"], row["window_start"])
                part[key] = hashlib.blake2b(json.dumps(row, sort_keys=True).encode(), digest_size=8).digest()
        rows.update(part)
    return rows


def compare(reference, candidate):
    missing = sum(1 for k in reference if k not in candidate)
    different = sum(1 for k, v in reference.items() if k in candidate and candidate[k] != v)
    extra = sum(1 for k in candidate if k not in reference)
    return missing, different, extra


def docker_cpu(path):
    """Mean and max of the summed CPU (in cores) over all sampled containers, peak total memory (MB)."""
    if not os.path.exists(path):
        return None, None, None
    cpu, mem = defaultdict(float), defaultdict(float)
    units = {"KiB": 1 / 1024, "MiB": 1, "GiB": 1024, "B": 1 / 1024 / 1024}
    with open(path) as f:
        for line in f:
            parts = line.split()
            if len(parts) < 4 or not parts[2].endswith("%"):
                continue
            t = parts[0]
            cpu[t] += float(parts[2].rstrip("%")) / 100
            m = re.match(r"([\d.]+)([KMG]?i?B)", parts[3])
            if m:
                mem[t] += float(m.group(1)) * units[m.group(2)]
    if not cpu:
        return None, None, None
    values = list(cpu.values())
    return statistics.mean(values), max(values), max(mem.values()) if mem else None


def config_of(label):
    name = label[len(PREFIX) + 1:]
    return re.sub(r"-r\d+$", "", name)


def main():
    groups = output_groups()
    runs = []
    for d in sorted(glob.glob(os.path.join(RESULTS, f"*-{PREFIX}-*/"))):
        path = os.path.join(d, "result.json")
        if not os.path.exists(path):
            continue
        r = json.load(open(path))
        if not r.get("label", "").startswith(PREFIX + "-"):
            continue
        finished = stamp_of(d)
        lat = r.get("latency_ms") or {}
        cpu_mean, cpu_max, mem_peak = docker_cpu(os.path.join(d, "docker-stats.txt"))
        c = r["counters"]
        runs.append({
            "dir": d, "kind": r["kind"], "config": config_of(r["label"]), "label": r["label"],
            "events": r["events"], "eps": r["events_per_second"], "seconds": r["active_seconds"],
            "p50": lat.get("p50"), "p95": lat.get("p95"), "p99": lat.get("p99"),
            "proc_cpu_s": r.get("cpu_seconds", r.get("python_cpu_seconds")),
            "proc_rss_mb": r.get("peak_rss_mb", r.get("peak_rss_mb_total", r.get("python_peak_rss_mb_total"))),
            "docker_cpu_mean": cpu_mean, "docker_cpu_max": cpu_max, "docker_mem_mb": mem_peak,
            "windows": c["windows"], "buy": c["buy"], "sell": c["sell"], "late": c["late"],
            "outputs": find_output(r["kind"], finished, groups),
        })

    # Correctness: each run against the reference of its group.
    cache = {}

    def rows(files):
        key = tuple(files)
        if key not in cache:
            cache[key] = load_rows(files)
        return cache[key]

    def reference_for(run):
        cfg = run["config"]
        if cfg.startswith("lat-"):
            speed = cfg.rsplit("-", 1)[-1]
            ref = [x for x in runs if x["config"] == f"lat-tcp-{speed}"]
        elif cfg.startswith("week"):
            return None
        else:
            ref = [x for x in runs if x["config"] == "tcp"]
        return ref[0] if ref else None

    for run in runs:
        ref = reference_for(run)
        if ref is run:
            run["check"] = "reference"
        elif ref is None or not run["outputs"] or not ref["outputs"]:
            run["check"] = None
        else:
            run["check"] = compare(rows(ref["outputs"]), rows(run["outputs"]))
            cache.pop(tuple(run["outputs"]), None)  # keep only references in memory

    # Aggregate per config.
    order = []
    by_cfg = defaultdict(list)
    for run in runs:
        if run["config"] not in by_cfg:
            order.append(run["config"])
        by_cfg[run["config"]].append(run)

    def ms(values, fmt="{:,.0f}"):
        values = [v for v in values if v is not None]
        if not values:
            return "–"
        if len(values) == 1:
            return fmt.format(values[0])
        return f"{fmt.format(statistics.mean(values))} ± {fmt.format(statistics.stdev(values))}"

    header = ["config", "n", "events", "events/s", "p50 ms", "p95 ms", "p99 ms",
              "CPU cores mean (max)", "peak mem MB", "windows", "BUY", "SELL", "late", "vs reference (missing/diff/extra)"]
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    table = []
    for cfg in order:
        rs = by_cfg[cfg]
        check = ", ".join("ref" if c == "reference" else "–" if c is None else "{}/{}/{}".format(*c)
                          for c in (r["check"] for r in rs))
        cpu_mean = [r["docker_cpu_mean"] for r in rs if r["docker_cpu_mean"] is not None]
        cpu_max = [r["docker_cpu_max"] for r in rs if r["docker_cpu_max"] is not None]
        cpu = f"{statistics.mean(cpu_mean):.2f} ({max(cpu_max):.2f})" if cpu_mean else "–"
        row = [cfg, str(len(rs)), f"{rs[0]['events']:,}", ms([r["eps"] for r in rs]),
               ms([r["p50"] for r in rs], "{:,.1f}"), ms([r["p95"] for r in rs], "{:,.1f}"),
               ms([r["p99"] for r in rs], "{:,.1f}"), cpu,
               ms([r["docker_mem_mb"] for r in rs]),
               "/".join(sorted({f"{r['windows']:,}" for r in rs})),
               "/".join(sorted({str(r["buy"]) for r in rs})), "/".join(sorted({str(r["sell"]) for r in rs})),
               "/".join(sorted({str(r["late"]) for r in rs})), check]
        lines.append("| " + " | ".join(row) + " |")
        table.append(dict(zip(header, row)))

    md = "\n".join(lines)
    notes = (
        "\n\nevents/s: first to last processed event (Kafka: includes waiting on the producer; "
        "Flink: consumer only, the topic was filled before the job started). Latency: send of the event "
        "that closes a window to the window row written; only valid for paced runs. CPU and memory: "
        "sum over the pipeline's containers from docker stats (Kafka broker included for Kafka and Flink).\n"
    )
    with open(os.path.join(RESULTS, f"summary-{PREFIX}.md"), "w") as f:
        f.write(f"# Benchmark summary ({PREFIX})\n\n" + md + notes)
    with open(os.path.join(RESULTS, f"summary-{PREFIX}-runs.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[k for k in runs[0] if k not in ("outputs",)] + ["check"],
                           extrasaction="ignore")
        w.writeheader()
        for r in runs:
            w.writerow({**r, "check": "/".join(map(str, r["check"])) if isinstance(r["check"], tuple) else (r["check"] or "")})
    print(md + notes)


if __name__ == "__main__":
    main()

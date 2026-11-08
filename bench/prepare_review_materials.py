#!/usr/bin/env python3
"""Prepare a reproducible benchmark review package for trading-trend-detector.

Run from repo root: py -3 bench/prepare_review_materials.py
Standard-library only; does not launch Docker, alter existing results, or push to GitHub.
"""
from __future__ import annotations

import csv
from collections import defaultdict
from datetime import datetime
from html import escape
import json
from pathlib import Path
import re
import statistics

ROOT = Path(__file__).resolve().parents[1]
BENCH = ROOT / "results" / "benchmarks"
DOCS = ROOT / "docs"
FIGURES = DOCS / "figures"


def load_csv(path):
    if not path.is_file():
        raise SystemExit(f"Missing benchmark summary: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def svg_bar_chart(path, title, subtitle, bars, ylabel="Events / second"):
    """Draw standalone vector chart using standard library only.

    bars = [(short_label, mean, sample_std_dev)]
    """
    width, height = 900, 515
    left, right, top, bottom = 105, 40, 105, 120
    plot_w, plot_h = width-left-right, height-top-bottom
    values = [mean+sd for _,mean,sd in bars]
    ymax = max(values) * 1.18 if values else 1
    chunks = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#fff"/>',
        f'<text x="{left}" y="38" font-size="24" font-family="Arial" font-weight="bold" fill="#172638">{escape(title)}</text>',
        f'<text x="{left}" y="65" font-size="14" font-family="Arial" fill="#555">{escape(subtitle)}</text>',
        f'<text x="28" y="90" font-size="13" font-family="Arial" fill="#555">{escape(ylabel)}</text>',
    ]
    for i in range(6):
        v = ymax * i / 5
        y = top+plot_h - (v/ymax)*plot_h
        chunks += [f'<line x1="{left}" y1="{y:.1f}" x2="{width-right}" y2="{y:.1f}" stroke="#e5e9ed"/>',
                   f'<text x="{left-12}" y="{y+5:.1f}" text-anchor="end" font-size="13" font-family="Arial" fill="#555">{v:,.0f}</text>']
    cell = plot_w/max(len(bars),1)
    for i, (label, mean, sd) in enumerate(bars):
        center = left + (i+.5)*cell
        bw = min(85,cell*.48)
        bar_h = mean/ymax*plot_h
        y = top+plot_h-bar_h
        chunks += [
            f'<rect x="{center-bw/2:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{bar_h:.1f}" fill="#276a9d" rx="3"/>',
            f'<text x="{center:.1f}" y="{y-23:.1f}" text-anchor="middle" font-size="14" font-family="Arial" fill="#172638">{mean:,.0f}</text>',
            f'<text x="{center:.1f}" y="{top+plot_h+27:.1f}" text-anchor="middle" font-size="14" font-family="Arial" fill="#172638">{escape(label)}</text>',
        ]
        if sd:
            ey_low = top+plot_h-(mean-sd)/ymax*plot_h
            ey_high = top+plot_h-(mean+sd)/ymax*plot_h
            chunks += [f'<line x1="{center:.1f}" y1="{ey_low:.1f}" x2="{center:.1f}" y2="{ey_high:.1f}" stroke="#172638" stroke-width="2"/>',
                       f'<line x1="{center-7:.1f}" y1="{ey_low:.1f}" x2="{center+7:.1f}" y2="{ey_low:.1f}" stroke="#172638" stroke-width="2"/>',
                       f'<line x1="{center-7:.1f}" y1="{ey_high:.1f}" x2="{center+7:.1f}" y2="{ey_high:.1f}" stroke="#172638" stroke-width="2"/>']
    chunks += [f'<text x="{left}" y="{height-34}" font-size="13" font-family="Arial" fill="#555">Error bars: ±1 sample SD; n=3 for each configuration.</text>', '</svg>']
    path.write_text("\n".join(chunks), encoding="utf-8")


def svg_window_chart(path, source):
    """Optionally visualize actual query output from an existing TCP 1M JSONL."""
    if not source.is_file():
        return None
    by_symbol = defaultdict(list)
    with source.open(encoding="utf-8") as fh:
        for line in fh:
            row = json.loads(line)
            if row.get("type") == "window" and not row.get("partial"):
                by_symbol[row["symbol"]].append(row)
    candidates = [(len(rows), any(row.get("advisory") == "BUY" for row in rows), symbol)
                  for symbol, rows in by_symbol.items() if len(rows) >= 5]
    if not candidates:
        return None
    _, _, symbol = max(candidates)
    rows = sorted(by_symbol[symbol], key=lambda r: r["window_start"])
    series = [("Close", "close", "#244b91"), ("EMA38", "ema38", "#16825a"), ("EMA100", "ema100", "#bc6d21")]
    nums = [float(row[key]) for row in rows for _,key,_ in series]
    vmin, vmax = min(nums), max(nums)
    if vmax == vmin: vmax = vmin + 1
    pad = (vmax-vmin)*.07
    vmin -= pad; vmax += pad
    w,h=1000,540; left,top,pw,ph=95,105,820,315
    x=lambda i:left + i*pw/max(len(rows)-1,1)
    y=lambda v:top+ph-(v-vmin)/(vmax-vmin)*ph
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}">',
           '<rect width="100%" height="100%" fill="white"/>',
           f'<text x="{left}" y="40" font-family="Arial" font-size="22" font-weight="bold" fill="#172638">Five-minute window output — {escape(symbol)}</text>',
           f'<text x="{left}" y="65" font-family="Arial" font-size="13" fill="#555">Actual 1M TCP JSONL output; {len(rows)} completed windows; example symbol chosen automatically</text>']
    for j in range(6):
        value=vmin+(vmax-vmin)*j/5
        yy=y(value)
        out += [f'<line x1="{left}" y1="{yy:.1f}" x2="{left+pw}" y2="{yy:.1f}" stroke="#e5e9ed"/>',
                f'<text x="{left-10}" y="{yy+5:.1f}" font-family="Arial" font-size="12" text-anchor="end">{value:.2f}</text>']
    for name,key,color in series:
        pts=" ".join(f"{x(i):.1f},{y(float(row[key])):.1f}" for i,row in enumerate(rows))
        out.append(f'<polyline fill="none" stroke="{color}" stroke-width="2.5" points="{pts}"/>')
    for i,row in enumerate(rows):
        if row.get("advisory") in ("BUY", "SELL"):
            buy=row["advisory"]=="BUY"
            out.append(f'<circle cx="{x(i):.1f}" cy="{y(float(row["close"])):.1f}" r="6" fill="{"#16825a" if buy else "#c0392b"}" stroke="white" stroke-width="2"/>')
    for j in range(6):
        i=round(j*(len(rows)-1)/5)
        stamp=rows[i]["window_start"][11:16]
        out.append(f'<text x="{x(i):.1f}" y="{top+ph+24}" text-anchor="middle" font-size="12" font-family="Arial">{escape(stamp)}</text>')
    for j,(name,_,color) in enumerate(series):
        lx=left+j*145
        out += [f'<line x1="{lx}" y1="{h-65}" x2="{lx+28}" y2="{h-65}" stroke="{color}" stroke-width="3"/>',
                f'<text x="{lx+35}" y="{h-61}" font-size="14" font-family="Arial">{name}</text>']
    out += [f'<text x="{left+490}" y="{h-60}" font-size="12" font-family="Arial" fill="#555">Dots mark BUY/SELL advisories (where present)</text>', '</svg>']
    path.write_text("\n".join(out), encoding="utf-8")
    return symbol, len(rows)


def select_raw_records():
    kafka = re.compile(r"^kafka-[124]w-(?:300k|1m)(?:-run[23])?$")
    flink = re.compile(r"^flink-4part-[12]p-300k-(?:isolation|r[23])$|^flink-4p-300k-validation$|^flink-4part-4p-300k-r[23]$")
    special = {"kafka-1w-1m-crc32c-trial1", "flink-4part-4p-1m-validation"}
    selected = []
    for path in BENCH.glob("*/result.json"):
        try:
            record=json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        label=record.get("label", "")
        if kafka.fullmatch(label) or flink.fullmatch(label) or label in special:
            selected.append({"source_path":path.relative_to(ROOT).as_posix(), "result":record})
    selected.sort(key=lambda obj: obj["result"].get("label", ""))
    return selected


def main():
    kafka=load_csv(BENCH/"kafka_scalability_summary.csv")
    flink=load_csv(BENCH/"flink_scalability_300k_4part_summary.csv")
    if len(kafka)!=6 or len(flink)!=3:
        raise SystemExit("Expected 6 Kafka and 3 Flink summary rows; check CSV input")
    DOCS.mkdir(exist_ok=True); FIGURES.mkdir(exist_ok=True)
    kafka_1m=sorted((r for r in kafka if r["dataset"].lower()=="1m"),key=lambda r:int(r["workers"]))
    flink_sorted=sorted(flink,key=lambda r:int(r["Parallelism"]))
    svg_bar_chart(FIGURES/"kafka_scalability_1m.svg","Kafka pipeline throughput — original producer", "1M events; 3 runs each; CRC32C not installed for these runs", [(f'{r["workers"]} worker(s)',float(r["mean_throughput"]),float(r["std_throughput"])) for r in kafka_1m])
    svg_bar_chart(FIGURES/"flink_scalability_300k.svg","Flink consumer-only throughput", "300K events; 4 Kafka partitions fixed; 3 runs each", [(f'{r["Parallelism"]}P',float(r["MeanThroughput"]),float(r["StdDev"])) for r in flink_sorted])
    windows=svg_window_chart(FIGURES/"window_query_example.svg",ROOT/"output"/"windows-2026-11-08T01-15-31Z.jsonl")
    selected=select_raw_records()
    labels=[obj["result"].get("label","") for obj in selected]
    counts={"kafka_original":sum(bool(re.fullmatch(r"kafka-[124]w-(?:300k|1m)(?:-run[23])?",l)) for l in labels),
            "flink_300k_fixed4":sum(bool(re.fullmatch(r"flink-4part-[12]p-300k-(?:isolation|r[23])|flink-4p-300k-validation|flink-4part-4p-300k-r[23]",l)) for l in labels),
            "kafka_crc32c_1m":int("kafka-1w-1m-crc32c-trial1" in labels),
            "flink_1m":int("flink-4part-4p-1m-validation" in labels)}
    evidence={"study":"CS-E4780 trading-trend-detector reproducibility snapshot",
              "environment_observed":{"docker_visible_logical_cpus":16,"docker_memory_bytes":8250089472,
                 "container_nano_cpus":0,"container_memory_limit_bytes":0,
                 "note":"Observed during project experiments; per-run raw JSON host fields retained where available. 0 means no explicit Docker per-container limit, not fixed resource usage."},
              "measurement_notes":{"kafka":"Parallel Kafka workers measured first-to-last processed event while the producer was sending. Includes producer waiting/backpressure; excludes startup.",
                 "flink":"Flink reads a pre-filled bounded Kafka topic. active_seconds is first parse record to last window output; startup excluded. Python CPU/RSS excludes JVM.",
                 "comparison":"Do not treat Kafka pipeline and Flink consumer-only figures as directly comparable end-to-end throughput."},
              "selected_run_counts":counts,"raw_runs":selected}
    (BENCH/"review_evidence_2026-11-08.json").write_text(json.dumps(evidence,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
    fmt=lambda v:f"{float(v):,.2f}"
    krows="\n".join(f'| {r["dataset"]} | {r["workers"]} | {r["runs"]} | {fmt(r["mean_throughput"])} | {fmt(r["std_throughput"])} | {fmt(r["mean_p95_ms"])} | {fmt(r["mean_worker_memory_mb"])} |' for r in sorted(kafka,key=lambda x:(x["dataset"],int(x["workers"]))))
    frows="\n".join(f'| {r["Parallelism"]} | {r["Runs"]} | {fmt(r["MeanThroughput"])} | {fmt(r["StdDev"])} | {fmt(r["Speedup"])} | {fmt(r["MeanCPUSeconds"])} | {fmt(r["MeanMemoryMB"])} |' for r in flink_sorted)
    note="![Example query results](figures/window_query_example.svg)" if windows else "Query visualization omitted: matching 1M TCP output JSONL not found locally."
    symbolinfo=f"Illustrative symbol: `{windows[0]}` with {windows[1]} complete five-minute windows; auto-selected from the 1M TCP output." if windows else ""
    doc=f"""# Performance evaluation and reproducibility notes (8 November 2026)

## Environment and data

- Project: CS-E4780 trading-trend-detector; local Docker Desktop/WSL2 experiments on Windows.
- Observed Docker runtime: 16 logical CPUs; 8,250,089,472 bytes (~7.68 GiB) available memory. No explicit container CPU quota, memory limit or pinned CPU set were configured in the inspected Flink container. These are **environment observations**, not proof that every run used an identical effective CPU allocation.
- Input: pre-extracted `data/prices-300k.jsonl` and `data/prices-1m.jsonl` (DEBS 2022, Monday 8 November 2021); unpaced replay, starting from 08:00. The raw dataset is not committed.
- Scope: 5-minute event-time windows, EMA38/EMA100, BUY/SELL crossover advisories. See the code and source JSON records for exact parsing/validation parameters.
- Source summaries: `results/benchmarks/kafka_scalability_summary.csv` and `results/benchmarks/flink_scalability_300k_4part_summary.csv`.
- Raw selected run records including original run labels and host/config fields: `results/benchmarks/review_evidence_2026-11-08.json` (selected counts: {counts}).

## Kafka pipeline scalability — before CRC32C

![Kafka throughput](figures/kafka_scalability_1m.svg)

| Dataset | Workers | Runs | Mean events/s | Sample SD | Mean p95 ms | Worker RSS total MB |
|---|---:|---:|---:|---:|---:|---:|
{krows}

**Measurement definition:** Kafka Workers process events while the Kafka Producer publishes. Overall `events_per_second` is the total events divided by the interval from the earliest worker event to the latest worker event. As such, it includes waiting for producer throughput and backpressure; it excludes startup. The measured latency is send timestamp on the event finalizing a window to flushed output, not a universal per-event end-to-end latency. Worker RSS excludes Kafka broker and Producer memory.

**Interpretation:** The original pipeline stays near 12K events/s across 1, 2 and 4 workers, which suggests a bottleneck outside the workers. A separate producer-only run was ~12,406 events/s (1M events; 1 partition). CPU profiling found significant time spent in CRC32C. Installing native `crc32c==2.9.post0` increased the producer-only run to ~20,371 events/s and a **single** optimized Kafka 1-worker run to ~20,078 events/s, versus a baseline 1-worker mean of ~12,163 events/s (about +65.1%). No optimized 2/4-worker repetitions have been run, so do not extrapolate that result to scaling.

## Flink window-operator scaling — 4 Kafka partitions fixed

![Flink throughput](figures/flink_scalability_300k.svg)

| Window parallelism | Runs | Mean events/s | Sample SD | Observed speedup | Python CPU s | Python RSS total MB |
|---:|---:|---:|---:|---:|---:|---:|
{frows}

**Measurement definition:** The Flink job is started **after** the Producer has finished filling a bounded Kafka topic. `active_seconds` is measured from the earliest ParseFunction event to the last WindowFunction activity, excluding job startup. The reported CPU and memory sum only Flink Python worker processes, not JVM memory or the Kafka broker. This is **consumer-only**, so these rates must not be compared directly with Kafka pipeline end-to-end rates.

**Interpretation:** With 4 Kafka partitions fixed, Flink shows a reproducible throughput increase as Window Parallelism rises: observed 4P/1P speedup is 8.48x, an unusually superlinear observation. That is not established computational parallel efficiency; different operator bottlenecks, JVM/Python boundaries, buffering, backpressure and runtime overhead may contribute. Detailed profiling would be needed to isolate the mechanism. No explicit per-container CPU cap was used.

## Correctness and constraints

- All 41 Python unit tests passed (`results/benchmarks/unit_tests_final.txt`).
- TCP, Kafka Workers, and Flink output comparisons on the 1M-event pre-extracted input matched 30,386 completed windows: 0 missing, 0 different, 0 extra. Flink's 300K 1P/2P/4P representative runs matched 14,856 TCP windows.
- Flink 1M 4P consumer-only trial: ~8,192.92 events/s, 30,386 output windows, 0 late events; this is a single run, not a repeated-study mean.
- Original CSV and extracted JSONL replay may differ materially in parsing overhead; compare systems only when data input and timing boundaries agree.

## Example query results

{note}

{symbolinfo}

### Next checks suggested by code review

1. Repeat optimized Kafka Producer+Workers for 1/2/4 workers under the same input and recording protocol; measure producer, worker and broker CPU separately.
2. Test native CRC32C on Kafka consumer separately before changing `receiver/requirements.txt`.
3. Record actual per-container Docker CPU and memory usage over time; consider explicitly capped CPU resources to evaluate throughput versus CPU budget.
4. Add raw run records and the visualizations to the PR; have teammates review any changed resource assumptions and compare-method definitions.
"""
    (DOCS/"performance_evaluation_2026-11-08.md").write_text(doc,encoding="utf-8")
    print("Generated files:")
    for f in [DOCS/"performance_evaluation_2026-11-08.md",FIGURES/"kafka_scalability_1m.svg",FIGURES/"flink_scalability_300k.svg",BENCH/"review_evidence_2026-11-08.json"]:
        print(" ",f.relative_to(ROOT).as_posix())
    if windows: print(" ", (FIGURES/"window_query_example.svg").relative_to(ROOT).as_posix(), "(symbol", windows[0]+")")
    print("Selected raw records:",counts,"total",len(selected))
    if len(selected)<29:
        print("NOTE: Expected 29 selected records, but some labels were not found. Check the source result.json folders before committing.")

if __name__ == "__main__":
    main()

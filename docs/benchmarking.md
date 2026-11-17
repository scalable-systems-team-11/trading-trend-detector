# Benchmarking

Back to the [README](../README.md). Pipeline details: [pipelines.md](pipelines.md).


Every receiver run reports a `benchmark` block in its final `closed;` log line
and, under Docker, writes it to its own folder `results/benchmarks/<UTC time>-<kind>-<label>/result.json`
(set the `RECEIVER_BENCH_OUTPUT` directory for direct Python runs). Other files of a run, such as
`docker stats` samples, go in the same folder. These files are
committed: they are the evidence for the design choices in the report, so keep them.
Label runs to tell them apart:

```bash
# as fast as possible, without the dashboard
REPLAY_SPEED=0 BENCH_LABEL=week-tcp docker compose up --build receiver replayer
```

| Field | Meaning |
| --- | --- |
| `events_per_second` | Received events / time from first to last event (includes pacing waits) |
| `processor_events_per_second` | Events / time spent inside `StreamProcessor.process` only |
| `cpu_seconds`, `cpu_utilization` | Receiver CPU time and CPU/wall ratio (1.0 = one core busy) |
| `peak_rss_mb` | Peak receiver memory (not reported on Windows) |
| `latency_ms` | p50/p95/p99/max from replayer send (`sent_ns`) to window row written |

The replayer stamps `sent_ns` on each event (disable with `--no-stamp`), so
latency is only valid when replayer and receiver share a clock. All windows
closing at one boundary share one trigger event, so percentiles are coarse.
Measure throughput with `REPLAY_SPEED=0` and latency at a paced speed below
saturation, in separate runs.

To benchmark the processor alone (no TCP, no CSV re-scan), extract price
events once and run the in-process benchmark on that file:

```bash
python3 replayer/replay.py data/debs2022-gc-trading-day-08-11-21.csv \
    --speed 0 --only-prices --no-stamp --progress 0 > data/prices-08-11-21.jsonl
python3 bench/bench_processor.py data/prices-08-11-21.jsonl --label baseline
```

The result goes to `results/benchmarks/` with `"kind": "processor"`.

To check a multi-day run against an offline recomputation of the same symbols:

```bash
python3 bench/check_week.py output/windows-<time>.jsonl "data/prices-*.jsonl" --symbols 20
```

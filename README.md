## CS-E4780 Scalable Systems and Data Management Course Project
### Detecting Trading Trends in Financial Tick Data

### Data

We use the [DEBS 2022 Grand Challenge Trading Data](https://doi.org/10.5281/zenodo.6382482)
(one week of tick data from 8-14 Nov 2021, one CSV per day, ~5 GB per weekday).
CSV files go in `data/` (git-ignored). (Currently have been using a subset of monday to test)


### Running

```
docker compose up --build
```

This starts two services:

- **replayer** ([replayer/replay.py](replayer/replay.py)): reads the CSV and replays the
  events as a paced stream (JSON lines), by default price events from 08:00 at 60x speed.
- **receiver** ([receiver/receiver.py](receiver/receiver.py)): TCP ingestion feeding
  [the processor](receiver/processor.py), which calculates per-symbol five-minute
  windows, EMA38/EMA100 and BUY/SELL crossovers. Diagnostics and rate summaries
  go to stderr; result rows are appended to `output/windows.jsonl` on the host.

Both are configured with environment variables in [docker-compose.yml](docker-compose.yml)
(`REPLAY_*`, `RECEIVER_*`). Run `python3 replayer/replay.py --help` for all replayer
options. Useful overrides:

```
docker compose run --rm -e REPLAY_SPEED=0 replayer      # no pacing, as fast as possible
docker compose run --rm -e REPLAY_SYMBOLS=ENGI.FR replayer
```

### Data quirks

- Events are only roughly ordered by `Time`; small backwards jumps occur.
- ~12k rows per day have no `Time` at all; the replayer skips them for now.
- Rows with `Last = 0.000000` (and `Trading time = 00:00:00.000`) are placeholders with
  arbitrary timestamps, not trades; `--only-prices` drops them.
- `--start` and `--end` filter the system `Time` column. Because rows can be
  out of order, `--end` skips later timestamps and scans the entire input unless
  `--limit` is reached; it does not terminate at the first later row.

### Stream processing

The processor uses `Trading date` and `Trading time` for event-time windows.
Observed index rows (`SecType=I`) can lack `Trading date`; for these rows only,
the processor uses system `Date` with `Trading time` and counts accepted
inferences as `index_date_fallback`. This assumes same-day index updates and
must be explained in the report. Equities still require a trading date;
the replayer still uses system `Time` for pacing. Each nonempty window produces
one JSON row with `symbol`, `window_start`, `window_end`, `close`, `ema38`,
`ema100` and `advisory` (`BUY`, `SELL`, or null). A `run_id` separates sequential
replays. EMA is initialized to **zero**, as required by the assignment.

Windows are `[start, end)`, aligned to midnight in the dataset's fixed CEST
clock. By default they finalize when the stream reaches the next boundary.
An optional event-time delay tolerates small backwards jumps:

```bash
# Linux / Cloud Shell
RECEIVER_ALLOWED_LATENESS_SECONDS=2 docker compose up --build
```

```powershell
# Windows PowerShell
$env:RECEIVER_ALLOWED_LATENESS_SECONDS = "2"
docker compose up --build
```

Leave this at `0` for immediate next-window evaluation; use a positive value
only when evaluating the latency/correctness tradeoff for out-of-order input.
Events targeting finalized windows are dropped and counted as `late`.

`RECEIVER_SYMBOLS=SIE.ETR,ENGI.FR` filters **output**, while all received symbols
are still calculated. Filtering with `REPLAY_SYMBOLS` instead reduces the input
workload, so it should not be used to claim processing of the full symbol set.

The final incomplete window is withheld on disconnect. For previews only, set
`RECEIVER_FLUSH_PARTIAL=1`; those rows are marked `partial: true`.
See [semantics and limitations](docs/stream_processor.md), including the
proposed empty-window policy that the team should confirm.

### Benchmarking

Every receiver run reports a `benchmark` block in its final `closed;` log line
and, under Docker, appends it to `output/benchmarks.jsonl` (set
`RECEIVER_BENCH_OUTPUT` for direct Python runs). Label runs to tell them apart:

```bash
BENCH_LABEL=speed0-fullday docker compose up --build
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

Results are appended to `output/benchmarks.jsonl` with `"kind": "processor"`.

### Tests and running without Docker

No additional Python packages are needed (Python 3.10+; Docker uses 3.12).
Run from the repository root:

```bash
python3 -m unittest discover -s tests -v
python3 -m receiver.receiver
```

In a second terminal, feed a real CSV file to the running receiver:

```bash
python3 replayer/replay.py data/debs2022-gc-trading-day-08-11-21.csv --speed 0 --only-prices --sink tcp
```

On Windows use `python` in place of `python3`. Without `RECEIVER_OUTPUT`, result
JSON lines go to stdout. Each connection begins a fresh replay; only one active
connection is accepted. Keep all daily files in one replayer invocation to
preserve EMA across days. Use a new output file or distinguish rows by `run_id`
when comparing repeated runs.

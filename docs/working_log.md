# A simple log to keep track of what we have done and when, and what decisions where made and why.
## Keep this really straight forward but try to remember to fill it in so we can use this when writing report later

## Log
- 2x.9 (All) First meeting planning architecture
- 2x.9 (All) Team decided to try and replicate "real time" streaming in some way, although we might have to use smaller dataset/slower speed depending on hardware
- 28.9 (Jens) Using AI to set up project structure, and trying to create a data loader. Using a smaller subset of one days data.
- 29.9 (Jens) Setting it up to run in docker, and adding a minimal TCP server to recieve (will be replaced with actual handler later). Testing that this MVP works, writing a readme etc
- 6.10 (Yuhang, AI-assisted candidate implementation) Added a separate per-symbol stream processor, five-minute event-time windows, zero-seeded EMA38/100 and BUY/SELL detection; connected the receiver and Docker output configuration. Unit and real-TCP synthetic-fixture tests pass. Empty-window, timestamp and lateness policies documented for team review. Docker and real-data performance validation remain to be done; team ownership is not yet confirmed.
- 6.10 (AI-assisted local follow-up) Windows replay logs showed a successful TCP connection but zero emitted events with an 08:00–09:00 filter. Reproduced an early-return bug using a late placeholder followed by in-range trades; changed the end-time filter to skip rows and continue scanning. All 12 synthetic tests pass; real-data rerun remains pending.
- 6.10 (AI-assisted local follow-up) User's real-data sample showed five index rows with nonzero Last, valid Trading time and NULL Trading date. Added an explicit index-only same-day date inference from system Date, with an accepted-record counter. Supplied trading dates retain priority; missing equity dates or trading times remain invalid. All 14 tests pass; full real-data correctness and the date-inference assumption still need validation.
- 7.10 (Klaus, AI-assisted) Added simple benchmarking: receiver reports throughput (overall and processor-only), CPU, peak memory and send-to-emit latency percentiles per run into `output/benchmarks.jsonl`; replayer stamps `sent_ns` on events; `bench/bench_processor.py` benchmarks the processor in-process from a CSV or extracted JSONL.

## 6.10.2026 — Yuhang: local implementation and validation 

This entry records the current state after the debugging and validation above.
The comparison baseline is the group's repo (29 September MVP). That version already provided CSV replay, pacing, TCP transport and Docker scaffolding; its receiver only printed events and counts.
The local implementation adds five-minute per-symbol windows, closing-price
selection, zero-seeded EMA38/EMA100 and BUY/SELL crossover detection. 

### New project files — 8

| File | Added content and purpose |
|---|---|
| `receiver/processor.py` | Per-symbol event-time state; midnight-aligned, non-overlapping five-minute windows; latest trading-timestamp close with last-arrival tie breaking; EMA38/EMA100 and BUY/SELL detection; validation, late-event handling, optional subscriptions and EOF processing. Includes explicit index-only date inference when Trading date is missing. |
| `receiver/models.py` | Dataclasses for pending windows, per-symbol EMA state and processing counters, including `index_date_fallback`. |
| `receiver/__init__.py` | Makes receiver a Python package so it can run with `python -m receiver.receiver`. |
| `tests/test_processor.py` | Core algorithm tests: window boundaries, zero seeds, independent Decimal EMA reference, both crossovers, timestamp ordering/ties, date rollover, empty windows, subscriptions, lateness, invalid input, EOF and index date inference. |
| `tests/test_pipeline.py` | Synthetic CSV → actual TCP connection → receiver → JSONL window output integration test, including malformed JSON handling. |
| `tests/test_replayer.py` | Regression test ensuring late placeholders and out-of-order rows do not stop an end-time-filtered replay before subsequent in-range events. |
| `docs/stream_processor.md` | Implementation rules, configuration, engineering assumptions, test coverage and remaining validation. |
| `docs/windows_validation_2026-10-06.md` | Evidence and limits of the Windows real-data run, all-output consistency checks and independent source-event sample comparison. |

### Modified existing files — 7

| File | Change relative to the MVP |
|---|---|
| `receiver/receiver.py` | Replaces print-only handling with the stream processor; emits JSONL window/advisory results with run IDs; adds counters, malformed-JSON handling, fresh EMA state for each connection, configuration and a single-active-stream guard. |
| `replayer/replay.py` | Moves price/symbol filtering before time-range checks and changes `--end` from early return to row skipping. Later out-of-order in-range events can therefore still be replayed; end-filtered input is fully scanned unless `--limit` is reached. Updates CLI help. |
| `receiver/Dockerfile` | Copies the receiver package modules and uses `python -m receiver.receiver` as the entry point. |
| `docker-compose.yml` | Adds the receiver output volume and output, lateness, partial-window and subscription settings. Retains the existing replayer configuration. |
| `README.md` | Adds processor behavior, configuration, Windows/local run instructions, tests and the explicit index date-inference and replay filtering policies. |
| `docs/working_log.md` | Adds the AI-assisted implementation/debugging history and this file inventory, result inventory and validation summary. |
| `.gitignore` | Ignores generated output and Python caches; raw CSV data remains ignored. |

### Generated run and validation files — not source code

These files were generated on Yuhang's Windows computer and uploaded for verification. 

| File in the Windows project | Origin | Contents and role |
|---|---|---|
| `output/windows.jsonl` | Automatically written by the receiver when `RECEIVER_OUTPUT=output/windows.jsonl`. | 14,856 completed-window records from one 08:00–09:00 replay. Each record contains run ID, symbol, window boundaries, close, event count, current/previous EMAs, advisory and partial flag. This file appends results across runs; run IDs distinguish them. |
| `output/price_sample.jsonl` | Exported using the replayer with `--symbols 2ICEU.FR --start 08:00 --limit 100 --sink stdout`, saved with PowerShell `Set-Content -Encoding utf8`. | 100 extracted source-event records used to independently check sample window membership/counts, close, EMA and signals against the earlier window output. It is replayer output, not a copy of the original CSV rows. |

Input data is separate: `data/debs2022-gc-trading-day-08-11-21.csv` is the
downloaded Monday DEBS 2022 dataset (4,672,671,094 bytes), not a newly implemented
file or a run result. Keep it in `data/` when updating source code. Generated
`output/` files can be retained as evidence; a new run can generate new results.


### Validation completed

- All 14 standard-library unit/regression/integration tests passed in the
  implementation environment. The pipeline test uses real TCP with synthetic
  events; it is separate from the Windows real-data run.
- The Windows replay used Monday's CSV, system-Time filter 08:00–09:00,
  `--only-prices`, speed 0 and TCP on port 9999. Replayer reported 300,200 emitted
  events and 11,999 rows skipped for unparseable/missing system Time.
- Receiver reported 299,877 accepted events, 323 invalid events, 298,504 accepted
  index-date inferences, zero late/malformed records, 1,395 symbols, 14,856
  completed windows and 1,395 pending windows. BUY=1,240 and SELL=0. Pending
  windows are withheld by default when input ends before their next boundary.
- All 14,856 uploaded output records passed checks for JSON structure, window
  duration/alignment, uniqueness, chronological order, positive values/counts,
  previous/current EMA recurrence and advisory consistency. Independently
  recalculated the EMAs with 50-digit Decimal arithmetic; maximum relative
  error was approximately `8e-16`, within floating-point rounding.
- Output contains 1,240 symbols with completed windows: 1,236 have 12 windows
  and four have six windows. The other 155 symbols have no completed output
  in this slice, consistent with the receiver's reported pending-state totals.
- The 100-event `2ICEU.FR` sample independently matched four complete windows
  (08:00–08:20), each with 20 records and close 92.501, including EMA values and
  BUY/null signals. The fifth group's observed values also match, but the sample
  stops at 08:24:45.370 before the 08:25 boundary. All sample prices are equal,
  so this sample does not exercise choosing between different closing prices.
- Receiver session elapsed time was 153.515 seconds, corresponding to about
  1,955.51 received events/second overall. This includes waiting while excluded
  CSV rows are scanned; the transient approximately 88k events/second log is
  not a full-run throughput or latency result.

### Decisions and remaining work

- Index rows with missing Trading date use system Date with Trading time.
  This is an explicit same-day assumption, not a supplied trade date or a
  confirmed course requirement. Supplied dates take priority; malformed dates,
  missing equity trade dates and missing trading times remain invalid.
- Empty windows retain EMA without output, and finalized windows do not accept
  late updates. These policies and the fixed CEST (+02:00) clock need team review.
- Current evidence validates output consistency and a bounded source-event
  sample. CSV column mapping and all original ticks have not been independently
  audited; uploaded pending-state contents were unavailable.
- Next validate equities and changing prices after 09:00, investigate the
  323 invalid events, validate real-data SELL behavior, test Docker Compose,
  and measure throughput, latency and resource usage. Distributed execution,
  visualization and the final group report remain separate project work.

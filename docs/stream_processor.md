# Stream processor: candidate Query 1 and Query 2 implementation

This module is a proposed contribution for Yuhang to review and coordinate
with the group. It extends the existing replayer/receiver interface; ownership
of this work has not yet been agreed by the team.

## Responsibility and module boundaries

| File | Role |
| --- | --- |
| `receiver/receiver.py` | TCP/JSON ingestion, one replay session, JSON output and operational counters |
| `receiver/processor.py` | Event validation, window finalization, EMA38/100 and crossovers |
| `receiver/models.py` | Window, EMA and counter state |
| `tests/test_processor.py` | Query correctness, boundary and failure cases |
| `tests/test_pipeline.py` | Existing CSV replayer through real TCP to processor output |

The existing `replayer/replay.py` has a time-filter fix: rows after `--end` are
skipped instead of terminating the scan, so placeholders and out-of-order rows
cannot hide subsequent in-range trades. GUI, distributed execution,
real-data performance experiments, and the final report remain separate work.

## Rules taken from the project specification

- Use the full symbol ID, including exchange suffix, as the state key.
- Calculate received symbols independently. Optional subscriptions filter
  emission, not internal calculation.
- Five-minute non-overlapping windows start at midnight CEST.
- Initialize both previous EMAs to 0. For each nonempty window:
  `EMA_j = close * (2 / (1 + j)) + previous_EMA_j * (1 - 2 / (1 + j))`.
- BUY: current EMA38 > EMA100 and previous EMA38 <= EMA100.
- SELL: current EMA38 < EMA100 and previous EMA38 >= EMA100.
- Equality alone emits no advisory. The zero seed means that the first positive
  close emits BUY under the specified comparison rules.

## Engineering policies to confirm with the team

**Clock.** Use the starred `Trading date` / `Trading time` fields. In the observed
real-data sample, index (`SecType=I`) rows have prices and `Trading time` but
NULL `Trading date`. For indexes only, infer a missing/empty trading date from
system `Date`, keeping `Trading time`. This is an explicit same-day assumption,
not a date provided by the original trade or a specified course requirement;
confirm it with the group and include it in the report. Count accepted inferred
records as `index_date_fallback` in summaries. A supplied trading date always
takes priority; an invalid supplied date is rejected. Equities with missing
trading dates and all records missing trading times remain invalid. Treat the supplied
CEST clock as fixed UTC+02:00, rather than applying European November DST rules.
Timestamps are parsed to integer microseconds, including the four-digit
fractional seconds described by the dataset. Replayer pacing remains based on
system `Time`; that clock does not define the processor's windows.

**Close.** Choose the price at the greatest trading timestamp in a window.
For equal timestamps, the last received event wins. This is an explicit
event-time interpretation of "last price observed"; small backwards jumps
within an open window cannot replace a later trade with an older trade.

**Empty windows.** No price means no update and no result row: retain both
EMAs until the next nonempty window. The specification does not define an
empty-window close. This is a proposed policy, not an explicit course rule.
Do not synthesize zero prices or carry-forward prices through overnight gaps.

**Global progress.** A watermark is the greatest accepted trading timestamp
minus configured lateness. A min-heap finalizes all open windows ending at or
before it, including symbols that no longer trade. Default lateness is 0, for
evaluation at the next boundary. Positive lateness delays evaluation by that
amount of event time; it is an optional deviation for an out-of-order-input
experiment. Any event targeting an already finalized window is dropped and
counted. Lateness must be selected from real-data measurements; zero lateness
does not promise exact results for arbitrary out-of-order input. No-price or
malformed events do not advance this trading watermark. There is no wall-clock
timer: when the input pauses, event-time progress also pauses.

**End of input.** EOF declares the replay finished. Complete windows held only
for lateness are released. Windows ending beyond the maximum trading timestamp
remain incomplete, and do not affect EMA. Optional preview flushing explicitly
marks their output `partial: true`; exclude those rows from correctness and
latency evaluation. A replay subset ending mid-window is not a complete window.

**Validation.** Accept security types E and I, finite positive prices, nonempty
symbol IDs, and valid trading timestamps. Missing or zero Last is a non-price
row. Negative/nonfinite prices and malformed fields are invalid. These are
counted separately from too-late rows and malformed JSON lines.

**Replay session.** One active TCP connection represents the entire input
stream. Disconnecting ends that run; reconnecting starts from zero. Supply all
days in one replayer invocation for cross-day continuity. A second concurrent
connection is rejected. There is no recovery/checkpointing or coordination
between multiple producers.

## Output and metrics

Each finalized nonempty window emits a JSON row with both current and previous
EMA values, window boundaries, close, number of accepted price events, advisory,
and partial flag. The receiver adds `run_id` and flushes the result immediately
after finalization. Docker writes to host `output/windows.jsonl`; direct Python
runs default to stdout. Logs and per-run summaries go to stderr.

Counters include received, accepted, index-date-fallback, invalid, no-price, late, windows, BUY and
SELL, symbol count and pending windows. Receiver summaries additionally include
wire line count, malformed JSON, elapsed wall time and input events/second.
This rate includes socket waiting and output work. It is **not** an end-to-end
latency measurement, and no real-data throughput result is claimed here.

State size is O(symbols + open symbol-windows); one heap entry exists per open
window. Events are not stored individually. Positive lateness can retain
multiple windows per symbol. Finishing a window costs O(log(open windows)) plus
serialization. This is a single-process baseline, not demonstrated horizontal
scalability.

## Validation completed and remaining

The standard-library test suite checks exact boundary assignment, zero seed,
400 windows against an independent 50-digit Decimal recurrence, both crossover
directions, no repeated advisories, cross-symbol separation, inactive-symbol
finalization, date rollover, gaps, timestamp ties, late arrivals, subscription
filtering, invalid values, incomplete EOF, and CSV-to-TCP integration.

Before merging, review the engineering policies above with the group, run
Docker Compose with a real dataset, compare a small real-data slice against a
batch reference, inspect late/invalid counters, then measure throughput,
latency and resource use at several input sizes and replay speeds. Neither
Docker execution nor real tick-data performance was tested in the implementation
environment because Docker and the CSV dataset were unavailable.

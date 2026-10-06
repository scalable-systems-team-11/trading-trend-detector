# Validation of uploaded real-data window output — 2026-10-06

## Evidence and scope

Input: user's `windows.jsonl` (5,885,688 bytes), SHA-256 `e17450a5316ab6fe5e5dc5956b5e3ff1a8b06021039c23df0d5600aa783f8143`.
Run: `1791247879320202600-127.0.0.1:58620`.
User replayed Monday DEBS CSV with system-Time filter 08:00–09:00,
`--only-prices`, speed 0, into the Windows receiver.
The original CSV was not available in the verification environment.

## Checks performed

Parsed every JSON line; checked five-minute duration, midnight alignment,
fixed UTC+02:00 timestamps, complete-window flags, positive finite closes,
positive integer event counts, unique (run, symbol, window-start) keys,
chronological window order, and per-symbol ordering without overlap.

Independently recalculated EMA38 and EMA100 with 50-digit Decimal arithmetic
from the emitted closes, starting each symbol at zero, using
`(2 * close + (j - 1) * previous) / (j + 1)`.
Checked recorded previous EMAs and every BUY/SELL/null result against the
recalculated recurrence and crossover inequalities. Numerical tolerance was
absolute 1e-10 or relative 1e-12, whichever is larger.

## Results

| Item | Result |
|---|---:|
| Output rows / unique windows | 14,856 / 14,856 |
| Runs | 1 |
| Symbols with completed windows | 1,240 |
| Symbols with 12 windows | 1,236 |
| Symbols with 6 windows | 4 |
| BUY / SELL / no advisory | 1,240 / 0 / 13,616 |
| Validation issues | 0 |
| Maximum EMA38 absolute error | 9.93155560387009e-11 |
| Maximum EMA100 absolute error | 1.02267821696691e-11 |
| Maximum EMA38 relative error | 7.97624229840732e-16 |
| Maximum EMA100 relative error | 6.31514019216837e-16 |
| Sum of completed-window event counts | 297,094 |
| Symbols with changing emitted closes | 423 |

Completed windows span 08:00–09:00 on 2021-11-08, with 1,236 symbols for
each of the first six windows and 1,240 for each of the last six.
Every output symbol emitted BUY in its first completed positive-price
window and no later advisory; this is consistent with zero-seeded EMAs.

## Reconciliation with user-supplied receiver log

The receiver reported 300,200 received, 299,877 accepted, 323 invalid,
298,504 accepted index-date inferences, zero late/malformed records,
14,856 completed windows, 1,395 symbols and 1,395 pending windows.
Output window and signal totals agree with that log.
The 155 symbols without completed output remain represented in pending
state according to the reported totals. Accepted events exceed emitted
window event counts by 2,783, consistent with events retained in pending
windows; pending state contents were not supplied and this is not an
independent check of those contents.

Elapsed receiver session time was 153.515 seconds; its overall input rate
was about 1,955.51 events/second and includes waiting while the replayer
scans excluded CSV rows. The transient 88k events/second log must not be
reported as overall session throughput or system latency.

## Limits and next validation

This establishes internal output consistency, not correctness of CSV
column mapping, source-event membership/counts, selected closing prices,
late-event exclusion, pending state, or the same-day date-inference policy.
The first output close and all other closes were taken from the supplied
output, not independently selected from source ticks. Index records with
NULL Trading date use system Date with Trading time; that assumption must
be described and reviewed. The 323 invalid records cannot be diagnosed
from window output. No real-data SELL case occurred in this slice; synthetic
unit tests exercise both crossovers.

Next compare a bounded source-event sample against an independent batch
aggregation, then validate a longer trading interval containing equities
and price changes. No GitHub writes were performed.

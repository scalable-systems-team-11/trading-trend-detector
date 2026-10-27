#!/usr/bin/env python3
"""Compare window rows of a reference run (TCP receiver) with a candidate run (Kafka workers).

    python3 bench/compare_outputs.py output/windows.jsonl "output/kafka-windows.w*.jsonl"

Each file is appended to across runs; only the last run_id in each file is used.
Rows are matched on (symbol, window_start). Both runs must replay the same input
(same file, --start, --limit). The candidate may contain extra rows: a worker
only sees its own symbols, so its watermark is lower and it closes fewer windows
early and drops fewer events as late. Missing or different rows are errors.
"""

import glob
import json
import sys


def last_run(path):
    rows = {}
    run = None
    with open(path, encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            if row["run_id"] != run:
                run, rows = row["run_id"], {}
            rows[(row["symbol"], row["window_start"])] = {k: v for k, v in row.items() if k != "run_id"}
    return rows


def main():
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    reference = last_run(sys.argv[1])
    candidate = {}
    paths = sorted(glob.glob(sys.argv[2]))
    if not paths:
        raise SystemExit(f"no files match {sys.argv[2]!r}")
    for path in paths:
        candidate.update(last_run(path))
    missing = [k for k in reference if k not in candidate]
    different = [k for k in reference if k in candidate and candidate[k] != reference[k]]
    extra = [k for k in candidate if k not in reference]
    print(f"reference rows {len(reference)}, candidate rows {len(candidate)} from {len(paths)} files")
    print(f"missing {len(missing)}, different {len(different)}, extra {len(extra)}")
    for name, keys in (("missing", missing), ("different", different)):
        for key in keys[:5]:
            print(f"  {name}: {key}")
    sys.exit(1 if missing or different else 0)


if __name__ == "__main__":
    main()

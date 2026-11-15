"""Summarise a (multi-day) windows output and check it against an offline batch recomputation.

Usage:
    python3 bench/check_week.py output/windows-<time>.jsonl data/prices-*.jsonl [--symbols N]

1. Summary: rows, symbols, BUY/SELL per day (window_end date, CEST).
2. Reference check: for N symbols (top by window count plus a random sample), recompute
   windows, EMA38/EMA100 and advisories offline from the extracted price events (all events,
   no watermark, so nothing is "late") and compare with the streaming output.
   Differences are expected only where the stream dropped late events.
"""
import argparse
import glob
import json
import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

CEST = timezone(timedelta(hours=2))  # same fixed offset as receiver/processor.py
WINDOW_S = 300


def ts(date, time):
    d = datetime.strptime(date, "%d-%m-%Y").replace(tzinfo=CEST)
    h, m, s = time.split(":")
    return d.timestamp() + int(h) * 3600 + int(m) * 60 + float(s)


def reference(price_files, symbols):
    """symbol -> list of (window_start_epoch, close) in window order."""
    windows = defaultdict(dict)  # symbol -> start -> (timestamp, close)
    needles = {s: f'"id": "{s}"' for s in symbols}
    for path in price_files:
        with open(path, encoding="utf-8") as f:
            for line in f:
                if '"id": "' not in line:
                    continue
                # cheap prefilter before json parsing
                sym = line.split('"id": "', 1)[1].split('"', 1)[0]
                if sym not in needles:
                    continue
                ev = json.loads(line)
                if ev.get("sec_type") not in ("E", "I") or not ev.get("last") or float(ev["last"]) == 0:
                    continue
                date = ev.get("trading_date") or ev.get("date")
                try:
                    t = ts(date, ev["trading_time"])
                except (TypeError, ValueError, AttributeError):
                    continue
                start = int(t // WINDOW_S * WINDOW_S)
                prev = windows[sym].get(start)
                if prev is None or t >= prev[0]:
                    windows[sym][start] = (t, float(ev["last"]))
    return {s: [(st, c) for st, (_, c) in sorted(w.items())] for s, w in windows.items()}


def ema_rows(closes):
    a38, a100 = 2 / 39, 2 / 101
    e38 = e100 = 0.0
    out = []
    for start, close in closes:
        p38, p100 = e38, e100
        e38 = a38 * close + (1 - a38) * e38
        e100 = a100 * close + (1 - a100) * e100
        adv = "BUY" if e38 > e100 and p38 <= p100 else "SELL" if e38 < e100 and p38 >= p100 else None
        out.append((start, close, e38, e100, adv))
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("output")
    p.add_argument("prices", nargs="+")
    p.add_argument("--symbols", type=int, default=20)
    p.add_argument("--seed", type=int, default=1)
    args = p.parse_args()
    price_files = sorted(f for pat in args.prices for f in glob.glob(pat))

    rows = defaultdict(list)
    adv_per_day = Counter()
    n = 0
    with open(args.output) as f:
        for line in f:
            r = json.loads(line)
            if r.get("type") != "window":
                continue
            n += 1
            rows[r["symbol"]].append(r)
            if r["advisory"]:
                adv_per_day[(r["window_end"][:10], r["advisory"])] += 1
    print(f"rows {n}, symbols {len(rows)}")
    print("advisories per day (window_end date):")
    for day in sorted({d for d, _ in adv_per_day}):
        print(f"  {day}  BUY {adv_per_day[(day, 'BUY')]:6d}  SELL {adv_per_day[(day, 'SELL')]:6d}")
    with_sell = sum(1 for rs in rows.values() if any(r["advisory"] == "SELL" for r in rs))
    print(f"symbols with >=1 SELL: {with_sell}")

    by_count = sorted(rows, key=lambda s: -len(rows[s]))
    rnd = random.Random(args.seed)
    rest = by_count[args.symbols // 2:]
    sample = by_count[: args.symbols // 2] + rnd.sample(rest, min(len(rest), args.symbols - args.symbols // 2))
    ref = reference(price_files, set(sample))

    print(f"\nreference check on {len(sample)} symbols ({len(price_files)} price files)")
    total = mismatched_syms = 0
    for s in sample:
        got = rows[s]
        exp = ema_rows(ref.get(s, []))
        got_starts = [datetime.fromisoformat(r["window_start"]).timestamp() for r in got]
        same_len = len(got) == len(exp)
        bad = 0
        for r, st, e in zip(got, got_starts, exp):
            if (abs(st - e[0]) > 1e-6 or abs(r["close"] - e[1]) > 1e-9 or abs(r["ema38"] - e[2]) > 1e-6 * max(1, abs(e[2]))
                    or abs(r["ema100"] - e[3]) > 1e-6 * max(1, abs(e[3])) or r["advisory"] != e[4]):
                bad += 1
        total += len(got)
        flag = "OK" if same_len and bad == 0 else "DIFF"
        mismatched_syms += flag == "DIFF"
        advs = Counter(r["advisory"] for r in got if r["advisory"])
        print(f"  {flag:4s} {s:14s} windows {len(got):5d} ref {len(exp):5d} differing {bad:5d}  "
              f"BUY {advs['BUY']:3d} SELL {advs['SELL']:3d}")
    print(f"{len(sample) - mismatched_syms}/{len(sample)} symbols identical, {total} windows compared")


if __name__ == "__main__":
    main()

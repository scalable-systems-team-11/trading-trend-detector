"""Time range filters must tolerate placeholders and out-of-order rows."""
import csv
import json
from pathlib import Path
import tempfile
import unittest

from replayer.replay import FIELDS, parse_args, replay


class CaptureSink:
    def __init__(self):
        self.events = []

    def send(self, line):
        self.events.append(json.loads(line))


class ReplayerTests(unittest.TestCase):
    def test_end_filter_does_not_stop_at_placeholder_or_out_of_order_trade(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "fixture.csv"
            with path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(FIELDS)
                for price, timestamp in [(0, "23:59:00"), (99, "07:59:59"),
                                         (100, "08:00:00"), (200, "09:00:01"),
                                         (110, "08:05:00"), (120, "09:00:00")]:
                    writer.writerow(["SIE.ETR", "E", "08-11-2021", timestamp,
                                     price, timestamp, "08-11-2021"])
            args = parse_args([str(path), "--speed", "0", "--only-prices",
                               "--start", "08:00", "--end", "09:00",
                               "--progress", "0"])
            sink = CaptureSink()
            self.assertEqual(replay(args, sink), (3, 0))
            self.assertEqual([e["last"] for e in sink.events], ["100", "110", "120"])

    def test_extracted_jsonl_replays_like_the_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path, jsonl_path = Path(tmp) / "day.csv", Path(tmp) / "prices.jsonl"
            with csv_path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(FIELDS)
                writer.writerow(["SIE.ETR", "E", "08-11-2021", "08:00:00", "100", "08:00:00", "08-11-2021"])
                writer.writerow(["SIE.ETR", "E", "08-11-2021", "08:00:01", "", "", ""])
                writer.writerow(["AEX.NL", "I", "08-11-2021", "08:00:02", "700", "08:00:02", ""])
            extract = CaptureSink()
            replay(parse_args([str(csv_path), "--speed", "0", "--only-prices", "--progress", "0"]), extract)
            # A stamped extract: the old sent_ns must not be replayed.
            jsonl_path.write_text("".join(json.dumps(e) + "\n" for e in extract.events))
            again = CaptureSink()
            replay(parse_args([str(jsonl_path), "--speed", "0", "--no-stamp", "--progress", "0"]), again)
            expected = [{k: v for k, v in e.items() if k != "sent_ns"} for e in extract.events]
            self.assertEqual(len(expected), 2)
            self.assertEqual(again.events, expected)


if __name__ == "__main__":
    unittest.main()

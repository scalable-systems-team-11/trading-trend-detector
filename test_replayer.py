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


if __name__ == "__main__":
    unittest.main()

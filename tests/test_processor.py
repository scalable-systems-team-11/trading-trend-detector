import unittest
from datetime import datetime, timedelta
from decimal import Decimal, localcontext

from receiver.processor import StreamProcessor


def event(price=100, time="08:00:00.000", symbol="SIE.ETR", date="08-11-2021",
          system_date="01-01-2000", **extra):
    return {"id": symbol, "sec_type": "E", "last": str(price),
            "trading_date": date, "trading_time": time,
            "date": system_date, "time": "00:00:00.000", **extra}


class ProcessorTests(unittest.TestCase):
    def test_index_missing_trade_date_uses_system_date_and_trading_time(self):
        p = StreamProcessor()
        p.process(event(16022.6, symbol="A0C4CA.ETR",
                        sec_type="I", trading_date=None, system_date="08-11-2021",
                        time="23:59:00.000", trading_time="08:00:00.000"))
        row, = p.process(event(16030, "08:05:00.000", symbol="A0C4CA.ETR",
                              sec_type="I", trading_date="", system_date="08-11-2021"))
        self.assertEqual(row["close"], 16022.6)
        self.assertTrue(row["window_start"].startswith("2021-11-08T08:00:00"))
        self.assertEqual(row["advisory"], "BUY")
        self.assertEqual(p.metrics.accepted, 2)
        self.assertEqual(p.metrics.index_date_fallback, 2)
        self.assertEqual(p.metrics.invalid, 0)

    def test_index_date_fallback_does_not_replace_bad_or_present_trade_fields(self):
        p = StreamProcessor()
        for extra in [dict(sec_type="E", trading_date=None),
                      dict(sec_type="I", trading_date=None, system_date=None),
                      dict(sec_type="I", trading_date=None, trading_time=None),
                      dict(sec_type="I", trading_date="bad", system_date="08-11-2021")]:
            p.process(event(**extra))
        self.assertEqual(p.metrics.invalid, 4)
        self.assertEqual(p.metrics.index_date_fallback, 0)
        # A supplied trading date takes priority over a different system date.
        p.process(event(sec_type="I", system_date="09-11-2021"))
        row, = p.process(event(time="08:05:00", sec_type="I", system_date="09-11-2021"))
        self.assertTrue(row["window_start"].startswith("2021-11-08T08:00:00"))
        self.assertEqual(p.metrics.index_date_fallback, 0)

    def test_boundary_and_zero_seed(self):
        p = StreamProcessor()
        self.assertEqual(p.process(event(90, "08:00:00.000")), [])
        self.assertEqual(p.process(event(100, "08:04:59.999999")), [])
        row, = p.process(event(200, "08:05:00.000000"))
        self.assertEqual(row["close"], 100)
        self.assertEqual(row["event_count"], 2)
        self.assertTrue(row["window_start"].startswith("2021-11-08T08:00:00"))
        self.assertTrue(row["window_end"].endswith("+02:00"))
        self.assertAlmostEqual(row["ema38"], 100 * 2 / 39)
        self.assertAlmostEqual(row["ema100"], 100 * 2 / 101)
        self.assertEqual(row["advisory"], "BUY")
        self.assertEqual(p.finish(), [])
        self.assertEqual(p.summary()["pending_windows"], 1)

    def test_latest_event_time_and_arrival_tie(self):
        p = StreamProcessor()
        p.process(event(100, "08:04:59.000"))
        p.process(event(999, "08:00:00.000"))
        p.process(event(101, "08:04:59.000"))
        row, = p.process(event(1, "08:05:00.000"))
        self.assertEqual(row["close"], 101)
        self.assertEqual(row["event_count"], 3)

    def test_watermark_flushes_inactive_symbols(self):
        p = StreamProcessor()
        p.process(event(100, symbol="A.FR"))
        p.process(event(200, symbol="B.NL"))
        rows = p.process(event(300, "08:05:00.000", symbol="B.NL"))
        self.assertEqual({r["symbol"] for r in rows}, {"A.FR", "B.NL"})
        self.assertAlmostEqual(p.states["A.FR"].ema38, 100 * 2 / 39)
        self.assertAlmostEqual(p.states["B.NL"].ema38, 200 * 2 / 39)

    def test_configurable_lateness_and_finalized_window_rejection(self):
        p = StreamProcessor(allowed_lateness_seconds=2)
        p.process(event(100, "08:04:58.000"))
        self.assertEqual(p.process(event(200, "08:05:00.000")), [])
        p.process(event(110, "08:04:59.000"))
        row, = p.process(event(201, "08:05:02.000"))
        self.assertEqual(row["close"], 110)
        self.assertEqual(p.process(event(500, "08:04:59.999")), [])
        self.assertEqual(p.metrics.late, 1)
        self.assertEqual(p.metrics.accepted, 4)

    def test_end_of_input_releases_complete_not_partial_windows(self):
        p = StreamProcessor(allowed_lateness_seconds=10)
        p.process(event(100, "08:04:59.000"))
        p.process(event(200, "08:05:01.000"))
        row, = p.finish()
        self.assertEqual(row["close"], 100)
        self.assertFalse(row["partial"])
        self.assertEqual(p.finish(), [])
        with self.assertRaises(RuntimeError):
            p.process(event())
        preview = StreamProcessor()
        preview.process(event())
        row, = preview.finish(include_partial=True)
        self.assertTrue(row["partial"])

    def test_day_rollover_and_empty_windows(self):
        p = StreamProcessor()
        p.process(event(100, "23:59:59.999999"))
        row, = p.process(event(200, "00:00:00.000", date="09-11-2021"))
        self.assertTrue(row["window_start"].startswith("2021-11-08T23:55"))
        self.assertTrue(row["window_end"].startswith("2021-11-09T00:00"))
        row, = p.process(event(300, "01:00:00.000", date="09-11-2021"))
        self.assertAlmostEqual(row["ema38"], (2 / 39) * 200 + (37 / 39) * (100 * 2 / 39))
        self.assertEqual(p.metrics.windows, 2)

    def test_invalid_events_and_placeholders_do_not_advance_clock(self):
        p = StreamProcessor()
        invalid = [None, [], event(-1), event("NaN"), event("inf"), event(True),
                   event("oops"), event(id=""), event(sec_type="X"),
                   event(trading_date=None), event(trading_date="31-02-2021"),
                   event(time="garbage", trading_time="24:00:00"),
                   event(trading_time="08:60:00"), event(trading_time="08:00:60"),
                   event(trading_time="08:00:00."), event(trading_time="08:00:00.0000001")]
        for ev in invalid:
            self.assertEqual(p.process(ev), [])
        for price in (0, "", None):
            self.assertEqual(p.process(event(price, "23:59:59", last=price)), [])
        self.assertEqual(p.metrics.invalid, len(invalid))
        self.assertEqual(p.metrics.no_price, 3)
        self.assertIsNone(p.max_timestamp)
        self.assertEqual(p.states, {})

    def test_subscriptions_filter_output_not_computation(self):
        p = StreamProcessor(subscribed_symbols={"A.FR"})
        p.process(event(100, symbol="A.FR"))
        p.process(event(200, symbol="B.NL"))
        row, = p.process(event(1, "08:05:00", symbol="A.FR"))
        self.assertEqual(row["symbol"], "A.FR")
        self.assertGreater(p.states["B.NL"].ema100, 0)
        self.assertEqual(p.metrics.windows, 2)

    def test_decimal_reference_and_both_crossovers(self):
        # Independent high-precision batch recurrence over known closes.
        prices = [Decimal("100")] * 200 + [Decimal("1")] * 100 + [Decimal("300")] * 100
        p = StreamProcessor()
        origin = datetime(2021, 11, 8)
        actual = []
        for i, price in enumerate(prices + [Decimal("300")]):
            timestamp = origin + timedelta(minutes=5 * i)
            actual.extend(p.process(event(price, timestamp.strftime("%H:%M:%S"), date=timestamp.strftime("%d-%m-%Y"))))
        with localcontext() as ctx:
            ctx.prec = 50
            fast = slow = Decimal(0)
            signals = []
            for price, row in zip(prices, actual):
                old_fast, old_slow = fast, slow
                fast = price * Decimal(2) / 39 + fast * Decimal(37) / 39
                slow = price * Decimal(2) / 101 + slow * Decimal(99) / 101
                signal = "BUY" if fast > slow and old_fast <= old_slow else "SELL" if fast < slow and old_fast >= old_slow else None
                self.assertAlmostEqual(row["ema38"], float(fast), places=10)
                self.assertAlmostEqual(row["ema100"], float(slow), places=10)
                self.assertEqual(row["advisory"], signal)
                if signal:
                    signals.append(signal)
        self.assertEqual(len(actual), len(prices))
        self.assertEqual(signals, ["BUY", "SELL", "BUY"])

    def test_invalid_lateness(self):
        for value in (-1, float("inf"), float("nan")):
            with self.assertRaises(ValueError):
                StreamProcessor(value)


if __name__ == "__main__":
    unittest.main()

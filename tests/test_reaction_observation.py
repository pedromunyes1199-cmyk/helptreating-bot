"""Автооценка реакции 15m (OBSERVATION ONLY) — паттерны и критерии без торгового потока."""

import unittest

import scanner


class TestShortLongRejection(unittest.TestCase):
    def test_short_rejection_true(self):
        c = {"o": 102, "h": 105, "l": 99, "c": 98}
        self.assertTrue(scanner.short_rejection_candle(c, zone_low=100.0))

    def test_long_rejection_true(self):
        c = {"o": 102, "h": 108, "l": 98, "c": 111}
        self.assertTrue(scanner.long_rejection_candle(c, zone_high=110.0))


class TestEngulfing(unittest.TestCase):
    def test_bearish_engulfing_true(self):
        prev = {"o": 100, "h": 102, "l": 99, "c": 101}
        cur = {"o": 102, "h": 103, "l": 95, "c": 99}
        self.assertTrue(scanner.bearish_engulfing(prev, cur))

    def test_bullish_engulfing_true(self):
        prev = {"o": 101, "h": 102, "l": 99, "c": 100}
        cur = {"o": 99, "h": 106, "l": 98, "c": 104}
        self.assertTrue(scanner.bullish_engulfing(prev, cur))


class TestPinBars(unittest.TestCase):
    def test_bearish_pin_bar_true(self):
        c = {"o": 92, "h": 100, "l": 90, "c": 91}
        self.assertTrue(scanner.bearish_pin_bar(c))

    def test_bullish_pin_bar_true(self):
        c = {"o": 97, "h": 100, "l": 90, "c": 99}
        self.assertTrue(scanner.bullish_pin_bar(c))


class TestStructureBreak(unittest.TestCase):
    def test_short_structure_break_after_touch(self):
        w = {
            "trade_dir": "SHORT",
            "zone_low": 100.0,
            "zone_high": 110.0,
            "pre_hit_local_low_5": 100.0,
            "pre_hit_local_high_5": 120.0,
            "criteria_hit": {"rejection": False, "trigger": False, "structure_break": False},
            "touch_seen": False,
            "score": 0,
        }
        touch = {"o": 105, "h": 102, "l": 99, "c": 101}
        scanner._apply_reaction_candle(w, None, touch)
        self.assertTrue(w["touch_seen"])
        nxt = {"o": 99, "h": 101, "l": 90, "c": 95}
        scanner._apply_reaction_candle(w, touch, nxt)
        self.assertTrue(w["criteria_hit"]["structure_break"])

    def test_long_structure_break_after_touch(self):
        w = {
            "trade_dir": "LONG",
            "zone_low": 90.0,
            "zone_high": 110.0,
            "pre_hit_local_low_5": 80.0,
            "pre_hit_local_high_5": 120.0,
            "criteria_hit": {"rejection": False, "trigger": False, "structure_break": False},
            "touch_seen": False,
            "score": 0,
        }
        touch = {"o": 115, "h": 116, "l": 109, "c": 112}
        scanner._apply_reaction_candle(w, None, touch)
        self.assertTrue(w["touch_seen"])
        nxt = {"o": 118, "h": 125, "l": 117, "c": 121}
        scanner._apply_reaction_candle(w, touch, nxt)
        self.assertTrue(w["criteria_hit"]["structure_break"])


class TestWatchExpireSimulation(unittest.TestCase):
    def test_expires_after_four_neutral_candles(self):
        """4 свечи без критериев → score 0 → условие FAILED (как в process_reaction_watches)."""
        w = {
            "trade_dir": "SHORT",
            "zone_low": 5000.0,
            "zone_high": 5100.0,
            "pre_hit_local_low_5": 4900.0,
            "pre_hit_local_high_5": 5200.0,
            "criteria_hit": {"rejection": False, "trigger": False, "structure_break": False},
            "touch_seen": False,
            "score": 0,
        }
        # Цена далеко ниже зоны — нет касания, нет паттернов
        candles = [
            {"o": 4000, "h": 4010, "l": 3990, "c": 4005},
            {"o": 4005, "h": 4012, "l": 3998, "c": 4008},
            {"o": 4008, "h": 4015, "l": 4000, "c": 4003},
            {"o": 4003, "h": 4010, "l": 3995, "c": 4007},
        ]
        prev = None
        for cur in candles:
            scanner._apply_reaction_candle(w, prev, cur)
            prev = cur
        self.assertLessEqual(w["score"], 1)
        self.assertEqual(len(candles), scanner.REACTION_WINDOW_BARS)


class TestNoTradingPipeline(unittest.TestCase):
    def test_process_reaction_does_not_call_fire_zone_hit(self):
        names = scanner.process_reaction_watches.__code__.co_names
        self.assertNotIn("fire_zone_hit", names)
        self.assertNotIn("log_signal", names)

    def test_register_watch_does_not_touch_evaluate_setup(self):
        names = scanner.register_reaction_watch_on_zone_hit.__code__.co_names
        self.assertNotIn("evaluate_setup", names)

    def test_smart_status_not_in_reaction_handlers(self):
        for fn in (scanner.process_reaction_watches, scanner.register_reaction_watch_on_zone_hit):
            self.assertNotIn("smart_status", fn.__code__.co_names)


if __name__ == "__main__":
    unittest.main()

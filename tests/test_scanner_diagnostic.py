"""Диагностика и будущий RSI_FILTER_MODE=zone_reaction — торговая логика по умолчанию не меняется."""

import unittest

from scanner import (
    SMART_STATUSES,
    ZONE_REACTION_RSI_FAIL,
    evaluate_setup,
    zone_reaction_rsi_detail,
    zone_reaction_rsi_ok,
)


class TestZoneReactionRsiFuture(unittest.TestCase):
    """Кейсы для RSI_FILTER_MODE=zone_reaction (пока не дефолт)."""

    def test_1_long_near_down_ok(self):
        ok, _ = zone_reaction_rsi_detail("LONG", "NEAR", "DOWN")
        self.assertTrue(ok)

    def test_2_short_near_up_ok(self):
        ok, _ = zone_reaction_rsi_detail("SHORT", "NEAR", "UP")
        self.assertTrue(ok)

    def test_3_long_inside_up_ok(self):
        ok, _ = zone_reaction_rsi_detail("LONG", "INSIDE", "UP")
        self.assertTrue(ok)

    def test_4_long_inside_flat_ok(self):
        ok, _ = zone_reaction_rsi_detail("LONG", "INSIDE", "FLAT")
        self.assertTrue(ok)

    def test_5_long_inside_down_fail(self):
        ok, reason = zone_reaction_rsi_detail("LONG", "INSIDE", "DOWN")
        self.assertFalse(ok)
        self.assertEqual(reason, ZONE_REACTION_RSI_FAIL)

    def test_6_short_inside_down_ok(self):
        ok, _ = zone_reaction_rsi_detail("SHORT", "INSIDE", "DOWN")
        self.assertTrue(ok)

    def test_7_short_inside_flat_ok(self):
        ok, _ = zone_reaction_rsi_detail("SHORT", "INSIDE", "FLAT")
        self.assertTrue(ok)

    def test_8_short_inside_up_fail(self):
        ok, reason = zone_reaction_rsi_detail("SHORT", "INSIDE", "UP")
        self.assertFalse(ok)
        self.assertEqual(reason, ZONE_REACTION_RSI_FAIL)

    def test_9_far_any_rsi_false_for_mode(self):
        self.assertFalse(zone_reaction_rsi_ok("LONG", "FAR", "UP"))
        self.assertFalse(zone_reaction_rsi_ok("SHORT", "FAR", "DOWN"))


def _minimal_long_zone():
    return {
        "low": 100.0,
        "high": 110.0,
        "direction": "LONG",
        "score": 4,
        "retests": 0,
        "impulse_body": 20.0,
        "atr_at_form": 5.0,
        "formed_at_idx": 50,
    }


class TestEvaluateSetupSafety(unittest.TestCase):
    def test_far_not_ready_or_action(self):
        z = _minimal_long_zone()
        row = {
            "coin": "BTC",
            "price": 200.0,
            "best_zone": z,
            "trend_aligned": True,
            "atr_mode": "NORMAL",
            "rsi_dir": "UP",
            "atr_1h": 2.0,
        }
        evaluate_setup(row)
        self.assertEqual(row["proximity"], "FAR")
        self.assertNotIn(row["smart_status"], ("READY", "ACTION"))

    def test_no_zone_no_typeerror(self):
        row = {
            "coin": "ETH",
            "price": 100.0,
            "best_zone": None,
            "trend_aligned": True,
            "atr_mode": "NORMAL",
            "rsi_dir": "UP",
            "atr_1h": 1.0,
        }
        out = evaluate_setup(row)
        self.assertEqual(out["smart_status"], "IGNORE")
        self.assertFalse((out.get("_scanner_diag") or {}).get("rsi_ok"))

    def test_smart_status_contract(self):
        z = _minimal_long_zone()
        cases = [
            {
                "coin": "BTC",
                "price": 105.0,
                "best_zone": z,
                "trend_aligned": True,
                "atr_mode": "NORMAL",
                "rsi_dir": "UP",
                "atr_1h": 2.0,
            },
            {
                "coin": "BTC",
                "price": 200.0,
                "best_zone": z,
                "trend_aligned": False,
                "atr_mode": "NORMAL",
                "rsi_dir": "DOWN",
                "atr_1h": 2.0,
            },
        ]
        for base in cases:
            out = evaluate_setup(dict(base))
            self.assertIn(out["smart_status"], SMART_STATUSES)


if __name__ == "__main__":
    unittest.main()

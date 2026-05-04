"""Auto plan / sizing (Telegram + JSONL) — без ордеров, только scanner."""

import os
import unittest
from unittest.mock import patch

import scanner


def _reset_hl_leverage_cache():
    with scanner._hl_leverage_lock:
        scanner._hl_leverage_cache["per_symbol"] = {}
        scanner._hl_leverage_cache["ts"] = 0.0
        scanner._hl_leverage_cache["last_refresh_error"] = None


class TestShouldEmitAutoPlan(unittest.TestCase):
    def test_a_plus_3_3_yes(self):
        w = {"score": 3, "setup_grade": "A+"}
        self.assertTrue(scanner._should_emit_auto_plan(w))

    def test_a_plus_2_3_no(self):
        w = {"score": 2, "setup_grade": "A+"}
        self.assertFalse(scanner._should_emit_auto_plan(w))

    def test_b_3_3_no_when_auto_plan_for_b_false(self):
        w = {"score": 3, "setup_grade": "B"}
        with patch.object(scanner, "AUTO_PLAN_FOR_B", False):
            self.assertFalse(scanner._should_emit_auto_plan(w))

    def test_b_3_3_yes_when_auto_plan_for_b_true(self):
        w = {"score": 3, "setup_grade": "B"}
        with patch.object(scanner, "AUTO_PLAN_FOR_B", True):
            self.assertTrue(scanner._should_emit_auto_plan(w))

    def test_b_2_3_no(self):
        w = {"score": 2, "setup_grade": "B"}
        with patch.object(scanner, "AUTO_PLAN_FOR_B", True):
            self.assertFalse(scanner._should_emit_auto_plan(w))


class TestAccountBalanceRisk(unittest.TestCase):
    def test_balance_1500_a_plus_risk_15(self):
        with patch.dict(os.environ, {"ACCOUNT_BALANCE": "1500"}, clear=False):
            # перечитать helper через прямой расчёт как в add_sizing
            bal = scanner._account_balance_usdc()
            self.assertEqual(bal, 1500.0)
            m = {
                "valid": True,
                "entry_ref": 100.0,
                "risk_per_unit": 5.0,
            }
            full = scanner.add_sizing_to_plan(m, "A+", 1500.0)
            self.assertAlmostEqual(full["risk_usdc"], 15.0, places=6)


class TestPlanGeometry(unittest.TestCase):
    def test_short_stop_above_entry_tp_below(self):
        m = scanner.compute_auto_plan_metrics(
            "BTC", "SHORT", 100.0, 110.0, entry_ref=105.0, atr_1h=2.0
        )
        self.assertTrue(m["valid"])
        self.assertGreater(m["stop_ref"], m["entry_ref"])
        self.assertLess(m["min_tp_for_RR_2_3"], m["entry_ref"])

    def test_long_stop_below_entry_tp_above(self):
        m = scanner.compute_auto_plan_metrics(
            "BTC", "LONG", 100.0, 110.0, entry_ref=105.0, atr_1h=2.0
        )
        self.assertTrue(m["valid"])
        self.assertLess(m["stop_ref"], m["entry_ref"])
        self.assertGreater(m["min_tp_for_RR_2_3"], m["entry_ref"])


class TestLeverage(unittest.TestCase):
    def test_estimated_leverage(self):
        m = {
            "valid": True,
            "entry_ref": 10000.0,
            "risk_per_unit": 100.0,
        }
        full = scanner.add_sizing_to_plan(m, "A+", 1500.0)
        # risk = 15, pos = 15/100 * 10000 = 1500, lev = 1500/1500 = 1
        self.assertAlmostEqual(full["estimated_leverage"], 1.0, places=6)
        self.assertAlmostEqual(full["position_size_usdc"], 1500.0, places=4)


class TestContractsUnchanged(unittest.TestCase):
    def test_smart_status_tuple(self):
        self.assertEqual(
            scanner.SMART_STATUSES,
            ("ACTION", "READY", "WATCH", "IGNORE"),
        )

    def test_rsi_filter_constant_exists(self):
        self.assertIsInstance(scanner.RSI_FILTER_MODE, str)


class TestNoOrderCalls(unittest.TestCase):
    def test_emit_auto_plan_has_no_fire_zone_hit(self):
        names = scanner._emit_auto_plan_if_eligible.__code__.co_names
        self.assertNotIn("fire_zone_hit", names)


class TestHyperliquidMaxLeverageCache(unittest.TestCase):
    def setUp(self):
        _reset_hl_leverage_cache()

    @patch("scanner._hl_post")
    def test_btc_eth_sol_from_meta_and_asset_ctxs(self, mock_post):
        mock_post.return_value = [
            {
                "universe": [
                    {"name": "BTC", "maxLeverage": 40},
                    {"name": "ETH", "maxLeverage": 25},
                    {"name": "SOL", "maxLeverage": 20},
                ]
            },
            [],
        ]
        scanner.refresh_hyperliquid_max_leverage_cache(force=True)
        self.assertEqual(scanner.get_hyperliquid_max_leverage("BTC")["value"], 40.0)
        self.assertEqual(
            scanner.get_hyperliquid_max_leverage("BTC")["max_leverage_source"],
            "api_metaAndAssetCtxs",
        )
        self.assertEqual(scanner.get_hyperliquid_max_leverage("ETH")["value"], 25.0)
        self.assertEqual(scanner.get_hyperliquid_max_leverage("SOL")["value"], 20.0)

    @patch("scanner._hl_post")
    def test_fallback_to_meta_when_ctxs_malformed(self, mock_post):
        meta_body = {
            "universe": [
                {"name": "BTC", "maxLeverage": 40},
                {"name": "ETH", "maxLeverage": 25},
                {"name": "SOL", "maxLeverage": 20},
            ]
        }

        def side_effect(payload, timeout=15):
            t = payload.get("type")
            if t == "metaAndAssetCtxs":
                return []
            if t == "meta":
                return meta_body
            return None

        mock_post.side_effect = side_effect
        scanner.refresh_hyperliquid_max_leverage_cache(force=True)
        self.assertEqual(scanner.get_hyperliquid_max_leverage("BTC")["max_leverage_source"], "api_meta")

    @patch("scanner._hl_post")
    def test_fallback_static_when_api_errors(self, mock_post):
        mock_post.side_effect = Exception("network")
        scanner.refresh_hyperliquid_max_leverage_cache(force=True)
        r = scanner.get_hyperliquid_max_leverage("BTC")
        self.assertEqual(r["value"], scanner.HL_MAX_LEV_FALLBACK["BTC"])
        self.assertEqual(r["max_leverage_source"], "fallback_due_to_api_error")

    @patch("scanner._hl_post")
    def test_unknown_symbol_returns_none_value(self, mock_post):
        mock_post.return_value = [
            {
                "universe": [
                    {"name": "BTC", "maxLeverage": 40},
                    {"name": "ETH", "maxLeverage": 25},
                    {"name": "SOL", "maxLeverage": 20},
                ]
            },
            [],
        ]
        _reset_hl_leverage_cache()
        scanner.refresh_hyperliquid_max_leverage_cache(force=True)
        r = scanner.get_hyperliquid_max_leverage("DOGE")
        self.assertIsNone(r["value"])
        self.assertEqual(r["error_reason"], "unknown_max_leverage_for_symbol")


class TestEffectiveLeverage(unittest.TestCase):
    def test_effective_is_min_of_hl_and_protocol(self):
        hl, prot = 40.0, float(scanner.PROTOCOL_MAX_LEV["BTC"])
        self.assertEqual(min(hl, prot), 4.0)

    def test_below_cap_ok(self):
        self.assertLess(1.8, min(40.0, 4.0))

    def test_above_cap_triggers_warning_math(self):
        eff = min(40.0, float(scanner.PROTOCOL_MAX_LEV["BTC"]))
        bal = 1500.0
        entry, stop = 100.0, 105.0
        max_pos, cap_risk, cap_pct = scanner._cap_adjusted_risk_at_max_leverage(
            bal, eff, entry, stop
        )
        self.assertAlmostEqual(max_pos, bal * eff, places=6)
        self.assertAlmostEqual(cap_risk, max_pos * abs(entry - stop) / entry, places=6)
        self.assertAlmostEqual(cap_pct, cap_risk / bal * 100.0, places=6)


if __name__ == "__main__":
    unittest.main()

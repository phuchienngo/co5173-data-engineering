"""Unit tests for jobs/br5_promotion/rules.py (no Spark needed).

Run: make br5-test, or python -m unittest discover -s tests/br5
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jobs" / "br5_promotion"))

import rules  # noqa: E402

RULES = dict(rules.DEFAULT_RULES)
FULL = {m: 1 for m in range(1, 13)}


def demand_of(values):
    return {m: {"demand": v, "bookings": v} for m, v in zip(range(1, 13), values)}


def rows_for(target, nights_by_month, bookings=None):
    return [
        {"target": target, "month": m, "room_nights": n, "bookings": bookings if bookings is not None else n}
        for m, n in zip(range(1, 13), nights_by_month)
    ]


class MonthRanges(unittest.TestCase):
    def test_wraps_from_december_to_january(self):
        self.assertEqual(rules.month_ranges([1, 2, 11, 12]), [[11, 12, 1, 2]])

    def test_separate_ranges_in_calendar_order(self):
        self.assertEqual(rules.month_ranges([12, 6, 1, 3]), [[3], [6], [12, 1]])

    def test_no_wrap_when_not_at_both_ends(self):
        self.assertEqual(rules.month_ranges([1, 2, 5]), [[1, 2], [5]])

    def test_all_and_none(self):
        self.assertEqual(rules.month_ranges(range(1, 13)), [list(range(1, 13))])
        self.assertEqual(rules.month_ranges([]), [])

    def test_labels(self):
        self.assertEqual(rules.range_label([11, 12, 1, 2]), "Nov – Feb")
        self.assertEqual(rules.range_label([6]), "Jun")


class Demand(unittest.TestCase):
    def test_coverage_counts_years_per_month(self):
        # Data from Jul 2015 to Aug 2017, like the real file.
        yms = [2015 * 100 + m for m in range(7, 13)] + [2016 * 100 + m for m in range(1, 13)] \
            + [2017 * 100 + m for m in range(1, 9)]
        coverage = rules.month_coverage(yms)
        self.assertEqual(coverage[1], 2)
        self.assertEqual(coverage[7], 3)
        self.assertEqual(coverage[8], 3)
        self.assertEqual(coverage[9], 2)

    def test_average_per_year_and_multiplier(self):
        coverage = {m: (3 if m in (7, 8) else 2) for m in range(1, 13)}
        rows = [{"month": 7, "room_nights": 300, "bookings": 30}, {"month": 1, "room_nights": 100, "bookings": 10}]
        demand = rules.monthly_demand(rows, coverage, multiplier=10)
        self.assertAlmostEqual(demand[7]["demand"], 10.0)   # 300 / 3 years / ×10
        self.assertAlmostEqual(demand[1]["demand"], 5.0)    # 100 / 2 years / ×10
        self.assertEqual(demand[2]["demand"], 0.0)          # month exists, no booking

    def test_months_missing_from_data_are_left_out(self):
        coverage = {m: (1 if m <= 6 else 0) for m in range(1, 13)}
        demand = rules.monthly_demand([], coverage)
        self.assertEqual(sorted(demand), [1, 2, 3, 4, 5, 6])


class Season(unittest.TestCase):
    def test_peak_low_months_and_potential(self):
        info = rules.season(demand_of([40, 50, 70, 80, 90, 95, 100, 100, 80, 70, 30, 30]), 0.6)
        self.assertEqual(info["peak_month"], 7)                  # first of the tied peaks
        self.assertEqual(info["low_months"], [1, 2, 11, 12])
        self.assertAlmostEqual(info["low_demand"], 37.5)
        self.assertAlmostEqual(info["potential"], 62.5)
        self.assertAlmostEqual(info["demand_ratio"], 0.375)
        self.assertEqual(info["ranges"], [[11, 12, 1, 2]])

    def test_threshold_is_strict(self):
        info = rules.season(demand_of([60] + [100] * 11), 0.6)
        self.assertEqual(info["low_months"], [])                 # 60% of peak is not below 60%
        self.assertEqual(info["potential"], 0.0)

    def test_texts(self):
        demand = demand_of([40, 40, 100, 100, 100, 100, 100, 100, 100, 100, 20, 40])
        info = rules.season(demand, 0.6)
        self.assertEqual(rules.season_texts(demand, info), ["Nov – Feb: demand is 35% of peak"])

    def test_no_demand(self):
        self.assertIsNone(rules.season(demand_of([0] * 12), 0.6))


class DiscountTier(unittest.TestCase):
    def test_bounds(self):
        self.assertEqual(rules.discount_tier(0.39, RULES), RULES["tier1_label"])
        self.assertEqual(rules.discount_tier(0.40, RULES), RULES["tier2_label"])
        self.assertEqual(rules.discount_tier(0.50, RULES), RULES["tier3_label"])
        self.assertEqual(rules.discount_tier(0.65, RULES), RULES["tier3_label"])  # threshold raised


class Offers(unittest.TestCase):
    INFO = {"peak_month": 8, "demand_ratio": 0.45}
    PEAK = {"median_lead": 100, "avg_stay": 4.0, "family_share": 0.20, "avg_adr": 150.0}

    def offer_names(self, low, peak=None):
        found, tier = rules.offers(low, peak or self.PEAK, self.INFO, RULES)
        return [f["offer"].split(":")[0].split(" (")[0] for f in found], tier

    def test_early_bird_at_boundary(self):
        names, _ = self.offer_names({"median_lead": 30, "avg_stay": 4, "family_share": 0.2, "avg_adr": 140})
        self.assertIn("Early-bird offer", names)
        found, _ = rules.offers({"median_lead": 37}, self.PEAK, self.INFO, RULES)
        self.assertIn("at least 35 days", found[0]["offer"])

    def test_last_minute_at_boundary(self):
        names, _ = self.offer_names({"median_lead": 14})
        self.assertIn("Last-minute deal shown close to arrival", names)
        names, _ = self.offer_names({"median_lead": 15})
        self.assertNotIn("Last-minute deal shown close to arrival", names)
        self.assertNotIn("Early-bird offer", names)

    def test_stay_longer_at_boundary(self):
        names, _ = self.offer_names({"avg_stay": 3.0})       # exactly 25% shorter
        self.assertIn("Stay-longer offer", names)
        names, _ = self.offer_names({"avg_stay": 3.1})
        self.assertNotIn("Stay-longer offer", names)

    def test_family(self):
        names, _ = self.offer_names({"family_share": 0.09})
        self.assertIn("Family package", names)
        names, _ = self.offer_names({"family_share": 0.10})  # exactly half: not "less than half"
        self.assertNotIn("Family package", names)
        names, _ = self.offer_names({"family_share": 0.0}, {**self.PEAK, "family_share": 0.0})
        self.assertNotIn("Family package", names)

    def test_price_already_low_adds_value(self):
        names, tier = self.offer_names({"avg_adr": 105.0})   # exactly 30% below
        self.assertIn("Add value, don't cut the price further", names[0] if names else "")
        self.assertEqual(tier, "")

    def test_price_close_gives_discount_with_tier(self):
        names, tier = self.offer_names({"avg_adr": 140.0})
        self.assertEqual(names, ["Discount"])
        self.assertEqual(tier, RULES["tier2_label"])

    def test_discount_evidence_says_when_low_season_is_dearer(self):
        found, _ = rules.offers({"avg_adr": 160.0}, self.PEAK, self.INFO, RULES)
        self.assertIn("7% higher", found[-1]["evidence"])
        found, _ = rules.offers({"avg_adr": 135.0}, self.PEAK, self.INFO, RULES)
        self.assertIn("only 10% lower", found[-1]["evidence"])

    def test_no_prices_gives_general_discount(self):
        found, tier = rules.offers({"avg_adr": None}, self.PEAK, self.INFO, RULES)
        self.assertEqual(len(found), 1)
        self.assertTrue(found[0]["offer"].startswith("No specific offer pattern"))
        self.assertEqual(tier, RULES["tier2_label"])

    def test_every_offer_has_evidence(self):
        low = {"median_lead": 5, "avg_stay": 2.0, "family_share": 0.01, "avg_adr": 140.0}
        found, _ = rules.offers(low, self.PEAK, self.INFO, RULES)
        self.assertEqual(len(found), 4)
        self.assertTrue(all(f["evidence"] for f in found))


class EvaluateTargets(unittest.TestCase):
    SHAPE = [40, 40, 80, 90, 100, 100, 100, 100, 90, 80, 40, 40]

    def test_ranking_minimum_and_unknown(self):
        rows = (
            rows_for(("A", "PRT"), [v * 3 for v in self.SHAPE], bookings=20)     # biggest potential
            + rows_for(("A", "GBR"), self.SHAPE, bookings=20)
            + rows_for(("A", "DEU"), self.SHAPE, bookings=5)                     # 60 bookings: too few
            + rows_for(("A", ""), [v * 9 for v in self.SHAPE], bookings=50)      # Unknown: never a target
            + rows_for(("A", "FRA"), [100] * 12, bookings=20)                    # flat: no low season
        )
        result = rules.evaluate_targets(rows, FULL, 1, RULES)
        self.assertEqual(result["targets_found"], 5)
        self.assertEqual([c["target"] for c in result["top"]], [("A", "PRT"), ("A", "GBR")])
        # The selection includes every row, Unknown and thin targets too.
        self.assertAlmostEqual(result["selection"]["valid_bookings"], 12 * (20 + 20 + 5 + 50 + 20))
        self.assertEqual(result["selection"]["season"]["low_months"], [1, 2, 11, 12])

    def test_multiplier_gives_the_same_answer(self):
        base = rows_for(("A", "PRT"), self.SHAPE, bookings=10) + rows_for(("A", "GBR"), self.SHAPE, bookings=8)
        times10 = [{**r, "room_nights": r["room_nights"] * 10, "bookings": r["bookings"] * 10} for r in base]
        one = rules.evaluate_targets(base, FULL, 1, RULES)
        ten = rules.evaluate_targets(times10, FULL, 10, RULES)
        self.assertEqual([c["target"] for c in one["top"]], [c["target"] for c in ten["top"]])
        for a, b in zip(one["top"], ten["top"]):
            self.assertAlmostEqual(a["season"]["potential"], b["season"]["potential"])

    def test_ties_are_ordered_by_bookings_then_name(self):
        rows = rows_for(("A", "GBR"), self.SHAPE, bookings=10) + rows_for(("A", "ESP"), self.SHAPE, bookings=10) \
            + rows_for(("A", "ITA"), self.SHAPE, bookings=11)
        result = rules.evaluate_targets(rows, FULL, 1, RULES)
        self.assertEqual([c["target"][1] for c in result["top"]], ["ITA", "ESP", "GBR"])

    def test_top_n(self):
        rows = []
        for i in range(15):
            rows += rows_for(("A", f"C{i:02d}"), [v + i for v in self.SHAPE], bookings=10)
        result = rules.evaluate_targets(rows, FULL, 1, {**RULES, "top_n": 10})
        self.assertEqual(len(result["top"]), 10)
        self.assertEqual(result["targets_qualified"], 15)


class Cards(unittest.TestCase):
    def test_profile_and_channels(self):
        profile = rules.profile_figures({"bookings": 4, "median_lead": 12.5, "nights": 10, "family_bookings": 1,
                                         "adr_sum": 300.0, "adr_count": 3})
        self.assertEqual(profile["avg_stay"], 2.5)
        self.assertEqual(profile["family_share"], 0.25)
        self.assertEqual(profile["avg_adr"], 100.0)
        self.assertIsNone(rules.profile_figures({"bookings": 0})["avg_stay"])
        self.assertEqual(rules.top_channels({"Online TA": 60, "Direct": 30, "Groups": 10}),
                         ["Online TA (60%)", "Direct (30%)"])
        self.assertEqual(rules.top_channels({}), [])


if __name__ == "__main__":
    unittest.main()

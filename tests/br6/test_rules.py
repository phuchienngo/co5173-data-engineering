"""Unit tests for the BR6 Route Finder rules (route_rules.py): geometry, the
grid, filters, scoring and ordering. No Spark or ClickHouse needed."""

import math
import random
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "jobs" / "br6_route"))

import route_rules as rules  # noqa: E402

RULES = {
    "max_detour_km": 1.0, "top_n": 2, "weight_popularity": 0.5, "weight_fit": 0.3, "weight_closeness": 0.2,
    "general_weight_popularity": 0.7, "general_weight_closeness": 0.3, "min_place_visitors": 3,
    "min_visitor_checkins": 10, "max_centre_km": 100.0,
}


def place(key, detour, visitors, place_type="Food & drink", checkins=None, leg=0, position=1.0, offered=True):
    return {"place_key": key, "detour": detour, "leg": leg, "position": position, "visitors": visitors,
            "checkins": checkins if checkins is not None else visitors * 2, "place_type": place_type,
            "is_offered": int(offered)}


class Geometry(unittest.TestCase):
    def test_flat_map(self):
        m = rules.FlatMap(0.0, 100.0)
        x, y = m.xy(1.0, 101.0)
        self.assertAlmostEqual(x, rules.KM_PER_DEGREE)
        self.assertAlmostEqual(y, rules.KM_PER_DEGREE)
        x, _ = rules.FlatMap(60.0, 0.0).xy(60.0, 1.0)
        self.assertAlmostEqual(x, rules.KM_PER_DEGREE / 2)

    def test_leg_distance_inside_and_beyond_the_ends(self):
        self.assertEqual(rules.leg_distance(5, 3, 0, 0, 10, 0), (3.0, 0.5))
        self.assertEqual(rules.leg_distance(-4, 3, 0, 0, 10, 0), (5.0, 0.0))
        self.assertEqual(rules.leg_distance(13, 4, 0, 0, 10, 0), (5.0, 1.0))
        self.assertEqual(rules.leg_distance(3, 4, 1, 1, 1, 1)[0], math.sqrt(13))  # zero-length leg

    def test_nearest_leg_position_and_ties(self):
        legs = rules.build_legs([(0, 0), (10, 0), (10, 10)])
        self.assertEqual(rules.route_km(legs), 20)
        self.assertEqual(rules.nearest(4, 1, legs), (1.0, 0, 4.0))
        self.assertEqual(rules.nearest(9, 6, legs), (1.0, 1, 16.0))
        # (11, -1) is equally far from the corner on both legs: the earlier leg counts.
        d, leg, position = rules.nearest(11, -1, legs)
        self.assertEqual((leg, position), (0, 10.0))
        self.assertAlmostEqual(d, math.sqrt(2))

    def test_grid_never_loses_a_place(self):
        rnd = random.Random(6)
        for _ in range(40):
            points = [(rnd.uniform(-15, 15), rnd.uniform(-15, 15)) for _ in range(rnd.randint(2, 5))]
            legs = rules.build_legs(points)
            detour = rnd.choice([0.3, 1.0, 2.5])
            size = rnd.choice([0.1, 0.5, 1.0, 3.0])
            cells = set(rules.route_cells(legs, detour, size))
            for _ in range(400):
                q = (rnd.uniform(-20, 20), rnd.uniform(-20, 20))
                if rules.nearest(*q, legs)[0] <= detour:
                    self.assertIn(rules.cell_of(*q, size), cells)

    def test_grid_skips_far_squares(self):
        legs = rules.build_legs([(0, 0), (20, 0)])
        cells = rules.route_cells(legs, 1.0, 1.0)
        self.assertTrue(all(-2 <= cy <= 1 for _, cy in cells))
        self.assertLess(len(cells), 100)

    def test_labels(self):
        labels = rules.point_labels([0, 1, 2, 3])
        self.assertEqual(labels, ["the start", "stop 1", "stop 2", "the end"])
        self.assertEqual(rules.position_text(3.24, 0, labels), "3.2 km from the start, between the start and stop 1")


class Ranking(unittest.TestCase):
    def test_filters_and_statuses(self):
        found = [
            place(1, 0.2, 10), place(2, 0.5, 8), place(3, 0.1, 6), place(4, 0.3, 2),
            place(5, 0.4, 9, "Shopping"), place(6, 0.6, 50, "Not offered", offered=False),
            place(7, 1.2, 99), place(8, 0.0, 99),
        ]
        rows, counts = rules.rank_places(found, {8}, ["Food & drink"], None, RULES)
        status = {r["place_key"]: r["status"] for r in rows}
        self.assertEqual(status, {1: "suggested", 2: "suggested", 3: "below the top N", 4: "few visitors",
                                  5: "type not chosen", 6: "not offered"})
        self.assertEqual(counts["found"], 6)
        self.assertEqual(counts["after_filters"], 3)
        self.assertEqual([r["place_key"] for r in rows if r["rank"]], [1, 2])

    def test_general_visitor_score(self):
        rows, _ = rules.rank_places([place(1, 0.0, 10), place(2, 0.5, 5)], set(), ["Food & drink"], None, RULES)
        by_key = {r["place_key"]: r for r in rows}
        self.assertEqual(by_key[1]["score"], 1.0)
        self.assertAlmostEqual(by_key[2]["score"], 0.7 * 0.5 + 0.3 * 0.5)
        self.assertEqual(by_key[2]["fit"], 0.0)

    def test_fit_from_type_shares_and_visits_do_not_count(self):
        habits = {"type_checkins": {"Food & drink": 50, "Shopping": 30, "Sights & culture": 20}, "visits": {2: 4}}
        found = [place(1, 0.0, 10, "Shopping"), place(2, 0.0, 10, "Sights & culture")]
        rows, _ = rules.rank_places(found, set(), ["Shopping", "Sights & culture"], habits, RULES)
        by_key = {r["place_key"]: r for r in rows}
        self.assertAlmostEqual(by_key[1]["fit"], 0.6)
        self.assertAlmostEqual(by_key[2]["fit"], 0.4)
        self.assertEqual(by_key[2]["visitor_visits"], 4)
        self.assertAlmostEqual(by_key[1]["type_share"], 0.3)
        self.assertAlmostEqual(by_key[1]["score"], 0.5 + 0.3 * 0.6 + 0.2)
        self.assertEqual(by_key[1]["rank"], 1)

    def test_ties_go_to_visitors_then_checkins_then_place_id(self):
        found = [place(5, 0.0, 10, checkins=20), place(3, 0.0, 10, checkins=30), place(4, 0.0, 10, checkins=30)]
        rules_ = {**RULES, "top_n": 3}
        rows, _ = rules.rank_places(found, set(), ["Food & drink"], None, rules_)
        self.assertEqual([r["place_key"] for r in rows], [3, 4, 5])

    def test_rounding_hides_last_digit_noise(self):
        a = place(1, 0.3 + 1e-12, 10)
        b = place(2, 0.3, 10)
        rows, _ = rules.rank_places([a, b], set(), ["Food & drink"], None, RULES)
        self.assertEqual(rows[0]["score"], rows[1]["score"])
        self.assertEqual([r["place_key"] for r in rows], [1, 2])

    def test_messages(self):
        _, counts = rules.rank_places([], set(), ["Food & drink"], None, RULES)
        self.assertTrue(rules.no_result_message(counts, RULES).startswith("No places within the allowed detour"))
        _, counts = rules.rank_places([place(1, 0.1, 1)], set(), ["Food & drink"], None, RULES)
        self.assertIn("fewer than 3 different visitors", rules.no_result_message(counts, RULES))
        _, counts = rules.rank_places([place(1, 0.1, 9, "Shopping")], set(), ["Food & drink"], None, RULES)
        self.assertIn("try choosing more place types", rules.no_result_message(counts, RULES))
        _, counts = rules.rank_places([place(1, 0.1, 9)], set(), ["Food & drink"], None, RULES)
        self.assertIsNone(rules.no_result_message(counts, RULES))

    def test_favourite_types(self):
        self.assertEqual(rules.favourite_types({"Shopping": 3, "Food & drink": 7, "Nature & outdoors": 3}),
                         ["Food & drink", "Nature & outdoors"])


if __name__ == "__main__":
    unittest.main()

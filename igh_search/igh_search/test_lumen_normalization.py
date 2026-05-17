from frappe.tests.utils import FrappeTestCase

from igh_search.igh_search.lumen_normalization import build_lumen_overlap_filter, parse_lumen


class TestLumenNormalization(FrappeTestCase):
    def test_single(self):
        result = parse_lumen("3680lm")
        self.assertEqual(result["min"], 3680)
        self.assertEqual(result["max"], 3680)
        self.assertEqual(result["unit"], "lm")

    def test_range_unit(self):
        result = parse_lumen("700-800lm/m")
        self.assertEqual(result["min"], 700)
        self.assertEqual(result["max"], 800)
        self.assertEqual(result["unit"], "lm/m")

    def test_lm_per_watt(self):
        result = parse_lumen("65lm/W")
        self.assertEqual(result["min"], 65)
        self.assertEqual(result["unit"], "lm/w")

    def test_slash_multi(self):
        result = parse_lumen("2120lm/2960lm")
        self.assertEqual(result["values"], [2120, 2960])
        self.assertEqual(result["min"], 2120)
        self.assertEqual(result["max"], 2960)

    def test_multiplication(self):
        self.assertEqual(parse_lumen("2x700lm")["min"], 1400.0)
        self.assertEqual(parse_lumen("1204×2lm")["max"], 2408.0)
        self.assertEqual(parse_lumen("2×297lm")["min"], 594.0)

    def test_rgb(self):
        result = parse_lumen("R(91lm), G(281lm), B(50lm)")
        self.assertEqual(result["values"], [91, 281, 50])
        self.assertEqual(result["min"], 50)
        self.assertEqual(result["max"], 281)

    def test_up_down(self):
        result = parse_lumen("U1000lm/D2000lm")
        self.assertEqual(result["values"], [1000, 2000])
        self.assertEqual(result["min"], 1000)
        self.assertEqual(result["max"], 2000)

    def test_comma_values(self):
        result = parse_lumen("800lm,350lm")
        self.assertEqual(result["values"], [800, 350])
        self.assertEqual(result["min"], 350)
        self.assertEqual(result["max"], 800)

    def test_thousands_separator(self):
        result = parse_lumen("3,800lm")
        self.assertEqual(result["min"], 3800)
        self.assertEqual(result["max"], 3800)

    def test_invalid(self):
        self.assertEqual(parse_lumen("NA")["status"], "invalid")
        self.assertEqual(parse_lumen("")["status"], "invalid")
        self.assertEqual(parse_lumen(None)["status"], "invalid")

    def test_overlap_clause(self):
        clause = build_lumen_overlap_filter("lm/w", 60, 75)
        self.assertEqual(clause, '(lumen_unit:="lm/w" && lumen_min:<=75.0 && lumen_max:>=60.0)')

from frappe.tests.utils import FrappeTestCase

from igh_search.igh_search.search_normalization import (
    build_search_keywords,
    build_spec_summary,
    extract_numeric_specs,
    normalize_color_temp,
    normalize_ip_rate,
)


class TestSearchNormalization(FrappeTestCase):
    def test_normalize_color_temp_maps_alias(self):
        self.assertEqual(normalize_color_temp("warm white"), "3000K")

    def test_normalize_ip_rate_extracts_numeric_code(self):
        self.assertEqual(normalize_ip_rate("ip 65"), "IP65")

    def test_extract_numeric_specs_parses_values(self):
        specs = extract_numeric_specs(
            {"power": "50W", "color_temp": "3000K", "ip_rate": "IP44"}
        )
        self.assertEqual(specs["power_value"], 50)
        self.assertEqual(specs["color_temp_kelvin"], 3000)
        self.assertEqual(specs["ip_rating_numeric"], 44)

    def test_build_search_keywords_and_spec_summary_include_specs(self):
        row = {
            "item_code": "SKU-001",
            "item_name": "Warm White Down Light",
            "brand": "LUMI",
            "item_group": "Lighting",
            "category_list": "Downlight",
            "product_type": "Listed",
            "parent_item_code": "PARENT-1",
            "parent_item_name": "Parent Product",
            "power": "10W",
            "color_temp": "3000K",
            "ip_rate": "IP65",
            "mounting": "Ceiling",
            "body_finish": "White",
            "input_voltage": "220V",
            "output_voltage": "24V",
            "output_current": "350mA",
            "lamp_type": "LED",
            "material": "Aluminium",
            "warranty": "2 Years",
        }
        keywords = build_search_keywords(row)
        summary = build_spec_summary(row)

        self.assertIn("SKU-001", keywords)
        self.assertIn("3000K", keywords)
        self.assertIn("IP65", keywords)
        self.assertIn("Power: 10W", summary)


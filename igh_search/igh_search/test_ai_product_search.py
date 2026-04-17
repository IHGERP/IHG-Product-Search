# Copyright (c) 2026, Aerele and Contributors
# See license.txt

from unittest.mock import patch

from frappe.tests.utils import FrappeTestCase

from igh_search.igh_search.ai_product_search import (
    build_default_response,
    parse_page_context,
    parse_product_search_intent,
    sanitize_ai_product_search_response,
)


KNOWN_VALUES = {
    "brand": ["LUMIBRIGHT", "ACME"],
    "category_list": ["Outdoor Lighting", "Office Lighting"],
    "product_type": ["Listed", "Unlisted", "Obsolete"],
    "item_group": ["Outdoor Lighting", "Drivers"],
    "ip_rate": ["IP65", "IP66"],
    "power": ["6W", "12W"],
    "color_temp_": ["3000K", "4000K"],
    "body_finish": ["White", "Black"],
    "input": ["220-240V"],
    "mounting": ["Surface", "Recessed"],
    "output_current": ["350mA"],
    "output_voltage": ["24V"],
    "lamp_type": ["LED"],
    "lumen_output": ["345LM"],
    "beam_angle": ["24D", "36D"],
    "material": ["Aluminium"],
    "warranty_": ["2 Years"],
}


class TestAIProductSearch(FrappeTestCase):
    def test_parse_page_context_accepts_json_string(self):
        context = parse_page_context(
            '{"route":"/list","category":"Outdoor","brand":"LUMIBRIGHT","search":"driver"}'
        )

        self.assertEqual(
            context,
            {
                "route": "/list",
                "category": "Outdoor",
                "brand": "LUMIBRIGHT",
                "search": "driver",
            },
        )

    def test_sanitize_response_keeps_only_supported_values(self):
        ai_response = {
            "query": "waterproof outdoor lights",
            "sort_by": "inventory_value:desc",
            "filters": {
                "item_group": ["Outdoor Lighting", "Imaginary Group"],
                "brand": ["LUMIBRIGHT", 99],
                "ip_rate": ["IP65", "WRONG"],
                "price_range": {"min": 100, "max": 5000},
                "stock_range": {"min": "20", "max": "100"},
                "in_stock": "true",
                "show_promotion": "no",
                "unsupported": ["value"],
            },
            "explanation": "Mapped request safely.",
        }

        sanitized = sanitize_ai_product_search_response(ai_response, KNOWN_VALUES)

        self.assertEqual(sanitized["sort_by"], "inventory_value:desc")
        self.assertEqual(sanitized["filters"]["item_group"], ["Outdoor Lighting"])
        self.assertEqual(sanitized["filters"]["brand"], ["LUMIBRIGHT"])
        self.assertEqual(sanitized["filters"]["ip_rate"], ["IP65"])
        self.assertEqual(sanitized["filters"]["price_range"], {"min": 100.0, "max": 5000.0})
        self.assertEqual(sanitized["filters"]["stock_range"], {"min": 20.0, "max": 100.0})
        self.assertTrue(sanitized["filters"]["in_stock"])
        self.assertFalse(sanitized["filters"]["show_promotion"])

    def test_sanitize_response_resets_invalid_sort_and_ranges(self):
        ai_response = {
            "query": "drivers",
            "sort_by": "totally_invalid",
            "filters": {
                "stock_range": {"min": "bad", "max": None},
                "price_range": {"min": 9000, "max": 1000},
            },
        }

        sanitized = sanitize_ai_product_search_response(ai_response, KNOWN_VALUES)

        self.assertEqual(sanitized["sort_by"], "")
        self.assertEqual(sanitized["filters"]["stock_range"], {"min": 0, "max": 1000000000})
        self.assertEqual(sanitized["filters"]["price_range"], {"min": 1000.0, "max": 9000.0})

    @patch("igh_search.igh_search.ai_product_search.is_ai_product_search_enabled", return_value=False)
    def test_parse_product_search_intent_returns_safe_response_when_feature_disabled(self, _mock_enabled):
        response = parse_product_search_intent("warm white office panel lights")

        self.assertEqual(response["query"], "")
        self.assertEqual(response["sort_by"], "")
        self.assertEqual(response["filters"], build_default_response()["filters"])
        self.assertIn("disabled", response["explanation"].lower())

    @patch("igh_search.igh_search.ai_product_search.get_known_filter_values", return_value=KNOWN_VALUES)
    @patch("igh_search.igh_search.ai_product_search.call_openai_for_product_search")
    @patch("igh_search.igh_search.ai_product_search.is_ai_product_search_enabled", return_value=True)
    def test_parse_product_search_intent_sanitizes_ai_output(self, _mock_enabled, mock_openai, _mock_known_values):
        mock_openai.return_value = (
            '{"query":"drivers","sort_by":"inventory_value:desc","filters":{"brand":["ACME"],"stock_range":{"min":10,"max":100}}}',
            {
                "query": "drivers",
                "sort_by": "inventory_value:desc",
                "filters": {"brand": ["ACME"], "stock_range": {"min": 10, "max": 100}},
            },
        )

        response = parse_product_search_intent(
            "drivers with high stock",
            {"route": "/list", "search": ""},
        )

        self.assertEqual(response["query"], "drivers")
        self.assertEqual(response["sort_by"], "inventory_value:desc")
        self.assertEqual(response["filters"]["brand"], ["ACME"])
        self.assertEqual(response["filters"]["stock_range"], {"min": 10.0, "max": 100.0})
        self.assertTrue(response["explanation"])

    @patch("igh_search.igh_search.ai_product_search.get_known_filter_values", return_value=KNOWN_VALUES)
    @patch("igh_search.igh_search.ai_product_search.call_openai_for_product_search", side_effect=Exception("boom"))
    @patch("igh_search.igh_search.ai_product_search.is_ai_product_search_enabled", return_value=True)
    def test_parse_product_search_intent_falls_back_safely_on_ai_failure(
        self,
        _mock_enabled,
        _mock_openai,
        _mock_known_values,
    ):
        response = parse_product_search_intent("highest value industrial lighting products")

        self.assertEqual(response["query"], "")
        self.assertEqual(response["sort_by"], "")
        self.assertEqual(response["filters"], build_default_response()["filters"])
        self.assertIn("failed", response["explanation"].lower())

# Copyright (c) 2026, Aerele and Contributors
# See license.txt

from unittest.mock import patch

from frappe.tests.utils import FrappeTestCase

from igh_search.igh_search.ai_product_search import (
    build_ai_display_filters,
    build_ai_display_query,
    build_default_filters,
    build_default_response,
    extract_deterministic_intent,
    parse_page_context,
    parse_product_search_intent,
    preprocess_user_message,
    resolve_ai_search_intent,
    sanitize_ai_product_search_response,
)


VOCABULARY = {
    "known_values": {
        "brand": ["LUMIBRIGHT", "ACME"],
        "category_list": ["Outdoor Lighting", "Office Lighting", "Downlight"],
        "product_type": ["Listed", "Unlisted", "Obsolete"],
        "item_group": ["Outdoor Lighting", "Drivers", "Downlight"],
        "ip_rate": ["IP65", "IP66"],
        "power": ["6W", "12W", "50W"],
        "color_temp": ["3000K", "4000K"],
        "body_finish": ["White", "Black"],
        "input_voltage": ["220-240V", "24V"],
        "mounting": ["Surface", "Recessed"],
        "output_current": ["350MA"],
        "output_voltage": ["24V"],
        "lamp_type": ["LED"],
        "beam_angle": ["24D", "36D"],
        "material": ["Aluminium"],
        "warranty": ["2 Years"],
        "variant_of": [],
    },
    "sort_values": [],
    "field_aliases": {},
    "sort_aliases": {},
    "glossary_entries": [],
    "alias_map": {},
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

    def test_sanitize_response_maps_legacy_keys_to_v2_contract(self):
        ai_response = {
            "query": "waterproof lights",
            "sort_by": "creation_on:desc",
            "filters": {
                "item_group": ["Outdoor Lighting", "Imaginary Group"],
                "brand": ["LUMIBRIGHT", 99],
                "ip_rate": ["IP65", "WRONG"],
                "price_range": {"min": 100, "max": 5000},
                "stock_range": {"min": "20", "max": "100"},
                "in_stock": "true",
                "color_temp_": ["3000K"],
                "input": ["24V"],
                "warranty_": ["2 Years"],
            },
            "explanation": "Mapped request safely.",
        }

        sanitized = sanitize_ai_product_search_response(ai_response, VOCABULARY)

        self.assertEqual(sanitized["sort_by"], "creation:desc")
        self.assertEqual(sanitized["filters"]["item_group"], ["Outdoor Lighting"])
        self.assertEqual(sanitized["filters"]["brand"], ["LUMIBRIGHT"])
        self.assertEqual(sanitized["filters"]["ip_rate"], ["IP65"])
        self.assertEqual(sanitized["filters"]["rate_range"], {"min": 100.0, "max": 5000.0})
        self.assertEqual(sanitized["filters"]["stock_range"], {"min": 20.0, "max": 100.0})
        self.assertEqual(sanitized["filters"]["color_temp"], ["3000K"])
        self.assertEqual(sanitized["filters"]["input_voltage"], ["24V"])
        self.assertEqual(sanitized["filters"]["warranty"], ["2 Years"])
        self.assertTrue(sanitized["filters"]["in_stock"])

    def test_extract_deterministic_intent_handles_specs_and_stock_sort(self):
        preprocessed = preprocess_user_message("ip65 3000k downlight stock high to low under 500")
        intent = extract_deterministic_intent(preprocessed, VOCABULARY)

        self.assertEqual(intent["sort_by"], "stock:desc")
        self.assertIn("IP65", intent["filters"]["ip_rate"])
        self.assertIn("3000K", intent["filters"]["color_temp"])
        self.assertEqual(intent["filters"]["rate_range"]["max"], 500.0)
        self.assertEqual(intent["intent_class"], "stock_priority")

    def test_build_ai_display_filters_hides_default_and_derived_ranges(self):
        filters = build_default_filters()
        filters["color_temp"] = ["3000K"]
        filters["ip_rate"] = ["IP65"]
        filters["in_stock"] = True
        filters["color_temp_kelvin_range"] = {"min": 2500, "max": 3500}
        filters["rate_range"] = {"min": 50, "max": 1000000000}

        display_filters = build_ai_display_filters(filters)
        display_pairs = [(item["key"], item["value_display"]) for item in display_filters]

        self.assertIn(("color_temp", "3000K"), display_pairs)
        self.assertIn(("ip_rate", "IP65"), display_pairs)
        self.assertIn(("in_stock", "Yes"), display_pairs)
        self.assertIn(("rate_range", "Above 50 AED"), display_pairs)
        self.assertNotIn(("color_temp_kelvin_range", "2500-3500 K"), display_pairs)

    def test_build_ai_display_query_removes_structured_terms_generally(self):
        filters = build_default_filters()
        filters["color_temp"] = ["3000K"]
        filters["ip_rate"] = ["IP65"]
        filters["in_stock"] = True

        query = build_ai_display_query(
            "spotlights ip65 3000k in stock",
            {"query": "spotlights in stock", "item_code_hint": ""},
            filters,
            "",
        )

        self.assertEqual(query, "spotlights")

    def test_build_ai_display_query_removes_price_and_power_phrases(self):
        filters = build_default_filters()
        filters["color_temp"] = ["4000K"]
        filters["rate_range"] = {"min": 50, "max": 1000000000}

        query = build_ai_display_query(
            "downlight above 50aed 4000k",
            {"query": "downlight above 50aed 4000k", "item_code_hint": ""},
            filters,
            "",
        )
        self.assertEqual(query, "downlight")

        power_filters = build_default_filters()
        power_filters["color_temp"] = ["3000K"]
        power_filters["power"] = ["10W"]
        power_filters["power_value_range"] = {"min": 9, "max": 11}

        query = build_ai_display_query(
            "spotlights below 10w 3000k",
            {"query": "spotlights below 10w 3000k", "item_code_hint": ""},
            power_filters,
            "",
        )
        self.assertEqual(query, "spotlights")

    def test_extract_deterministic_intent_parses_comparative_power_as_range(self):
        preprocessed = preprocess_user_message("downlights 3000k below 20w")
        intent = extract_deterministic_intent(preprocessed, VOCABULARY)

        self.assertEqual(intent["filters"]["power"], [])
        self.assertEqual(intent["filters"]["power_value_range"]["max"], 20.0)
        self.assertEqual(intent["filters"]["power_value_range"]["min"], 0)
        self.assertIn("3000K", intent["filters"]["color_temp"])
        self.assertEqual(intent["query"], "downlights")

    def test_extract_deterministic_intent_keeps_exact_power_without_comparator(self):
        preprocessed = preprocess_user_message("20w downlight")
        intent = extract_deterministic_intent(preprocessed, VOCABULARY)

        self.assertEqual(intent["filters"]["power"], ["20W"])
        self.assertEqual(intent["filters"]["power_value_range"]["min"], 18.0)
        self.assertEqual(intent["filters"]["power_value_range"]["max"], 22.0)

    def test_extract_deterministic_intent_parses_price_comparatives(self):
        preprocessed = preprocess_user_message("strip lights between 20 and 50 aed")
        intent = extract_deterministic_intent(preprocessed, VOCABULARY)
        self.assertEqual(intent["filters"]["rate_range"], {"min": 20.0, "max": 50.0})
        self.assertEqual(intent["query"], "strip lights")

        preprocessed = preprocess_user_message("spotlights above 100aed")
        intent = extract_deterministic_intent(preprocessed, VOCABULARY)
        self.assertEqual(intent["filters"]["rate_range"]["min"], 100.0)
        self.assertEqual(intent["filters"]["rate_range"]["max"], 1000000000)
        self.assertEqual(intent["query"], "spotlights")

    def test_extract_deterministic_intent_parses_stock_comparatives(self):
        preprocessed = preprocess_user_message("drivers stock above 10 qty")
        intent = extract_deterministic_intent(preprocessed, VOCABULARY)
        self.assertEqual(intent["filters"]["stock_range"]["min"], 10.0)
        self.assertEqual(intent["filters"]["stock_range"]["max"], 1000000000)
        self.assertEqual(intent["query"], "drivers")

        preprocessed = preprocess_user_message("drivers stock below 10 qty")
        intent = extract_deterministic_intent(preprocessed, VOCABULARY)
        self.assertEqual(intent["filters"]["stock_range"]["min"], 0)
        self.assertEqual(intent["filters"]["stock_range"]["max"], 10.0)
        self.assertEqual(intent["query"], "drivers")

    def test_extract_deterministic_intent_does_not_misclassify_ambiguous_between(self):
        preprocessed = preprocess_user_message("between 20 and 50 downlight")
        intent = extract_deterministic_intent(preprocessed, VOCABULARY)

        self.assertEqual(
            intent["filters"]["rate_range"],
            build_default_filters()["rate_range"],
        )
        self.assertEqual(
            intent["filters"]["power_value_range"],
            build_default_filters()["power_value_range"],
        )

    def test_build_ai_display_filters_formats_comparative_ranges(self):
        filters = build_default_filters()
        filters["power_value_range"] = {"min": 0, "max": 20}
        filters["stock_range"] = {"min": 10, "max": 1000000000}
        filters["rate_range"] = {"min": 20, "max": 50}

        display_pairs = [(item["key"], item["value_display"]) for item in build_ai_display_filters(filters)]

        self.assertIn(("power_value_range", "Below 20 W"), display_pairs)
        self.assertIn(("stock_range", "Above 10"), display_pairs)
        self.assertIn(("rate_range", "20-50 AED"), display_pairs)

    @patch("igh_search.igh_search.ai_product_search.is_ai_product_search_enabled", return_value=False)
    def test_parse_product_search_intent_returns_safe_response_when_feature_disabled(self, _mock_enabled):
        response = parse_product_search_intent("warm white office panel lights")

        self.assertEqual(response["query"], "")
        self.assertEqual(response["sort_by"], "")
        self.assertEqual(response["filters"], build_default_response()["filters"])
        self.assertIn("disabled", response["explanation"].lower())

    @patch("igh_search.igh_search.ai_product_search.get_ai_search_vocabulary", return_value=VOCABULARY)
    @patch("igh_search.igh_search.ai_product_search.call_ai_for_product_search")
    @patch("igh_search.igh_search.ai_product_search.needs_model_reasoning", return_value=True)
    @patch("igh_search.igh_search.ai_product_search.is_ai_product_search_enabled", return_value=True)
    def test_resolve_ai_search_intent_merges_llm_output(
        self, _mock_enabled, _mock_needs_reasoning, mock_call_ai, _mock_vocab
    ):
        mock_call_ai.return_value = (
            "openai",
            '{"query":"downlight","sort_by":"","filters":{"brand":["ACME"],"mounting":["Surface"]}}',
            {
                "query": "downlight",
                "sort_by": "",
                "filters": {"brand": ["ACME"], "mounting": ["Surface"]},
            },
        )

        response = resolve_ai_search_intent(
            "need surface downlight from acme",
            {"route": "/list", "search": ""},
        )

        self.assertEqual(response["query"], "downlight")
        self.assertEqual(response["filters"]["brand"], ["ACME"])
        self.assertEqual(response["filters"]["mounting"], ["Surface"])
        self.assertTrue(response["resolved_intent"]["llm_used"])
        self.assertEqual(response["resolved_intent"]["provider"], "openai")

    @patch("igh_search.igh_search.ai_product_search.get_ai_search_vocabulary", return_value=VOCABULARY)
    @patch("igh_search.igh_search.ai_product_search.call_ai_for_product_search", side_effect=Exception("boom"))
    @patch("igh_search.igh_search.ai_product_search.is_ai_product_search_enabled", return_value=True)
    def test_resolve_ai_search_intent_falls_back_to_deterministic_on_ai_failure(
        self,
        _mock_enabled,
        _mock_call_ai,
        _mock_vocab,
    ):
        response = resolve_ai_search_intent("DL-100 ip65 3000k", {"route": "/list"})

        self.assertEqual(response["item_code_hint"], "DL-100")
        self.assertIn("IP65", response["filters"]["ip_rate"])
        self.assertFalse(response["resolved_intent"]["llm_used"])
        self.assertEqual(response["resolved_intent"]["provider"], "deterministic")

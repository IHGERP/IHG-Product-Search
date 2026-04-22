from frappe.tests.utils import FrappeTestCase

from igh_search.igh_search.product_search_v2 import (
    build_filter_by,
    calculate_similarity_score,
    get_product_v2_schema,
    rank_search_hits,
    sanitize_sort_by,
)


class TestProductSearchV2(FrappeTestCase):
    def test_product_v2_schema_contains_lifecycle_and_similarity_fields(self):
        field_names = {field["name"] for field in get_product_v2_schema()["fields"]}
        self.assertIn("is_active", field_names)
        self.assertIn("searchable_text", field_names)
        self.assertIn("similarity_signature", field_names)

    def test_build_filter_by_applies_default_active_filter_and_ranges(self):
        filter_by = build_filter_by(
            filters={
                "brand": ["LUMI", "ACME"],
                "stock_range": {"min": 1, "max": 50},
            }
        )
        self.assertIn("is_active:=1", filter_by)
        self.assertIn('brand:=["LUMI","ACME"]', filter_by)
        self.assertIn("stock:>=1.0", filter_by)
        self.assertIn("stock:<=50.0", filter_by)

    def test_rank_search_hits_prefers_exact_sku(self):
        hits = [
            {
                "text_match": 100,
                "document": {
                    "item_code": "DL-200",
                    "item_code_normalized": "DL200",
                    "item_name": "Other product",
                    "in_stock": 1,
                    "priority_score": 10,
                    "popularity_score": 10,
                    "spec_summary": "",
                },
            },
            {
                "text_match": 50,
                "document": {
                    "item_code": "DL-100",
                    "item_code_normalized": "DL100",
                    "item_name": "Target product",
                    "in_stock": 1,
                    "priority_score": 1,
                    "popularity_score": 1,
                    "spec_summary": "",
                },
            },
        ]

        ranked = rank_search_hits(hits, "DL-100")
        self.assertEqual(ranked[0]["document"]["item_code"], "DL-100")

    def test_calculate_similarity_score_rewards_matching_specs(self):
        source = {
            "category_list": "Downlight",
            "product_type": "Listed",
            "power_value": 10,
            "color_temp_kelvin": 3000,
            "ip_rating_numeric": 65,
            "mounting": "Ceiling",
            "lamp_type": "LED",
            "material": "Aluminium",
        }
        candidate = {
            "category_list": "Downlight",
            "product_type": "Listed",
            "power_value": 10.5,
            "color_temp_kelvin": 3000,
            "ip_rating_numeric": 65,
            "mounting": "Ceiling",
            "lamp_type": "LED",
            "material": "Aluminium",
            "in_stock": 1,
        }
        self.assertGreater(calculate_similarity_score(source, candidate), 0)

    def test_sanitize_sort_by_rejects_invalid_fields(self):
        self.assertIn("_text_match:desc", sanitize_sort_by("totally_invalid"))

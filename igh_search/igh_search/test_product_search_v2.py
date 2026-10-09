from unittest.mock import patch

from frappe.tests.utils import FrappeTestCase

from igh_search.igh_search.product_search_v2 import (
    SEARCH_RESULT_FIELDS,
    build_filter_by,
    calculate_similarity_score,
    get_product_v2_schema,
    parse_item_code_list,
    parse_search_filters,
    rank_search_hits,
    resolve_effective_query,
    resolve_sort_by,
    search_products_v2,
    sanitize_sort_by,
)


class TestProductSearchV2(FrappeTestCase):
    def _make_fake_client(self, response_payload=None):
        response_payload = response_payload or {"hits": [], "found": 0, "facet_counts": []}

        class FakeDocuments:
            def __init__(self, payload):
                self.payload = payload
                self.last_search_parameters = None

            def search(self, search_parameters):
                self.last_search_parameters = search_parameters
                if isinstance(self.payload, Exception):
                    raise self.payload
                return dict(self.payload)

        class FakeCollection:
            def __init__(self, payload):
                self.documents = FakeDocuments(payload)

        class FakeCollections(dict):
            def __getitem__(self, key):
                # Production may override the collection name in site_config.
                # Every fake collection shares the same document endpoint so
                # these unit tests stay independent of the site they run on.
                return dict.get(self, key) or dict.__getitem__(self, "product_v2")

        class FakeClient:
            def __init__(self, payload):
                self.collections = FakeCollections({"product_v2": FakeCollection(payload)})

        return FakeClient(response_payload)

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

    def test_build_filter_by_only_applies_commission_when_enabled(self):
        without_commission_filter = build_filter_by(filters={"has_commission": False})
        with_commission_filter = build_filter_by(filters={"has_commission": True})

        self.assertNotIn("has_commission", without_commission_filter)
        self.assertIn("has_commission:=1", with_commission_filter)

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

    def test_sanitize_sort_by_limits_typesense_to_three_fields(self):
        default_sort = sanitize_sort_by("")
        explicit_sort = sanitize_sort_by("rate:asc")
        sku_sort = sanitize_sort_by("", sku_like=True)

        self.assertLessEqual(len(default_sort.split(",")), 3)
        self.assertLessEqual(len(explicit_sort.split(",")), 3)
        self.assertLessEqual(len(sku_sort.split(",")), 3)

    def test_search_result_fields_stay_lean_but_include_frontend_essentials(self):
        self.assertIn("image", SEARCH_RESULT_FIELDS)
        self.assertIn("stock_uom", SEARCH_RESULT_FIELDS)
        self.assertIn("color_temp", SEARCH_RESULT_FIELDS)
        self.assertIn("ip_rate", SEARCH_RESULT_FIELDS)
        self.assertNotIn("description", SEARCH_RESULT_FIELDS)

    def test_resolve_effective_query_uses_item_code_hint_when_query_missing(self):
        resolution = resolve_effective_query(query="", item_code_hint="DL-100")
        self.assertEqual(resolution["effective_query"], "dl-100")
        self.assertTrue(resolution["sku_like"])

    def test_parse_item_code_list_accepts_common_paste_separators(self):
        self.assertEqual(
            parse_item_code_list("FM121011.XGR.09.120, LF1122.G.XGR.30.WP\nDL-100"),
            ["FM121011.XGR.09.120", "LF1122.G.XGR.30.WP", "DL-100"],
        )

    def test_parse_item_code_list_rejects_natural_language(self):
        self.assertEqual(parse_item_code_list("downlights, warm white"), [])

    def test_resolve_sort_by_maps_creation_alias(self):
        sort_resolution = resolve_sort_by("creation:desc")
        self.assertEqual(sort_resolution["aliased_sort"], "creation_ts:desc")
        self.assertEqual(
            sort_resolution["final_sort"],
            "creation_ts:desc,in_stock:desc,business_score:desc",
        )
        self.assertFalse(sort_resolution["should_rerank"])

    def test_resolve_sort_by_accepts_discount_percentage(self):
        sort_resolution = resolve_sort_by("discount_percentage:desc")
        self.assertEqual(
            sort_resolution["final_sort"],
            "discount_percentage:desc,in_stock:desc,business_score:desc",
        )
        self.assertFalse(sort_resolution["should_rerank"])

    def test_resolve_sort_by_supports_strict_sort_for_ai(self):
        sort_resolution = resolve_sort_by("stock:desc", strict_sort=1)
        self.assertEqual(
            sort_resolution["final_sort"],
            "stock:desc,in_stock:desc,business_score:desc",
        )
        self.assertFalse(sort_resolution["should_rerank"])

    def test_resolve_sort_by_falls_back_for_unknown_sort(self):
        sort_resolution = resolve_sort_by("unknown_field:desc")
        self.assertEqual(
            sort_resolution["final_sort"],
            "_text_match:desc,in_stock:desc,business_score:desc",
        )
        self.assertTrue(sort_resolution["should_rerank"])
        self.assertIn("unsupported_sort:fallback_to_relevance", sort_resolution["fallback_reasons"])

    def test_parse_search_filters_raises_clean_error_for_malformed_json(self):
        with self.assertRaises(Exception) as context:
            parse_search_filters('{"brand":')
        self.assertIn("Invalid filters payload", str(context.exception))

    @patch("igh_search.igh_search.product_search_v2.ensure_query_access")
    @patch("igh_search.igh_search.product_search_v2.create_typesense_client")
    @patch("igh_search.igh_search.product_search_v2.rank_search_hits")
    def test_search_products_v2_skips_rerank_for_explicit_sort(
        self, rank_search_hits_mock, create_client_mock, _ensure_query_access_mock
    ):
        fake_client = self._make_fake_client(
            {"hits": [{"document": {"item_code": "ITEM-001"}}], "found": 1, "facet_counts": []}
        )
        create_client_mock.return_value = fake_client

        response = search_products_v2(query="downlight", sort_by="rate:asc")

        rank_search_hits_mock.assert_not_called()
        self.assertEqual(response["query_debug"]["applied_sort"], "rate:asc,in_stock:desc,business_score:desc")

    def test_explicit_stock_sort_uses_field_first_precedence(self):
        sort_resolution = resolve_sort_by("stock:desc")
        self.assertEqual(
            sort_resolution["final_sort"],
            "stock:desc,in_stock:desc,business_score:desc",
        )

    @patch("igh_search.igh_search.product_search_v2.ensure_query_access")
    @patch("igh_search.igh_search.product_search_v2.create_typesense_client")
    @patch("igh_search.igh_search.product_search_v2.rank_search_hits")
    def test_search_products_v2_uses_strict_sort_for_ai_requests(
        self, rank_search_hits_mock, create_client_mock, _ensure_query_access_mock
    ):
        fake_client = self._make_fake_client(
            {"hits": [{"document": {"item_code": "ITEM-001"}}], "found": 1, "facet_counts": []}
        )
        create_client_mock.return_value = fake_client

        response = search_products_v2(query="downlight", sort_by="stock:desc", strict_sort=1)

        rank_search_hits_mock.assert_not_called()
        self.assertEqual(
            response["query_debug"]["applied_sort"],
            "stock:desc,in_stock:desc,business_score:desc",
        )

    @patch("igh_search.igh_search.product_search_v2.ensure_query_access")
    @patch("igh_search.igh_search.product_search_v2.create_typesense_client")
    @patch("igh_search.igh_search.product_search_v2.rank_search_hits", return_value=[])
    def test_search_products_v2_uses_item_code_hint_as_effective_query(
        self, _rank_search_hits_mock, create_client_mock, _ensure_query_access_mock
    ):
        fake_client = self._make_fake_client({"hits": [], "found": 0, "facet_counts": []})
        create_client_mock.return_value = fake_client

        response = search_products_v2(query="", item_code_hint="DL-100")

        self.assertEqual(response["query_debug"]["effective_query"], "dl-100")
        self.assertEqual(
            fake_client.collections["product_v2"].documents.last_search_parameters["q"],
            "dl-100",
        )

    @patch("igh_search.igh_search.product_search_v2.ensure_query_access")
    @patch("igh_search.igh_search.product_search_v2.create_typesense_client")
    @patch("igh_search.igh_search.product_search_v2.rank_search_hits", return_value=[])
    def test_search_products_v2_filters_pasted_item_code_list_exactly(
        self, _rank_search_hits_mock, create_client_mock, _ensure_query_access_mock
    ):
        fake_client = self._make_fake_client({"hits": [], "found": 0, "facet_counts": []})
        create_client_mock.return_value = fake_client

        with patch(
            "igh_search.igh_search.product_search_v2.get_response_cache_ttl",
            return_value=0,
        ):
            response = search_products_v2(
                query="FM121011.XGR.09.120,LF1122.G.XGR.30.WP",
                filters={"brand": ["LUMI"]},
            )

        parameters = fake_client.collections["product_v2"].documents.last_search_parameters
        self.assertEqual(parameters["q"], "*")
        self.assertIn("brand:=[\"LUMI\"]", parameters["filter_by"])
        self.assertIn(
            "item_code_normalized:=[`FM121011XGR09120`,`LF1122GXGR30WP`]",
            parameters["filter_by"],
        )
        self.assertEqual(response["matching_mode"], "item_code_list")

    @patch("igh_search.igh_search.product_search_v2.ensure_query_access")
    @patch("igh_search.igh_search.product_search_v2.create_typesense_client")
    def test_exact_item_search_falls_back_to_erp_when_typesense_times_out(
        self, create_client_mock, _ensure_query_access_mock
    ):
        create_client_mock.return_value = self._make_fake_client(TimeoutError("index timeout"))
        fallback_response = {
            "hits": [{"document": {"item_code": "FM121011.XGR.09.120", "stock": 590}}],
            "found": 1,
            "facet_counts": [],
        }

        with patch(
            "igh_search.igh_search.product_search_v2.get_response_cache_ttl",
            return_value=0,
        ), patch(
            "igh_search.igh_search.product_search_v2.resolve_exact_item_codes_for_fallback",
            return_value=["FM121011.XGR.09.120"],
        ), patch(
            "igh_search.igh_search.product_search_v2.build_erp_exact_item_response",
            return_value=fallback_response,
        ) as fallback_mock, patch(
            "igh_search.igh_search.product_search_v2.frappe.log_error"
        ):
            response = search_products_v2(
                query="FM121011.XGR.09.120",
                filters={
                    "brand": [],
                    "has_commission": False,
                    "rate_range": {"min": "", "max": ""},
                },
            )

        fallback_mock.assert_called_once_with(
            ["FM121011.XGR.09.120"],
            include_inactive=0,
        )
        self.assertEqual(response["found"], 1)
        self.assertEqual(response["matching_mode"], "exact_sku_erp_fallback")

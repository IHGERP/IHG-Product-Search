# Copyright (c) 2025, Aerele and Contributors
# See license.txt

from unittest.mock import patch

from frappe.tests.utils import FrappeTestCase
from igh_search.igh_search.doctype.typesense_settings.typesense_settings import (
    calculate_inventory_value,
    get_affected_item_codes,
    get_item_codes_for_typesense_update,
    get_product_schema_data,
    product_schema,
    update_product_schema_data,
)


class TestTypesenseSettings(FrappeTestCase):
    def test_product_schema_exposes_inventory_value_as_numeric_field(self):
        inventory_value_field = next(
            (
                field
                for field in product_schema["fields"]
                if field["name"] == "inventory_value"
            ),
            None,
        )

        self.assertIsNotNone(inventory_value_field)
        self.assertEqual(inventory_value_field["type"], "float")

    def test_calculate_inventory_value_multiplies_stock_and_rate(self):
        self.assertEqual(calculate_inventory_value(25, 120), 3000)

    def test_calculate_inventory_value_defaults_to_zero_when_inputs_missing(self):
        self.assertEqual(calculate_inventory_value(None, 120), 0)
        self.assertEqual(calculate_inventory_value(25, None), 0)
        self.assertEqual(calculate_inventory_value(None, None), 0)

    def test_get_item_codes_for_item_uses_document_name(self):
        self.assertEqual(
            get_item_codes_for_typesense_update(
                {"doctype": "Item", "name": "ITEM-001", "disabled": 0}
            ),
            ["ITEM-001"],
        )

    def test_get_item_codes_filters_empty_child_rows(self):
        self.assertEqual(
            get_item_codes_for_typesense_update(
                {
                    "doctype": "Stock Entry",
                    "items": [
                        {"item_code": "ITEM-001"},
                        {"item_code": None},
                        {},
                        {"item_code": "ITEM-002"},
                    ],
                }
            ),
            ["ITEM-001", "ITEM-002"],
        )

    @patch(
        "igh_search.igh_search.doctype.typesense_settings.typesense_settings.frappe.enqueue"
    )
    def test_update_product_schema_data_enqueues_after_commit(self, enqueue_mock):
        class DummyDoc:
            def get(self, key):
                data = {"doctype": "Sales Invoice", "items": [{"item_code": "ITEM-001"}]}
                return data.get(key)

            def as_dict(self):
                return {"doctype": "Sales Invoice", "items": [{"item_code": "ITEM-001"}]}

        update_product_schema_data(DummyDoc(), "on_submit")

        enqueue_mock.assert_called_once()
        self.assertTrue(enqueue_mock.call_args.kwargs["enqueue_after_commit"])

    @patch(
        "igh_search.igh_search.doctype.typesense_settings.typesense_settings.fetch_item_base_data"
    )
    @patch(
        "igh_search.igh_search.doctype.typesense_settings.typesense_settings.build_related_item_map"
    )
    def test_get_product_schema_data_returns_both_versions(
        self, related_map_mock, fetch_item_base_data_mock
    ):
        fetch_item_base_data_mock.return_value = [
            {
                "item_code": "ITEM-001",
                "id": "ITEM-001",
                "item_name": "Downlight",
                "item_group": "Lighting",
                "item_group_disabled": 0,
                "disabled": 0,
                "variant_of": "",
                "parent_item_name": "",
                "has_variants": 0,
                "best_selling": 1,
                "hot_product": 0,
                "popular_product": 1,
                "is_bundle_item": 0,
                "item_description": "Compact light",
                "full_description": "Compact light description",
                "stock_uom": "Nos",
                "series": "ITM-.####",
                "image": "/files/item.jpg",
                "is_stock_item": 1,
                "product_type": "Listed",
                "category_list": "Downlight",
                "height": "100",
                "width": "100",
                "depth": 55,
                "custom_moq": "5",
                "range": "Architectural",
                "lamp_qty": "1",
                "safety_class": "Class I",
                "eec": "A+",
                "beam_angle": "36D",
                "lumen_output": "500lm",
                "reflector": "Mirror",
                "mounting": "Ceiling",
                "att_heat_sink": "Aluminium",
                "ip_rate": "IP65",
                "output_signal": "PWM",
                "power_factor": "0.9",
                "working_temp": "-10C to 45C",
                "life_time": "50000h",
                "light_intensity": "Medium",
                "lamp_type": "LED",
                "power": "10W",
                "light_source": "Integrated LED",
                "cri": "90",
                "input": "220V",
                "efficacy": "100 lm/W",
                "operating_frequency": "50Hz",
                "input_signal": "DALI",
                "function": "Dimmable",
                "cut_out": "90mm",
                "dimension": "100x100",
                "material": "Aluminium",
                "body_finish": "White",
                "shade_material": "PC",
                "shade_finish": "Matt",
                "pole_dimension": "",
                "suspended_length": "",
                "warranty_type": "Standard",
                "warranty_": "2 Years",
                "warranty_in_yrs": 2,
                "diffuser": "Opal",
                "custom_esma_certified": 1,
                "output_voltage": "24V",
                "output_current": "350mA",
                "color_temp_": "3000K",
                "primary_material": "Aluminium",
                "secondary_material": "PC",
                "capacity": "",
                "country_of_orgin": "Italy",
                "number_of_pieces": "1",
                "leather_finish": "",
                "fabric_finish": "",
                "primary_color": "White",
                "secondary_color": "Black",
                "remarks": "Popular item",
                "bought_together": ["ACC-001"],
                "similar_range": ["DL-RANGE-001"],
                "related_products": ["DL-ALT-001"],
                "accessories": ["ACC-002"],
                "must_use": ["DRV-001"],
                "website_image_url": "/files/item.jpg",
                "brand": "Lumi",
                "new_arrival": 1,
                "promotion_item": 0,
                "creation": "2026-01-01",
                "creation_raw": "2026-01-01 00:00:00.000000",
                "modified": "2026-01-02 00:00:00.000000",
                "barcode": "123",
                "last_sold": 5,
                "last_brought": 4,
                "rate": 100,
                "offer_rate": 90,
                "discount_percentage": 10,
                "sold_last_30_days": 12,
                "stock": 15,
                "inventory_value": 1500,
                "frequently_bought_together": "",
            }
        ]
        related_map_mock.return_value = {"ITEM-001": {"related": [], "alternative": [], "bought_together": []}}

        payload = get_product_schema_data(version="both")

        self.assertEqual(len(payload["v1"]), 1)
        self.assertEqual(len(payload["v2"]), 1)
        self.assertEqual(payload["v2"][0]["item_code"], "ITEM-001")
        self.assertEqual(payload["v2"][0]["color_temp_kelvin"], 3000)
        self.assertEqual(payload["v2"][0]["image"], "/files/item.jpg")
        self.assertEqual(payload["v2"][0]["stock_uom"], "Nos")
        self.assertEqual(payload["v2"][0]["warranty_type"], "Standard")
        self.assertEqual(payload["v2"][0]["bought_together"], ["ACC-001"])

    def test_get_affected_item_codes_for_item_includes_parent_and_variants(self):
        with patch(
            "igh_search.igh_search.doctype.typesense_settings.typesense_settings.get_variant_codes_for_parent"
        ) as variant_mock:
            variant_mock.side_effect = lambda parent: ["VAR-001"] if parent == "PARENT-001" else []
            item_codes = get_affected_item_codes(
                {
                    "doctype": "Item",
                    "name": "CHILD-001",
                    "variant_of": "PARENT-001",
                    "_previous_variant_of": "PARENT-000",
                }
            )

        self.assertIn("CHILD-001", item_codes)
        self.assertIn("PARENT-001", item_codes)

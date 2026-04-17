# Copyright (c) 2025, Aerele and Contributors
# See license.txt

from frappe.tests.utils import FrappeTestCase
from igh_search.igh_search.doctype.typesense_settings.typesense_settings import (
    calculate_inventory_value,
    product_schema,
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

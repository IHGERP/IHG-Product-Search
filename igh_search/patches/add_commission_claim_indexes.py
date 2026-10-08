"""Composite indexes for claim reservation and Promotion history queries."""

import frappe


def execute():
	if frappe.db.table_exists("Sales Commission Entry"):
		frappe.db.add_index(
			"Sales Commission Entry", ["sales_person", "promotion_claim", "posting_date"]
		)
		frappe.db.add_index("Sales Commission Entry", ["promotion_claim", "sales_person"])

	if frappe.db.table_exists("Promotion"):
		columns = [
			"custom_is_commission_claim",
			"custom_commission_sales_person",
			"custom_commission_claim_month",
		]
		if all(_column_exists("tabPromotion", column) for column in columns):
			frappe.db.add_index("Promotion", columns)


def _column_exists(table, column):
	return bool(frappe.db.sql("SHOW COLUMNS FROM `{0}` LIKE %s".format(table), column))


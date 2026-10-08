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
		if (
			all(_column_exists("tabPromotion", column) for column in columns)
			and not _index_exists("tabPromotion", "commission_claim_lookup")
		):
			# frappe.db.add_index derives its name from every column. These custom
			# field names exceed MariaDB's 64-character identifier limit together.
			frappe.db.sql(
				"""ALTER TABLE `tabPromotion`
				ADD INDEX `commission_claim_lookup`
				(`custom_is_commission_claim`, `custom_commission_sales_person`,
				 `custom_commission_claim_month`)"""
			)


def _column_exists(table, column):
	return bool(frappe.db.sql("SHOW COLUMNS FROM `{0}` LIKE %s".format(table), column))


def _index_exists(table, index_name):
	return bool(frappe.db.sql("SHOW INDEX FROM `{0}` WHERE Key_name = %s".format(table), index_name))

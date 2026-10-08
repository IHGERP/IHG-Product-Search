"""Create Promotion metadata used by monthly commission claims."""

import frappe
from frappe.custom.doctype.custom_field.custom_field import create_custom_fields


def execute():
	if not frappe.db.table_exists("Promotion"):
		return

	create_custom_fields(
		{
			"Promotion": [
				{
					"fieldname": "custom_commission_claim_section",
					"label": "Commission Claim",
					"fieldtype": "Section Break",
					"insert_after": "amount",
					"collapsible": 1,
					"depends_on": "eval:doc.custom_is_commission_claim",
				},
				{
					"fieldname": "custom_is_commission_claim",
					"label": "Is Commission Claim",
					"fieldtype": "Check",
					"insert_after": "custom_commission_claim_section",
					"read_only": 1,
					"hidden": 1,
				},
				{
					"fieldname": "custom_commission_claim_month",
					"label": "Commission Claim Month",
					"fieldtype": "Date",
					"insert_after": "custom_is_commission_claim",
					"read_only": 1,
					"in_list_view": 1,
					"search_index": 1,
				},
				{
					"fieldname": "custom_commission_sales_person",
					"label": "Commission Sales Person",
					"fieldtype": "Link",
					"options": "Sales Person",
					"insert_after": "custom_commission_claim_month",
					"read_only": 1,
					"in_list_view": 1,
					"search_index": 1,
				},
				{
					"fieldname": "custom_commission_claim_column",
					"fieldtype": "Column Break",
					"insert_after": "custom_commission_sales_person",
				},
				{
					"fieldname": "custom_commission_entry_count",
					"label": "Commission Entries",
					"fieldtype": "Int",
					"insert_after": "custom_commission_claim_column",
					"read_only": 1,
				},
				{
					"fieldname": "custom_commission_base_amount",
					"label": "Monthly Commission",
					"fieldtype": "Currency",
					"insert_after": "custom_commission_entry_count",
					"read_only": 1,
				},
				{
					"fieldname": "custom_commission_late_adjustment",
					"label": "Late Adjustments",
					"fieldtype": "Currency",
					"insert_after": "custom_commission_base_amount",
					"read_only": 1,
				},
				{
					"fieldname": "custom_commission_payout",
					"label": "Commission Payout",
					"fieldtype": "Link",
					"options": "Commission Payout",
					"insert_after": "custom_commission_late_adjustment",
					"read_only": 1,
				},
				{
					"fieldname": "custom_commission_claim_key",
					"label": "Commission Claim Key",
					"fieldtype": "Data",
					"insert_after": "custom_commission_payout",
					"read_only": 1,
					"hidden": 1,
					"search_index": 1,
				},
				{
					"fieldname": "custom_commission_idempotency_key",
					"label": "Commission Idempotency Key",
					"fieldtype": "Data",
					"insert_after": "custom_commission_claim_key",
					"read_only": 1,
					"hidden": 1,
				},
			],
		},
		update=True,
	)


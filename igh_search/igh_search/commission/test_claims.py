# Copyright (c) 2026, IHG and contributors
# See license.txt

from datetime import date
from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase

from igh_search.igh_search.commission.claims import (
	_entry_rows,
	_new_promotion,
	before_promotion_save,
)


class DummyPromotion:
	def __init__(self):
		self.values = {}
		self.information = []

	def set(self, fieldname, value):
		self.values[fieldname] = value
		setattr(self, fieldname, value)

	def append(self, fieldname, value):
		assert fieldname == "information"
		self.information.append(value)


class TestCommissionClaims(FrappeTestCase):
	def test_paid_claim_cannot_leave_paid_state(self):
		class PaidClaim:
			workflow_state = "Draft"

			def get(self, fieldname):
				return 1 if fieldname == "custom_is_commission_claim" else None

			def is_new(self):
				return False

			def get_doc_before_save(self):
				return frappe._dict(workflow_state="Paid")

		with self.assertRaises(frappe.ValidationError):
			before_promotion_save(PaidClaim())

	@patch("igh_search.igh_search.commission.claims._claimed_source_months")
	@patch("igh_search.igh_search.commission.claims.frappe.db.sql")
	def test_late_adjustments_do_not_sweep_positive_unclaimed_months(self, sql, claimed_months):
		claimed_months.return_value = {date(2026, 1, 1)}
		base = [frappe._dict(name="MAR", posting_date=date(2026, 3, 2), commission_amount=20)]
		older = [
			frappe._dict(name="CLAIMED-LATE", posting_date=date(2026, 1, 5), commission_amount=5),
			frappe._dict(name="SEPARATE-MONTH", posting_date=date(2026, 2, 5), commission_amount=8),
			frappe._dict(name="NEGATIVE-CARRY", posting_date=date(2025, 12, 5), commission_amount=-3),
		]
		sql.side_effect = [base, older]

		selected, late = _entry_rows("Sales Person A", date(2026, 3, 1), date(2026, 3, 31))

		self.assertEqual([row.name for row in selected], ["MAR"])
		self.assertEqual([row.name for row in late], ["CLAIMED-LATE", "NEGATIVE-CARRY"])

	@patch("igh_search.igh_search.commission.claims.frappe.get_meta")
	@patch("igh_search.igh_search.commission.claims.frappe.new_doc")
	def test_promotion_contains_one_dynamic_link_per_commission_entry(self, new_doc, get_meta):
		doc = DummyPromotion()
		new_doc.return_value = doc
		get_meta.return_value.has_field.return_value = True
		claimant = frappe._dict(
			sales_person="Sales Person A",
			employee=frappe._dict(
				name="EMP-001", employee_name="Sales Person", branch="Dubai",
				company="IHG", payroll_cost_center="Sales - IHG",
			),
		)
		preview = {
			"month": "2026-09", "month_start": date(2026, 9, 1),
			"month_end": date(2026, 9, 30), "total_amount": 25,
			"monthly_amount": 20, "late_adjustment_amount": 5, "entry_count": 2,
			"entries": [
				frappe._dict(name="SCE-1", sales_invoice="SINV-1", item_code="ITEM-1", item_name="One", source="Invoice"),
				frappe._dict(name="SCE-2", sales_invoice="SINV-2", item_code="ITEM-2", item_name="Two", source="Adjustment"),
			],
		}

		promotion = _new_promotion(claimant, preview, "request-1")

		self.assertEqual(promotion.type_of_promotions, "Product Promotion")
		self.assertEqual(promotion.party, "EMP-001")
		self.assertEqual(promotion.amount, 25)
		self.assertEqual(len(promotion.information), 2)
		self.assertEqual(promotion.information[0]["reference_name"], "Sales Commission Entry")
		self.assertEqual(promotion.information[0]["voucher_number"], "SCE-1")

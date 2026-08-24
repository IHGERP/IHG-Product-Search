# Copyright (c) 2026, IHG and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils import flt, now, nowdate


class CommissionPayout(Document):
	def validate(self):
		if self.sales_person and not self.employee:
			self.employee = frappe.db.get_value("Sales Person", self.sales_person, "employee")
		if not self.payout_date:
			self.payout_date = nowdate()

	def on_submit(self):
		self.claim_entries()

	def on_cancel(self):
		self.release_entries()

	# ------------------------------------------------------------------
	# claim / release
	# ------------------------------------------------------------------

	def claim_entries(self):
		"""Stamp every unpaid entry for this salesperson up to the cutoff.

		Why there is no ``from_date`` in this WHERE clause, despite the field
		existing on the form: an entry for an August invoice can be *created* in
		September -- a late credit note, the nightly reconcile catching a missed
		invoice, or a sales-team edit after submit. If the claim filtered on
		``posting_date >= from_date`` that entry would fall into the gap between
		two payouts and would never be paid to anyone. Claiming "everything
		still unpaid, up to the cutoff" is exactly what makes late-arriving
		positives and negatives land in the next payout instead of vanishing.
		Do not add a lower bound here.

		The single UPDATE is also what makes this race-safe: it takes InnoDB row
		locks, so two payouts submitted concurrently for the same salesperson
		serialise, and the second one matches none of the rows the first claimed.
		"""
		frappe.db.sql(
			"""
			UPDATE `tabSales Commission Entry`
			   SET payout = %(payout)s,
			       payout_date = %(payout_date)s,
			       modified = %(now)s,
			       modified_by = %(user)s
			 WHERE sales_person = %(sales_person)s
			   AND posting_date <= %(cutoff_date)s
			   AND (payout IS NULL OR payout = '')
			""",
			{
				"payout": self.name,
				"payout_date": self.payout_date,
				"now": now(),
				"user": frappe.session.user,
				"sales_person": self.sales_person,
				"cutoff_date": self.cutoff_date,
			},
		)

		# Totals must be read back from what was actually claimed. Computing them
		# from a pre-scan would reintroduce the very race the UPDATE avoids.
		claimed = frappe.db.sql(
			"""
			SELECT COUNT(*) AS n,
			       COALESCE(SUM(base_amount), 0) AS base,
			       COALESCE(SUM(commission_amount), 0) AS commission
			  FROM `tabSales Commission Entry`
			 WHERE payout = %s
			""",
			self.name,
			as_dict=True,
		)[0]

		if not claimed.n:
			frappe.throw(
				_("No unpaid commission entries found for {0} up to {1}.").format(
					self.sales_person, self.cutoff_date
				)
			)

		self.db_set(
			{
				"total_entries": claimed.n,
				"total_base_amount": flt(claimed.base),
				"total_commission": flt(claimed.commission),
				"payable_amount": flt(claimed.commission) + flt(self.adjustment_amount),
			},
			update_modified=False,
		)

	def release_entries(self):
		"""Un-stamp on cancel, but never in a way that allows a double payment."""
		later = frappe.db.get_value(
			"Commission Payout",
			{
				"sales_person": self.sales_person,
				"docstatus": 1,
				"cutoff_date": (">=", self.cutoff_date),
				"name": ("!=", self.name),
			},
			"name",
		)
		if later:
			frappe.throw(
				_(
					"Cancel the later payout {0} first. Releasing these entries while it stands "
					"would let them be claimed and paid a second time."
				).format(later)
			)

		frappe.db.sql(
			"""
			UPDATE `tabSales Commission Entry`
			   SET payout = NULL, payout_date = NULL, modified = %(now)s, modified_by = %(user)s
			 WHERE payout = %(payout)s
			""",
			{"payout": self.name, "now": now(), "user": frappe.session.user},
		)

# Copyright (c) 2026, IHG and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.model.document import Document

from igh_search.igh_search.commission.engine import build_dedup_key


class SalesCommissionEntry(Document):
	"""One row per (Sales Invoice Item line) x (Sales Team member).

	Deliberately NOT submittable: the docstatus/amended_from ceremony fights the
	payout-stamping model, where a payout writes to existing rows. Immutability
	comes instead from read-only fields, role permissions, and the guards below.
	"""

	def before_insert(self):
		if not self.dedup_key:
			self.dedup_key = build_dedup_key(self.sales_invoice_item, self.sales_person)

	def on_update(self):
		"""A paid row is history. Nothing may rewrite it.

		The engine reverses paid rows by inserting a new unpaid delta row rather
		than editing the settled one, so this guard should never fire in normal
		operation -- it is here to catch a future code path that forgets.
		"""
		if self.flags.allow_paid_edit:
			return

		before = self.get_doc_before_save()
		if before and before.payout and before.payout == self.payout:
			frappe.throw(
				_("Commission entry {0} has already been paid out under {1} and cannot be edited.").format(
					self.name, before.payout
				)
			)

	def on_trash(self):
		if self.payout:
			frappe.throw(
				_("Commission entry {0} has been paid out under {1} and cannot be deleted.").format(
					self.name, self.payout
				)
			)

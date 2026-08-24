# Copyright (c) 2026, IHG and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils import flt


class ProductBasedCommission(Document):
	"""Per-item commission rate master.

	Named by ``field:item`` on purpose. That makes the item code the primary
	key, which (a) enforces one rate per item at the database level rather than
	via a racy validation, (b) lets the Typesense sync join ``pbc.name =
	it.name`` as a PK ``eq_ref`` probe across ~181k items -- a Link column would
	not be indexed -- and (c) lets Data Import update existing rows by setting
	``name`` to the item code.
	"""

	def validate(self):
		self.commission_percentage = flt(self.commission_percentage)

		if self.commission_percentage < 0 or self.commission_percentage > 100:
			frappe.throw(_("Commission Percentage must be between 0 and 100."))

		if self.item and frappe.db.get_value("Item", self.item, "disabled"):
			frappe.msgprint(
				_("Item {0} is disabled, so it will not earn commission until re-enabled.").format(
					self.item
				),
				indicator="orange",
				alert=True,
			)

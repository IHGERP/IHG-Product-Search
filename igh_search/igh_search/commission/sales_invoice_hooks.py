# Copyright (c) 2026, IHG and contributors
# For license information, please see license.txt

"""Sales Invoice document events.

Every handler swallows its exceptions. That is deliberate: this runs inside
invoice submission on a live system, and a commission bug must never stop
someone from invoicing a customer. The cost is that a failure is silent, which
is exactly what ``engine.reconcile_recent_invoices`` (nightly) exists to catch.
"""

import frappe

from igh_search.igh_search.commission import engine


def _guard(fn, doc, label):
	try:
		fn(doc)
	except Exception:
		frappe.log_error(
			frappe.get_traceback(),
			"Commission {0} failed for {1}".format(label, doc.name)[:140],
		)


def on_submit(doc, method=None):
	_guard(lambda d: engine.sync_invoice_entries(d, source="Invoice"), doc, "on_submit")


def on_cancel(doc, method=None):
	_guard(engine.remove_invoice_entries, doc, "on_cancel")


def on_update_after_submit(doc, method=None):
	"""All four Sales Team fields are allow_on_submit, so attribution can change
	after the invoice is submitted. Re-running the same idempotent sync
	reconciles the ledger to whatever the team now says.
	"""
	_guard(lambda d: engine.sync_invoice_entries(d, source="Invoice"), doc, "on_update_after_submit")

# Copyright (c) 2026, IHG and contributors
# For license information, please see license.txt

"""Row-level visibility for the commission doctypes.

Note the contract: returning "" means NO restriction. A user we cannot resolve
to a Sales Person must therefore get "1=0", never "". Getting that backwards
would show every salesperson's earnings to anyone who can open the list.

These conditions only reach frappe's DatabaseQuery (list views, get_list,
reports). Raw SQL in api.py must apply the same scope itself.
"""

import frappe

from igh_search.igh_search.commission.scope import visible_sales_persons


def _condition(table, user):
	user = user or frappe.session.user

	if user == "Administrator":
		return ""

	allowed = visible_sales_persons(user)
	if allowed is None:
		return ""
	if not allowed:
		return "1=0"

	joined = ", ".join(frappe.db.escape(name) for name in allowed)
	return "`tab{0}`.sales_person in ({1})".format(table, joined)


def commission_entry_query_conditions(user=None, doctype=None):
	return _condition("Sales Commission Entry", user)


def commission_payout_query_conditions(user=None, doctype=None):
	return _condition("Commission Payout", user)


def commission_entry_has_permission(doc, ptype=None, user=None):
	"""Guards single-document reads, which the list condition does not cover."""
	user = user or frappe.session.user

	if user == "Administrator":
		return True

	allowed = visible_sales_persons(user)
	if allowed is None:
		return True
	return doc.get("sales_person") in (allowed or [])


def commission_payout_has_permission(doc, ptype=None, user=None):
	return commission_entry_has_permission(doc, ptype=ptype, user=user)

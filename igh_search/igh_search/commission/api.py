# Copyright (c) 2026, IHG and contributors
# For license information, please see license.txt

"""Whitelisted commission endpoints for the storefront app.

None of these are allow_guest: commission terms and earnings are internal.

These build raw SQL, which frappe's permission_query_conditions does NOT reach,
so every query here applies the scope filter itself via _scope_clause().
"""

import frappe
from frappe import _
from frappe.utils import cint, flt

from igh_search.igh_search.commission.scope import (
	get_linked_sales_person,
	get_user_role_level,
	visible_sales_persons,
)

UNPAID = "(sce.payout IS NULL OR sce.payout = '')"


def _scope_clause(params, requested_sales_person=None):
	"""Return a SQL fragment restricting rows to what the caller may see.

	Fails closed: an unresolvable user gets 1=0, never an empty condition.
	"""
	allowed = visible_sales_persons()

	if requested_sales_person:
		if allowed is not None and requested_sales_person not in (allowed or []):
			frappe.throw(_("Not permitted to view commission for {0}.").format(requested_sales_person))
		params["sales_person"] = requested_sales_person
		return "AND sce.sales_person = %(sales_person)s"

	if allowed is None:
		return ""
	if not allowed:
		return "AND 1=0"

	params["allowed"] = tuple(allowed)
	return "AND sce.sales_person IN %(allowed)s"


def _date_clause(params, from_date, to_date):
	clause = ""
	if from_date:
		params["from_date"] = from_date
		clause += " AND sce.posting_date >= %(from_date)s"
	if to_date:
		params["to_date"] = to_date
		clause += " AND sce.posting_date <= %(to_date)s"
	return clause


@frappe.whitelist()
def get_my_commission_summary(from_date=None, to_date=None, sales_person=None):
	"""Unpaid balance, paid-to-date, and a breakdown for one salesperson."""
	params = {}
	scope = _scope_clause(params, sales_person or get_linked_sales_person())
	dates = _date_clause(params, from_date, to_date)

	totals = frappe.db.sql(
		"""
		SELECT
			COALESCE(SUM(CASE WHEN {unpaid} THEN sce.commission_amount ELSE 0 END), 0) AS unpaid_total,
			SUM(CASE WHEN {unpaid} THEN 1 ELSE 0 END) AS unpaid_count,
			COALESCE(SUM(CASE WHEN NOT {unpaid} THEN sce.commission_amount ELSE 0 END), 0) AS paid_total,
			COUNT(*) AS entry_count
		FROM `tabSales Commission Entry` sce
		WHERE 1=1 {scope} {dates}
		""".format(unpaid=UNPAID, scope=scope, dates=dates),
		params,
		as_dict=True,
	)[0]

	by_item = frappe.db.sql(
		"""
		SELECT sce.item_code, sce.item_name, sce.brand,
		       SUM(sce.qty) AS qty,
		       SUM(sce.base_amount) AS base_amount,
		       SUM(sce.commission_amount) AS commission_amount
		FROM `tabSales Commission Entry` sce
		WHERE {unpaid} {scope} {dates}
		GROUP BY sce.item_code, sce.item_name, sce.brand
		ORDER BY commission_amount DESC
		LIMIT 50
		""".format(unpaid=UNPAID, scope=scope, dates=dates),
		params,
		as_dict=True,
	)

	by_month = frappe.db.sql(
		"""
		-- CONCAT/LPAD rather than DATE_FORMAT here on purpose. frappe.db.sql only
		-- applies percent-substitution when the params dict is non-empty, so a
		-- DATE_FORMAT mask would either leak its escaping through on the
		-- no-filter call path or break substitution on the filtered one.
		SELECT CONCAT(YEAR(sce.posting_date), '-', LPAD(MONTH(sce.posting_date), 2, '0')) AS month,
		       SUM(sce.commission_amount) AS commission_amount
		FROM `tabSales Commission Entry` sce
		WHERE 1=1 {scope} {dates}
		GROUP BY month
		ORDER BY month DESC
		LIMIT 24
		""".format(scope=scope, dates=dates),
		params,
		as_dict=True,
	)

	return {
		"sales_person": sales_person or get_linked_sales_person(),
		"unpaid_total": flt(totals.unpaid_total),
		"unpaid_count": cint(totals.unpaid_count),
		"paid_total": flt(totals.paid_total),
		"entry_count": cint(totals.entry_count),
		"by_item": by_item,
		"by_month": by_month,
	}


@frappe.whitelist()
def get_my_commission_entries(
	status="unpaid", from_date=None, to_date=None, sales_person=None, limit=100, offset=0
):
	"""Line-level entries. status: unpaid | paid | all."""
	params = {"limit": cint(limit) or 100, "offset": cint(offset) or 0}
	scope = _scope_clause(params, sales_person or get_linked_sales_person())
	dates = _date_clause(params, from_date, to_date)

	if status == "unpaid":
		status_clause = "AND {0}".format(UNPAID)
	elif status == "paid":
		status_clause = "AND NOT {0}".format(UNPAID)
	else:
		status_clause = ""

	return frappe.db.sql(
		"""
		SELECT sce.name, sce.posting_date, sce.sales_invoice, sce.item_code, sce.item_name,
		       sce.brand, sce.customer, sce.qty, sce.net_amount, sce.allocated_percentage,
		       sce.commission_rate, sce.base_amount, sce.commission_amount,
		       sce.is_return, sce.source, sce.payout, sce.payout_date
		FROM `tabSales Commission Entry` sce
		WHERE 1=1 {scope} {dates} {status}
		ORDER BY sce.posting_date DESC, sce.name DESC
		LIMIT %(limit)s OFFSET %(offset)s
		""".format(scope=scope, dates=dates, status=status_clause),
		params,
		as_dict=True,
	)


@frappe.whitelist()
def get_payout_history(sales_person=None, limit=50):
	"""Submitted payouts. This is what makes the 'reset' non-destructive:
	the dashboards go back to zero, but every past payout stays openable.
	"""
	params = {"limit": cint(limit) or 50}
	target = sales_person or get_linked_sales_person()

	allowed = visible_sales_persons()
	if target:
		if allowed is not None and target not in (allowed or []):
			frappe.throw(_("Not permitted to view payouts for {0}.").format(target))
		params["sales_person"] = target
		scope = "AND cp.sales_person = %(sales_person)s"
	elif allowed is None:
		scope = ""
	elif not allowed:
		scope = "AND 1=0"
	else:
		params["allowed"] = tuple(allowed)
		scope = "AND cp.sales_person IN %(allowed)s"

	return frappe.db.sql(
		"""
		SELECT cp.name, cp.sales_person, cp.employee_name, cp.company,
		       cp.cutoff_date, cp.payout_date, cp.total_entries,
		       cp.total_base_amount, cp.total_commission,
		       cp.adjustment_amount, cp.payable_amount, cp.payment_reference
		FROM `tabCommission Payout` cp
		WHERE cp.docstatus = 1 {scope}
		ORDER BY cp.cutoff_date DESC, cp.name DESC
		LIMIT %(limit)s
		""".format(scope=scope),
		params,
		as_dict=True,
	)


@frappe.whitelist()
def get_commission_leaderboard(
	status="unpaid", from_date=None, to_date=None, company=None, limit=50
):
	"""The common area: every salesperson's totals, visible to everyone.

	This is a deliberate exception to the row-level scoping applied everywhere
	else -- a shared leaderboard is the point. It exposes TOTALS ONLY; no
	line-level detail, no customer or invoice identifiers. Do not add them here.
	"""
	params = {"limit": cint(limit) or 50}

	if status == "unpaid":
		status_clause = "AND {0}".format(UNPAID)
	elif status == "paid":
		status_clause = "AND NOT {0}".format(UNPAID)
	else:
		status_clause = ""

	company_clause = ""
	if company:
		params["company"] = company
		company_clause = "AND sce.company = %(company)s"

	return frappe.db.sql(
		"""
		SELECT sce.sales_person,
		       sce.branch,
		       COUNT(*) AS entry_count,
		       SUM(sce.base_amount) AS base_amount,
		       SUM(sce.commission_amount) AS commission_amount
		FROM `tabSales Commission Entry` sce
		WHERE 1=1 {status} {company} {dates}
		GROUP BY sce.sales_person, sce.branch
		ORDER BY commission_amount DESC
		LIMIT %(limit)s
		""".format(
			status=status_clause,
			company=company_clause,
			dates=_date_clause(params, from_date, to_date),
		),
		params,
		as_dict=True,
	)


@frappe.whitelist()
def get_commission_scope_info():
	"""What the current user is allowed to see. Lets the UI pick a view."""
	return {
		"role_level": get_user_role_level(),
		"sales_person": get_linked_sales_person(),
		"unrestricted": visible_sales_persons() is None,
	}

# Copyright (c) 2026, IHG and contributors
# For license information, please see license.txt

"""Monthly self-service commission claims backed by Promotion workflow records."""

import hashlib
import re
from calendar import monthrange
from contextlib import contextmanager
from datetime import date

import frappe
from frappe import _
from frappe.utils import cint, flt, getdate, now, nowdate

CLAIM_PROMOTION_TYPE = "Product Promotion"
REJECTED_STATE = "Rejected"
PAID_STATE = "Paid"
WORKFLOW_SUBMIT_ACTION = "Pending Request"

AVAILABLE_SQL = """
	(sce.payout IS NULL OR sce.payout = '')
	AND (sce.promotion_claim IS NULL OR sce.promotion_claim = '')
"""


def claims_enabled():
	return cint(frappe.db.get_single_value(
		"Sales Commission Settings", "enable_salesperson_commission_claims"
	)) == 1


def require_claims_enabled():
	if not claims_enabled():
		frappe.throw(_("Salesperson commission claims are not enabled yet."))


def get_claimant(user=None, require_promotion_fields=True):
	"""Resolve the current user to exactly one Employee and Sales Person."""
	user = user or frappe.session.user
	if not user or user == "Guest":
		frappe.throw(_("Please sign in to claim commission."), frappe.PermissionError)

	employee = frappe.db.get_value(
		"Employee",
		{"user_id": user},
		["name", "employee_name", "branch", "company", "payroll_cost_center"],
		as_dict=True,
	)
	if not employee:
		frappe.throw(_("Your user is not linked to an Employee."))

	sales_person = frappe.db.get_value("Sales Person", {"employee": employee.name}, "name")
	if not sales_person:
		frappe.throw(_("Your Employee is not linked to a Sales Person."))

	missing = []
	if require_promotion_fields:
		if not employee.branch:
			missing.append(_("Branch"))
		if not employee.company:
			missing.append(_("Company"))
		if not employee.payroll_cost_center:
			missing.append(_("Payroll Cost Center"))
	if missing:
		frappe.throw(
			_("Complete these Employee fields before claiming commission: {0}.").format(
				", ".join(missing)
			)
		)

	return frappe._dict(employee=employee, sales_person=sales_person, user=user)


def parse_completed_month(month):
	"""Return (YYYY-MM, first day, last day), rejecting current/future months."""
	month = (month or "").strip()
	if not re.match(r"^\d{4}-(0[1-9]|1[0-2])$", month):
		frappe.throw(_("Month must use YYYY-MM format."))

	year, month_number = [int(part) for part in month.split("-")]
	start = date(year, month_number, 1)
	end = date(year, month_number, monthrange(year, month_number)[1])
	if end >= getdate(nowdate()).replace(day=1):
		frappe.throw(_("Only completed months can be claimed."))
	return month, start, end


def _active_claim(sales_person, month_start, idempotency_key=None):
	filters = {
		"custom_is_commission_claim": 1,
		"custom_commission_sales_person": sales_person,
		"custom_commission_claim_month": month_start,
		"workflow_state": ("!=", REJECTED_STATE),
	}
	if idempotency_key:
		filters["custom_commission_idempotency_key"] = idempotency_key
	return frappe.db.get_value(
		"Promotion",
		filters,
		["name", "workflow_state", "amount", "custom_commission_entry_count"],
		as_dict=True,
	)


def _claimed_source_months(sales_person):
	months = frappe.get_all(
		"Promotion",
		filters={
			"custom_is_commission_claim": 1,
			"custom_commission_sales_person": sales_person,
			"workflow_state": ("!=", REJECTED_STATE),
		},
		pluck="custom_commission_claim_month",
	)
	return {getdate(value) for value in months if value}


def _entry_rows(sales_person, month_start, month_end):
	base = frappe.db.sql(
		"""
		SELECT sce.name, sce.posting_date, sce.sales_invoice, sce.item_code,
		       sce.item_name, sce.source, sce.base_amount, sce.commission_amount
		  FROM `tabSales Commission Entry` sce
		 WHERE sce.sales_person = %(sales_person)s
		   AND sce.posting_date BETWEEN %(month_start)s AND %(month_end)s
		   AND {available}
		 ORDER BY sce.posting_date, sce.name
		""".format(available=AVAILABLE_SQL),
		{"sales_person": sales_person, "month_start": month_start, "month_end": month_end},
		as_dict=True,
	)

	claimed_months = _claimed_source_months(sales_person)
	older = frappe.db.sql(
		"""
		SELECT sce.name, sce.posting_date, sce.sales_invoice, sce.item_code,
		       sce.item_name, sce.source, sce.base_amount, sce.commission_amount
		  FROM `tabSales Commission Entry` sce
		 WHERE sce.sales_person = %(sales_person)s
		   AND sce.posting_date < %(month_start)s
		   AND {available}
		 ORDER BY sce.posting_date, sce.name
		""".format(available=AVAILABLE_SQL),
		{"sales_person": sales_person, "month_start": month_start},
		as_dict=True,
	)
	# A source month already claimed can receive later reconciliation/return
	# rows. A source month whose whole available balance is non-positive cannot
	# be submitted by itself, so it also carries into the next positive claim.
	# Positive unclaimed months remain separate monthly claims.
	older_totals = {}
	for row in older:
		source_month = getdate(row.posting_date).replace(day=1)
		older_totals[source_month] = flt(
			older_totals.get(source_month, 0) + flt(row.commission_amount), 2
		)
	late = [
		row for row in older
		if getdate(row.posting_date).replace(day=1) in claimed_months
		or older_totals[getdate(row.posting_date).replace(day=1)] <= 0
	]

	return base, late


def build_claim_preview(month, claimant=None):
	claimant = claimant or get_claimant()
	month, month_start, month_end = parse_completed_month(month)
	base, late = _entry_rows(claimant.sales_person, month_start, month_end)
	base_amount = flt(sum(flt(row.commission_amount) for row in base), 2)
	late_amount = flt(sum(flt(row.commission_amount) for row in late), 2)
	total = flt(base_amount + late_amount, 2)
	return {
		"month": month,
		"month_start": month_start,
		"month_end": month_end,
		"monthly_amount": base_amount,
		"late_adjustment_amount": late_amount,
		"total_amount": total,
		"entry_count": len(base) + len(late),
		"monthly_entry_count": len(base),
		"late_adjustment_count": len(late),
		"entries": base + late,
	}


def claimable_months(claimant=None):
	claimant = claimant or get_claimant()
	first_of_this_month = getdate(nowdate()).replace(day=1)
	rows = frappe.db.sql(
		"""
		SELECT YEAR(sce.posting_date) AS year_number,
		       MONTH(sce.posting_date) AS month_number,
		       COUNT(*) AS entry_count,
		       COALESCE(SUM(sce.commission_amount), 0) AS monthly_amount
		  FROM `tabSales Commission Entry` sce
		 WHERE sce.sales_person = %(sales_person)s
		   AND sce.posting_date < %(first_of_this_month)s
		   AND {available}
		 GROUP BY YEAR(sce.posting_date), MONTH(sce.posting_date)
		 ORDER BY year_number DESC, month_number DESC
		""".format(available=AVAILABLE_SQL),
		{"sales_person": claimant.sales_person, "first_of_this_month": first_of_this_month},
		as_dict=True,
	)

	result = []
	for row in rows:
		month = "{0:04d}-{1:02d}".format(cint(row.year_number), cint(row.month_number))
		_, month_start, _ = parse_completed_month(month)
		if _active_claim(claimant.sales_person, month_start):
			continue
		preview = build_claim_preview(month, claimant=claimant)
		if preview["total_amount"] <= 0:
			continue
		result.append({key: preview[key] for key in (
			"month", "monthly_amount", "late_adjustment_amount", "total_amount",
			"entry_count", "monthly_entry_count", "late_adjustment_count",
		)})
	return result


@contextmanager
def claim_lock(sales_person, month):
	"""Serialize claims for one salesperson/month with a MariaDB advisory lock."""
	digest = hashlib.sha256("{0}:{1}".format(sales_person, month).encode("utf-8")).hexdigest()[:40]
	lock_name = "igh-commission-{0}".format(digest)
	acquired = frappe.db.sql("SELECT GET_LOCK(%s, 10)", lock_name)[0][0]
	if not cint(acquired):
		frappe.throw(_("Another commission claim is being processed. Please try again."))
	try:
		yield
	finally:
		frappe.db.sql("SELECT RELEASE_LOCK(%s)", lock_name)


def create_claim(month, idempotency_key=None):
	require_claims_enabled()
	claimant = get_claimant()
	month, month_start, month_end = parse_completed_month(month)
	idempotency_key = (idempotency_key or "").strip()[:140]

	with claim_lock(claimant.sales_person, month):
		existing = _active_claim(claimant.sales_person, month_start)
		if existing:
			return _claim_result(existing, existing=True)

		preview = build_claim_preview(month, claimant=claimant)
		if not preview["entry_count"]:
			frappe.throw(_("There are no available commission entries for {0}.").format(month))
		if preview["total_amount"] <= 0:
			frappe.throw(_(
				"This claim has a non-positive balance. It will carry forward to a future positive claim."
			))

		promotion = _new_promotion(claimant, preview, idempotency_key)
		promotion.insert(ignore_permissions=True)
		entry_names = [row.name for row in preview["entries"]]
		_reserved = _reserve_entries(entry_names, promotion.name, claimant.sales_person)
		if _reserved != len(entry_names):
			frappe.throw(_("Commission entries changed while the claim was being created. Please retry."))

		from frappe.model.workflow import apply_workflow

		promotion = apply_workflow(promotion, WORKFLOW_SUBMIT_ACTION)
		return _claim_result(promotion, existing=False)


def _new_promotion(claimant, preview, idempotency_key):
	employee = claimant.employee
	doc = frappe.new_doc("Promotion")
	values = {
		"type_of_promotions": CLAIM_PROMOTION_TYPE,
		"employee_name": employee.name,
		"name1": employee.employee_name,
		"claim_party": "Employee",
		"party": employee.name,
		"party_name": employee.employee_name,
		"branch": employee.branch,
		"reason_for_claim": _("Monthly sales commission claim for {0}").format(preview["month"]),
		"amount": preview["total_amount"],
		"custom_company": employee.company,
		"custom_cost_center": employee.payroll_cost_center,
		"custom_from_date": preview["month_start"],
		"custom_to_date": preview["month_end"],
		"custom_total_amount": preview["total_amount"],
		"custom_is_commission_claim": 1,
		"custom_commission_claim_month": preview["month_start"],
		"custom_commission_sales_person": claimant.sales_person,
		"custom_commission_claim_key": "{0}:{1}:{2}".format(
			claimant.sales_person, preview["month"], frappe.generate_hash(length=10)
		),
		"custom_commission_idempotency_key": idempotency_key,
		"custom_commission_entry_count": preview["entry_count"],
		"custom_commission_base_amount": preview["monthly_amount"],
		"custom_commission_late_adjustment": preview["late_adjustment_amount"],
	}
	meta = frappe.get_meta("Promotion")
	for fieldname, value in values.items():
		if meta.has_field(fieldname):
			doc.set(fieldname, value)

	for row in preview["entries"]:
		doc.append("information", {
			"reference_name": "Sales Commission Entry",
			"voucher_number": row.name,
			"reason": _("{0}: {1} ({2})").format(
				row.sales_invoice, row.item_code or row.item_name or row.name, row.source
			),
		})
	return doc


def _reserve_entries(entry_names, promotion, sales_person):
	if not entry_names:
		return 0
	placeholders = ", ".join(["%s"] * len(entry_names))
	params = [promotion, now(), frappe.session.user, sales_person] + entry_names
	frappe.db.sql(
		"""
		UPDATE `tabSales Commission Entry`
		   SET promotion_claim = %s, modified = %s, modified_by = %s
		 WHERE sales_person = %s
		   AND (payout IS NULL OR payout = '')
		   AND (promotion_claim IS NULL OR promotion_claim = '')
		   AND name IN ({0})
		""".format(placeholders),
		tuple(params),
	)
	return cint(frappe.db.sql("SELECT ROW_COUNT()")[0][0])


def _claim_result(doc, existing=False):
	return {
		"name": doc.name,
		"workflow_state": doc.workflow_state,
		"amount": flt(doc.amount),
		"entry_count": cint(doc.get("custom_commission_entry_count")),
		"existing": bool(existing),
	}


def before_promotion_save(doc, method=None):
	"""Paid automated claims are terminal even if the workflow offers a reverse action."""
	if not cint(doc.get("custom_is_commission_claim")) or doc.is_new():
		return
	previous = doc.get_doc_before_save()
	if previous and previous.workflow_state == PAID_STATE and doc.workflow_state != PAID_STATE:
		frappe.throw(_("A paid commission claim cannot leave the Paid state."))


def on_promotion_update(doc, method=None):
	if not cint(doc.get("custom_is_commission_claim")):
		return
	previous = doc.get_doc_before_save()
	previous_state = previous.workflow_state if previous else None
	if doc.workflow_state == REJECTED_STATE and previous_state != REJECTED_STATE:
		release_claim(doc)
	elif doc.workflow_state == PAID_STATE and previous_state != PAID_STATE:
		finalize_paid_claim(doc)


def release_claim(doc):
	if doc.get("custom_commission_payout"):
		frappe.throw(_("A paid commission claim cannot be released."))
	frappe.db.sql(
		"""
		UPDATE `tabSales Commission Entry`
		   SET promotion_claim = NULL, modified = %(now)s, modified_by = %(user)s
		 WHERE promotion_claim = %(promotion)s
		   AND (payout IS NULL OR payout = '')
		""",
		{"promotion": doc.name, "now": now(), "user": frappe.session.user},
	)


def finalize_paid_claim(doc):
	"""Create one internal payout; CommissionPayout claims only this Promotion's rows."""
	existing = doc.get("custom_commission_payout") or frappe.db.get_value(
		"Commission Payout", {"promotion_claim": doc.name, "docstatus": 1}, "name"
	)
	if existing:
		if not doc.get("custom_commission_payout"):
			doc.db_set("custom_commission_payout", existing, update_modified=False)
		return existing

	rows = frappe.db.sql(
		"""
		SELECT COUNT(*) AS entry_count,
		       COALESCE(SUM(commission_amount), 0) AS total_amount
		  FROM `tabSales Commission Entry`
		 WHERE promotion_claim = %s AND (payout IS NULL OR payout = '')
		""",
		doc.name,
		as_dict=True,
	)[0]
	if cint(rows.entry_count) != cint(doc.get("custom_commission_entry_count")):
		frappe.throw(_("Reserved commission entries no longer match this claim."))
	if abs(flt(rows.total_amount, 2) - flt(doc.amount, 2)) >= 0.005:
		frappe.throw(_("Reserved commission amount no longer matches this claim."))

	payout = frappe.get_doc({
		"doctype": "Commission Payout",
		"sales_person": doc.custom_commission_sales_person,
		"employee": doc.employee_name,
		"company": doc.custom_company,
		"from_date": doc.custom_from_date,
		"cutoff_date": doc.custom_to_date,
		"payout_date": nowdate(),
		"payment_reference": doc.name,
		"remarks": _("Generated when commission claim {0} reached Paid.").format(doc.name),
		"promotion_claim": doc.name,
	})
	payout.flags.ignore_permissions = True
	payout.insert(ignore_permissions=True)
	payout.submit()
	doc.db_set("custom_commission_payout", payout.name, update_modified=False)
	return payout.name

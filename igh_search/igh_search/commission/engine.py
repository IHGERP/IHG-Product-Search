# Copyright (c) 2026, IHG and contributors
# For license information, please see license.txt

"""Commission ledger engine.

Everything that writes to `Sales Commission Entry` goes through
``sync_invoice_entries``. That single idempotent function is what makes
"re-running must not double-log" and "the sales team changed after submit" the
same problem, solved once: it diffs the ledger against what the invoice
currently justifies and reconciles the difference.
"""

import hashlib

import frappe
from frappe.utils import cint, flt, getdate, nowdate

ROUNDING = 2

# Fields compared when deciding whether an existing entry still matches the
# invoice. Identity fields (invoice, line, salesperson) are in the dedup key.
_COMPARED_FIELDS = ("commission_rate", "allocated_percentage", "base_amount", "commission_amount")


def build_dedup_key(sales_invoice_item, sales_person):
	"""Stable identity for one ledger line.

	md5 rather than the raw string because Sales Person is autonamed
	``field:sales_person_name``, so names are free text and can be long; the
	digest always fits the column and can carry a UNIQUE index.
	"""
	raw = "{0}::{1}".format(sales_invoice_item or "", sales_person or "")
	return hashlib.md5(raw.encode("utf-8")).hexdigest()


# ----------------------------------------------------------------------
# settings
# ----------------------------------------------------------------------


def get_settings():
	return frappe.get_cached_doc("Sales Commission Settings")


def _excluded_companies(settings):
	raw = settings.get("excluded_companies") or ""
	return {line.strip().lower() for line in raw.splitlines() if line.strip()}


def is_invoice_in_scope(si, settings=None):
	"""Whether this invoice should produce commission at all."""
	settings = settings or get_settings()

	if not cint(settings.enabled):
		return False

	if settings.go_live_date and getdate(si.posting_date) < getdate(settings.go_live_date):
		return False

	if cint(settings.exclude_internal_customers) and cint(si.get("is_internal_customer")):
		return False

	if si.get("company") and si.company.strip().lower() in _excluded_companies(settings):
		return False

	return True


# ----------------------------------------------------------------------
# rate lookup
# ----------------------------------------------------------------------


def get_rate_map(item_codes):
	"""{item_code: commission_percentage} for enabled rates only.

	One query per invoice, keyed on the primary key (Product Based Commission
	is autonamed ``field:item``).
	"""
	item_codes = [c for c in set(item_codes or []) if c]
	if not item_codes:
		return {}

	rows = frappe.get_all(
		"Product Based Commission",
		filters={"name": ("in", item_codes), "enabled": 1},
		fields=["name", "commission_percentage"],
	)
	return {r.name: flt(r.commission_percentage) for r in rows if flt(r.commission_percentage)}


def _sales_person_meta(sales_person):
	"""(employee, branch) for a Sales Person.

	Deliberately not memoised in a module-level dict: workers are long-lived, so
	such a cache would keep serving a stale branch after HR moves someone. The
	call volume here is 1-2 per invoice.
	"""
	employee = frappe.db.get_value("Sales Person", sales_person, "employee")
	branch = frappe.db.get_value("Employee", employee, "branch") if employee else None
	return employee, branch


# ----------------------------------------------------------------------
# building desired state
# ----------------------------------------------------------------------


def _team_rows(si):
	return [
		row
		for row in (si.get("sales_team") or [])
		if row.sales_person and flt(row.allocated_percentage)
	]


def _base_entry(si, line, sales_person, allocated_percentage, rate, source, remarks=None):
	employee, branch = _sales_person_meta(sales_person)
	base_amount = flt(flt(line.net_amount) * flt(allocated_percentage) / 100.0, ROUNDING)
	return {
		"doctype": "Sales Commission Entry",
		"sales_person": sales_person,
		"employee": employee,
		"branch": branch,
		"company": si.company,
		"posting_date": si.posting_date,
		"customer": si.customer,
		"sales_invoice": si.name,
		"sales_invoice_item": line.name,
		"item_code": line.item_code,
		"item_name": line.get("item_name"),
		"brand": line.get("brand"),
		"is_return": cint(si.get("is_return")),
		"return_against": si.get("return_against"),
		"qty": flt(line.qty),
		"net_amount": flt(line.net_amount),
		"allocated_percentage": flt(allocated_percentage),
		"commission_rate": flt(rate),
		"base_amount": base_amount,
		"commission_amount": flt(base_amount * flt(rate) / 100.0, ROUNDING),
		"source": source,
		"remarks": remarks,
		"dedup_key": build_dedup_key(line.name, sales_person),
	}


def _build_sale_entries(si, source):
	desired = {}
	team = _team_rows(si)
	if not team:
		return desired

	rates = get_rate_map([d.item_code for d in si.items])

	for line in si.items:
		rate = rates.get(line.item_code)
		if not rate:
			continue  # unlisted items earn nothing
		for member in team:
			row = _base_entry(si, line, member.sales_person, member.allocated_percentage, rate, source)
			desired[_group_key(line.name, member.sales_person)] = row

	return desired


def _already_clawed_back(original_row, sales_person):
	"""Commission already reversed against one original line, for one person.

	Scoped per salesperson rather than per line: each person earned their own
	share, so each has their own ceiling on what can be taken back.
	"""
	value = frappe.db.sql(
		"""
		SELECT COALESCE(SUM(commission_amount), 0)
		  FROM `tabSales Commission Entry`
		 WHERE original_sales_invoice_item = %s
		   AND sales_person = %s
		   AND is_return = 1
		""",
		(original_row, sales_person),
	)
	return flt(value[0][0]) if value else 0.0


def _originals_for(original_row):
	return frappe.get_all(
		"Sales Commission Entry",
		filters={"sales_invoice_item": original_row, "is_return": 0},
		fields=["sales_person", "commission_rate", "allocated_percentage", "commission_amount"],
	)


def _build_return_entries(si):
	"""Mirror the original entries rather than re-deriving from the credit note.

	ERPNext's Make Return stamps ``Sales Invoice Item.sales_invoice_item`` with
	the source row name (sales_and_purchase_return.py). Using it means a credit
	note whose sales team differs from the original invoice still claws back
	from the people who actually earned the commission, at the rate that applied
	when they earned it.
	"""
	desired = {}
	rates = None

	for line in si.items:
		original_row = line.get("sales_invoice_item")
		originals = _originals_for(original_row) if original_row else []

		if originals:
			# line.net_amount is already negative and already scaled to the
			# returned qty, so partial returns need no ratio maths.
			for original in originals:
				row = _base_entry(
					si,
					line,
					original.sales_person,
					original.allocated_percentage,
					original.commission_rate,
					"Return",
				)
				row["original_sales_invoice_item"] = original_row
				row["commission_amount"] = _cap_clawback(
					row["commission_amount"],
					flt(original.commission_amount),
					_already_clawed_back(original_row, original.sales_person),
				)
				desired[_group_key(line.name, original.sales_person)] = row
			continue

		# No original: a manual credit note, or the sale predates go-live.
		# Fall back to this document's own team and today's rate, and say so.
		if rates is None:
			rates = get_rate_map([d.item_code for d in si.items])
		rate = rates.get(line.item_code)
		if not rate:
			continue
		for member in _team_rows(si):
			row = _base_entry(
				si,
				line,
				member.sales_person,
				member.allocated_percentage,
				rate,
				"Return",
				remarks="No original commission entry found; used the credit note's own "
				"sales team and the item's current rate.",
			)
			desired[_group_key(line.name, member.sales_person)] = row

	return desired


def _cap_clawback(proposed, earned, already_clawed):
	"""Never claw back more than this person earned on that original line.

	``already_clawed`` is negative (return rows carry negative commission), so
	adding it to ``earned`` leaves the amount still available to reverse. Guards
	the multi-step partial-return case, where several credit notes against one
	invoice line could otherwise over-reverse in aggregate.
	"""
	remaining = flt(earned) + flt(already_clawed)
	if remaining <= 0:
		return 0.0
	return -min(abs(flt(proposed)), abs(flt(remaining, ROUNDING)))


def _build_entries(si, source):
	if cint(si.get("is_return")):
		return _build_return_entries(si)
	return _build_sale_entries(si, source)


# ----------------------------------------------------------------------
# reconciliation
# ----------------------------------------------------------------------


def _group_key(sales_invoice_item, sales_person):
	"""Identity of one logical (line x salesperson) position in the ledger.

	A position can be represented by several rows: the original entry plus any
	later Adjustment/Reversal rows. Reconciliation works on the SUM of those,
	which is what keeps it idempotent -- see sync_invoice_entries.
	"""
	return (sales_invoice_item or "", sales_person or "")


def _insert(row):
	doc = frappe.get_doc(row)
	doc.flags.ignore_permissions = True
	doc.insert(ignore_permissions=True)
	return doc


def _correction_row(template, commission_delta, base_delta, source, remarks):
	"""A new unpaid row carrying only the difference.

	Settled rows are never edited; a correction is always additive. The dedup
	key is randomised because a position may legitimately need several
	corrections over time, and they must not collide on the unique index.
	"""
	row = dict(template)
	row["doctype"] = "Sales Commission Entry"
	row.pop("name", None)
	row["commission_amount"] = flt(commission_delta, ROUNDING)
	row["base_amount"] = flt(base_delta, ROUNDING)
	row["source"] = source
	row["remarks"] = remarks
	row["payout"] = None
	row["payout_date"] = None
	row["dedup_key"] = build_dedup_key(
		"{0}::{1}::{2}".format(
			row.get("sales_invoice_item"), source.lower(), frappe.generate_hash(length=10)
		),
		row.get("sales_person"),
	)
	return _insert(row)


def _fetch_entry(name):
	return frappe.db.get_value(
		"Sales Commission Entry",
		name,
		[
			"sales_person", "employee", "branch", "company", "posting_date", "customer",
			"sales_invoice", "sales_invoice_item", "original_sales_invoice_item",
			"item_code", "item_name", "brand", "qty", "net_amount",
			"allocated_percentage", "commission_rate", "base_amount", "commission_amount",
			"is_return",
		],
		as_dict=True,
	)


def sync_invoice_entries(si, source="Invoice"):
	"""Make the ledger match the invoice. Safe to call any number of times.

	Reconciles on the EFFECTIVE TOTAL per (invoice line x salesperson), not on
	individual rows. That is what makes repeat runs -- the nightly reconcile,
	a re-submit, a sales-team edit -- converge instead of stacking corrections:
	once a correction has been written the totals already agree, so the next run
	finds nothing to do.
	"""
	settings = get_settings()
	if not is_invoice_in_scope(si, settings):
		return

	desired = _build_entries(si, source)

	groups = {}
	for row in frappe.get_all(
		"Sales Commission Entry",
		filters={"sales_invoice": si.name},
		fields=["name", "sales_invoice_item", "sales_person", "payout", "source",
				"commission_amount", "base_amount"],
	):
		groups.setdefault(_group_key(row.sales_invoice_item, row.sales_person), []).append(row)

	for key, want in desired.items():
		group = groups.pop(key, [])
		effective = sum(flt(r.commission_amount) for r in group)
		effective_base = sum(flt(r.base_amount) for r in group)
		target = flt(want["commission_amount"])

		if _same_money(effective, target) and _same_money(effective_base, want["base_amount"]):
			continue

		base_row = next((r for r in group if r.source in ("Invoice", "Return")), None)

		if base_row is None:
			_insert(want)
		elif len(group) == 1 and not base_row.payout:
			# Clean case: a single unpaid row. Correct it in place so the ledger
			# stays one row per position rather than accreting corrections.
			frappe.db.set_value(
				"Sales Commission Entry",
				base_row.name,
				{field: want[field] for field in _COMPARED_FIELDS},
			)
		else:
			_correction_row(
				want,
				target - effective,
				flt(want["base_amount"]) - effective_base,
				"Adjustment",
				"Invoice changed after this commission was settled.",
			)

	# Whatever is left is no longer justified by the invoice.
	for key, group in groups.items():
		_retire_group(group, "Line no longer present on the invoice.")


def _same_money(left, right):
	return abs(flt(left) - flt(right)) < 0.005


def _retire_group(group, reason):
	"""Take a position back to zero.

	Unpaid rows are simply removed. If anything in the position has been paid,
	the settled rows stay untouched and a negative unpaid row cancels them out,
	so the reduction lands in the next payout instead of rewriting history.
	"""
	if not any(r.payout for r in group):
		for row in group:
			frappe.delete_doc("Sales Commission Entry", row.name, force=1, ignore_permissions=True)
		return

	effective = sum(flt(r.commission_amount) for r in group)
	effective_base = sum(flt(r.base_amount) for r in group)
	if _same_money(effective, 0) and _same_money(effective_base, 0):
		return

	template = _fetch_entry(group[0].name)
	if not template:
		return
	template["original_sales_invoice_item"] = template.get("sales_invoice_item")
	template["is_return"] = 1
	_correction_row(template, -effective, -effective_base, "Reversal", reason)


def remove_invoice_entries(si):
	"""Invoice cancelled: drop unpaid rows, reverse settled ones.

	Deliberately never blocks the cancellation. Refusing to cancel an invoice
	whose commission has been paid would hand finance a hard wall on a live
	system; a negative unpaid row that reduces the next payout is the correct
	remedy.
	"""
	groups = {}
	for row in frappe.get_all(
		"Sales Commission Entry",
		filters={"sales_invoice": si.name},
		fields=["name", "sales_invoice_item", "sales_person", "payout", "source",
				"commission_amount", "base_amount"],
	):
		groups.setdefault(_group_key(row.sales_invoice_item, row.sales_person), []).append(row)

	for group in groups.values():
		_retire_group(group, "Sales Invoice cancelled.")


# ----------------------------------------------------------------------
# nightly safety net
# ----------------------------------------------------------------------


def reconcile_recent_invoices():
	"""Re-run the same idempotent sync over recent invoices.

	The document hooks swallow their exceptions so a commission bug can never
	block invoicing. That trades a hard failure for a silent one, so this is the
	net that catches it -- along with sales-team edits written via
	frappe.db.set_value, which fire no document event at all.
	"""
	settings = get_settings()
	if not cint(settings.enabled):
		return

	lookback = cint(settings.reconcile_lookback_days) or 7
	from_date = frappe.utils.add_days(nowdate(), -lookback)
	if settings.go_live_date and getdate(from_date) < getdate(settings.go_live_date):
		from_date = settings.go_live_date

	invoices = frappe.get_all(
		"Sales Invoice",
		filters={"docstatus": 1, "posting_date": (">=", from_date)},
		pluck="name",
	)

	for name in invoices:
		try:
			sync_invoice_entries(frappe.get_doc("Sales Invoice", name))
			frappe.db.commit()
		except Exception:
			frappe.db.rollback()
			frappe.log_error(
				frappe.get_traceback(), "Commission reconcile failed for {0}".format(name)
			)

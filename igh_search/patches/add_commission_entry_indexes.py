"""Composite indexes for the commission ledger.

Frappe's ``search_index`` flag only produces single-column indexes, and every
hot query here filters on a combination:

* the payout claim  -> (sales_person, payout, posting_date)
* "my unpaid"       -> (sales_person, payout, posting_date)
* the leaderboard   -> (payout, sales_person)

Without these, each of those degrades to a scan once the ledger grows past a
few hundred thousand rows, which it will: roughly one row per invoice line per
salesperson.
"""

import frappe

INDEXES = (
	("sales_person", "payout", "posting_date"),
	("payout", "sales_person"),
)


def execute():
	if not frappe.db.table_exists("Sales Commission Entry"):
		return

	for columns in INDEXES:
		missing = [c for c in columns if not _column_exists("tabSales Commission Entry", c)]
		if missing:
			frappe.log_error(
				"Skipping index on tabSales Commission Entry{0}: missing column(s) {1}".format(
					list(columns), missing
				),
				"igh_search: add_commission_entry_indexes",
			)
			continue

		# add_index is a no-op when an index of the same generated name exists.
		frappe.db.add_index("Sales Commission Entry", list(columns))


def _column_exists(table, column):
	return bool(
		frappe.db.sql("SHOW COLUMNS FROM `{table}` LIKE %s".format(table=table), column)
	)

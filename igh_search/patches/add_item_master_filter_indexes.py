"""Index the columns ``get_all_masters`` scans to build the filter panel.

``tabItem`` holds ~181k rows here. The brand and category_list master lists are
built with ``SELECT DISTINCT <col> ... WHERE disabled = 0``, and with only
``disabled_index`` available MariaDB reads ~80k rows and then does
``Using temporary; Using filesort`` — measured at 1296ms (brand) and 1401ms
(category_list), i.e. ~2.7s of the ~3.5s cold cost of the filter panel.

A composite ``(disabled, <col>)`` index makes each of these a covering index
scan: the filter is the leading column and the values come out already sorted,
so both the temp table and the filesort disappear. There are only 244 distinct
brands and 574 distinct categories behind those 80k rows.
"""

import frappe

INDEXES = (
    ("disabled", "brand"),
    ("disabled", "category_list"),
)


def execute():
    if not frappe.db.table_exists("Item"):
        return

    for columns in INDEXES:
        missing = [c for c in columns if not _column_exists("tabItem", c)]
        if missing:
            frappe.log_error(
                f"Skipping index on tabItem{list(columns)}: missing column(s) {missing}",
                "igh_search: add_item_master_filter_indexes",
            )
            continue

        # add_index is a no-op when an index of the same generated name exists.
        frappe.db.add_index("Item", list(columns))


def _column_exists(table, column):
    return bool(
        frappe.db.sql(
            "SHOW COLUMNS FROM `{table}` LIKE %s".format(table=table),
            column,
        )
    )

"""Migrate legacy ``Product Data Issue`` records into the new ``Product Query``
desk (chat + ticket). Issue comments become ``Product Query Message`` rows.

Listed in patches.txt so Frappe runs it exactly once.
"""

import frappe

STATUS_MAP = {
    "open": "open",
    "triaged": "in_progress",
    "in_progress": "in_progress",
    "fixed": "resolved",
    "closed": "closed",
    "reopened": "reopened",
}


def execute():
    if not frappe.db.table_exists("Product Data Issue"):
        return
    if not frappe.db.exists("DocType", "Product Query"):
        return

    issues = frappe.get_all(
        "Product Data Issue",
        fields=[
            "name",
            "item_code",
            "item_name_snapshot",
            "reporter_user",
            "reporter_name",
            "reporter_role_snapshot",
            "issue_type",
            "severity",
            "affected_field",
            "current_value_snapshot",
            "suggested_value",
            "description",
            "attachment",
            "status",
            "assigned_to",
            "resolution_notes",
            "creation",
            "modified",
        ],
        limit_page_length=0,
    )

    for issue in issues:
        if frappe.db.exists("Product Query", {"legacy_issue": issue.name}):
            continue

        status = STATUS_MAP.get((issue.status or "open").lower(), "open")
        query = frappe.get_doc(
            {
                "doctype": "Product Query",
                "item_code": issue.item_code,
                "item_name_snapshot": issue.item_name_snapshot,
                "subject": issue.item_name_snapshot or issue.item_code,
                "query_type": issue.issue_type or "other",
                "affected_field": issue.affected_field,
                "severity": issue.severity or "medium",
                "current_value_snapshot": issue.current_value_snapshot,
                "suggested_value": issue.suggested_value,
                "reporter_user": issue.reporter_user,
                "reporter_name": issue.reporter_name,
                "reporter_role_snapshot": issue.reporter_role_snapshot,
                "stage": "ticket",
                "status": status,
                "assigned_to": issue.assigned_to if frappe.db.exists("User", issue.assigned_to) else None,
                "solution_notes": issue.resolution_notes,
                "last_message_preview": (issue.description or "")[:140],
            }
        )
        # Preserve a back-reference if the optional field exists.
        if query.meta.has_field("legacy_issue"):
            query.legacy_issue = issue.name
        query.flags.ignore_mandatory = True
        query.insert(ignore_permissions=True)

        # First message = the original issue description.
        if issue.description or issue.attachment:
            frappe.get_doc(
                {
                    "doctype": "Product Query Message",
                    "query": query.name,
                    "sender_user": issue.reporter_user,
                    "sender_name": issue.reporter_name,
                    "sender_role": "reporter",
                    "message_type": "message",
                    "message": issue.description or "",
                    "attachment": issue.attachment or "",
                }
            ).insert(ignore_permissions=True)

        # Carry over native Comment thread.
        comments = frappe.get_all(
            "Comment",
            filters={"reference_doctype": "Product Data Issue", "reference_name": issue.name, "comment_type": "Comment"},
            fields=["content", "owner", "creation"],
            order_by="creation asc",
        )
        for comment in comments:
            owner = comment.get("owner")
            roles = frappe.get_roles(owner) if owner else []
            sender_role = "admin" if ({"Item Manager", "System Manager"} & set(roles)) else "reporter"
            frappe.get_doc(
                {
                    "doctype": "Product Query Message",
                    "query": query.name,
                    "sender_user": owner,
                    "sender_name": frappe.db.get_value("User", owner, "full_name") or owner,
                    "sender_role": sender_role,
                    "message_type": "message",
                    "message": frappe.utils.strip_html(comment.get("content") or ""),
                }
            ).insert(ignore_permissions=True)

    frappe.db.commit()

import json

import frappe
from frappe import _
from frappe.utils import cstr

ISSUE_DOCTYPE = "Product Data Issue"
PRODUCT_DATA_MANAGER_ROLES = {"System Manager", "Product Data Manager"}
OPEN_STATUSES = {"open", "triaged", "in_progress", "reopened"}
ALLOWED_STATUSES = ["open", "triaged", "in_progress", "fixed", "closed", "reopened"]
ALLOWED_SEVERITIES = ["low", "medium", "high"]
ALLOWED_ISSUE_TYPES = [
    "wrong_spec",
    "missing_spec",
    "wrong_image",
    "wrong_category_brand",
    "duplicate_product",
    "stock_pricing_mismatch",
    "other",
]


def _require_login():
    if not frappe.session.user or frappe.session.user == "Guest":
        frappe.throw(_("Authentication required"), frappe.AuthenticationError)


def _get_roles(user=None):
    try:
        return frappe.get_roles(user or frappe.session.user) or []
    except Exception:
        return []


def _is_manager(user=None):
    roles = set(_get_roles(user))
    return bool(PRODUCT_DATA_MANAGER_ROLES.intersection(roles))


def _normalize_text(value):
    return cstr(value).strip()


def _normalize_select(value, allowed, field_label):
    normalized = _normalize_text(value).lower()
    if normalized and normalized not in allowed:
        frappe.throw(_("Invalid {0}: {1}").format(field_label, value))
    return normalized


def _validate_issue_access(doc):
    # V1 shared inbox: any authenticated catalog user can read issue detail.
    return


def _serialize_comment(comment):
    return {
        "name": comment.get("name"),
        "content": comment.get("content") or "",
        "owner": comment.get("owner") or comment.get("comment_email") or "",
        "creation": comment.get("creation"),
        "comment_type": comment.get("comment_type") or "Comment",
    }


def _serialize_issue(doc):
    return {
        "name": doc.name,
        "item_code": doc.item_code,
        "item_name_snapshot": doc.item_name_snapshot,
        "reporter_user": doc.reporter_user,
        "reporter_name": doc.reporter_name,
        "reporter_role_snapshot": doc.reporter_role_snapshot,
        "issue_type": doc.issue_type,
        "severity": doc.severity,
        "affected_field": doc.affected_field,
        "current_value_snapshot": doc.current_value_snapshot,
        "suggested_value": doc.suggested_value,
        "description": doc.description,
        "attachment": doc.attachment,
        "status": doc.status,
        "assigned_to": doc.assigned_to,
        "resolution_notes": doc.resolution_notes,
        "creation": doc.creation,
        "modified": doc.modified,
    }


def _get_comments(issue_id):
    rows = frappe.get_all(
        "Comment",
        filters={
            "reference_doctype": ISSUE_DOCTYPE,
            "reference_name": issue_id,
        },
        fields=["name", "content", "owner", "creation", "comment_type", "comment_email"],
        order_by="creation asc",
    )
    return [_serialize_comment(row) for row in rows]


def _issue_response(doc):
    return {
        "issue": _serialize_issue(doc),
        "comments": _get_comments(doc.name),
        "can_manage": _is_manager(),
        "can_reopen": _is_manager() or doc.reporter_user == frappe.session.user,
    }


def _compute_summary(items):
    summary = {"total": len(items)}
    for row in items:
        status = row.get("status") or "open"
        summary[status] = summary.get(status, 0) + 1
    return summary


@frappe.whitelist(methods=["POST"])
def create_product_data_issue(**kwargs):
    _require_login()

    item_code = _normalize_text(kwargs.get("item_code"))
    if not item_code:
        frappe.throw(_("item_code is required"))
    if not frappe.db.exists("Item", item_code):
        frappe.throw(_("Item {0} was not found").format(item_code))

    issue_type = _normalize_select(kwargs.get("issue_type"), ALLOWED_ISSUE_TYPES, "issue type")
    severity = _normalize_select(kwargs.get("severity"), ALLOWED_SEVERITIES, "severity")
    affected_field = _normalize_text(kwargs.get("affected_field")) or "other"
    description = _normalize_text(kwargs.get("description"))

    if not issue_type:
        frappe.throw(_("issue_type is required"))
    if not severity:
        frappe.throw(_("severity is required"))
    if not description:
        frappe.throw(_("description is required"))

    user_id = frappe.session.user
    user_doc = frappe.get_doc("User", user_id)
    issue = frappe.get_doc(
        {
            "doctype": ISSUE_DOCTYPE,
            "item_code": item_code,
            "item_name_snapshot": _normalize_text(kwargs.get("item_name_snapshot")) or frappe.db.get_value("Item", item_code, "item_name") or item_code,
            "reporter_user": user_id,
            "reporter_name": user_doc.full_name or user_doc.name,
            "reporter_role_snapshot": ", ".join(_get_roles(user_id)),
            "issue_type": issue_type,
            "severity": severity,
            "affected_field": affected_field,
            "current_value_snapshot": _normalize_text(kwargs.get("current_value_snapshot")),
            "suggested_value": _normalize_text(kwargs.get("suggested_value")),
            "description": description,
            "attachment": _normalize_text(kwargs.get("attachment")),
            "status": "open",
        }
    )
    issue.insert(ignore_permissions=True)
    frappe.db.commit()
    return _issue_response(issue)


@frappe.whitelist(methods=["GET", "POST"])
def list_product_data_issues(**kwargs):
    _require_login()

    filters = []
    if cstr(kwargs.get("mine")).strip() in {"1", "true", "True"}:
        filters.append([ISSUE_DOCTYPE, "reporter_user", "=", frappe.session.user])

    item_code = _normalize_text(kwargs.get("item_code"))
    if item_code:
        filters.append([ISSUE_DOCTYPE, "item_code", "like", f"%{item_code}%"])

    for fieldname in ["status", "severity", "issue_type", "affected_field", "assigned_to"]:
        value = _normalize_text(kwargs.get(fieldname))
        if value:
            filters.append([ISSUE_DOCTYPE, fieldname, "=", value.lower() if fieldname in {"status", "severity", "issue_type"} else value])

    open_only = cstr(kwargs.get("open_only")).strip() in {"1", "true", "True"}
    if open_only:
        filters.append([ISSUE_DOCTYPE, "status", "in", list(OPEN_STATUSES)])

    limit_page_length = int(kwargs.get("page_length") or 50)
    items = frappe.get_all(
        ISSUE_DOCTYPE,
        filters=filters,
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
        order_by="modified desc",
        limit_page_length=limit_page_length,
    )

    return {
        "items": items,
        "summary": _compute_summary(items),
        "can_manage": _is_manager(),
    }


@frappe.whitelist(methods=["GET", "POST"])
def get_product_data_issue(issue_id=None, **kwargs):
    _require_login()
    issue_id = issue_id or kwargs.get("issue_id")
    if not issue_id:
        frappe.throw(_("issue_id is required"))

    doc = frappe.get_doc(ISSUE_DOCTYPE, issue_id)
    _validate_issue_access(doc)
    return _issue_response(doc)


@frappe.whitelist(methods=["POST"])
def update_product_data_issue(issue_id=None, **kwargs):
    _require_login()
    if not _is_manager():
        frappe.throw(_("Only the product team can update issue workflow fields."), frappe.PermissionError)

    issue_id = issue_id or kwargs.get("issue_id")
    if not issue_id:
        frappe.throw(_("issue_id is required"))

    doc = frappe.get_doc(ISSUE_DOCTYPE, issue_id)
    allowed_updates = {
        "status": _normalize_select(kwargs.get("status"), ALLOWED_STATUSES, "status") or doc.status,
        "assigned_to": _normalize_text(kwargs.get("assigned_to")) if kwargs.get("assigned_to") is not None else doc.assigned_to,
        "resolution_notes": _normalize_text(kwargs.get("resolution_notes")) if kwargs.get("resolution_notes") is not None else doc.resolution_notes,
        "severity": _normalize_select(kwargs.get("severity"), ALLOWED_SEVERITIES, "severity") or doc.severity,
    }

    for key, value in allowed_updates.items():
        setattr(doc, key, value)

    doc.save(ignore_permissions=True)
    frappe.db.commit()
    return _issue_response(doc)


@frappe.whitelist(methods=["POST"])
def add_product_data_issue_comment(issue_id=None, comment=None, **kwargs):
    _require_login()
    issue_id = issue_id or kwargs.get("issue_id")
    comment = comment or kwargs.get("comment")
    if not issue_id:
        frappe.throw(_("issue_id is required"))
    if not _normalize_text(comment):
        frappe.throw(_("comment is required"))

    doc = frappe.get_doc(ISSUE_DOCTYPE, issue_id)
    _validate_issue_access(doc)
    doc.add_comment("Comment", _normalize_text(comment))
    frappe.db.commit()
    return _issue_response(doc)


@frappe.whitelist(methods=["POST"])
def reopen_product_data_issue(issue_id=None, comment=None, **kwargs):
    _require_login()
    issue_id = issue_id or kwargs.get("issue_id")
    comment = comment or kwargs.get("comment")
    if not issue_id:
        frappe.throw(_("issue_id is required"))

    doc = frappe.get_doc(ISSUE_DOCTYPE, issue_id)
    if not (_is_manager() or doc.reporter_user == frappe.session.user):
        frappe.throw(_("You are not allowed to reopen this issue."), frappe.PermissionError)

    doc.status = "reopened"
    doc.save(ignore_permissions=True)
    if _normalize_text(comment):
        doc.add_comment("Comment", _normalize_text(comment))
    frappe.db.commit()
    return _issue_response(doc)

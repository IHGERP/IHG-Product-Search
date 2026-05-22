"""Product Query Desk – live chat + ticketing backed by ERPNext DocTypes.

Replaces the old one-shot "Product Data Issue" flow. A Product Query is a single
thread that starts as a live chat between a sales user (reporter) and the product
team (admins = ``Item Manager`` / ``System Manager``). An admin can escalate the
same thread into a tracked ticket, record a solution, and the reporter rates it.
Super admins (``System Manager``) get an agent ranking/SLA report.

All chat messages live in the ``Product Query Message`` DocType so polling can
fetch only rows newer than a timestamp.
"""

import frappe
from frappe import _
from frappe.utils import (
    add_days,
    cstr,
    get_datetime,
    now_datetime,
    time_diff_in_seconds,
)

QUERY_DOCTYPE = "Product Query"
MESSAGE_DOCTYPE = "Product Query Message"

ADMIN_ROLES = {"Item Manager", "System Manager"}
SUPER_ADMIN_ROLES = {"System Manager"}

OPEN_STATUSES = {"open", "in_progress", "awaiting_reporter", "reopened"}
RESOLVED_STATUSES = {"resolved", "closed"}
ALLOWED_STATUSES = ["open", "in_progress", "awaiting_reporter", "resolved", "closed", "reopened"]
ALLOWED_SEVERITIES = ["low", "medium", "high"]
ALLOWED_STAGES = ["chat", "ticket"]
ALLOWED_QUERY_TYPES = [
    "wrong_spec",
    "missing_spec",
    "wrong_image",
    "wrong_category_brand",
    "duplicate_product",
    "stock_pricing_mismatch",
    "general_query",
    "other",
]

QUERY_FIELDS = [
    "name",
    "item_code",
    "item_name_snapshot",
    "brand",
    "category_list",
    "website_image_url",
    "subject",
    "query_type",
    "affected_field",
    "severity",
    "current_value_snapshot",
    "suggested_value",
    "reporter_user",
    "reporter_name",
    "reporter_role_snapshot",
    "stage",
    "status",
    "assigned_to",
    "solution_notes",
    "resolved_by",
    "resolved_at",
    "first_response_at",
    "solution_rating",
    "rating_comment",
    "unread_for_reporter",
    "unread_for_admin",
    "last_message_at",
    "last_message_preview",
    "creation",
    "modified",
]


# ──────────────────────────────────────────────────────────────────────────────
# Auth / role helpers
# ──────────────────────────────────────────────────────────────────────────────
def _require_login():
    if not frappe.session.user or frappe.session.user == "Guest":
        frappe.throw(_("Authentication required"), frappe.AuthenticationError)


def _get_roles(user=None):
    try:
        return frappe.get_roles(user or frappe.session.user) or []
    except Exception:
        return []


def _is_admin(user=None):
    return bool(ADMIN_ROLES.intersection(set(_get_roles(user))))


def _is_super_admin(user=None):
    return bool(SUPER_ADMIN_ROLES.intersection(set(_get_roles(user))))


def _normalize_text(value):
    return cstr(value).strip()


def _normalize_select(value, allowed, field_label):
    normalized = _normalize_text(value).lower()
    if normalized and normalized not in allowed:
        frappe.throw(_("Invalid {0}: {1}").format(field_label, value))
    return normalized


def _caller_side(doc):
    """Return 'admin' if the current user is a product-team admin, else 'reporter'."""
    if _is_admin():
        return "admin"
    return "reporter"


def _assert_can_view(doc):
    if _is_admin():
        return
    if doc.reporter_user == frappe.session.user:
        return
    frappe.throw(_("You are not allowed to view this query."), frappe.PermissionError)


def _assert_admin():
    if not _is_admin():
        frappe.throw(_("Only the product team can perform this action."), frappe.PermissionError)


# ──────────────────────────────────────────────────────────────────────────────
# Serializers
# ──────────────────────────────────────────────────────────────────────────────
def _serialize_query(doc):
    data = {}
    for field in QUERY_FIELDS:
        data[field] = doc.get(field) if hasattr(doc, "get") else getattr(doc, field, None)
    return data


def _serialize_message(row):
    return {
        "name": row.get("name"),
        "query": row.get("query"),
        "sender_user": row.get("sender_user") or "",
        "sender_name": row.get("sender_name") or "",
        "sender_role": row.get("sender_role") or "reporter",
        "message_type": row.get("message_type") or "message",
        "message": row.get("message") or "",
        "attachment": row.get("attachment") or "",
        "creation": row.get("creation"),
    }


def _get_messages(query_id, after=None):
    filters = {"query": query_id}
    if after:
        filters["creation"] = [">", get_datetime(after)]
    rows = frappe.get_all(
        MESSAGE_DOCTYPE,
        filters=filters,
        fields=[
            "name",
            "query",
            "sender_user",
            "sender_name",
            "sender_role",
            "message_type",
            "message",
            "attachment",
            "creation",
        ],
        order_by="creation asc",
        limit_page_length=0,
    )
    return [_serialize_message(row) for row in rows]


def _query_response(doc, after=None, include_messages=True):
    is_admin = _is_admin()
    return {
        "query": _serialize_query(doc),
        "messages": _get_messages(doc.name, after=after) if include_messages else [],
        "can_manage": is_admin,
        "can_rate": (doc.reporter_user == frappe.session.user) and (doc.status in RESOLVED_STATUSES),
        "can_reopen": is_admin or doc.reporter_user == frappe.session.user,
        "viewer_side": _caller_side(doc),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Message + bookkeeping
# ──────────────────────────────────────────────────────────────────────────────
def _append_message(doc, message, attachment="", message_type="message", sender_role=None, system=False):
    user_id = frappe.session.user
    if system:
        sender_role = "system"
        sender_name = "System"
    else:
        sender_role = sender_role or _caller_side(doc)
        sender_name = frappe.db.get_value("User", user_id, "full_name") or user_id

    msg = frappe.get_doc(
        {
            "doctype": MESSAGE_DOCTYPE,
            "query": doc.name,
            "sender_user": None if system else user_id,
            "sender_name": sender_name,
            "sender_role": sender_role,
            "message_type": message_type,
            "message": _normalize_text(message),
            "attachment": _normalize_text(attachment),
        }
    )
    msg.insert(ignore_permissions=True)

    preview = _normalize_text(message)[:140]
    doc.last_message_at = now_datetime()
    doc.last_message_preview = preview or ("[attachment]" if attachment else "")

    # Unread bookkeeping: a message raises the *other* side's unread counter.
    if sender_role == "admin":
        doc.unread_for_reporter = (doc.unread_for_reporter or 0) + 1
        if not doc.first_response_at:
            doc.first_response_at = now_datetime()
        if not doc.assigned_to:
            doc.assigned_to = user_id
        if doc.status == "open":
            doc.status = "in_progress"
    elif sender_role == "reporter":
        doc.unread_for_admin = (doc.unread_for_admin or 0) + 1
        if doc.status == "awaiting_reporter":
            doc.status = "in_progress"
    return msg


# ──────────────────────────────────────────────────────────────────────────────
# Whitelisted API
# ──────────────────────────────────────────────────────────────────────────────
@frappe.whitelist(methods=["POST"])
def create_product_query(**kwargs):
    _require_login()

    item_code = _normalize_text(kwargs.get("item_code"))
    if not item_code:
        frappe.throw(_("item_code is required"))
    if not frappe.db.exists("Item", item_code):
        frappe.throw(_("Item {0} was not found").format(item_code))

    message = _normalize_text(kwargs.get("message")) or _normalize_text(kwargs.get("description"))
    if not message:
        frappe.throw(_("A message is required to start a query"))

    query_type = _normalize_select(kwargs.get("query_type"), ALLOWED_QUERY_TYPES, "query type") or "general_query"
    severity = _normalize_select(kwargs.get("severity"), ALLOWED_SEVERITIES, "severity") or "medium"

    user_id = frappe.session.user
    user_doc = frappe.get_doc("User", user_id)
    item_name = _normalize_text(kwargs.get("item_name_snapshot")) or frappe.db.get_value("Item", item_code, "item_name") or item_code

    doc = frappe.get_doc(
        {
            "doctype": QUERY_DOCTYPE,
            "item_code": item_code,
            "item_name_snapshot": item_name,
            "brand": _normalize_text(kwargs.get("brand")),
            "category_list": _normalize_text(kwargs.get("category_list")),
            "website_image_url": _normalize_text(kwargs.get("website_image_url")),
            "subject": _normalize_text(kwargs.get("subject")) or item_name,
            "query_type": query_type,
            "affected_field": _normalize_text(kwargs.get("affected_field")),
            "severity": severity,
            "current_value_snapshot": _normalize_text(kwargs.get("current_value_snapshot")),
            "suggested_value": _normalize_text(kwargs.get("suggested_value")),
            "reporter_user": user_id,
            "reporter_name": user_doc.full_name or user_doc.name,
            "reporter_role_snapshot": ", ".join(_get_roles(user_id)),
            "stage": "chat",
            "status": "open",
            "unread_for_reporter": 0,
            "unread_for_admin": 1,
        }
    )
    doc.insert(ignore_permissions=True)

    _append_message(
        doc,
        message,
        attachment=_normalize_text(kwargs.get("attachment")),
        message_type="message",
        sender_role="reporter",
    )
    # _append_message bumped unread_for_admin again; first message already counted.
    doc.unread_for_admin = 1
    doc.save(ignore_permissions=True)
    frappe.db.commit()
    return _query_response(doc)


@frappe.whitelist(methods=["GET", "POST"])
def list_product_queries(**kwargs):
    _require_login()
    is_admin = _is_admin()

    filters = {}
    mine = cstr(kwargs.get("mine")).strip() in {"1", "true", "True"}
    if mine or not is_admin:
        # Non-admins always see only their own queries.
        filters["reporter_user"] = frappe.session.user

    for fieldname in ["status", "severity", "query_type", "stage"]:
        value = _normalize_text(kwargs.get(fieldname))
        if value:
            filters[fieldname] = value.lower()

    assigned_to = _normalize_text(kwargs.get("assigned_to"))
    if assigned_to:
        filters["assigned_to"] = assigned_to

    item_code = _normalize_text(kwargs.get("item_code"))
    if item_code:
        filters["item_code"] = item_code

    if cstr(kwargs.get("open_only")).strip() in {"1", "true", "True"}:
        filters["status"] = ["in", list(OPEN_STATUSES)]

    limit_page_length = int(kwargs.get("page_length") or 50)
    start = int(kwargs.get("start") or 0)

    items = frappe.get_all(
        QUERY_DOCTYPE,
        filters=filters,
        fields=QUERY_FIELDS,
        order_by="last_message_at desc, modified desc",
        limit_page_length=limit_page_length,
        limit_start=start,
    )

    summary = {"total": len(items)}
    for row in items:
        status = row.get("status") or "open"
        summary[status] = summary.get(status, 0) + 1

    return {"items": items, "summary": summary, "can_manage": is_admin}


@frappe.whitelist(methods=["GET", "POST"])
def get_product_query(query_id=None, after=None, **kwargs):
    _require_login()
    query_id = query_id or kwargs.get("query_id")
    if not query_id:
        frappe.throw(_("query_id is required"))

    doc = frappe.get_doc(QUERY_DOCTYPE, query_id)
    _assert_can_view(doc)

    # Opening the thread clears the caller's unread counter.
    side = _caller_side(doc)
    changed = False
    if side == "admin" and (doc.unread_for_admin or 0):
        doc.db_set("unread_for_admin", 0, update_modified=False)
        changed = True
    elif side == "reporter" and (doc.unread_for_reporter or 0):
        doc.db_set("unread_for_reporter", 0, update_modified=False)
        changed = True
    if changed:
        frappe.db.commit()

    return _query_response(doc, after=after)


@frappe.whitelist(methods=["POST"])
def post_product_query_message(query_id=None, message=None, attachment=None, **kwargs):
    _require_login()
    query_id = query_id or kwargs.get("query_id")
    message = message if message is not None else kwargs.get("message")
    attachment = attachment if attachment is not None else kwargs.get("attachment")

    if not query_id:
        frappe.throw(_("query_id is required"))
    if not _normalize_text(message) and not _normalize_text(attachment):
        frappe.throw(_("message is required"))

    doc = frappe.get_doc(QUERY_DOCTYPE, query_id)
    _assert_can_view(doc)

    side = _caller_side(doc)
    _append_message(doc, message, attachment=attachment, message_type="message", sender_role=side)
    # Sender's own side is considered read.
    if side == "admin":
        doc.unread_for_admin = 0
    else:
        doc.unread_for_reporter = 0
    doc.save(ignore_permissions=True)
    frappe.db.commit()
    return _query_response(doc)


@frappe.whitelist(methods=["GET", "POST"])
def poll_product_query_updates(since=None, **kwargs):
    """Lightweight badge poller: returns global unread + per-query deltas."""
    _require_login()
    is_admin = _is_admin()

    if is_admin:
        unread_field = "unread_for_admin"
        filters = {}
    else:
        unread_field = "unread_for_reporter"
        filters = {"reporter_user": frappe.session.user}

    filters[unread_field] = [">", 0]
    rows = frappe.get_all(
        QUERY_DOCTYPE,
        filters=filters,
        fields=["name", unread_field, "last_message_at", "last_message_preview", "item_name_snapshot", "status", "stage"],
        order_by="last_message_at desc",
        limit_page_length=50,
    )

    total_unread = 0
    items = []
    for row in rows:
        count = int(row.get(unread_field) or 0)
        total_unread += count
        items.append(
            {
                "id": row.get("name"),
                "unread": count,
                "last_message_at": row.get("last_message_at"),
                "last_message_preview": row.get("last_message_preview"),
                "item_name_snapshot": row.get("item_name_snapshot"),
                "status": row.get("status"),
                "stage": row.get("stage"),
            }
        )

    # Open-queue size is useful for the admin launcher badge.
    open_count = frappe.db.count(QUERY_DOCTYPE, {"status": ["in", list(OPEN_STATUSES)]}) if is_admin else None

    return {
        "unread_total": total_unread,
        "threads": items,
        "open_count": open_count,
        "is_admin": is_admin,
    }


@frappe.whitelist(methods=["POST"])
def mark_product_query_read(query_id=None, **kwargs):
    _require_login()
    query_id = query_id or kwargs.get("query_id")
    if not query_id:
        frappe.throw(_("query_id is required"))

    doc = frappe.get_doc(QUERY_DOCTYPE, query_id)
    _assert_can_view(doc)
    if _caller_side(doc) == "admin":
        doc.db_set("unread_for_admin", 0, update_modified=False)
    else:
        doc.db_set("unread_for_reporter", 0, update_modified=False)
    frappe.db.commit()
    return {"ok": True}


@frappe.whitelist(methods=["POST"])
def escalate_product_query_to_ticket(query_id=None, **kwargs):
    _require_login()
    _assert_admin()
    query_id = query_id or kwargs.get("query_id")
    if not query_id:
        frappe.throw(_("query_id is required"))

    doc = frappe.get_doc(QUERY_DOCTYPE, query_id)

    severity = _normalize_select(kwargs.get("severity"), ALLOWED_SEVERITIES, "severity")
    if severity:
        doc.severity = severity
    query_type = _normalize_select(kwargs.get("query_type"), ALLOWED_QUERY_TYPES, "query type")
    if query_type:
        doc.query_type = query_type
    affected_field = _normalize_text(kwargs.get("affected_field"))
    if affected_field:
        doc.affected_field = affected_field

    doc.stage = "ticket"
    if doc.status == "open":
        doc.status = "in_progress"
    if not doc.assigned_to:
        doc.assigned_to = frappe.session.user

    _append_message(doc, "Escalated to a tracked ticket.", message_type="status_change", system=True)
    doc.save(ignore_permissions=True)
    frappe.db.commit()
    return _query_response(doc)


@frappe.whitelist(methods=["POST"])
def update_product_query(query_id=None, **kwargs):
    _require_login()
    _assert_admin()
    query_id = query_id or kwargs.get("query_id")
    if not query_id:
        frappe.throw(_("query_id is required"))

    doc = frappe.get_doc(QUERY_DOCTYPE, query_id)

    if kwargs.get("status") is not None:
        new_status = _normalize_select(kwargs.get("status"), ALLOWED_STATUSES, "status")
        if new_status and new_status != doc.status:
            doc.status = new_status
            _append_message(doc, "Status changed to {0}.".format(new_status), message_type="status_change", system=True)
    if kwargs.get("severity") is not None:
        sev = _normalize_select(kwargs.get("severity"), ALLOWED_SEVERITIES, "severity")
        if sev:
            doc.severity = sev
    if kwargs.get("assigned_to") is not None:
        doc.assigned_to = _normalize_text(kwargs.get("assigned_to"))

    doc.save(ignore_permissions=True)
    frappe.db.commit()
    return _query_response(doc)


@frappe.whitelist(methods=["POST"])
def resolve_product_query(query_id=None, solution_notes=None, **kwargs):
    _require_login()
    _assert_admin()
    query_id = query_id or kwargs.get("query_id")
    solution_notes = solution_notes if solution_notes is not None else kwargs.get("solution_notes")
    if not query_id:
        frappe.throw(_("query_id is required"))
    if not _normalize_text(solution_notes):
        frappe.throw(_("A solution note is required to resolve"))

    doc = frappe.get_doc(QUERY_DOCTYPE, query_id)
    doc.solution_notes = _normalize_text(solution_notes)
    doc.status = "resolved"
    doc.resolved_by = frappe.session.user
    doc.resolved_at = now_datetime()
    if doc.stage == "chat":
        doc.stage = "ticket"

    _append_message(doc, doc.solution_notes, message_type="solution", sender_role="admin")
    doc.save(ignore_permissions=True)
    frappe.db.commit()
    return _query_response(doc)


@frappe.whitelist(methods=["POST"])
def rate_product_query_solution(query_id=None, rating=None, comment=None, **kwargs):
    _require_login()
    query_id = query_id or kwargs.get("query_id")
    rating = rating if rating is not None else kwargs.get("rating")
    comment = comment if comment is not None else kwargs.get("comment")
    if not query_id:
        frappe.throw(_("query_id is required"))

    doc = frappe.get_doc(QUERY_DOCTYPE, query_id)
    if doc.reporter_user != frappe.session.user:
        frappe.throw(_("Only the reporter can rate the solution."), frappe.PermissionError)
    if doc.status not in RESOLVED_STATUSES:
        frappe.throw(_("You can rate the solution only after it is resolved."))

    try:
        rating_value = int(rating)
    except (TypeError, ValueError):
        frappe.throw(_("rating must be a number between 1 and 5"))
    if rating_value < 1 or rating_value > 5:
        frappe.throw(_("rating must be between 1 and 5"))

    doc.solution_rating = rating_value
    doc.rating_comment = _normalize_text(comment)
    if doc.status == "resolved":
        doc.status = "closed"

    _append_message(
        doc,
        "Rated the solution {0}/5.".format(rating_value) + (" – " + doc.rating_comment if doc.rating_comment else ""),
        message_type="status_change",
        sender_role="reporter",
    )
    doc.save(ignore_permissions=True)
    frappe.db.commit()
    return _query_response(doc)


@frappe.whitelist(methods=["POST"])
def reopen_product_query(query_id=None, reason=None, **kwargs):
    _require_login()
    query_id = query_id or kwargs.get("query_id")
    reason = reason if reason is not None else kwargs.get("reason")
    if not query_id:
        frappe.throw(_("query_id is required"))

    doc = frappe.get_doc(QUERY_DOCTYPE, query_id)
    if not (_is_admin() or doc.reporter_user == frappe.session.user):
        frappe.throw(_("You are not allowed to reopen this query."), frappe.PermissionError)

    doc.status = "reopened"
    note = "Reopened." + ((" " + _normalize_text(reason)) if _normalize_text(reason) else "")
    _append_message(doc, note, message_type="status_change", sender_role=_caller_side(doc))
    doc.save(ignore_permissions=True)
    frappe.db.commit()
    return _query_response(doc)


@frappe.whitelist(methods=["GET", "POST"])
def get_product_query_rankings(period_days=30, **kwargs):
    """Super-admin agent leaderboard: volume, SLA, and solution ratings."""
    _require_login()
    if not _is_super_admin():
        frappe.throw(_("Only super admins can view rankings."), frappe.PermissionError)

    try:
        period_days = int(period_days or kwargs.get("period_days") or 30)
    except (TypeError, ValueError):
        period_days = 30
    since = add_days(now_datetime(), -period_days)

    rows = frappe.get_all(
        QUERY_DOCTYPE,
        filters={"assigned_to": ["is", "set"], "creation": [">=", since]},
        fields=[
            "assigned_to",
            "status",
            "creation",
            "first_response_at",
            "resolved_at",
            "solution_rating",
        ],
        limit_page_length=0,
    )

    agents = {}

    def _agent(name):
        if name not in agents:
            agents[name] = {
                "agent": name,
                "agent_name": frappe.db.get_value("User", name, "full_name") or name,
                "resolved": 0,
                "open_load": 0,
                "_fr_sum": 0.0,
                "_fr_n": 0,
                "_res_sum": 0.0,
                "_res_n": 0,
                "_rating_sum": 0,
                "_rating_n": 0,
            }
        return agents[name]

    for row in rows:
        agent = _agent(row.get("assigned_to"))
        status = row.get("status")
        if status in OPEN_STATUSES:
            agent["open_load"] += 1
        if status in RESOLVED_STATUSES:
            agent["resolved"] += 1
        if row.get("first_response_at") and row.get("creation"):
            agent["_fr_sum"] += time_diff_in_seconds(row.get("first_response_at"), row.get("creation"))
            agent["_fr_n"] += 1
        if row.get("resolved_at") and row.get("creation"):
            agent["_res_sum"] += time_diff_in_seconds(row.get("resolved_at"), row.get("creation"))
            agent["_res_n"] += 1
        if row.get("solution_rating"):
            agent["_rating_sum"] += int(row.get("solution_rating"))
            agent["_rating_n"] += 1

    leaderboard = []
    for agent in agents.values():
        avg_fr = (agent["_fr_sum"] / agent["_fr_n"]) if agent["_fr_n"] else None
        avg_res = (agent["_res_sum"] / agent["_res_n"]) if agent["_res_n"] else None
        avg_rating = (agent["_rating_sum"] / agent["_rating_n"]) if agent["_rating_n"] else None
        leaderboard.append(
            {
                "agent": agent["agent"],
                "agent_name": agent["agent_name"],
                "resolved": agent["resolved"],
                "open_load": agent["open_load"],
                "avg_first_response_secs": round(avg_fr) if avg_fr is not None else None,
                "avg_resolution_secs": round(avg_res) if avg_res is not None else None,
                "avg_rating": round(avg_rating, 2) if avg_rating is not None else None,
                "rated_count": agent["_rating_n"],
            }
        )

    leaderboard.sort(key=lambda a: (a["resolved"], a["avg_rating"] or 0), reverse=True)
    for index, agent in enumerate(leaderboard, start=1):
        agent["rank"] = index

    return {"period_days": period_days, "leaderboard": leaderboard}

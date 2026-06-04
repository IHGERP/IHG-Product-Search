# Phase 4 — Learning loop + observability.
#
# Turns the data the system already collects (AI Product Search Event telemetry +
# AI Assistant Conversation feedback) into action:
#   build_gap_list()        -> where the AI fails reps (zero-result / "didn't find" /
#                              not-satisfied queries), ranked — feeds catalog data
#                              fixes (Phase 1) and new benchmark cases (Phase 0).
#   get_ai_observability()  -> the dashboard payload: volume, zero-result rate,
#                              satisfaction distribution, top queries, gap list.
#   record_assistant_feedback_signal() -> route a chat 👍/👎 into the existing
#                              feedback-reranker pipeline (closes the ranking loop).
# All read-only except record_assistant_feedback_signal (writes a telemetry event).

from collections import Counter, defaultdict

import frappe
from frappe.utils import add_days, cint, cstr, now_datetime

SEARCH_EVENT = "AI Product Search Event"
CONVERSATION = "AI Assistant Conversation"


def _has(doctype):
    try:
        return bool(frappe.db.exists("DocType", doctype))
    except Exception:
        return False


@frappe.whitelist()
def build_gap_list(days=60, limit=80):
    """Ranked list of queries where the AI is failing reps. Read-only.
    Signals: zero-result searches (telemetry), 'found_required_data=No' and
    'Not Satisfied' conversations (feedback)."""
    if frappe.session.user == "Guest":
        frappe.throw("Authentication required")
    days = cint(days) or 60
    since = add_days(now_datetime(), -days)
    gaps = defaultdict(lambda: {"query": "", "zero_result_hits": 0, "not_found_chats": 0,
                                "unsatisfied": 0, "occurrences": 0})

    if _has(SEARCH_EVENT):
        for r in frappe.get_all(SEARCH_EVENT,
                                filters={"creation": [">=", since], "normalized_query": ["!=", ""]},
                                fields=["normalized_query", "result_count"],
                                limit_page_length=20000):
            q = cstr(r.normalized_query).strip().lower()
            if not q:
                continue
            g = gaps[q]; g["query"] = q; g["occurrences"] += 1
            if cint(r.result_count) == 0:
                g["zero_result_hits"] += 1

    if _has(CONVERSATION):
        for r in frappe.get_all(CONVERSATION,
                                filters={"modified": [">=", since]},
                                fields=["first_query", "found_required_data", "satisfaction"],
                                limit_page_length=20000):
            q = cstr(r.first_query).strip().lower()
            if not q:
                continue
            g = gaps[q]; g["query"] = q
            if cstr(r.found_required_data) == "No":
                g["not_found_chats"] += 1
            if cstr(r.satisfaction) == "Not Satisfied":
                g["unsatisfied"] += 1

    items = []
    for g in gaps.values():
        # "didn't find" (explicit) weighted highest, then zero-result, then unsatisfied
        g["gap_score"] = g["not_found_chats"] * 3 + g["zero_result_hits"] * 2 + g["unsatisfied"] * 2
        if g["gap_score"] > 0:
            items.append(g)
    items.sort(key=lambda x: -x["gap_score"])
    return {"days": days, "gap_count": len(items), "gaps": items[:cint(limit) or 80]}


@frappe.whitelist()
def get_ai_observability(days=30):
    """Aggregate dashboard payload. Read-only."""
    if frappe.session.user == "Guest":
        frappe.throw("Authentication required")
    days = cint(days) or 30
    since = add_days(now_datetime(), -days)
    out = {"days": days}

    if _has(SEARCH_EVENT):
        rows = frappe.get_all(SEARCH_EVENT, filters={"creation": [">=", since]},
                              fields=["normalized_query", "result_count", "event_type"],
                              limit_page_length=50000)
        total = len(rows)
        named = [r for r in rows if cstr(r.normalized_query).strip()]
        zero = len([r for r in named if cint(r.result_count) == 0])
        topq = Counter(cstr(r.normalized_query).strip().lower() for r in named)
        out["search_events"] = total
        out["zero_result_rate"] = round(zero / max(1, len(named)), 3)
        out["top_queries"] = topq.most_common(15)

    if _has(CONVERSATION):
        rows = frappe.get_all(CONVERSATION, filters={"modified": [">=", since]},
                              fields=["satisfaction", "found_required_data", "message_count"],
                              limit_page_length=50000)
        out["conversations"] = len(rows)
        out["satisfaction"] = dict(Counter(cstr(r.satisfaction) or "(unrated)" for r in rows))
        out["found_required_data"] = dict(Counter(cstr(r.found_required_data) or "(unrated)" for r in rows))

    out["gap_list"] = build_gap_list(days=days, limit=20).get("gaps", [])
    return out


def record_assistant_feedback_signal(normalized_query, item_codes, rating):
    """Route a chat thumb into the feedback-reranker pipeline so 👍'd products rank
    higher for that query next time (and 👎'd ones lower). Writes a lightweight
    telemetry event per product; get_query_feedback_scores already aggregates these.
    Best-effort, called from assistant_history.submit_assistant_feedback."""
    if not normalized_query or not item_codes or rating not in ("up", "down"):
        return
    try:
        from igh_search.igh_search.ai_product_search import log_ai_search_event, normalize_text
    except Exception:
        return
    event_type = "search_click" if rating == "up" else "shortlist"  # reuse existing weighted types
    nq = normalize_text(normalized_query)
    for code in (item_codes or [])[:12]:
        code = cstr(code).strip()
        if not code:
            continue
        try:
            log_ai_search_event(event_type, {
                "normalized_query": nq,
                "raw_message": normalized_query,
                "selected_item_code": code,
                "outcome_status": f"assistant_thumb_{rating}",
            })
        except Exception:
            continue

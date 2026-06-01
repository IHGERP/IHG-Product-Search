# AI Assistant conversation history — server-side persistence + feedback capture.
#
# Replaces the browser-only localStorage history with durable, per-user records in
# ERPNext (DocType "AI Assistant Conversation") so the team can: (a) see every
# question reps ask the assistant, (b) know whether they found the data they
# needed, (c) capture satisfaction, and (d) export the corpus for retraining /
# gap analysis. All operations are scoped to the logged-in user; a user can never
# read or overwrite another user's conversation. Doc ops use ignore_permissions
# (the whitelisted entry points enforce ownership themselves).

import json

import frappe
from frappe.utils import now_datetime

DOCTYPE = "AI Assistant Conversation"
MAX_MESSAGES_STORED = 60        # cap stored turns per conversation
MAX_JSON_CHARS = 800000         # guard against pathological payloads


def _user():
	u = frappe.session.user
	return u if u and u != "Guest" else None


def _coerce_list(value):
	if isinstance(value, str):
		try:
			value = json.loads(value)
		except Exception:
			value = []
	return value if isinstance(value, list) else []


def _extract(messages):
	"""Pull the queryable signals out of the raw message list."""
	user_queries, product_codes = [], []
	for m in messages or []:
		if not isinstance(m, dict):
			continue
		if m.get("role") == "user" and m.get("content"):
			user_queries.append(str(m["content"]).strip())
		for p in (m.get("products") or []):
			code = p.get("item_code") if isinstance(p, dict) else None
			if code and code not in product_codes:
				product_codes.append(code)
	return user_queries, product_codes


def _owned_doc_or_none(conversation_id, user):
	if not conversation_id or not frappe.db.exists(DOCTYPE, conversation_id):
		return None
	doc = frappe.get_doc(DOCTYPE, conversation_id)
	if doc.assistant_user and doc.assistant_user != user:
		return False  # exists but belongs to someone else
	return doc


def save_assistant_conversation(conversation_id=None, title=None, messages=None, route=None):
	user = _user()
	if not user or not conversation_id:
		return {"status": "skipped"}
	messages = _coerce_list(messages)
	if not messages:
		return {"status": "empty"}
	messages = messages[-MAX_MESSAGES_STORED:]
	user_queries, product_codes = _extract(messages)

	existing = _owned_doc_or_none(conversation_id, user)
	if existing is False:
		return {"status": "forbidden"}
	if existing:
		doc = existing
	else:
		doc = frappe.new_doc(DOCTYPE)
		doc.conversation_id = conversation_id
		doc.assistant_user = user
		doc.started_on = now_datetime()
		doc.status = "Active"

	doc.title = (title or (user_queries[0] if user_queries else "New chat"))[:140]
	doc.message_count = len(messages)
	doc.first_query = (user_queries[0] if user_queries else "")[:500]
	doc.user_queries = "\n".join(user_queries)[:5000]
	doc.product_codes = ", ".join(product_codes)[:5000]
	doc.messages_json = json.dumps(messages)[:MAX_JSON_CHARS]
	doc.last_activity = now_datetime()
	if route:
		doc.route = str(route)[:140]
	doc.save(ignore_permissions=True)
	frappe.db.commit()
	return {"status": "success", "conversation_id": conversation_id}


def list_assistant_conversations(limit=30, offset=0):
	user = _user()
	if not user:
		return {"conversations": []}
	rows = frappe.get_all(
		DOCTYPE,
		filters={"assistant_user": user},
		fields=[
			"name as conversation_id", "title", "message_count",
			"satisfaction", "found_required_data", "last_activity",
		],
		order_by="last_activity desc",
		limit_page_length=int(limit or 30),
		start=int(offset or 0),
	)
	for r in rows:
		if r.get("last_activity") is not None:
			r["last_activity"] = str(r["last_activity"])
	return {"conversations": rows}


def get_assistant_conversation(conversation_id=None):
	user = _user()
	if not user or not conversation_id:
		return {"messages": []}
	doc = _owned_doc_or_none(conversation_id, user)
	if not doc:  # None (missing) or False (not owned)
		return {"messages": []}
	try:
		messages = json.loads(doc.messages_json or "[]")
	except Exception:
		messages = []
	return {
		"conversation_id": conversation_id,
		"title": doc.title,
		"messages": messages,
		"satisfaction": doc.satisfaction,
		"found_required_data": doc.found_required_data,
		"feedback_comment": doc.feedback_comment,
	}


def delete_assistant_conversation(conversation_id=None):
	user = _user()
	if not user or not conversation_id:
		return {"status": "skipped"}
	doc = _owned_doc_or_none(conversation_id, user)
	if doc is None:
		return {"status": "success"}  # already gone
	if doc is False:
		return {"status": "forbidden"}
	frappe.delete_doc(DOCTYPE, conversation_id, ignore_permissions=True, force=True)
	frappe.db.commit()
	return {"status": "success"}


def submit_assistant_feedback(conversation_id=None, satisfaction=None, found_required_data=None, comment=None, message_ratings=None):
	user = _user()
	if not user or not conversation_id:
		return {"status": "skipped"}
	doc = _owned_doc_or_none(conversation_id, user)
	if doc is None:
		return {"status": "not_found"}
	if doc is False:
		return {"status": "forbidden"}

	if satisfaction is not None and satisfaction in ("", "Satisfied", "Partially", "Not Satisfied"):
		doc.satisfaction = satisfaction
	if found_required_data is not None and found_required_data in ("", "Yes", "Partially", "No"):
		doc.found_required_data = found_required_data
	if comment is not None:
		doc.feedback_comment = (comment or "")[:1000]

	# Optional per-reply thumbs: {"<message_index>": "up"|"down"} merged into the blob.
	if message_ratings:
		if isinstance(message_ratings, str):
			try:
				message_ratings = json.loads(message_ratings)
			except Exception:
				message_ratings = None
		if isinstance(message_ratings, dict):
			try:
				msgs = json.loads(doc.messages_json or "[]")
				for idx_str, rating in message_ratings.items():
					i = int(idx_str)
					if 0 <= i < len(msgs) and isinstance(msgs[i], dict):
						msgs[i]["rating"] = rating
				doc.messages_json = json.dumps(msgs)[:MAX_JSON_CHARS]
			except Exception:
				pass

	doc.save(ignore_permissions=True)
	frappe.db.commit()
	return {"status": "success"}

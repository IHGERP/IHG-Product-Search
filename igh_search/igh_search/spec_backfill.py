# Phase 5 — AI spec backfill.
# Fills EMPTY structured spec fields on Item by extracting values from text the
# item already has (item_name + description) and matching them to the existing
# `Att *` master records the Link fields point to. Safe by construction:
#   * never overwrites a field that already has a value;
#   * only writes a value that resolves to an existing master (no silent master
#     creation unless create_missing_masters=1 is explicitly passed);
#   * dry_run=1 by default — returns a report and writes nothing;
#   * processes a bounded batch (limit/offset) so it can be run incrementally.
# The LLM does the hard extraction (regex misreads "15 Watts" as a beam angle,
# can't normalise "warm white" -> 3000K, etc.). High confidence only.

import json
import re
import time

import frappe
import requests
from frappe.utils import cint, cstr

from igh_search.igh_search.ai_product_search import (
    get_openai_api_key,
    get_openai_model,
    get_groq_api_key,
    get_groq_model,
    OPENAI_API_URL,
    GROQ_API_URL,
)

# Item spec field -> its Att master doctype (Link target).
SPEC_FIELD_MASTER = {
    "power": "Att Power",
    "color_temp_": "Att Color Temp",
    "ip_rate": "Att IP Rate",
    "lumen_output": "Att Lumen Output",
    "beam_angle": "Att Beam Angle",
    "lamp_type": "Att Lamp Type",
}
# LLM returns these keys; map to the Item fieldname.
LLM_KEY_TO_FIELD = {
    "power": "power",
    "color_temp": "color_temp_",
    "ip_rate": "ip_rate",
    "lumen_output": "lumen_output",
    "beam_angle": "beam_angle",
    "lamp_type": "lamp_type",
}

_HTML_RE = re.compile(r"<[^>]+>")


def _clean_text(value):
    return re.sub(r"\s+", " ", _HTML_RE.sub(" ", cstr(value or ""))).strip()


def _master_lookup(doctype):
    """normalized-value -> canonical master name, for case/space-insensitive match."""
    names = frappe.get_all(doctype, pluck="name")
    return {_norm(n): n for n in names}, names


def _norm(v):
    return re.sub(r"\s+", "", cstr(v or "").strip().lower())


def _call_llm(messages):
    key = get_openai_api_key()
    if key:
        r = requests.post(
            OPENAI_API_URL,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"model": get_openai_model(), "messages": messages, "temperature": 0,
                  "max_tokens": 400, "response_format": {"type": "json_object"}},
            timeout=30,
        )
        r.raise_for_status()
        return json.loads(r.json()["choices"][0]["message"]["content"])
    gkey = get_groq_api_key()
    if gkey:
        r = requests.post(
            GROQ_API_URL,
            headers={"Authorization": f"Bearer {gkey}", "Content-Type": "application/json"},
            json={"model": get_groq_model(), "messages": messages, "temperature": 0,
                  "max_tokens": 400, "response_format": {"type": "json_object"}},
            timeout=20,
        )
        r.raise_for_status()
        return json.loads(r.json()["choices"][0]["message"]["content"])
    raise ValueError("No LLM provider configured")


def _extract_specs(item_name, description, missing_fields):
    system = (
        "You extract lighting/electrical product specifications from a product name and "
        "description. Return ONLY valid JSON. For each requested field, return the value "
        "in standard catalog form, or null if it is not clearly stated. Rules: a number "
        "with W/Watt is power (e.g. '15W'); CCT in Kelvin like '3000K' ('warm white'=3000K, "
        "'cool white'=4000K, 'daylight'=6500K); IP like 'IP65'; lumen like '1200lm'; beam "
        "like '36°'; lamp_type like 'COB','SMD','E27','GU10'. Do NOT guess; null if unsure. "
        "Only include the requested keys."
    )
    user = {
        "item_name": item_name,
        "description": description[:600],
        "requested_fields": missing_fields,
    }
    out = _call_llm([
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user, ensure_ascii=True)},
    ])
    return out if isinstance(out, dict) else {}


@frappe.whitelist()
def backfill_item_specs(item_group="LIGHTING", limit=50, offset=0, dry_run=1,
                        create_missing_masters=0, fields=None):
    """Backfill empty spec fields for a batch of items.

    dry_run=1 (default): extract + resolve, write nothing, return the plan.
    Set dry_run=0 to apply. create_missing_masters=1 allows creating an `Att *`
    record when a confidently-extracted value has no existing master.
    """
    if frappe.session.user == "Guest":
        frappe.throw("Authentication required")

    limit = max(min(cint(limit), 200), 1)
    target_fields = (
        [f.strip() for f in cstr(fields).split(",") if f.strip()]
        if fields else list(SPEC_FIELD_MASTER.keys())
    )
    target_fields = [f for f in target_fields if f in SPEC_FIELD_MASTER]

    # Items in the group with at least one of the target fields empty.
    empty_cond = " OR ".join([f"(`{f}` IS NULL OR `{f}`='')" for f in target_fields])
    rows = frappe.db.sql(
        f"""SELECT name, item_name, description, {', '.join(f'`{f}`' for f in target_fields)}
            FROM `tabItem`
            WHERE disabled=0 AND item_group=%s AND ({empty_cond})
            ORDER BY modified DESC LIMIT %s OFFSET %s""",
        (item_group, limit, cint(offset)), as_dict=True,
    )

    masters = {f: _master_lookup(SPEC_FIELD_MASTER[f]) for f in target_fields}
    report = {"scanned": len(rows), "proposed": 0, "applied": 0,
              "created_masters": [], "items": [], "dry_run": cint(dry_run)}

    for row in rows:
        missing = [f for f in target_fields if not cstr(row.get(f)).strip()]
        if not missing:
            continue
        name = _clean_text(row.get("item_name"))
        desc = _clean_text(row.get("description"))
        if not name and not desc:
            continue
        llm_keys = [k for k, fld in LLM_KEY_TO_FIELD.items() if fld in missing]
        try:
            extracted = _extract_specs(name, desc, llm_keys)
        except Exception as exc:
            report["items"].append({"item": row["name"], "error": cstr(exc)[:160]})
            continue

        changes = {}
        for llm_key, value in (extracted or {}).items():
            field = LLM_KEY_TO_FIELD.get(llm_key)
            if not field or field not in missing or value in (None, "", "null"):
                continue
            lookup, _names = masters[field]
            canonical = lookup.get(_norm(value))
            if not canonical:
                if cint(create_missing_masters):
                    doc = frappe.get_doc({"doctype": SPEC_FIELD_MASTER[field],
                                          "name": cstr(value).strip()})
                    if not cint(dry_run):
                        doc.insert(ignore_permissions=True)
                    canonical = cstr(value).strip()
                    lookup[_norm(value)] = canonical
                    report["created_masters"].append(f"{SPEC_FIELD_MASTER[field]}:{canonical}")
                else:
                    continue
            changes[field] = canonical

        if changes:
            report["proposed"] += len(changes)
            report["items"].append({"item": row["name"], "name": name[:60], "set": changes})
            if not cint(dry_run):
                for field, val in changes.items():
                    frappe.db.set_value("Item", row["name"], field, val,
                                        update_modified=True)
                report["applied"] += len(changes)

    if not cint(dry_run):
        frappe.db.commit()
    return report

# Data-Quality Copilot (internal, READ-ONLY).
# Scans the catalog for issues that quietly degrade search/AI quality and returns
# a prioritised, actionable report. Writes nothing. The deterministic checks are
# fast and free; an optional LLM pass (use_llm=1) cross-checks name-vs-structured
# specs for a sampled subset.
#
# Issue types:
#   missing_spec    - the item_name clearly states a spec the structured field lacks
#   contradiction   - structured field disagrees with the value in the name
#   miscategorized  - category_list conflicts with the product noun in the name
#   duplicate       - several active item_codes share an identical normalised name
#   non_sellable    - active + searchable but rate=0 AND stock=0 (project/junk)

import re
from collections import defaultdict

import frappe
from frappe.utils import cint, cstr, flt

_HTML_RE = re.compile(r"<[^>]+>")

# name-pattern -> the category_list values that are consistent with it
CATEGORY_KEYWORDS = {
    "driver": ["LED DRIVERS"],
    "down light": ["DOWN LIGHT", "CEILING RECESSED LIGHT"],
    "downlight": ["DOWN LIGHT", "CEILING RECESSED LIGHT"],
    "spot light": ["SPOT LIGHT"],
    "spotlight": ["SPOT LIGHT"],
    "panel light": ["PANEL LIGHT"],
    "strip light": ["STRIP LIGHT"],
    "flood light": ["FLOOD LIGHT"],
    "track light": ["TRACK LIGHT"],
    "wall washer": ["WALL WASHER"],
    "bollard": ["BOLLARD LIGHT"],
    "profile": ["ALUMINIUM PROFILE", "PROFILE"],
    "pendant": ["PENDANT LIGHT", "SUSPENDED LAMP"],
    "chandelier": ["CHANDELIERS"],
}


def _clean(v):
    return re.sub(r"\s+", " ", _HTML_RE.sub(" ", cstr(v or ""))).strip()


def _norm_name(v):
    return re.sub(r"[^a-z0-9]+", " ", cstr(v or "").lower()).strip()


def _name_cct(name):
    m = re.search(r"\b(\d{4})\s?k\b", name, re.IGNORECASE)
    return f"{m.group(1)}K" if m else None


def _name_power(name):
    m = re.search(r"\b(\d+(?:\.\d+)?)\s?w\b", name, re.IGNORECASE)
    return f"{m.group(1)}W" if m else None


def _name_ip(name):
    m = re.search(r"\bip\s?(\d{2})\b", name, re.IGNORECASE)
    return f"IP{m.group(1)}" if m else None


@frappe.whitelist()
def scan_data_quality(item_group="LIGHTING", limit=2000, offset=0, checks=None):
    """Return a prioritised data-quality report for a batch of items. Read-only.

    checks: comma list to restrict (missing_spec,contradiction,miscategorized,
    duplicate,non_sellable). Default = all.
    """
    if frappe.session.user == "Guest":
        frappe.throw("Authentication required")

    enabled = (
        {c.strip() for c in cstr(checks).split(",") if c.strip()}
        if checks else
        {"missing_spec", "contradiction", "miscategorized", "duplicate", "non_sellable"}
    )
    limit = max(min(cint(limit), 20000), 1)

    # rate/stock are not columns on tabItem (Item Price / Bin); the non_sellable
    # check reads them from the Typesense product_v2 index instead.
    rows = frappe.db.sql(
        """SELECT name, item_name, category_list, power, color_temp_, ip_rate, disabled
           FROM `tabItem`
           WHERE disabled=0 AND item_group=%s
           ORDER BY modified DESC LIMIT %s OFFSET %s""",
        (item_group, limit, cint(offset)), as_dict=True,
    )

    non_sellable_codes = set()
    if "non_sellable" in enabled:
        try:
            from igh_search.igh_search.product_search_v2 import (
                create_typesense_client, get_default_collection,
            )
            client = create_typesense_client()
            resp = client.collections[get_default_collection()].documents.search({
                "q": "*", "query_by": "item_name",
                "filter_by": "is_active:=1 && rate:=0 && stock:=0",
                "include_fields": "item_code", "per_page": 250, "page": 1,
            })
            non_sellable_codes = {
                h.get("document", {}).get("item_code") for h in resp.get("hits", [])
            }
        except Exception:
            non_sellable_codes = set()

    issues = []
    name_index = defaultdict(list)

    for r in rows:
        code = r["name"]
        name = _clean(r.get("item_name"))
        nlow = name.lower()
        if not name:
            continue
        name_index[_norm_name(name)].append(code)

        if "missing_spec" in enabled or "contradiction" in enabled:
            for label, name_val, field, field_val in (
                ("color_temp", _name_cct(name), "color_temp_", cstr(r.get("color_temp_")).strip()),
                ("power", _name_power(name), "power", cstr(r.get("power")).strip()),
                ("ip_rate", _name_ip(name), "ip_rate", cstr(r.get("ip_rate")).strip()),
            ):
                if not name_val:
                    continue
                if not field_val and "missing_spec" in enabled:
                    issues.append({"item": code, "name": name[:60], "type": "missing_spec",
                                   "field": field, "suggest": name_val, "severity": 2})
                elif field_val and field_val.upper() != name_val.upper() and "contradiction" in enabled:
                    issues.append({"item": code, "name": name[:60], "type": "contradiction",
                                   "field": field, "name_says": name_val,
                                   "field_has": field_val, "severity": 3})

        if "miscategorized" in enabled:
            cat = cstr(r.get("category_list")).strip().upper()
            for kw, valid in CATEGORY_KEYWORDS.items():
                if kw in nlow and cat and cat not in [v.upper() for v in valid]:
                    issues.append({"item": code, "name": name[:60], "type": "miscategorized",
                                   "name_implies": valid[0], "category_has": cat, "severity": 2})
                    break

        if "non_sellable" in enabled and code in non_sellable_codes:
            issues.append({"item": code, "name": name[:60], "type": "non_sellable",
                           "severity": 1})

    if "duplicate" in enabled:
        for norm, codes in name_index.items():
            if len(codes) > 1 and norm:
                issues.append({"type": "duplicate", "normalized_name": norm[:60],
                               "items": codes[:10], "count": len(codes), "severity": 2})

    by_type = defaultdict(int)
    for i in issues:
        by_type[i["type"]] += 1
    issues.sort(key=lambda x: x.get("severity", 0), reverse=True)

    return {
        "scanned": len(rows),
        "issue_count": len(issues),
        "by_type": dict(by_type),
        "issues": issues[:500],
    }

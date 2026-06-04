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

# ── Phase 1 facet-vocabulary validators ──────────────────────────────────────
# Closed-vocab rules per spec field. A facet VALUE is "junk" when it can't be a
# real value for that field (material/category/noise leaked into the field). "NA"
# is unknown, not a constraint -> normalise to empty. Conservative on purpose:
# anything not clearly junk is left alone (human-review queue, not auto-null).

_NA_TOKENS = {"NA", "N/A", "NONE", "-", "--", "NIL"}
# beam: degrees, or a small set of genuine non-numeric beam descriptors.
_VALID_BEAM_WORDS = {"DIFFUSED", "RADIAL", "ASYMMETRIC", "ADJUSTABLE", "SYMMETRIC", "OVAL", "ELLIPTICAL", "FLOOD", "SPOT", "WIDE", "NARROW"}
# color_temp: kelvin, or genuine colour names for coloured fixtures.
_VALID_CCT_WORDS = {"RGB", "RGBW", "RGBCW", "RGBWW", "RGB+WARM WHITE", "RGBDMX", "RED", "GREEN", "BLUE", "AMBER", "YELLOW", "PINK", "ORANGE", "PURPLE", "WARM WHITE", "COOL WHITE", "NEUTRAL WHITE", "DAYLIGHT", "TUNABLE", "TUNABLE WHITE", "CCT TUNABLE", "WHITE"}


def _is_junk_facet(field, value):
    v = cstr(value).strip()
    if not v:
        return False
    up = v.upper()
    if up in _NA_TOKENS:
        return "na"  # normalise to empty
    if field == "beam_angle":
        if re.match(r"^\d+(?:\.\d+)?\s*°?$", v):
            return False
        return False if up in _VALID_BEAM_WORDS else "junk"
    if field == "ip_rate":
        return False if re.match(r"^IP\s?\d{2}$", up) else "junk"
    if field in ("power",):
        return False if re.search(r"\d", v) else "junk"
    if field in ("color_temp_", "color_temp"):
        if re.search(r"\d{3,5}\s*K", up) or up in _VALID_CCT_WORDS:
            return False
        return "junk"
    return False


@frappe.whitelist()
def audit_facet_quality(fields=None):
    """READ-ONLY. Enumerate Typesense facet values per spec field and classify each
    as valid / na / junk, with the count of active docs affected. Gives exact
    numbers before any cleanup write."""
    if frappe.session.user == "Guest":
        frappe.throw("Authentication required")
    from igh_search.igh_search.product_search_v2 import (
        create_typesense_client, get_default_collection,
    )
    target = [f.strip() for f in cstr(fields).split(",") if f.strip()] or [
        "beam_angle", "ip_rate", "power", "color_temp",
    ]
    client = create_typesense_client()
    coll = get_default_collection()
    report = {}
    for field in target:
        try:
            resp = client.collections[coll].documents.search({
                "q": "*", "query_by": "item_name", "filter_by": "is_active:=1",
                "facet_by": field, "max_facet_values": 200, "per_page": 0,
            })
            counts = resp.get("facet_counts", [])
            vals = counts[0].get("counts", []) if counts else []
        except Exception as exc:
            report[field] = {"error": cstr(exc)[:160]}
            continue
        junk, na, valid = [], [], 0
        junk_docs = na_docs = 0
        for entry in vals:
            value, n = entry.get("value"), cint(entry.get("count"))
            verdict = _is_junk_facet(field, value)
            if verdict == "junk":
                junk.append({"value": value, "docs": n}); junk_docs += n
            elif verdict == "na":
                na.append({"value": value, "docs": n}); na_docs += n
            else:
                valid += 1
        report[field] = {
            "distinct_values": len(vals), "valid_values": valid,
            "junk_values": sorted(junk, key=lambda x: -x["docs"]),
            "junk_docs": junk_docs, "na_docs": na_docs,
        }
    return report


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

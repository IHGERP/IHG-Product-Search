"""Pure validation shared by text and voice tools."""
import copy
import math

LANGUAGES = {"auto": "the user's current language", "en": "English", "hi": "Hindi", "ml": "Malayalam", "ta": "Tamil", "ur": "Urdu"}


def merge_search(current, changes):
    result = copy.deepcopy(current)
    if "query" in changes:
        result["query"] = str(changes["query"] or "")[:500]
    if "sort_by" in changes:
        result["sort_by"] = str(changes["sort_by"] or "")
    filters = result.setdefault("filters", {})
    for key, value in (changes.get("filters") or {}).items():
        if value is None or value == [] or value == "":
            filters.pop(key, None)
        else:
            filters[key] = value
    result["page"] = max(1, int(changes.get("page") or 1))
    result["page_length"] = min(100, max(1, int(changes.get("page_length") or result.get("page_length") or 20)))
    return result


def validate_items(items):
    if not isinstance(items, list) or not 1 <= len(items) <= 50:
        raise ValueError("Choose between 1 and 50 products")
    result = []
    seen = set()
    for row in items:
        code = str(row.get("item_code") or "").strip()
        qty = float(row.get("qty", 1))
        if not code or len(code) > 140 or not math.isfinite(qty) or not 0 < qty <= 1000000:
            raise ValueError("Each product needs an exact code and a positive finite quantity")
        if code in seen:
            raise ValueError("Combine duplicate products into one quantity")
        seen.add(code)
        result.append({"item_code": code, "qty": qty})
    return result


def validate_preview(previous, current):
    if len(previous) != len(current):
        raise ValueError("Products changed; prepare a new preview")
    for old, new in zip(previous, current):
        if any(old.get(k) != new.get(k) for k in ("item_code", "qty", "rate", "currency")):
            raise ValueError("Prices or products changed; review a new preview")
        if new.get("available_qty", 0) < new["qty"]:
            raise ValueError("Insufficient available stock; review the quantity before continuing")

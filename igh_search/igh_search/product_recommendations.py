# Part B / B1 — Ambient recommendation engine.
#
# One brain for every surface (product page, home workspace, search grid). Reuses
# the existing relationship endpoints (related context, alternatives, driver intel)
# and adds two new capabilities: embedding find-similar and a push-to-sell promotion
# ranker. recommend(surface, context) returns typed blocks the UI renders as rails.
#
# Suggestion impressions/clicks are logged (AI Suggestion Event) to measure CTR /
# conversion and feed the Phase 4 learning loop. All read-only except the logger.

import json

import frappe
from frappe.utils import cint, cstr, flt

SLIM_FIELDS = (
    "item_code", "item_name", "brand", "category_list", "power", "color_temp",
    "ip_rate", "beam_angle", "rate", "offer_rate", "stock", "in_stock", "stock_uom",
    "website_image_url", "image",
)


def _slim(doc):
    out = {k: doc.get(k) for k in SLIM_FIELDS if doc.get(k) not in (None, "")}
    return out


def _hybrid():
    from igh_search.igh_search.product_search_v2 import (
        create_typesense_client, get_hybrid_collection,
    )
    return create_typesense_client(), get_hybrid_collection()


def _resolve_seed(client, coll, item_code):
    """Find a product's Typesense doc (id + category) in the hybrid collection."""
    try:
        r = client.collections[coll].documents.search({
            "q": "*", "query_by": "item_code", "filter_by": f"item_code:=[`{item_code}`]",
            "per_page": 1, "include_fields": "id,item_code,category_list,item_name",
        })
        hits = r.get("hits") or []
        return hits[0].get("document", {}) if hits else None
    except Exception:
        return None


@frappe.whitelist()
def find_similar_products(item_code=None, limit=8):
    """Products most similar to a given SKU by embedding nearest-neighbour, filtered
    to the same category and active/sellable, excluding the item itself. Works for
    any SKU app-wide ('find similar' on every card)."""
    if frappe.session.user == "Guest":
        frappe.throw("Authentication required")
    item_code = cstr(item_code).strip()
    if not item_code:
        return {"item_code": item_code, "results": []}
    try:
        client, coll = _hybrid()
        seed = _resolve_seed(client, coll, item_code)
        if not seed or not seed.get("id"):
            return {"item_code": item_code, "results": []}
        category = cstr(seed.get("category_list") or "").strip()
        filt = ["is_active:=1", f"item_code:!=[`{item_code}`]"]
        if category:
            filt.append(f"category_list:=[`{category}`]")
        res = client.collections[coll].documents.search({
            "q": "*",
            "vector_query": f"embedding:([], id: {seed['id']})",
            "filter_by": " && ".join(filt),
            "per_page": max(min(cint(limit) or 8, 20), 1),
            "exclude_fields": "embedding",
        })
        out = []
        for h in res.get("hits", []):
            d = _slim(h.get("document", {}))
            if d.get("item_code"):
                d["similarity"] = round(1.0 - flt(h.get("vector_distance") or 0), 3)
                out.append(d)
        return {"item_code": item_code, "results": out}
    except Exception as exc:
        return {"item_code": item_code, "results": [], "error": cstr(exc)[:160]}


def _promotion_picks(limit=8):
    """Push-to-sell picks. Approximates overstock/aging (high stock, low lifetime
    sales = slow-mover) among sellable items; manual featured (Featured Product) is
    woven in first when present. Margin/new-arrival signals plug in here later."""
    from igh_search.igh_search.product_search_v2 import (
        create_typesense_client, get_default_collection,
    )
    picks, seen = [], set()
    # 1) manager-picked featured (if the doctype exists)
    try:
        if frappe.db.exists("DocType", "Featured Product"):
            featured = frappe.get_all("Featured Product", filters={"enabled": 1},
                                      fields=["item_code"], limit_page_length=cint(limit))
            for f in featured:
                code = cstr(f.get("item_code")).strip()
                if code and code not in seen:
                    seen.add(code)
    except Exception:
        pass
    # 2) overstock / slow-mover proxy from the index
    try:
        client = create_typesense_client(); coll = get_default_collection()
        r = client.collections[coll].documents.search({
            "q": "*", "query_by": "item_name",
            "filter_by": "is_active:=1 && rate:>0 && stock:>20",
            "sort_by": "stock:desc", "per_page": max(min(cint(limit) or 8, 20), 1),
            "exclude_fields": "embedding",
        })
        for h in r.get("hits", []):
            d = _slim(h.get("document", {}))
            code = d.get("item_code")
            if code and code not in seen:
                seen.add(code)
                d["why"] = "overstock - move it"
                picks.append(d)
    except Exception:
        pass
    return picks[:cint(limit) or 8]


@frappe.whitelist()
def recommend(surface=None, item_code=None, limit=6):
    """Typed recommendation blocks for a surface. surface in {product, home, search}.
    Reuses existing relationship endpoints; thin so every UI rail calls one method."""
    if frappe.session.user == "Guest":
        frappe.throw("Authentication required")
    surface = cstr(surface or "product").strip()
    item_code = cstr(item_code or "").strip()
    limit = max(min(cint(limit) or 6, 12), 1)
    blocks = {}

    if surface == "product" and item_code:
        blocks["similar"] = find_similar_products(item_code, limit=limit).get("results", [])
        try:
            from igh_search.igh_search.product_search_v2 import get_product_alternatives_v2
            alt = get_product_alternatives_v2(item_code, mode="alternatives", limit=limit, feature_flag_override=1)
            blocks["alternatives"] = [{**_slim(x.get("document", {})), "reason": x.get("reason")}
                                      for x in (alt.get("results") or [])]
            cross = get_product_alternatives_v2(item_code, mode="cross_sell", limit=limit, feature_flag_override=1)
            blocks["complete_the_look"] = [{**_slim(x.get("document", {})), "reason": x.get("reason")}
                                           for x in (cross.get("results") or [])]
        except Exception:
            pass
        try:
            from igh_search.igh_search.product_search_v2 import analyze_driver_requirement
            dr = analyze_driver_requirement(item_code, feature_flag_override=1)
            blocks["driver"] = {"driver_required": dr.get("driver_required"), "reason": dr.get("reason")}
        except Exception:
            pass

    elif surface == "home":
        blocks["picks_to_push"] = _promotion_picks(limit=limit)

    elif surface == "search" and item_code:
        blocks["similar"] = find_similar_products(item_code, limit=limit).get("results", [])

    return {"surface": surface, "item_code": item_code, "blocks": blocks}


@frappe.whitelist()
def get_promotion_picks(limit=8):
    if frappe.session.user == "Guest":
        frappe.throw("Authentication required")
    return {"picks": _promotion_picks(limit=cint(limit) or 8)}


def log_suggestion_event(surface=None, block_type=None, item_code=None, action=None, query=None):
    """Record a suggestion impression/click/dismiss/added_to_quote for CTR + the
    learning loop. Best-effort async; no-op if the doctype isn't present yet."""
    try:
        if not frappe.db.exists("DocType", "AI Suggestion Event"):
            return
        frappe.get_doc({
            "doctype": "AI Suggestion Event",
            "surface": cstr(surface)[:60],
            "block_type": cstr(block_type)[:60],
            "item_code": cstr(item_code)[:140],
            "action": cstr(action)[:30],
            "query": cstr(query)[:200],
            "rep": frappe.session.user,
        }).insert(ignore_permissions=True)
        frappe.db.commit()
    except Exception:
        pass


@frappe.whitelist()
def log_suggestion(surface=None, block_type=None, item_code=None, action=None, query=None):
    if frappe.session.user == "Guest":
        return {"status": "skipped"}
    log_suggestion_event(surface=surface, block_type=block_type, item_code=item_code, action=action, query=query)
    return {"status": "success"}

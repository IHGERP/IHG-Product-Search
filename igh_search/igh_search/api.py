"""
IHG Search – Sales Portal API
File location on server: apps/igh_search/igh_search/igh_search/api.py

Implements:
  Cart       : get_cart_items, insert_cart_items, update_cartitem,
               delete_cart_items, clear_cart, clear_cartitem,
               move_item_to_cart, move_all_tocart
  Quotation  : search_opportunities, get_user_opportunities,
               create_quotation_from_portal, get_recent_quotations
  Product    : get_product_info, get_product_details
  Masters    : get_all_masters
  AI Search  : ai_product_search (keyword fallback – replace with LLM later)
  Customer   : get_customer_info

Cart storage: Frappe Redis cache, key = "ihg_cart:{user}", TTL = 24 h.
No new Doctypes required.
"""

import json
import uuid
from collections import defaultdict

import frappe
from frappe import _
from frappe.utils import flt, cstr, getdate, nowdate
from igh_search.igh_search.lumen_normalization import (
    build_lumen_overlap_filter,
    normalize_lumen_fields,
)

try:
    import requests
except Exception:  # pragma: no cover
    requests = None


# ─────────────────────────────────────────────────────────────────────────────
#  CART HELPERS  (Redis-backed, per user, 24-hour TTL)
# ─────────────────────────────────────────────────────────────────────────────

_CART_TTL = 86_400  # seconds


def _cart_key():
    return f"ihg_cart:{frappe.session.user}"


def _get_raw_cart():
    """Return the current user's cart as a Python list."""
    raw = frappe.cache().get_value(_cart_key())
    if not raw:
        return []
    try:
        return json.loads(raw) if isinstance(raw, str) else list(raw)
    except Exception:
        return []


def _save_raw_cart(items):
    frappe.cache().set_value(
        _cart_key(), json.dumps(items), expires_in_sec=_CART_TTL
    )


def _get_item_rate(item_code):
    """Return the selling rate for an item from the default price list."""
    price_list = (
        frappe.db.get_single_value("Selling Settings", "selling_price_list")
        or "Standard Selling"
    )
    rate = frappe.db.get_value(
        "Item Price",
        {"item_code": item_code, "price_list": price_list, "selling": 1},
        "price_list_rate",
    )
    return flt(rate)


def _enrich_item(item_code, qty):
    """
    Fetch item master data and build a cart-item dict.
    Returns None if the item does not exist.
    """
    try:
        item = frappe.db.get_value(
            "Item",
            item_code,
            ["item_code", "item_name", "stock_uom", "brand", "item_group", "image"],
            as_dict=True,
        )
        if not item:
            return None

        rate = _get_item_rate(item_code)
        qty = flt(qty) or 1

        return {
            "name": str(uuid.uuid4()),         # used as cart_id on frontend
            "item_code": item.item_code,
            "item_name": item.item_name or item_code,
            "quantity": qty,
            "rate": rate,
            "amount": round(qty * rate, 2),
            "stock_uom": item.stock_uom or "Nos",
            "brand": item.brand or "",
            "item_group": item.item_group or "",
            "website_image_url": item.image or "",
            "image": item.image or "",
        }
    except Exception:
        frappe.log_error(frappe.get_traceback(), "IHG Cart: _enrich_item error")
        return None


def _cart_totals(items):
    total = sum(flt(i.get("rate", 0)) * flt(i.get("quantity", 0)) for i in items)
    return round(total, 2)


def _require_login():
    if not frappe.session.user or frappe.session.user == "Guest":
        frappe.throw(_("Authentication required"), frappe.AuthenticationError)


def _apply_lumen_normalization(item_doc):
    """
    Normalize lumen_output text into numeric filter fields.
    Safe to call during sync/backfill/update pipelines.
    """
    normalized = normalize_lumen_fields(item_doc.get("lumen_output"))
    for key, value in normalized.items():
        item_doc[key] = value
    return item_doc


@frappe.whitelist()
def backfill_lumen_normalization(limit=500, offset=0, commit=0):
    """
    Backfill helper for ERP Item records.
    Expected custom fields on Item:
      lumen_raw, lumen_min, lumen_max, lumen_unit, lumen_values, lumen_parse_status
    """
    _require_login()

    limit = int(limit or 500)
    offset = int(offset or 0)
    rows = frappe.get_all(
        "Item",
        fields=["name", "lumen_output"],
        start=offset,
        page_length=limit,
        order_by="modified desc",
    )

    status_counts = {"parsed": 0, "partial": 0, "invalid": 0, "unsupported": 0}
    updated = 0

    for row in rows:
        normalized = normalize_lumen_fields(row.get("lumen_output"))
        status = normalized.get("lumen_parse_status") or "invalid"
        status_counts[status] = status_counts.get(status, 0) + 1

        frappe.db.set_value(
            "Item",
            row["name"],
            {
                "lumen_raw": normalized["lumen_raw"],
                "lumen_min": normalized["lumen_min"],
                "lumen_max": normalized["lumen_max"],
                "lumen_unit": normalized["lumen_unit"],
                "lumen_values": json.dumps(normalized["lumen_values"] or []),
                "lumen_parse_status": normalized["lumen_parse_status"],
            },
            update_modified=False,
        )
        updated += 1

    if int(commit or 0):
        frappe.db.commit()

    return {"status": "success", "updated": updated, "status_counts": status_counts}


def build_typesense_filter_with_lumen(base_filter, lumen_unit=None, lumen_min=None, lumen_max=None):
    clause = build_lumen_overlap_filter(lumen_unit, lumen_min, lumen_max)
    if not clause:
        return base_filter or ""
    return f"{base_filter} && {clause}" if base_filter else clause


def _calculate_star_rating(total_sold_qty):
    """
    Business rule (lifetime, excluding internal customers):
      qty <= 50    -> 3.5
      qty >= 500   -> 5.0
      between      -> linear interpolation, rounded to nearest 0.1
    """
    qty = flt(total_sold_qty or 0)
    if qty <= 50:
        return 3.5
    if qty >= 500:
        return 5.0

    rating = 3.5 + ((qty - 50.0) / 450.0) * 1.5
    return round(rating, 1)


def _fetch_sales_metrics_by_item(item_codes):
    if not item_codes:
        return {}

    rows = frappe.db.sql(
        """
        SELECT
            sii.item_code AS item_code,
            SUM(COALESCE(sii.qty, 0)) AS total_sold_qty,
            COUNT(DISTINCT si.name) AS invoice_count
        FROM `tabSales Invoice Item` sii
        INNER JOIN `tabSales Invoice` si
            ON si.name = sii.parent
        WHERE
            sii.item_code IN %(item_codes)s
            AND si.docstatus = 1
            AND COALESCE(si.is_internal_customer, 0) = 0
        GROUP BY sii.item_code
        """,
        {"item_codes": tuple(item_codes)},
        as_dict=True,
    )

    metrics = {}
    for row in rows:
        qty = flt(row.get("total_sold_qty") or 0)
        invoices = int(row.get("invoice_count") or 0)
        metrics[row["item_code"]] = {
            "total_sold_qty_lifetime": qty,
            "customer_count": invoices,
            "product_star_rating": _calculate_star_rating(qty),
        }
    return metrics


def _fetch_manufactured_item_set(item_codes):
    if not item_codes:
        return set()

    rows = frappe.db.sql(
        """
        SELECT DISTINCT sed.item_code AS item_code
        FROM `tabStock Entry Detail` sed
        INNER JOIN `tabStock Entry` se
            ON se.name = sed.parent
        WHERE
            sed.item_code IN %(item_codes)s
            AND COALESCE(sed.is_finished_item, 0) = 1
            AND se.docstatus = 1
            AND se.stock_entry_type = 'Manufacture'
        """,
        {"item_codes": tuple(item_codes)},
        as_dict=True,
    )
    return {row["item_code"] for row in rows if row.get("item_code")}


def _compute_item_intelligence(item_codes):
    item_codes = [cstr(code).strip() for code in (item_codes or []) if cstr(code).strip()]
    if not item_codes:
        return {}

    sales_metrics = _fetch_sales_metrics_by_item(item_codes)
    manufactured_items = _fetch_manufactured_item_set(item_codes)

    result = {}
    for item_code in item_codes:
        base = sales_metrics.get(
            item_code,
            {
                "total_sold_qty_lifetime": 0.0,
                "customer_count": 0,
                "product_star_rating": _calculate_star_rating(0),
            },
        )
        base["is_manufactured_item"] = 1 if item_code in manufactured_items else 0
        result[item_code] = base

    return result


@frappe.whitelist()
def backfill_product_intelligence(limit=500, offset=0, commit=0):
    """
    Backfill helper that computes product intelligence and pushes directly
    to Typesense (no Item custom fields required).

    Params:
      limit, offset: pagination over active Item records
      commit: kept for API compatibility; ignored here
      dry_run: if 1, only returns payload preview/counts
      collection: optional Typesense collection name override
      typesense_host/typesense_api_key: optional runtime overrides
    """
    _require_login()

    limit = int(limit or 500)
    offset = int(offset or 0)
    dry_run = int(frappe.form_dict.get("dry_run") or 0)
    collection_override = cstr(frappe.form_dict.get("collection") or "").strip()
    host_override = cstr(frappe.form_dict.get("typesense_host") or "").strip()
    api_key_override = cstr(frappe.form_dict.get("typesense_api_key") or "").strip()

    items = frappe.get_all(
        "Item",
        fields=["item_code"],
        filters={"disabled": 0},
        start=offset,
        page_length=limit,
        order_by="name asc",
    )
    item_codes = [row.get("item_code") for row in items if row.get("item_code")]
    intelligence = _compute_item_intelligence(item_codes)

    rating_buckets = defaultdict(int)
    manufactured_count = 0
    docs = []

    for item_code in item_codes:
        metrics = intelligence.get(item_code, {})

        rating_value = flt(metrics.get("product_star_rating") or 0)
        rating_buckets[str(rating_value)] += 1
        if int(metrics.get("is_manufactured_item") or 0) == 1:
            manufactured_count += 1

        docs.append(
            {
                "id": item_code,
                "item_code": item_code,
                "total_sold_qty_lifetime": flt(metrics.get("total_sold_qty_lifetime") or 0),
                "product_star_rating": rating_value,
                "customer_count": int(metrics.get("customer_count") or 0),
                "is_manufactured_item": int(metrics.get("is_manufactured_item") or 0),
            }
        )

    if not dry_run and docs:
        _update_product_intelligence_to_typesense(
            docs,
            collection_override=collection_override,
            host_override=host_override,
            api_key_override=api_key_override,
        )

    return {
        "status": "success",
        "updated": len(docs),
        "dry_run": bool(dry_run),
        "manufactured_items": manufactured_count,
        "sample": docs[:5],
        "rating_distribution": dict(sorted(rating_buckets.items(), key=lambda x: float(x[0]))),
    }


def _resolve_typesense_config(collection_override="", host_override="", api_key_override=""):
    conf = frappe.conf
    host = host_override or conf.get("typesense_host") or conf.get("TYPESENSE_HOST")
    api_key = api_key_override or conf.get("typesense_api_key") or conf.get("TYPESENSE_API_KEY")
    collection = (
        collection_override
        or conf.get("igh_search_v2_default_collection")
        or conf.get("typesense_collection")
        or conf.get("TYPESENSE_COLLECTION")
        or "product_v2"
    )

    if (not host or not api_key) and frappe.db.exists("DocType", "Typesense Settings"):
        settings = frappe.get_doc("Typesense Settings")
        protocol = settings.protocol if settings.protocol in ("http", "https") else "https"
        if not host:
            host = f"{protocol}://{settings.host}:{settings.port}"
        if not api_key:
            try:
                api_key = settings.get_password("api_key")
            except Exception:
                api_key = ""

    host = cstr(host or "").strip().rstrip("/")
    api_key = cstr(api_key or "").strip()
    collection = cstr(collection or "").strip()

    if not host or not api_key or not collection:
        frappe.throw(
            _(
                "Typesense config missing. Please set typesense_host, typesense_api_key and typesense_collection in site_config.json "
                "or pass overrides in the API call."
            )
        )

    return {"host": host, "api_key": api_key, "collection": collection}


def _update_product_intelligence_to_typesense(
    docs,
    collection_override="",
    host_override="",
    api_key_override="",
):
    if requests is None:
        frappe.throw(_("Python package `requests` is required for Typesense sync."))

    config = _resolve_typesense_config(
        collection_override=collection_override,
        host_override=host_override,
        api_key_override=api_key_override,
    )

    headers = {
        "X-TYPESENSE-API-KEY": config["api_key"],
        "Content-Type": "application/json",
    }
    failures = []
    skipped_missing = 0

    def resolve_doc_id_by_item_code(item_code):
        try:
            response = requests.get(
                f"{config['host']}/collections/{config['collection']}/documents/search",
                params={
                    "q": cstr(item_code),
                    "query_by": "item_code",
                    "per_page": 5,
                    "include_fields": "id,item_code",
                },
                headers={"X-TYPESENSE-API-KEY": config["api_key"]},
                timeout=20,
            )
            if response.status_code >= 400:
                return None
            payload = response.json()
            for hit in payload.get("hits", []):
                document = hit.get("document", {})
                if cstr(document.get("item_code")) == cstr(item_code):
                    return cstr(document.get("id") or "")
            return None
        except Exception:
            return None
    for doc in docs:
        doc_id = cstr(doc.get("id") or doc.get("item_code") or "").strip()
        if not doc_id:
            continue

        payload = {
            "product_star_rating": doc.get("product_star_rating"),
            "customer_count": doc.get("customer_count"),
            "is_manufactured_item": doc.get("is_manufactured_item"),
            "total_sold_qty_lifetime": doc.get("total_sold_qty_lifetime"),
        }
        url = f"{config['host']}/collections/{config['collection']}/documents/{doc_id}"
        response = requests.patch(url, headers=headers, data=json.dumps(payload), timeout=30)
        if response.status_code == 404:
            resolved_doc_id = resolve_doc_id_by_item_code(doc.get("item_code") or doc_id)
            if resolved_doc_id:
                retry_url = f"{config['host']}/collections/{config['collection']}/documents/{resolved_doc_id}"
                retry_response = requests.patch(
                    retry_url, headers=headers, data=json.dumps(payload), timeout=30
                )
                if retry_response.status_code < 400:
                    continue
                failures.append(
                    {
                        "id": doc_id,
                        "resolved_id": resolved_doc_id,
                        "status": retry_response.status_code,
                        "error": cstr(retry_response.text)[:400],
                    }
                )
                continue
            skipped_missing += 1
            continue
        if response.status_code >= 400:
            failures.append(
                {
                    "id": doc_id,
                    "status": response.status_code,
                    "error": cstr(response.text)[:400],
                }
            )

    if failures:
        frappe.throw(
            _("Typesense update failed for {0} document(s). Sample: {1}").format(
                len(failures), json.dumps(failures[:5])
            )
        )

    return {"updated": len(docs) - skipped_missing, "failed": 0, "skipped_missing": skipped_missing}


@frappe.whitelist()
def sync_product_intelligence_to_typesense(
    limit=2000,
    offset=0,
    chunk_size=250,
    dry_run=0,
    collection="",
    typesense_host="",
    typesense_api_key="",
):
    """
    Nightly/on-demand sync endpoint:
    computes product intelligence and pushes to Typesense directly.
    """
    _require_login()

    limit = max(1, int(limit or 2000))
    offset = max(0, int(offset or 0))
    chunk_size = max(1, int(chunk_size or 250))
    dry_run = int(dry_run or 0)

    rows = frappe.get_all(
        "Item",
        fields=["item_code"],
        filters={"disabled": 0},
        start=offset,
        page_length=limit,
        order_by="name asc",
    )
    item_codes = [r.get("item_code") for r in rows if r.get("item_code")]
    if not item_codes:
        return {"status": "success", "updated": 0, "dry_run": bool(dry_run), "chunks": 0}

    total_updated = 0
    chunks = 0
    for start in range(0, len(item_codes), chunk_size):
        chunk_codes = item_codes[start : start + chunk_size]
        metrics_map = _compute_item_intelligence(chunk_codes)
        docs = []
        for item_code in chunk_codes:
            metrics = metrics_map.get(item_code, {})
            docs.append(
                {
                    "id": item_code,
                    "item_code": item_code,
                    "total_sold_qty_lifetime": flt(metrics.get("total_sold_qty_lifetime") or 0),
                    "product_star_rating": flt(metrics.get("product_star_rating") or 3.5),
                    "customer_count": int(metrics.get("customer_count") or 0),
                    "is_manufactured_item": int(metrics.get("is_manufactured_item") or 0),
                }
            )
        if not dry_run:
            _update_product_intelligence_to_typesense(
                docs,
                collection_override=collection,
                host_override=typesense_host,
                api_key_override=typesense_api_key,
            )
        total_updated += len(docs)
        chunks += 1

    return {
        "status": "success",
        "updated": total_updated,
        "dry_run": bool(dry_run),
        "chunks": chunks,
        "offset": offset,
        "limit": limit,
    }


@frappe.whitelist()
def run_nightly_product_intelligence_sync(**kwargs):
    """
    Wrapper intended for scheduler usage (nightly cron).
    Syncs all active items in chunks.
    """
    rows = frappe.get_all(
        "Item",
        fields=["name"],
        filters={"disabled": 0},
        page_length=1,
    )
    # Quick no-op shortcut
    if not rows:
        return {"status": "success", "updated": 0, "chunks": 0}

    return sync_product_intelligence_to_typesense(
        limit=200000,
        offset=0,
        chunk_size=250,
        dry_run=0,
    )


# ─────────────────────────────────────────────────────────────────────────────
#  CART APIs
# ─────────────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_cart_items(**kwargs):
    _require_login()

    items = _get_raw_cart()
    total = _cart_totals(items)

    return {
        "status": "success",
        "cart": {"marketplace_items": items},
        "total": total,
        "grand_total": total,
    }


@frappe.whitelist()
def insert_cart_items(item_code=None, qty=1, **kwargs):
    _require_login()

    if not item_code:
        return {"status": "error", "message": "item_code is required"}

    qty = flt(qty) or 1
    items = _get_raw_cart()

    existing = next((i for i in items if i["item_code"] == item_code), None)
    if existing:
        # Increment if already in cart
        existing["quantity"] = flt(existing["quantity"]) + qty
        existing["amount"] = round(
            existing["quantity"] * flt(existing.get("rate", 0)), 2
        )
    else:
        new_item = _enrich_item(item_code, qty)
        if not new_item:
            return {"status": "error", "message": f"Item '{item_code}' not found"}
        items.append(new_item)

    _save_raw_cart(items)
    total = _cart_totals(items)
    return {
        "status": "success",
        "message": "Added to cart",
        "cart": {"marketplace_items": items},
        "total": total,
        "grand_total": total,
    }


@frappe.whitelist()
def update_cartitem(item_code=None, qty=0, **kwargs):
    _require_login()

    if not item_code:
        return {"status": "error", "message": "item_code is required"}

    qty = flt(qty)
    items = _get_raw_cart()

    if qty <= 0:
        items = [i for i in items if i["item_code"] != item_code]
    else:
        found = False
        for item in items:
            if item["item_code"] == item_code:
                item["quantity"] = qty
                item["amount"] = round(qty * flt(item.get("rate", 0)), 2)
                found = True
                break
        if not found:
            return {"status": "error", "message": "Item not found in cart"}

    _save_raw_cart(items)
    return {"status": "success", "message": "Cart updated"}


@frappe.whitelist()
def delete_cart_items(cart_id=None, **kwargs):
    _require_login()

    items = _get_raw_cart()
    before = len(items)

    # Match by UUID (cart_id) first, then fall back to item_code
    filtered = [i for i in items if i.get("name") != cart_id]
    if len(filtered) == before:
        filtered = [i for i in items if i.get("item_code") != cart_id]

    _save_raw_cart(filtered)
    return {"status": "success", "message": "Item removed"}


@frappe.whitelist()
def clear_cart(**kwargs):
    _require_login()
    frappe.cache().delete_value(_cart_key())
    return {"status": "success", "message": "Cart cleared"}


# Aliases expected by the api.py wrapper
@frappe.whitelist()
def clear_cartitem(**kwargs):
    return clear_cart(**kwargs)


@frappe.whitelist()
def move_item_to_cart(**kwargs):
    return {"status": "success", "message": "Not applicable for sales portal"}


@frappe.whitelist()
def move_all_tocart(**kwargs):
    return {"status": "success", "message": "Not applicable for sales portal"}


# ─────────────────────────────────────────────────────────────────────────────
#  OPPORTUNITY & QUOTATION APIs
# ─────────────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def search_opportunities(search="", customer_id="", **kwargs):
    _require_login()

    search = cstr(search).strip()

    base_filters = [
        ["status", "not in", ["Closed", "Lost", "Converted"]],
    ]

    if search:
        # Use OR across three fields via SQL
        rows = frappe.db.sql(
            """
            SELECT name, customer_name, customer, title, status, expected_closing
            FROM   `tabOpportunity`
            WHERE  status NOT IN ('Closed', 'Lost', 'Converted')
              AND  (name         LIKE %(s)s
                OR  customer_name LIKE %(s)s
                OR  title        LIKE %(s)s)
            ORDER BY modified DESC
            LIMIT  25
            """,
            {"s": f"%{search}%"},
            as_dict=True,
        )
    else:
        rows = frappe.get_list(
            "Opportunity",
            filters=base_filters,
            fields=["name", "customer_name", "customer", "title", "status", "expected_closing"],
            order_by="modified desc",
            limit=25,
        )

    return {"status": "success", "data": rows}


@frappe.whitelist()
def get_user_opportunities(**kwargs):
    _require_login()

    rows = frappe.get_list(
        "Opportunity",
        filters={"status": ["not in", ["Closed", "Lost", "Converted"]]},
        fields=["name", "customer_name", "customer", "title", "status", "expected_closing"],
        order_by="modified desc",
        limit=50,
    )
    return {"status": "success", "data": rows}


@frappe.whitelist()
def create_quotation_from_portal(opportunity=None, items=None, **kwargs):
    _require_login()

    if not opportunity:
        frappe.throw(_("opportunity is required"))
    if not items:
        frappe.throw(_("items list is required"))

    if isinstance(items, str):
        try:
            items = json.loads(items)
        except Exception:
            frappe.throw(_("items must be a valid JSON list"))

    if not isinstance(items, list) or len(items) == 0:
        frappe.throw(_("items list is empty"))

    # Fetch opportunity to get customer details
    opp = frappe.db.get_value(
        "Opportunity",
        opportunity,
        ["customer", "customer_name", "opportunity_from", "party_name"],
        as_dict=True,
    )
    if not opp:
        frappe.throw(_(f"Opportunity '{opportunity}' not found"))

    # Resolve party
    party_name = opp.customer or opp.party_name or opp.customer_name
    quotation_to = "Customer"

    # If no linked customer, use Lead or fallback
    if not party_name:
        frappe.throw(_("Opportunity has no linked customer"))

    # Build items list
    q_items = []
    for row in items:
        code = cstr(row.get("item_code", "")).strip()
        if not code:
            continue
        if not frappe.db.exists("Item", code):
            frappe.throw(_(f"Item '{code}' does not exist in ERPNext"))
        q_items.append({
            "doctype": "Quotation Item",
            "item_code": code,
            "qty": flt(row.get("qty", 1)) or 1,
            "rate": flt(row.get("rate", 0)),
            "description": cstr(row.get("description", "")),
        })

    if not q_items:
        frappe.throw(_("No valid items provided"))

    # Create quotation
    quotation = frappe.get_doc({
        "doctype": "Quotation",
        "quotation_to": quotation_to,
        "party_name": party_name,
        "opportunity": opportunity,
        "transaction_date": getdate(nowdate()),
        "valid_till": frappe.utils.add_days(nowdate(), 30),
        "order_type": "Sales",
        "items": q_items,
    })
    quotation.flags.ignore_permissions = False   # enforce normal doctype perms
    quotation.insert()

    return {"status": "success", "quotation": quotation.name}


@frappe.whitelist()
def get_recent_quotations(**kwargs):
    _require_login()

    rows = frappe.get_list(
        "Quotation",
        filters={"owner": frappe.session.user},
        fields=[
            "name", "party_name", "customer_name", "opportunity",
            "grand_total", "status", "transaction_date", "creation",
        ],
        order_by="creation desc",
        limit=20,
    )

    # Normalize: frontend expects customer_name
    for r in rows:
        if not r.get("customer_name") and r.get("party_name"):
            r["customer_name"] = r["party_name"]

    return {"status": "success", "data": rows}


# ─────────────────────────────────────────────────────────────────────────────
#  PRODUCT INFO
# ─────────────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_product_info(item_code=None, **kwargs):
    if not item_code:
        return {"status": "error", "message": "item_code is required"}

    item_code = cstr(item_code).strip()
    item = frappe.db.get_value(
        "Item",
        item_code,
        [
            "item_code", "item_name", "description", "brand",
            "item_group", "stock_uom", "image", "min_order_qty",
            "has_variants",
        ],
        as_dict=True,
    )
    if not item:
        return {"status": "error", "message": f"Item '{item_code}' not found"}

    rate = _get_item_rate(item_code)

    # Warehouse-level stock breakdown
    stock_rows = frappe.db.sql(
        """
        SELECT warehouse, actual_qty, reserved_qty,
               GREATEST(actual_qty - reserved_qty, 0) AS available_qty
        FROM   `tabBin`
        WHERE  item_code = %s
        ORDER  BY actual_qty DESC
        """,
        item_code,
        as_dict=True,
    )
    stock_list = [dict(r) for r in stock_rows]
    total_stock = sum(flt(r.actual_qty) for r in stock_rows)
    in_stock = total_stock > 0

    return {
        "item_code":         item.item_code,
        "item_name":         item.item_name,
        "description":       item.description or "",
        "brand":             item.brand or "",
        "item_group":        item.item_group or "",
        "category_list":     item.item_group or "",
        "stock_uom":         item.stock_uom or "Nos",
        "image":             item.image or "",
        "website_image_url": item.image or "",
        "rate":              rate,
        "offer_rate":        0,
        "in_stock":          in_stock,
        "stock":             stock_list,           # array – warehouse breakdown
        "total_stock":       total_stock,          # numeric total for display
        "minimum_order_qty": flt(item.min_order_qty) or 1,
        "has_variants":      item.has_variants,
    }


# Alias: frontend calls get_product_details but the URL resolves to get_product_info
@frappe.whitelist()
def get_product_details(item_code=None, **kwargs):
    return get_product_info(item_code=item_code, **kwargs)


# ─────────────────────────────────────────────────────────────────────────────
#  MASTERS  (for filter panel in search UI)
# ─────────────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_all_masters(**kwargs):
    brands = frappe.db.sql(
        """
        SELECT DISTINCT brand AS name
        FROM   `tabItem`
        WHERE  brand IS NOT NULL AND brand != '' AND disabled = 0
        ORDER  BY brand
        LIMIT  500
        """,
        as_dict=True,
    )

    item_groups = frappe.db.sql(
        """
        SELECT name
        FROM   `tabItem Group`
        WHERE  name != 'All Item Groups'
        ORDER  BY name
        LIMIT  300
        """,
        as_dict=True,
    )

    # Item-level attribute values
    attr_rows = frappe.db.sql(
        """
        SELECT iav.parent AS attribute, iav.attribute_value AS value
        FROM   `tabItem Attribute Value` iav
        ORDER  BY iav.parent, iav.attribute_value
        LIMIT  3000
        """,
        as_dict=True,
    )
    attributes = {}
    for r in attr_rows:
        attributes.setdefault(r.attribute, []).append(r.value)

    # Product intelligence master options for frontend filters.
    star_rating_options = [f"{value:.1f}" for value in [3.5, 3.7, 3.9, 4.1, 4.3, 4.5, 4.7, 4.9, 5.0]]
    happy_customer_options = ["50", "75", "100"]
    manufactured_item_options = ["1", "0"]

    return {
        "brands":      [b.name for b in brands],
        "item_groups": [g.name for g in item_groups],
        "attributes":  attributes,
        "product_star_rating": star_rating_options,
        "customer_count": happy_customer_options,
        "is_manufactured_item": manufactured_item_options,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  AI / KEYWORD PRODUCT SEARCH
# ─────────────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def ai_product_search(query=None, filters=None, **kwargs):
    """
    Keyword-based search fallback.
    Replace the SQL body with a vector/LLM search when infrastructure is ready.
    """
    if not query:
        return {"status": "error", "message": "query is required"}

    query = cstr(query).strip()
    if not query:
        return {"status": "success", "items": []}

    rows = frappe.db.sql(
        """
        SELECT item_code, item_name, brand, item_group, image
        FROM   `tabItem`
        WHERE  disabled = 0
          AND  (item_code   LIKE %(q)s
             OR item_name   LIKE %(q)s
             OR description LIKE %(q)s)
        ORDER  BY item_name
        LIMIT  50
        """,
        {"q": f"%{query}%"},
        as_dict=True,
    )
    return {"status": "success", "items": [dict(r) for r in rows]}


# ─────────────────────────────────────────────────────────────────────────────
#  CUSTOMER INFO
# ─────────────────────────────────────────────────────────────────────────────

@frappe.whitelist()
def get_customer_info(**kwargs):
    _require_login()

    user = frappe.session.user

    # 1. Direct match on Customer.email_id
    customer_name = frappe.db.get_value("Customer", {"email_id": user}, "name")

    # 2. Via Contact → Dynamic Link
    if not customer_name:
        contact = frappe.db.get_value("Contact", {"email_id": user}, "name")
        if contact:
            customer_name = frappe.db.get_value(
                "Dynamic Link",
                {"parent": contact, "link_doctype": "Customer"},
                "link_name",
            )

    if customer_name:
        cust = frappe.db.get_value(
            "Customer",
            customer_name,
            ["name", "customer_name", "customer_group", "territory", "email_id"],
            as_dict=True,
        )
        return {
            "status": "success",
            "message": [
                {
                    "name":           cust.name,
                    "full_name":      cust.customer_name,
                    "email":          cust.email_id or user,
                    "customer_group": cust.customer_group,
                    "territory":      cust.territory,
                    # Legacy keys expected by storeCustomerInfo()
                    "first_name":     cust.customer_name,
                    "last_name":      "",
                    "phone":          "",
                    "user_id":        user,
                    "referral_code":  "",
                }
            ],
        }

    # Fallback: return logged-in user details without a customer record
    user_doc = frappe.db.get_value(
        "User", user, ["name", "full_name", "email"], as_dict=True
    )
    return {
        "status": "success",
        "message": [
            {
                "name":      user_doc.name if user_doc else user,
                "full_name": user_doc.full_name if user_doc else user,
                "email":     user_doc.email if user_doc else user,
                "user_id":   user,
            }
        ],
    }

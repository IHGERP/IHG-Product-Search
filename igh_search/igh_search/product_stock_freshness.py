"""Stock freshness pipeline for igh_search.

Goals:
- Keep Typesense stock fields fresh from ERP tabBin deltas.
- Provide runtime reconciliation guardrail for search responses.
- Offer observability + drift repair endpoints.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime

import frappe
from frappe import _
from frappe.utils import cint, cstr, flt, get_datetime, now_datetime, time_diff_in_seconds


FLAG_EVENT_SYNC = "stock_event_sync_enabled"
FLAG_RUNTIME_RECONCILE = "stock_runtime_reconcile_enabled"
FLAG_DRIFT_SCAN = "stock_drift_scan_enabled"

CACHE_KEY_PENDING = "igh_search:stock_sync:pending"
CACHE_KEY_METRICS = "igh_search:stock_sync:metrics"
CACHE_KEY_LAST_DRIFT = "igh_search:stock_sync:last_drift"
CACHE_KEY_LATENCY = "igh_search:stock_sync:latency_ms"
CACHE_PREFIX_SNAPSHOT = "igh_search:stock_snapshot:"

DEFAULT_SNAPSHOT_TTL_SECONDS = 30
DEFAULT_DEBOUNCE_SECONDS = 10
DEFAULT_RUNTIME_TOP_N = 8
DEFAULT_DRIFT_LIMIT = 400

SKU_REGEX = re.compile(r"[A-Za-z].*[0-9]|[0-9].*[A-Za-z]")


def _require_login():
    if not frappe.session.user or frappe.session.user == "Guest":
        frappe.throw(_("Authentication required"), frappe.AuthenticationError)


def _require_system_manager():
    _require_login()
    if "System Manager" not in (frappe.get_roles(frappe.session.user) or []):
        frappe.throw(_("Only System Manager can perform this action"), frappe.PermissionError)


def _coerce_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _feature_enabled(name, default=False):
    return _coerce_bool((frappe.conf or {}).get(name), default)


def _safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value, default=0.0):
    try:
        parsed = float(value)
        if parsed != parsed:
            return default
        return parsed
    except Exception:
        return default


def _normalize_item_codes(item_codes):
    if item_codes is None:
        return []
    if isinstance(item_codes, str):
        item_codes = [chunk.strip() for chunk in item_codes.split(",") if chunk.strip()]
    return sorted({cstr(code).strip() for code in (item_codes or []) if cstr(code).strip()})


def _cache_get_json(key, default):
    raw = frappe.cache().get_value(key)
    if not raw:
        return default
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return default


def _cache_set_json(key, value, expires_in_sec=None):
    frappe.cache().set_value(
        key,
        json.dumps(value, default=str),
        expires_in_sec=expires_in_sec,
    )


def _metrics():
    return _cache_get_json(
        CACHE_KEY_METRICS,
        {
            "events_enqueued": 0,
            "event_batches_processed": 0,
            "items_reindexed": 0,
            "runtime_hits_checked": 0,
            "runtime_hits_reconciled": 0,
            "drift_items_detected": 0,
            "drift_items_repaired": 0,
            "typesense_patch_failures": 0,
            "updated_at": None,
        },
    )


def _bump_metrics(updates):
    metrics = _metrics()
    for key, delta in (updates or {}).items():
        current = metrics.get(key)
        if isinstance(current, (int, float)):
            metrics[key] = current + delta
        else:
            metrics[key] = delta
    metrics["updated_at"] = str(now_datetime())
    _cache_set_json(CACHE_KEY_METRICS, metrics)
    return metrics


def _pending_map():
    return _cache_get_json(CACHE_KEY_PENDING, {})


def _get_snapshot_cache_key(item_code):
    return f"{CACHE_PREFIX_SNAPSHOT}{item_code}"


def _looks_like_sku(value):
    text = cstr(value).strip()
    if len(text) < 5:
        return False
    if not SKU_REGEX.search(text):
        return False
    return any(ch in text for ch in ".-_/") or len(text) >= 8


def _get_target_collections(collection_override=""):
    if collection_override:
        return [cstr(collection_override).strip()]

    from igh_search.igh_search.product_search_v2 import get_default_collection

    collections = []
    default_collection = cstr(get_default_collection() or "").strip()
    if default_collection:
        collections.append(default_collection)

    legacy_collection = cstr((frappe.conf or {}).get("typesense_collection") or "").strip()
    if legacy_collection and legacy_collection not in collections:
        collections.append(legacy_collection)

    # Optional toggle for legacy "product" collection maintenance.
    if _feature_enabled("stock_sync_include_legacy_product_collection", False) and "product" not in collections:
        collections.append("product")

    return collections


def _fetch_authoritative_stock_rows(item_codes):
    if not item_codes:
        return {}

    rows = frappe.db.sql(
        """
        SELECT
            i.name AS item_code,
            COALESCE(i.stock_uom, 'Nos') AS stock_uom,
            COALESCE(SUM(b.actual_qty), 0) AS total_stock
        FROM `tabItem` i
        LEFT JOIN `tabBin` b ON b.item_code = i.name
        LEFT JOIN `tabWarehouse` w ON w.name = b.warehouse
        WHERE i.name IN %(codes)s
          AND (
            b.name IS NULL
            OR (
              LOWER(COALESCE(w.name, '')) NOT LIKE '%%damage%%'
              AND LOWER(COALESCE(w.name, '')) NOT LIKE '%%missing%%'
            )
          )
        GROUP BY i.name, i.stock_uom
        """,
        {"codes": tuple(item_codes)},
        as_dict=True,
    )

    now_ts = str(now_datetime())
    snapshot_map = {}
    for row in rows or []:
        item_code = cstr(row.get("item_code")).strip()
        total_stock = flt(row.get("total_stock") or 0)
        snapshot_map[item_code] = {
            "item_code": item_code,
            "stock": total_stock,
            "total_stock": total_stock,
            "in_stock": 1 if total_stock > 0 else 0,
            "stock_uom": cstr(row.get("stock_uom") or "Nos").strip() or "Nos",
            "snapshot_ts": now_ts,
        }

    for item_code in item_codes:
        if item_code not in snapshot_map:
            snapshot_map[item_code] = {
                "item_code": item_code,
                "stock": 0.0,
                "total_stock": 0.0,
                "in_stock": 0,
                "stock_uom": "Nos",
                "snapshot_ts": now_ts,
            }

    return snapshot_map


def get_authoritative_stock_snapshot(item_codes, cache_ttl_seconds=DEFAULT_SNAPSHOT_TTL_SECONDS, force_refresh=False):
    item_codes = _normalize_item_codes(item_codes)
    if not item_codes:
        return {}

    out = {}
    missing = []
    for item_code in item_codes:
        if not force_refresh:
            cached = _cache_get_json(_get_snapshot_cache_key(item_code), None)
            if cached:
                out[item_code] = cached
                continue
        missing.append(item_code)

    if missing:
        fresh = _fetch_authoritative_stock_rows(missing)
        for item_code, snapshot in fresh.items():
            out[item_code] = snapshot
            _cache_set_json(
                _get_snapshot_cache_key(item_code),
                snapshot,
                expires_in_sec=max(5, _safe_int(cache_ttl_seconds, DEFAULT_SNAPSHOT_TTL_SECONDS)),
            )

    return out


def _patch_typesense_stock_map(stock_map, collections=None):
    from igh_search.igh_search.product_search_v2 import create_typesense_client

    if not stock_map:
        return {"updated": 0, "failed": 0, "failures": []}

    client = create_typesense_client()
    target_collections = collections or _get_target_collections()

    updated = 0
    failures = []
    for collection_name in target_collections:
        if not collection_name:
            continue

        for item_code, snapshot in (stock_map or {}).items():
            payload = {
                "stock": _safe_float(snapshot.get("stock"), 0),
                "in_stock": _safe_int(snapshot.get("in_stock"), 0),
                "stock_uom": cstr(snapshot.get("stock_uom") or "Nos"),
            }
            try:
                client.collections[collection_name].documents[item_code].update(payload)
                updated += 1
            except Exception as exc:
                failures.append(
                    {
                        "collection": collection_name,
                        "item_code": item_code,
                        "error": cstr(exc)[:280],
                    }
                )

    if failures:
        _bump_metrics({"typesense_patch_failures": len(failures)})
    return {"updated": updated, "failed": len(failures), "failures": failures}


def reindex_stock_for_items(item_codes, source="manual", collection=""):
    item_codes = _normalize_item_codes(item_codes)
    if not item_codes:
        return {"status": "success", "source": source, "updated": 0, "failed": 0, "failures": []}

    stock_map = get_authoritative_stock_snapshot(item_codes, force_refresh=True)
    collections = _get_target_collections(collection_override=collection)
    patch = _patch_typesense_stock_map(stock_map, collections=collections)
    return {
        "status": "success",
        "source": source,
        "collections": collections,
        "updated": _safe_int(patch.get("updated"), 0),
        "failed": _safe_int(patch.get("failed"), 0),
        "failures": (patch.get("failures") or [])[:50],
    }


def enqueue_stock_sync_for_items(item_codes, reason="stock_change", debounce_seconds=DEFAULT_DEBOUNCE_SECONDS):
    if not _feature_enabled(FLAG_EVENT_SYNC, False):
        return {"status": "skipped", "reason": "feature_disabled", "items": 0}

    codes = _normalize_item_codes(item_codes)
    if not codes:
        return {"status": "skipped", "reason": "empty", "items": 0}

    pending = _pending_map()
    now_iso = now_datetime().isoformat()
    for code in codes:
        pending[code] = {
            "queued_at": now_iso,
            "reason": cstr(reason).strip() or "stock_change",
        }
    _cache_set_json(CACHE_KEY_PENDING, pending)
    _bump_metrics({"events_enqueued": len(codes)})

    frappe.enqueue(
        "igh_search.igh_search.product_stock_freshness.process_pending_stock_sync_batch",
        queue="short",
        enqueue_after_commit=True,
        debounce_seconds=_safe_int(debounce_seconds, DEFAULT_DEBOUNCE_SECONDS),
    )
    return {"status": "queued", "items": len(codes)}


def _parse_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return get_datetime(value)
    except Exception:
        return None


def process_pending_stock_sync_batch(debounce_seconds=DEFAULT_DEBOUNCE_SECONDS, batch_limit=300):
    pending = _pending_map()
    if not pending:
        return {"status": "noop", "items": 0}

    now_dt = now_datetime()
    due = []
    keep = {}
    debounce = max(1, _safe_int(debounce_seconds, DEFAULT_DEBOUNCE_SECONDS))
    max_batch = max(1, min(_safe_int(batch_limit, 300), 2000))

    for code, meta in pending.items():
        queued_at = _parse_datetime((meta or {}).get("queued_at")) or now_dt
        age = max(0, time_diff_in_seconds(now_dt, queued_at))
        if age >= debounce:
            due.append(code)
        else:
            keep[code] = meta

    due = due[:max_batch]
    for code in due:
        keep.pop(code, None)

    _cache_set_json(CACHE_KEY_PENDING, keep)
    if not due:
        return {"status": "noop", "items": 0}

    result = reindex_stock_for_items(due, source="event_batch")
    _bump_metrics(
        {
            "event_batches_processed": 1,
            "items_reindexed": _safe_int(result.get("updated"), 0),
        }
    )
    return {"status": "success", "items": len(due), **result}


def reconcile_search_hits_stock(hits, query="", item_code_hint="", top_n=DEFAULT_RUNTIME_TOP_N):
    """Runtime guardrail for stale stock on search hits.

    Behavior:
    - Always reconcile all hits for exact SKU-like query/hint.
    - Otherwise reconcile top-N hits and only mark as runtime-reconciled when mismatch found.
    """
    if not _feature_enabled(FLAG_RUNTIME_RECONCILE, True):
        return {
            "hits": hits or [],
            "checked_count": 0,
            "reconciled_count": 0,
            "freshness_source": "index",
        }

    safe_hits = list(hits or [])
    if not safe_hits:
        return {
            "hits": safe_hits,
            "checked_count": 0,
            "reconciled_count": 0,
            "freshness_source": "index",
        }

    query_text = cstr(query).strip()
    hint_text = cstr(item_code_hint).strip()
    exact_sku = bool(hint_text) or _looks_like_sku(query_text)
    n = len(safe_hits) if exact_sku else max(1, min(_safe_int(top_n, DEFAULT_RUNTIME_TOP_N), len(safe_hits)))

    codes = []
    for i in range(n):
        hit = safe_hits[i]
        doc = hit.get("document") if isinstance(hit, dict) and isinstance(hit.get("document"), dict) else hit
        if not isinstance(doc, dict):
            continue
        code = cstr(doc.get("item_code") or "").strip()
        if code:
            codes.append(code)

    codes = sorted(set(codes))
    if not codes:
        return {
            "hits": safe_hits,
            "checked_count": 0,
            "reconciled_count": 0,
            "freshness_source": "index",
        }

    snapshots = get_authoritative_stock_snapshot(codes)
    checked = 0
    reconciled = 0
    now_ts = str(now_datetime())

    for i in range(n):
        hit = safe_hits[i]
        has_document_wrapper = isinstance(hit, dict) and isinstance(hit.get("document"), dict)
        doc = hit.get("document") if has_document_wrapper else hit
        if not isinstance(doc, dict):
            continue

        code = cstr(doc.get("item_code") or "").strip()
        if not code:
            continue

        checked += 1
        snapshot = snapshots.get(code)
        if not snapshot:
            doc["stock_freshness_source"] = "index"
            doc["stock_freshness_ts"] = now_ts
            continue

        indexed_stock = _safe_float(doc.get("stock"), 0)
        indexed_in_stock = _safe_int(doc.get("in_stock"), 1 if indexed_stock > 0 else 0)
        auth_stock = _safe_float(snapshot.get("stock"), 0)
        auth_in_stock = _safe_int(snapshot.get("in_stock"), 0)

        mismatch = abs(indexed_stock - auth_stock) > 1e-9 or indexed_in_stock != auth_in_stock
        if exact_sku or mismatch:
            doc["stock"] = auth_stock
            doc["total_stock"] = auth_stock
            doc["in_stock"] = auth_in_stock
            doc["stock_uom"] = cstr(snapshot.get("stock_uom") or doc.get("stock_uom") or "Nos")
            doc["stock_freshness_source"] = "runtime_reconciled"
            doc["stock_freshness_ts"] = cstr(snapshot.get("snapshot_ts") or now_ts)
            reconciled += 1
        else:
            doc["total_stock"] = indexed_stock
            doc["stock_freshness_source"] = "index"
            doc["stock_freshness_ts"] = now_ts

        if has_document_wrapper:
            hit["document"] = doc
            safe_hits[i] = hit
        else:
            safe_hits[i] = doc

    _bump_metrics({"runtime_hits_checked": checked, "runtime_hits_reconciled": reconciled})
    return {
        "hits": safe_hits,
        "checked_count": checked,
        "reconciled_count": reconciled,
        "freshness_source": "runtime_reconciled" if reconciled else "index",
    }


def _fetch_index_stock_map(item_codes):
    from igh_search.igh_search.product_search_v2 import get_documents_by_codes

    docs = get_documents_by_codes(item_codes, include_inactive=1) or []
    out = {}
    for doc in docs:
        code = cstr(doc.get("item_code") or "").strip()
        if not code:
            continue
        stock = _safe_float(doc.get("stock"), 0)
        out[code] = {
            "stock": stock,
            "in_stock": _safe_int(doc.get("in_stock"), 1 if stock > 0 else 0),
        }
    return out


def record_search_latency(latency_ms):
    values = _cache_get_json(CACHE_KEY_LATENCY, [])
    if not isinstance(values, list):
        values = []
    values.append(max(0, _safe_int(latency_ms, 0)))
    values = values[-400:]
    _cache_set_json(CACHE_KEY_LATENCY, values)


def _p95_latency_ms():
    values = _cache_get_json(CACHE_KEY_LATENCY, [])
    if not values:
        return 0
    values = sorted(max(0, _safe_int(v, 0)) for v in values)
    idx = min(len(values) - 1, int(0.95 * (len(values) - 1)))
    return values[idx]


def _queue_lag_seconds(pending):
    if not pending:
        return 0
    now_dt = now_datetime()
    ages = []
    for _, meta in pending.items():
        queued_at = _parse_datetime((meta or {}).get("queued_at"))
        if queued_at:
            ages.append(max(0, _safe_int(time_diff_in_seconds(now_dt, queued_at), 0)))
    return max(ages) if ages else 0


@frappe.whitelist()
def run_stock_drift_repair(limit=DEFAULT_DRIFT_LIMIT, dry_run=1, collection=""):
    _require_system_manager()

    if not _feature_enabled(FLAG_DRIFT_SCAN, True):
        return {"status": "skipped", "reason": "feature_disabled"}

    limit = max(1, min(_safe_int(limit, DEFAULT_DRIFT_LIMIT), 5000))
    dry_run = _coerce_bool(dry_run, True)

    item_rows = frappe.get_all(
        "Item",
        fields=["name"],
        filters={"disabled": 0},
        order_by="modified desc",
        page_length=limit,
    )
    item_codes = _normalize_item_codes([row.get("name") for row in item_rows])

    authoritative = get_authoritative_stock_snapshot(item_codes, force_refresh=True)
    indexed = _fetch_index_stock_map(item_codes)

    mismatches = []
    for code in item_codes:
        auth = authoritative.get(code)
        idx = indexed.get(code)
        if not auth or not idx:
            continue

        a_stock = _safe_float(auth.get("stock"), 0)
        i_stock = _safe_float(idx.get("stock"), 0)
        a_flag = _safe_int(auth.get("in_stock"), 0)
        i_flag = _safe_int(idx.get("in_stock"), 0)

        if abs(a_stock - i_stock) > 1e-9 or a_flag != i_flag:
            mismatches.append(
                {
                    "item_code": code,
                    "authoritative_stock": a_stock,
                    "indexed_stock": i_stock,
                    "authoritative_in_stock": a_flag,
                    "indexed_in_stock": i_flag,
                }
            )

    repaired_count = 0
    failures = []
    if mismatches and not dry_run:
        codes = [row["item_code"] for row in mismatches]
        result = reindex_stock_for_items(codes, source="drift_repair", collection=collection)
        repaired_count = _safe_int(result.get("updated"), 0)
        failures = (result.get("failures") or [])[:50]

    _bump_metrics(
        {
            "drift_items_detected": len(mismatches),
            "drift_items_repaired": repaired_count,
        }
    )

    report = {
        "status": "success",
        "dry_run": dry_run,
        "limit": limit,
        "mismatch_count": len(mismatches),
        "repaired_count": repaired_count,
        "sample_mismatches": mismatches[:50],
        "failures": failures,
        "run_at": str(now_datetime()),
    }
    _cache_set_json(CACHE_KEY_LAST_DRIFT, report)
    return report


@frappe.whitelist()
def get_search_freshness_health():
    _require_system_manager()

    pending = _pending_map()
    metrics = _metrics()

    checked = _safe_int(metrics.get("runtime_hits_checked"), 0)
    reconciled = _safe_int(metrics.get("runtime_hits_reconciled"), 0)
    patch_failures = _safe_int(metrics.get("typesense_patch_failures"), 0)
    items_reindexed = max(1, _safe_int(metrics.get("items_reindexed"), 0))

    reconcile_rate = round((reconciled / checked) * 100, 2) if checked else 0.0
    patch_failure_rate = round((patch_failures / items_reindexed) * 100, 2) if items_reindexed else 0.0

    return {
        "status": "success",
        "feature_flags": {
            FLAG_EVENT_SYNC: _feature_enabled(FLAG_EVENT_SYNC, False),
            FLAG_RUNTIME_RECONCILE: _feature_enabled(FLAG_RUNTIME_RECONCILE, True),
            FLAG_DRIFT_SCAN: _feature_enabled(FLAG_DRIFT_SCAN, True),
        },
        "queue": {
            "pending_items": len(pending),
            "queue_lag_seconds": _queue_lag_seconds(pending),
            "pending_sample": sorted(list(pending.keys()))[:25],
        },
        "metrics": metrics,
        "derived": {
            "reconciliation_rate_pct": reconcile_rate,
            "patch_failure_rate_pct": patch_failure_rate,
            "p95_search_latency_ms": _p95_latency_ms(),
        },
        "last_drift": _cache_get_json(CACHE_KEY_LAST_DRIFT, {}),
        "reported_at": str(now_datetime()),
    }


@frappe.whitelist()
def reindex_stock_for_items_api(item_codes=None, collection=""):
    _require_system_manager()

    if isinstance(item_codes, str):
        text = item_codes.strip()
        if text.startswith("["):
            try:
                item_codes = json.loads(text)
            except Exception:
                item_codes = [chunk.strip() for chunk in text.split(",") if chunk.strip()]
        else:
            item_codes = [chunk.strip() for chunk in text.split(",") if chunk.strip()]

    return reindex_stock_for_items(item_codes or [], source="manual_api", collection=collection)


def on_bin_change(doc, method=None):
    item_code = cstr(getattr(doc, "item_code", None) or doc.get("item_code") or "").strip()
    if item_code:
        enqueue_stock_sync_for_items([item_code], reason=f"bin:{method or 'update'}")


def on_stock_ledger_entry_change(doc, method=None):
    item_code = cstr(getattr(doc, "item_code", None) or doc.get("item_code") or "").strip()
    if item_code:
        enqueue_stock_sync_for_items([item_code], reason=f"sle:{method or 'update'}")

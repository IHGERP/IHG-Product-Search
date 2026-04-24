import copy
import json
import os
import re
import time
from collections import defaultdict

import frappe
import requests
from frappe import _
from frappe.utils import cint, cstr, flt, now_datetime

from igh_search.igh_search.search_normalization import (
    expand_search_aliases,
    get_alias_map,
    load_glossary,
    normalize_color_temp,
    normalize_ip_rate,
    normalize_text,
)

OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
DEFAULT_OPENAI_MODEL = "gpt-4o-mini"
DEFAULT_GROQ_MODEL = "llama-3.3-70b-versatile"
DEFAULT_RANGE_MAX = 1000000000
KNOWN_VALUES_CACHE_TTL = 60 * 60
VOCABULARY_CACHE_TTL = 15 * 60
METRICS_CACHE_TTL = 24 * 60 * 60
FEEDBACK_WEIGHTS = {
    "search_click": 1,
    "shortlist": 3,
    "quotation_created": 5,
}

SUPPORTED_SORT_VALUES = {
    "",
    "creation:asc",
    "creation:desc",
    "rate:asc",
    "rate:desc",
    "offer_rate:asc",
    "offer_rate:desc",
    "stock:asc",
    "stock:desc",
    "sold_last_30_days:asc",
    "sold_last_30_days:desc",
    "discount_percentage:asc",
    "discount_percentage:desc",
    "priority_score:asc",
    "priority_score:desc",
    "popularity_score:asc",
    "popularity_score:desc",
    "business_score:asc",
    "business_score:desc",
    "modified_ts:asc",
    "modified_ts:desc",
}

FILTER_MASTER_KEY_MAP = {
    "brand": "brand",
    "category_list": "category_list",
    "product_type": "product_type",
    "item_group": "item_group",
    "ip_rate": "ip_rate",
    "power": "power",
    "color_temp": "color_temp_",
    "body_finish": "body_finish",
    "input_voltage": "input",
    "mounting": "mounting",
    "output_current": "output_current",
    "output_voltage": "output_voltage",
    "lamp_type": "lamp_type",
    "beam_angle": "beam_angle",
    "material": "material",
    "warranty": "warranty_",
}

ARRAY_FILTER_KEYS = tuple(FILTER_MASTER_KEY_MAP.keys()) + ("variant_of",)
BOOLEAN_FILTER_KEYS = ("in_stock",)
RANGE_FILTER_DEFAULTS = {
    "rate_range": {"min": 0, "max": DEFAULT_RANGE_MAX},
    "offer_rate_range": {"min": 0, "max": DEFAULT_RANGE_MAX},
    "discount_percentage_range": {"min": 0, "max": DEFAULT_RANGE_MAX},
    "stock_range": {"min": 0, "max": DEFAULT_RANGE_MAX},
    "sold_last_30_days_range": {"min": 0, "max": DEFAULT_RANGE_MAX},
    "inventory_value_range": {"min": 0, "max": DEFAULT_RANGE_MAX},
    "priority_score_range": {"min": 0, "max": DEFAULT_RANGE_MAX},
    "popularity_score_range": {"min": 0, "max": DEFAULT_RANGE_MAX},
    "business_score_range": {"min": 0, "max": DEFAULT_RANGE_MAX},
    "power_value_range": {"min": 0, "max": DEFAULT_RANGE_MAX},
    "color_temp_kelvin_range": {"min": 0, "max": DEFAULT_RANGE_MAX},
    "ip_rating_numeric_range": {"min": 0, "max": DEFAULT_RANGE_MAX},
}
PAGE_CONTEXT_KEYS = ("route", "category", "brand", "search")
LEGACY_FILTER_KEY_ALIASES = {
    "color_temp_": "color_temp",
    "input": "input_voltage",
    "warranty_": "warranty",
    "price_range": "rate_range",
}
LEGACY_SORT_ALIASES = {
    "creation_on:desc": "creation:desc",
    "creation_on:asc": "creation:asc",
}


def build_default_filters():
    filters = {key: [] for key in ARRAY_FILTER_KEYS}
    filters.update({key: False for key in BOOLEAN_FILTER_KEYS})
    filters.update(copy.deepcopy(RANGE_FILTER_DEFAULTS))
    return filters


def build_default_response(explanation=""):
    return {
        "query": "",
        "sort_by": "",
        "filters": build_default_filters(),
        "explanation": cstr(explanation).strip(),
    }


def _get_conf():
    return frappe.get_conf() or {}


def is_ai_product_search_enabled():
    return bool(cint(_get_conf().get("enable_ai_product_search") or 0))


def get_ai_product_search_rate_limit():
    return max(cint(_get_conf().get("ai_product_search_rate_limit") or 20), 1)


def get_openai_api_key():
    conf = _get_conf()
    return os.environ.get("OPENAI_API_KEY") or conf.get("openai_api_key")


def get_openai_model():
    conf = _get_conf()
    return (
        conf.get("ai_product_search_model")
        or conf.get("openai_model")
        or DEFAULT_OPENAI_MODEL
    )


def get_groq_api_key():
    conf = _get_conf()
    return os.environ.get("GROQ_API_KEY") or conf.get("groq_api_key")


def get_groq_model():
    conf = _get_conf()
    return conf.get("ai_product_search_groq_model") or DEFAULT_GROQ_MODEL


def log_ai_product_search(event, payload):
    try:
        frappe.logger().info(
            "AI Product Search %s: %s",
            event,
            json.dumps(payload, ensure_ascii=True, default=str),
        )
    except Exception:
        pass


def record_ai_metric(metric_name, increment=1):
    try:
        key = f"ai_product_search|metric|{metric_name}"
        cache = frappe.cache()
        current = cint(cache.get_value(key) or 0)
        cache.set_value(key, current + cint(increment), expires_in_sec=METRICS_CACHE_TTL)
    except Exception:
        pass


def get_ai_search_quality_report():
    metrics = {}
    for metric_name in (
        "requests",
        "deterministic_only",
        "provider_openai",
        "provider_groq",
        "provider_fallback",
        "llm_failures",
        "search_relaxations",
        "zero_results",
        "final_results",
    ):
        try:
            metrics[metric_name] = cint(
                frappe.cache().get_value(f"ai_product_search|metric|{metric_name}") or 0
            )
        except Exception:
            metrics[metric_name] = 0
    metrics.update(get_ai_event_quality_report())
    return metrics


def _json_dumps(value):
    return json.dumps(value or {}, ensure_ascii=True, default=str)


def _json_loads(value, default=None):
    default = {} if default is None else default
    if not value:
        return copy.deepcopy(default)
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return copy.deepcopy(default)


def _doctype_exists(doctype_name):
    try:
        return bool(frappe.db.exists("DocType", doctype_name))
    except Exception:
        return False


def _serialize_roles():
    try:
        return sorted(frappe.get_roles())
    except Exception:
        return []


def log_ai_search_event(event_type, payload=None):
    payload = payload or {}
    if not _doctype_exists("AI Product Search Event"):
        return None

    try:
        page_context = payload.get("page_context") or {}
        resolved_intent = payload.get("resolved_intent") or {}
        top_item_codes = payload.get("top_item_codes") or []
        deterministic_signals = payload.get("deterministic_signals") or resolved_intent.get("signals") or []
        event = frappe.get_doc(
            {
                "doctype": "AI Product Search Event",
                "event_type": event_type,
                "search_event_reference": cstr(payload.get("search_event_reference") or ""),
                "raw_message": cstr(payload.get("raw_message") or ""),
                "normalized_query": cstr(payload.get("normalized_query") or ""),
                "selected_item_code": cstr(payload.get("selected_item_code") or ""),
                "related_item_code": cstr(payload.get("related_item_code") or ""),
                "provider": cstr(payload.get("provider") or ""),
                "llm_used": cint(payload.get("llm_used") or 0),
                "applied_sort": cstr(payload.get("applied_sort") or ""),
                "result_count": cint(payload.get("result_count") or 0),
                "latency_ms": flt(payload.get("latency_ms") or 0),
                "route": cstr(page_context.get("route") or payload.get("route") or ""),
                "session_id": cstr(getattr(frappe.session, "sid", "") or ""),
                "user_id": cstr(getattr(frappe.session, "user", "") or ""),
                "user_roles": _json_dumps(_serialize_roles()),
                "resolved_intent_json": _json_dumps(resolved_intent),
                "deterministic_signals_json": _json_dumps(deterministic_signals),
                "applied_filters_json": _json_dumps(payload.get("applied_filters") or {}),
                "applied_relaxations_json": _json_dumps(payload.get("applied_relaxations") or []),
                "quality_signals_json": _json_dumps(payload.get("quality_signals") or {}),
                "top_item_codes_json": _json_dumps(top_item_codes),
                "page_context_json": _json_dumps(page_context),
                "benchmark_case_name": cstr(payload.get("benchmark_case_name") or ""),
                "outcome_status": cstr(payload.get("outcome_status") or ""),
                "reformulated_from": cstr(payload.get("reformulated_from") or ""),
            }
        )
        event.insert(ignore_permissions=True)
        return event.name
    except Exception:
        log_ai_product_search(
            "event_log_failure",
            {"event_type": event_type, "error": frappe.get_traceback()},
        )
        return None


def _fetch_event_doc(event_name):
    if not event_name or not _doctype_exists("AI Product Search Event"):
        return None
    if not frappe.db.exists("AI Product Search Event", event_name):
        return None
    return frappe.get_doc("AI Product Search Event", event_name)


def get_ai_event_quality_report():
    if not _doctype_exists("AI Product Search Event"):
        return {
            "tracked_searches": 0,
            "top_failed_queries": [],
            "top_reformulated_queries": [],
            "top_clicked_results": [],
            "zero_result_rate": 0,
            "relaxation_rate": 0,
        }

    search_rows = frappe.get_all(
        "AI Product Search Event",
        filters={"event_type": "search_issued"},
        fields=["name", "normalized_query", "result_count", "applied_relaxations_json", "selected_item_code"],
        limit_page_length=500,
        order_by="modified desc",
    )
    if not search_rows:
        return {
            "tracked_searches": 0,
            "top_failed_queries": [],
            "top_reformulated_queries": [],
            "top_clicked_results": [],
            "zero_result_rate": 0,
            "relaxation_rate": 0,
        }

    failed_counts = defaultdict(int)
    reformulation_counts = defaultdict(int)
    click_counts = defaultdict(int)
    zero_results = 0
    relaxations = 0

    for row in search_rows:
        normalized_query = cstr(row.normalized_query or "").strip()
        if cint(row.result_count) <= 0 and normalized_query:
            failed_counts[normalized_query] += 1
            zero_results += 1
        applied_relaxations = _json_loads(row.applied_relaxations_json, default=[])
        if applied_relaxations:
            relaxations += 1

    reformulation_rows = frappe.get_all(
        "AI Product Search Event",
        filters={"event_type": "reformulated_query"},
        fields=["reformulated_from", "raw_message"],
        limit_page_length=200,
        order_by="modified desc",
    )
    for row in reformulation_rows:
        reformulated_message = cstr(row.raw_message or "").strip()
        if reformulated_message:
            reformulation_counts[reformulated_message] += 1

    click_rows = frappe.get_all(
        "AI Product Search Event",
        filters={"event_type": ["in", ["search_click", "shortlist", "quotation_created"]]},
        fields=["selected_item_code", "event_type"],
        limit_page_length=500,
        order_by="modified desc",
    )
    for row in click_rows:
        if not cstr(row.selected_item_code or "").strip():
            continue
        click_counts[row.selected_item_code] += FEEDBACK_WEIGHTS.get(row.event_type, 1)

    tracked_searches = len(search_rows)
    return {
        "tracked_searches": tracked_searches,
        "top_failed_queries": [
            {"query": query, "count": count}
            for query, count in sorted(failed_counts.items(), key=lambda item: item[1], reverse=True)[:10]
        ],
        "top_reformulated_queries": [
            {"query": query, "count": count}
            for query, count in sorted(reformulation_counts.items(), key=lambda item: item[1], reverse=True)[:10]
        ],
        "top_clicked_results": [
            {"item_code": item_code, "score": score}
            for item_code, score in sorted(click_counts.items(), key=lambda item: item[1], reverse=True)[:10]
        ],
        "zero_result_rate": round((zero_results / tracked_searches) * 100, 2) if tracked_searches else 0,
        "relaxation_rate": round((relaxations / tracked_searches) * 100, 2) if tracked_searches else 0,
    }


def track_ai_search_outcome(
    event_type,
    search_event_id,
    item_code=None,
    related_item_code=None,
    reformulated_message=None,
    page_context=None,
    benchmark_case_name=None,
):
    event_doc = _fetch_event_doc(search_event_id)
    if not event_doc:
        frappe.throw(_("AI search event not found"))

    raw_message = cstr(reformulated_message or event_doc.raw_message or "").strip()
    if event_type == "reformulated_query" and not raw_message:
        frappe.throw(_("Reformulated message is required"))

    payload = {
        "search_event_reference": search_event_id,
        "raw_message": raw_message,
        "normalized_query": cstr(event_doc.normalized_query or ""),
        "selected_item_code": cstr(item_code or ""),
        "related_item_code": cstr(related_item_code or ""),
        "provider": cstr(event_doc.provider or ""),
        "llm_used": cint(event_doc.llm_used or 0),
        "applied_sort": cstr(event_doc.applied_sort or ""),
        "applied_filters": _json_loads(event_doc.applied_filters_json, default={}),
        "resolved_intent": _json_loads(event_doc.resolved_intent_json, default={}),
        "deterministic_signals": _json_loads(event_doc.deterministic_signals_json, default=[]),
        "applied_relaxations": _json_loads(event_doc.applied_relaxations_json, default=[]),
        "page_context": _json_loads(event_doc.page_context_json, default={}) or parse_page_context(page_context),
        "reformulated_from": search_event_id if event_type == "reformulated_query" else "",
        "benchmark_case_name": benchmark_case_name or "",
        "outcome_status": event_type,
        "quality_signals": _json_loads(event_doc.quality_signals_json, default={}),
    }
    outcome_event_id = log_ai_search_event(event_type, payload)
    return {"search_event_id": search_event_id, "outcome_event_id": outcome_event_id}


def load_ai_search_benchmark_cases():
    benchmark_path = frappe.get_app_path(
        "igh_search", "igh_search", "data", "ai_product_search_benchmark.json"
    )
    with open(benchmark_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload.get("cases", [])


def evaluate_ai_search_benchmark(feature_flag_override=0):
    from igh_search.igh_search.product_search_v2 import ensure_query_access

    ensure_query_access(feature_flag_override=feature_flag_override)

    summary = {
        "total_cases": 0,
        "intent_matches": 0,
        "sort_matches": 0,
        "non_zero_results": 0,
        "details": [],
    }
    for case in load_ai_search_benchmark_cases():
        result = ai_search_products_v2(
            message=case.get("message"),
            page_context=case.get("page_context"),
            page=1,
            page_length=5,
            include_inactive=0,
            feature_flag_override=feature_flag_override,
        )
        summary["total_cases"] += 1
        intent_match = (
            cstr(result.get("resolved_intent", {}).get("intent_class"))
            == cstr(case.get("expected_intent_class"))
        )
        sort_match = (
            cstr(case.get("expected_sort_by") or "")
            == cstr(result.get("applied_sort") or "")
        )
        if intent_match:
            summary["intent_matches"] += 1
        if sort_match:
            summary["sort_matches"] += 1
        if cint(result.get("found")) > 0:
            summary["non_zero_results"] += 1

        detail = {
            "case_name": case.get("name"),
            "message": case.get("message"),
            "expected_intent_class": case.get("expected_intent_class"),
            "actual_intent_class": result.get("resolved_intent", {}).get("intent_class"),
            "expected_sort_by": case.get("expected_sort_by") or "",
            "actual_sort_by": result.get("applied_sort") or "",
            "found": result.get("found"),
            "intent_match": intent_match,
            "sort_match": sort_match,
            "search_event_id": result.get("search_event_id"),
        }
        summary["details"].append(detail)
        if result.get("search_event_id"):
            log_ai_search_event(
                "benchmark_run",
                {
                    "search_event_reference": result.get("search_event_id"),
                    "raw_message": case.get("message"),
                    "normalized_query": normalize_text(case.get("message")),
                    "provider": result.get("resolved_intent", {}).get("provider"),
                    "llm_used": result.get("resolved_intent", {}).get("llm_used"),
                    "applied_sort": result.get("applied_sort"),
                    "result_count": result.get("found"),
                    "benchmark_case_name": case.get("name"),
                    "outcome_status": "benchmark",
                    "quality_signals": result.get("quality_signals"),
                },
            )

    if summary["total_cases"]:
        summary["intent_match_rate"] = round(
            (summary["intent_matches"] / summary["total_cases"]) * 100, 2
        )
        summary["sort_match_rate"] = round(
            (summary["sort_matches"] / summary["total_cases"]) * 100, 2
        )
        summary["non_zero_result_rate"] = round(
            (summary["non_zero_results"] / summary["total_cases"]) * 100, 2
        )
    else:
        summary["intent_match_rate"] = 0
        summary["sort_match_rate"] = 0
        summary["non_zero_result_rate"] = 0

    return summary


def parse_page_context(page_context):
    if not page_context:
        return {}

    if isinstance(page_context, str):
        try:
            page_context = json.loads(page_context)
        except Exception:
            return {}

    if not isinstance(page_context, dict):
        return {}

    return {
        key: cstr(page_context.get(key) or "").strip()
        for key in PAGE_CONTEXT_KEYS
        if cstr(page_context.get(key) or "").strip()
    }


def _get_model_names(model_name):
    cache_key = f"ai_product_search|values|{model_name}"
    cached_value = frappe.cache().get_value(cache_key)
    if cached_value:
        return cached_value

    if model_name == "Item Group":
        values = frappe.get_list(
            model_name,
            pluck="name",
            filters={"disable": 0, "name": ("!=", "All Item Groups")},
        )
    else:
        values = frappe.get_list(model_name, pluck="name")

    frappe.cache().set_value(cache_key, values, expires_in_sec=KNOWN_VALUES_CACHE_TTL)
    return values


def get_known_filter_values():
    cache_key = "ai_product_search|known_filter_values|v2"
    cached_value = frappe.cache().get_value(cache_key)
    if cached_value:
        return cached_value

    from igh_search.igh_search.api import get_all_masters

    masters = get_all_masters() or {}
    known_values = {}
    for filter_key in ARRAY_FILTER_KEYS:
        master_key = FILTER_MASTER_KEY_MAP.get(filter_key)
        if master_key is None:
            known_values[filter_key] = []
            continue
        known_values[filter_key] = masters.get(master_key) or []

    known_values["product_type"] = masters.get("product_type") or ["Listed", "Unlisted", "Obsolete"]
    known_values["variant_of"] = []

    frappe.cache().set_value(cache_key, known_values, expires_in_sec=KNOWN_VALUES_CACHE_TTL)
    return known_values


def get_typesense_vocabulary(max_values=50):
    cache_key = f"ai_product_search|typesense_vocabulary|{max_values}"
    cached_value = frappe.cache().get_value(cache_key)
    if cached_value:
        return cached_value

    vocabulary = {}
    try:
        from igh_search.igh_search.product_search_v2 import (
            FACET_FIELDS,
            create_typesense_client,
            get_default_collection,
        )

        client = create_typesense_client()
        response = client.collections[get_default_collection()].documents.search(
            {
                "q": "*",
                "query_by": "searchable_text",
                "facet_by": ",".join(FACET_FIELDS),
                "max_facet_values": max_values,
                "per_page": 1,
                "page": 1,
                "include_fields": "item_code",
            }
        )
        for facet in response.get("facet_counts", []):
            field_name = facet.get("field_name")
            counts = facet.get("counts") or []
            vocabulary[field_name] = [
                cstr(count.get("value")).strip()
                for count in counts
                if cstr(count.get("value")).strip()
            ]
    except Exception:
        vocabulary = {}

    frappe.cache().set_value(cache_key, vocabulary, expires_in_sec=VOCABULARY_CACHE_TTL)
    return vocabulary


def get_ai_search_vocabulary():
    cache_key = "ai_product_search|search_vocabulary|v2"
    cached_value = frappe.cache().get_value(cache_key)
    if cached_value:
        return cached_value

    master_values = get_known_filter_values()
    facet_values = get_typesense_vocabulary()
    merged = {}

    for field_name in ARRAY_FILTER_KEYS:
        values = []
        for source_values in (facet_values.get(field_name), master_values.get(field_name)):
            for value in source_values or []:
                cleaned = cstr(value).strip()
                if cleaned and cleaned not in values:
                    values.append(cleaned)
        merged[field_name] = values

    alias_map = {
        normalize_text(alias): normalize_text(canonical)
        for alias, canonical in get_alias_map().items()
    }
    glossary_entries = []
    for entry in load_glossary().get("entries", []):
        canonical = cstr(entry.get("canonical") or "").strip()
        if not canonical:
            continue
        glossary_entries.append(
            {
                "canonical": canonical,
                "aliases": [cstr(value).strip() for value in entry.get("aliases", []) if cstr(value).strip()],
            }
        )

    vocabulary = {
        "known_values": merged,
        "sort_values": sorted(SUPPORTED_SORT_VALUES),
        "field_aliases": copy.deepcopy(LEGACY_FILTER_KEY_ALIASES),
        "sort_aliases": copy.deepcopy(LEGACY_SORT_ALIASES),
        "glossary_entries": glossary_entries,
        "alias_map": alias_map,
    }
    frappe.cache().set_value(cache_key, vocabulary, expires_in_sec=VOCABULARY_CACHE_TTL)
    return vocabulary


def _trim_known_values_for_prompt(known_values, max_values=100):
    return {key: values[:max_values] for key, values in (known_values or {}).items()}


def preprocess_user_message(message, page_context=None):
    sanitized_page_context = parse_page_context(page_context)
    base_message = cstr(message).strip()
    normalized_message = normalize_text(base_message)
    expanded_message = expand_search_aliases(base_message)
    return {
        "message": base_message,
        "normalized_message": normalized_message,
        "expanded_message": expanded_message,
        "page_context": sanitized_page_context,
        "word_count": len([word for word in normalized_message.split(" ") if word]),
    }


def _build_intent_state():
    return {
        "query": "",
        "sort_by": "",
        "item_code_hint": "",
        "filters": build_default_filters(),
        "intent_class": "general_search",
        "signals": [],
        "hard_constraints": {"item_code_hint": False, "ranges": {}, "filters": set()},
        "confidence_map": {},
        "explanation_parts": [],
        "derived_specs": {},
    }


def _add_signal(intent, text):
    if text and text not in intent["signals"]:
        intent["signals"].append(text)


def _set_query(intent, query, source):
    query = cstr(query).strip()
    if not query:
        return
    if not intent["query"]:
        intent["query"] = query
        _add_signal(intent, f"query:{source}")


def _set_item_code_hint(intent, item_code):
    item_code = cstr(item_code).strip()
    if not item_code:
        return
    intent["item_code_hint"] = item_code
    intent["intent_class"] = "sku_lookup"
    intent["hard_constraints"]["item_code_hint"] = True
    _add_signal(intent, "sku")


def _set_sort(intent, sort_by, source, confidence=1.0):
    sort_by = LEGACY_SORT_ALIASES.get(cstr(sort_by).strip(), cstr(sort_by).strip())
    if sort_by not in SUPPORTED_SORT_VALUES:
        return
    if not intent["sort_by"] or confidence >= intent["confidence_map"].get("sort_by", 0):
        intent["sort_by"] = sort_by
        intent["confidence_map"]["sort_by"] = confidence
        _add_signal(intent, f"sort:{source}")


def _add_filter_value(intent, filter_key, value, source, confidence=1.0, hard=False):
    if filter_key not in intent["filters"] or filter_key not in ARRAY_FILTER_KEYS:
        return
    value = cstr(value).strip()
    if not value or value in intent["filters"][filter_key]:
        return
    intent["filters"][filter_key].append(value)
    intent["confidence_map"][filter_key] = max(
        flt(intent["confidence_map"].get(filter_key)),
        confidence,
    )
    if hard:
        intent["hard_constraints"]["filters"].add(filter_key)
    _add_signal(intent, f"{filter_key}:{source}")


def _set_boolean_filter(intent, filter_key, value, source, confidence=1.0):
    if filter_key not in BOOLEAN_FILTER_KEYS:
        return
    intent["filters"][filter_key] = bool(value)
    intent["confidence_map"][filter_key] = max(
        flt(intent["confidence_map"].get(filter_key)),
        confidence,
    )
    if value:
        intent["hard_constraints"]["filters"].add(filter_key)
    _add_signal(intent, f"{filter_key}:{source}")


def _set_range(intent, range_key, min_value=None, max_value=None, source="deterministic", confidence=1.0, hard=False):
    if range_key not in RANGE_FILTER_DEFAULTS:
        return
    current = copy.deepcopy(intent["filters"][range_key])
    if min_value is not None:
        current["min"] = flt(min_value)
    if max_value is not None:
        current["max"] = flt(max_value)
    if current["max"] < current["min"]:
        current["min"], current["max"] = current["max"], current["min"]
    intent["filters"][range_key] = current
    intent["confidence_map"][range_key] = max(
        flt(intent["confidence_map"].get(range_key)),
        confidence,
    )
    if hard:
        intent["hard_constraints"]["ranges"][range_key] = True
    _add_signal(intent, f"{range_key}:{source}")


def _normalize_allowed_lookup(known_values):
    lookup = {}
    for filter_key, values in (known_values or {}).items():
        field_lookup = {}
        for value in values or []:
            cleaned = cstr(value).strip()
            normalized = normalize_text(cleaned)
            if cleaned and normalized:
                field_lookup[normalized] = cleaned
        lookup[filter_key] = field_lookup
    return lookup


def _extract_sku_hint(normalized_message):
    candidates = re.findall(r"\b[a-z0-9]+(?:[-_/][a-z0-9]+)+\b|\b[a-z]*\d[a-z0-9-]{2,}\b", normalized_message)
    for candidate in candidates:
        normalized_candidate = normalize_text(candidate)
        if not any(char.isdigit() for char in normalized_candidate):
            continue
        if re.match(r"^ip\d{2,3}$", normalized_candidate):
            continue
        if re.match(r"^\d+(?:k|w|v|ma|a|d)$", normalized_candidate):
            continue
        if re.match(r"^\d{4,5}k$", normalized_candidate):
            continue
        if "-" not in candidate and "_" not in candidate and len(normalized_candidate) < 5:
            continue
        return cstr(candidate).upper()
    return ""


def _extract_voltage_values(normalized_message):
    return re.findall(r"\b\d+(?:\.\d+)?(?:\s*-\s*\d+(?:\.\d+)?)?\s*v\b", normalized_message)


def _extract_current_values(normalized_message):
    return re.findall(r"\b\d+(?:\.\d+)?\s*ma\b", normalized_message)


def _extract_beam_values(normalized_message):
    return re.findall(r"\b\d{1,3}\s*(?:d|deg|degree)\b", normalized_message)


def _extract_numeric_first(text):
    match = re.search(r"(-?\d+(?:\.\d+)?)", cstr(text or ""))
    return flt(match.group(1)) if match else None


def _extract_dimension_token(raw_message):
    match = re.search(
        r"\b(\d+(?:\.\d+)?\s*[x×]\s*\d+(?:\.\d+)?(?:\s*[x×]\s*\d+(?:\.\d+)?)?)\s*(?:mm|cm)?\b",
        cstr(raw_message or ""),
        re.IGNORECASE,
    )
    return cstr(match.group(1)).replace("×", "x").strip() if match else ""


def _extract_cut_out_token(normalized_message):
    match = re.search(
        r"\bcut\s*out\s*(\d+(?:\.\d+)?(?:\s*[x×]\s*\d+(?:\.\d+)?)?)\b",
        normalized_message,
    )
    return cstr(match.group(1)).replace("×", "x").strip() if match else ""


def _extract_lumens_value(normalized_message):
    match = re.search(r"\b(\d+(?:\.\d+)?)\s*l(?:m|umen|umens)\b", normalized_message)
    return flt(match.group(1)) if match else 0


def _extract_cri_value(normalized_message):
    match = re.search(r"\bcri\s*(?:>=|>|=)?\s*(\d+(?:\.\d+)?)\b", normalized_message)
    return flt(match.group(1)) if match else 0


def _infer_environment(normalized_message):
    if any(term in normalized_message for term in ("outdoor", "exterior", "landscape", "facade", "garden")):
        return "outdoor"
    if any(term in normalized_message for term in ("indoor", "interior", "office", "hotel room", "bedroom")):
        return "indoor"
    return ""


def _append_derived_query_token(intent, token, source):
    token = cstr(token).strip()
    if not token:
        return
    if not intent["query"]:
        intent["query"] = token
    elif token.lower() not in cstr(intent["query"]).lower():
        intent["query"] = f'{intent["query"]} {token}'.strip()
    _add_signal(intent, f"query_token:{source}")


def _extract_additional_specs(intent, preprocessed_message):
    normalized_message = preprocessed_message["normalized_message"]
    raw_message = preprocessed_message["message"]
    derived_specs = {}

    lumens = _extract_lumens_value(normalized_message)
    if lumens:
        derived_specs["lumens_min"] = lumens * 0.85
        derived_specs["lumens_max"] = lumens * 1.15
        _append_derived_query_token(intent, f"{int(lumens)}lm", "lumens")

    cri = _extract_cri_value(normalized_message)
    if cri:
        derived_specs["cri_min"] = cri
        _append_derived_query_token(intent, f"CRI {int(cri)}", "cri")

    dimension = _extract_dimension_token(raw_message)
    if dimension:
        derived_specs["dimension_text"] = dimension
        _append_derived_query_token(intent, dimension, "dimension")

    cut_out = _extract_cut_out_token(normalized_message)
    if cut_out:
        derived_specs["cut_out_text"] = cut_out
        _append_derived_query_token(intent, f"cut out {cut_out}", "cut_out")

    environment = _infer_environment(normalized_message)
    if environment:
        derived_specs["environment"] = environment
        if environment == "outdoor" and not intent["filters"]["ip_rate"]:
            _append_derived_query_token(intent, "outdoor", "environment")

    if derived_specs:
        intent["derived_specs"].update(derived_specs)
        _add_signal(intent, "derived_specs")


def _match_known_values(normalized_message, known_values):
    matches = {}
    for filter_key, values in (known_values or {}).items():
        for normalized_value, canonical_value in values.items():
            if not normalized_value:
                continue
            if re.search(rf"(?<![a-z0-9]){re.escape(normalized_value)}(?![a-z0-9])", normalized_message):
                matches.setdefault(filter_key, []).append(canonical_value)
    return matches


def extract_deterministic_intent(preprocessed_message, vocabulary):
    intent = _build_intent_state()
    normalized_message = preprocessed_message["normalized_message"]
    expanded_message = preprocessed_message["expanded_message"]
    page_context = preprocessed_message["page_context"]
    known_lookup = _normalize_allowed_lookup(vocabulary.get("known_values"))

    sku_hint = _extract_sku_hint(normalized_message)
    if sku_hint:
        _set_item_code_hint(intent, sku_hint)
        _set_query(intent, sku_hint, "sku")

    ip_match = re.search(r"\bip\s*([0-9]{2,3})\b", normalized_message)
    if ip_match:
        ip_rating = normalize_ip_rate(ip_match.group(0))
        _add_filter_value(intent, "ip_rate", ip_rating, "regex", confidence=1.0, hard=True)

    color_phrase_match = re.search(
        r"\b(\d{4,5}\s*k|warm white|cool white|daylight|neutral white)\b",
        normalized_message,
    )
    if color_phrase_match:
        color_temp = normalize_color_temp(color_phrase_match.group(1))
        _add_filter_value(intent, "color_temp", color_temp, "regex", confidence=1.0, hard=True)
        kelvin = _extract_numeric_first(color_temp)
        if kelvin is not None:
            _set_range(
                intent,
                "color_temp_kelvin_range",
                max(0, kelvin - 500),
                kelvin + 500,
                source="regex",
                confidence=1.0,
                hard=True,
            )

    power_match = re.search(r"\b(\d+(?:\.\d+)?)\s*w\b", normalized_message)
    if power_match:
        power_value = flt(power_match.group(1))
        _add_filter_value(intent, "power", f"{power_match.group(1)}W", "regex", confidence=1.0, hard=True)
        _set_range(
            intent,
            "power_value_range",
            power_value * 0.9,
            power_value * 1.1,
            source="regex",
            confidence=1.0,
            hard=True,
        )

    for voltage in _extract_voltage_values(normalized_message):
        _add_filter_value(intent, "input_voltage", voltage.upper().replace(" ", ""), "regex", confidence=0.95, hard=True)
        break

    for current in _extract_current_values(normalized_message):
        _add_filter_value(intent, "output_current", current.upper().replace(" ", ""), "regex", confidence=0.95, hard=True)
        break

    for beam in _extract_beam_values(normalized_message):
        beam_value = beam.upper().replace("DEG", "D").replace("DEGREE", "D").replace(" ", "")
        _add_filter_value(intent, "beam_angle", beam_value, "regex", confidence=0.9, hard=True)
        break

    if any(phrase in normalized_message for phrase in ("in stock", "available stock", "available now")):
        _set_boolean_filter(intent, "in_stock", True, "phrase", confidence=1.0)

    if any(phrase in normalized_message for phrase in ("high stock", "most stock", "stock high to low", "quantity wise", "quantity high to low")):
        _set_sort(intent, "stock:desc", "phrase", confidence=1.0)
        intent["intent_class"] = "stock_priority"
    elif any(phrase in normalized_message for phrase in ("low stock", "stock low to high", "quantity low to high")):
        _set_sort(intent, "stock:asc", "phrase", confidence=1.0)
        intent["intent_class"] = "stock_priority"
    elif any(phrase in normalized_message for phrase in ("highest discount", "discount high to low", "biggest discount")):
        _set_sort(intent, "discount_percentage:desc", "phrase", confidence=1.0)
        intent["intent_class"] = "discount_priority"
    elif any(phrase in normalized_message for phrase in ("latest", "newest", "recent")):
        _set_sort(intent, "creation:desc", "phrase", confidence=1.0)
        intent["intent_class"] = "recent_products"
    elif any(phrase in normalized_message for phrase in ("cheapest", "lowest price", "low price", "price low to high")):
        _set_sort(intent, "rate:asc", "phrase", confidence=1.0)
    elif any(phrase in normalized_message for phrase in ("highest price", "most expensive", "price high to low")):
        _set_sort(intent, "rate:desc", "phrase", confidence=1.0)

    under_match = re.search(r"\b(?:under|below|less than)\s*(aed\s*)?(\d+(?:\.\d+)?)\b", normalized_message)
    over_match = re.search(r"\b(?:over|above|more than)\s*(aed\s*)?(\d+(?:\.\d+)?)\b", normalized_message)
    between_match = re.search(
        r"\bbetween\s*(\d+(?:\.\d+)?)\s*(?:and|to)\s*(\d+(?:\.\d+)?)\b",
        normalized_message,
    )
    if between_match:
        _set_range(intent, "rate_range", between_match.group(1), between_match.group(2), source="regex", confidence=1.0, hard=True)
    elif under_match:
        _set_range(intent, "rate_range", None, under_match.group(2), source="regex", confidence=1.0, hard=True)
    elif over_match:
        _set_range(intent, "rate_range", over_match.group(2), None, source="regex", confidence=1.0, hard=True)

    _extract_additional_specs(intent, preprocessed_message)

    matched_values = _match_known_values(expanded_message, known_lookup)
    for filter_key, values in matched_values.items():
        if filter_key in ("ip_rate", "color_temp", "power"):
            continue
        for value in values[:3]:
            _add_filter_value(intent, filter_key, value, "vocabulary", confidence=0.9, hard=False)

    if page_context.get("brand"):
        _add_filter_value(intent, "brand", page_context["brand"], "page_context", confidence=0.6, hard=False)
    if page_context.get("category"):
        _add_filter_value(intent, "category_list", page_context["category"], "page_context", confidence=0.6, hard=False)
    if page_context.get("search") and not intent["query"]:
        _set_query(intent, page_context["search"], "page_context")

    query_candidate = preprocessed_message["message"]
    for value in re.findall(r"\b(?:ip\s*\d+|\d+(?:\.\d+)?\s*w|\d{4,5}\s*k)\b", normalized_message):
        query_candidate = re.sub(re.escape(value), " ", query_candidate, flags=re.IGNORECASE)
    query_candidate = re.sub(r"\s+", " ", query_candidate).strip()
    if intent["intent_class"] != "sku_lookup" and query_candidate:
        _set_query(intent, query_candidate, "message")

    if any(phrase in normalized_message for phrase in ("alternative", "similar", "equivalent", "replacement")):
        intent["intent_class"] = "alternatives"
        _add_signal(intent, "alternatives")
    elif (
        intent["intent_class"] == "general_search"
        and (intent["hard_constraints"]["filters"] or intent["hard_constraints"]["ranges"])
    ):
        intent["intent_class"] = "spec_match"

    return intent


def needs_model_reasoning(preprocessed_message, deterministic_intent):
    if deterministic_intent["intent_class"] == "sku_lookup":
        return False
    if len(deterministic_intent["signals"]) >= 3:
        return False
    if deterministic_intent["filters"]["in_stock"] and deterministic_intent["query"]:
        return False
    return preprocessed_message["word_count"] >= 4


def _build_model_messages(message, page_context, vocabulary, deterministic_intent):
    response_shape = {
        "query": "string",
        "sort_by": "one of the allowed sort values",
        "filters": {
            **{key: ["string"] for key in ARRAY_FILTER_KEYS},
            **copy.deepcopy(RANGE_FILTER_DEFAULTS),
            **{key: False for key in BOOLEAN_FILTER_KEYS},
        },
        "explanation": "short explanation string",
    }

    examples = [
        {
            "message": "ip65 3000k downlight",
            "response": {
                "query": "downlight",
                "sort_by": "",
                "filters": {"ip_rate": ["IP65"], "color_temp": ["3000K"]},
            },
        },
        {
            "message": "driver 24v 350ma",
            "response": {
                "query": "driver",
                "sort_by": "",
                "filters": {"input_voltage": ["24V"], "output_current": ["350MA"]},
            },
        },
        {
            "message": "high stock surface lights under 500",
            "response": {
                "query": "surface lights",
                "sort_by": "stock:desc",
                "filters": {"rate_range": {"min": 0, "max": 500}, "in_stock": True},
            },
        },
    ]

    system_prompt = f"""
You convert natural-language product discovery requests into structured search intent for a Typesense-backed product listing API.
Return ONLY valid JSON. Do not return markdown. Do not add keys outside the required schema.

Required JSON shape:
{json.dumps(response_shape, ensure_ascii=True)}

Rules:
- Use ONLY the current V2 contract field names.
- Never use legacy keys such as color_temp_, input, warranty_, or price_range.
- Prefer existing supported filters and known values.
- If the deterministic seed already contains strong filters, only add missing fields.
- Use sort_by only when the user clearly asked for ordering such as stock high-to-low, latest, cheapest, or highest discount.
- Keep query short and useful for product search.
- If unsure, leave arrays empty, booleans false, and ranges at broad defaults.

Allowed sort values:
{json.dumps(sorted(SUPPORTED_SORT_VALUES), ensure_ascii=True)}

Few-shot examples:
{json.dumps(examples, ensure_ascii=True)}
""".strip()

    user_payload = {
        "message": cstr(message).strip(),
        "page_context": page_context,
        "deterministic_seed": {
            "query": deterministic_intent.get("query"),
            "sort_by": deterministic_intent.get("sort_by"),
            "filters": deterministic_intent.get("filters"),
            "intent_class": deterministic_intent.get("intent_class"),
        },
        "known_filter_values": _trim_known_values_for_prompt(vocabulary.get("known_values")),
    }
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=True)},
    ]


def parse_json_response(content):
    content = (content or "").strip()
    if not content:
        raise ValueError("AI returned an empty response")

    try:
        return json.loads(content)
    except Exception:
        pass

    start = content.find("{")
    end = content.rfind("}")
    if start != -1 and end != -1 and end > start:
        return json.loads(content[start : end + 1])

    raise ValueError("AI response was not valid JSON")


def _call_ai_provider(provider, messages):
    if provider == "openai":
        api_key = get_openai_api_key()
        if not api_key:
            raise ValueError("OpenAI API key is not configured")
        response = requests.post(
            OPENAI_API_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": get_openai_model(),
                "messages": messages,
                "temperature": 0.1,
                "max_tokens": 1200,
                "response_format": {"type": "json_object"},
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json() or {}
        content = ((payload.get("choices") or [{}])[0].get("message") or {}).get("content", "")
        record_ai_metric("provider_openai")
        return content, parse_json_response(content)

    if provider == "groq":
        api_key = get_groq_api_key()
        if not api_key:
            raise ValueError("Groq API key is not configured")
        response = requests.post(
            GROQ_API_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": get_groq_model(),
                "messages": messages,
                "temperature": 0.1,
                "max_tokens": 1200,
                "response_format": {"type": "json_object"},
            },
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json() or {}
        content = ((payload.get("choices") or [{}])[0].get("message") or {}).get("content", "")
        record_ai_metric("provider_groq")
        return content, parse_json_response(content)

    raise ValueError(f"Unsupported AI provider: {provider}")


def call_ai_for_product_search(message, page_context, vocabulary, deterministic_intent):
    messages = _build_model_messages(message, page_context, vocabulary, deterministic_intent)
    last_error = None

    for provider in ("openai", "groq"):
        try:
            if provider == "openai" and not get_openai_api_key():
                continue
            if provider == "groq" and not get_groq_api_key():
                continue
            raw_content, parsed = _call_ai_provider(provider, messages)
            return provider, raw_content, parsed
        except Exception as exc:
            last_error = exc
            log_ai_product_search(
                "provider_failure",
                {"provider": provider, "error": cstr(exc)},
            )
            continue

    record_ai_metric("provider_fallback")
    raise last_error or ValueError("No AI provider is configured")


def _to_number(value):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        return flt(value)
    except Exception:
        return None


def _to_boolean(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    value = cstr(value).strip().lower()
    return value in {"true", "1", "yes", "y"}


def sanitize_string_list(values, allowed_values):
    if not isinstance(values, list):
        return []

    allowed_lookup = {
        normalize_text(value): value
        for value in (allowed_values or [])
        if cstr(value).strip()
    }
    sanitized_values = []
    seen = set()
    for value in values:
        cleaned = cstr(value).strip()
        if not cleaned:
            continue
        canonical_value = allowed_lookup.get(normalize_text(cleaned))
        if not canonical_value or canonical_value in seen:
            continue
        seen.add(canonical_value)
        sanitized_values.append(canonical_value)
    return sanitized_values


def sanitize_range(value, defaults):
    sanitized = copy.deepcopy(defaults)
    if not isinstance(value, dict):
        return sanitized

    parsed_min = _to_number(value.get("min"))
    parsed_max = _to_number(value.get("max"))
    if parsed_min is not None:
        sanitized["min"] = parsed_min
    if parsed_max is not None:
        sanitized["max"] = parsed_max
    if sanitized["max"] < sanitized["min"]:
        sanitized["min"], sanitized["max"] = sanitized["max"], sanitized["min"]
    return sanitized


def sanitize_ai_product_search_response(ai_response, vocabulary=None):
    vocabulary = vocabulary or get_ai_search_vocabulary()
    known_values = vocabulary.get("known_values") or {}
    sanitized_response = build_default_response()

    if not isinstance(ai_response, dict):
        return sanitized_response

    sanitized_response["query"] = cstr(ai_response.get("query") or "").strip()

    sort_by = LEGACY_SORT_ALIASES.get(
        cstr(ai_response.get("sort_by") or "").strip(),
        cstr(ai_response.get("sort_by") or "").strip(),
    )
    if sort_by in SUPPORTED_SORT_VALUES:
        sanitized_response["sort_by"] = sort_by

    filters = ai_response.get("filters")
    if not isinstance(filters, dict):
        filters = {}

    remapped_filters = {}
    for filter_key, value in filters.items():
        remapped_filters[LEGACY_FILTER_KEY_ALIASES.get(filter_key, filter_key)] = value

    for filter_key in ARRAY_FILTER_KEYS:
        sanitized_response["filters"][filter_key] = sanitize_string_list(
            remapped_filters.get(filter_key),
            known_values.get(filter_key),
        )

    for filter_key in BOOLEAN_FILTER_KEYS:
        sanitized_response["filters"][filter_key] = _to_boolean(remapped_filters.get(filter_key))

    for filter_key, defaults in RANGE_FILTER_DEFAULTS.items():
        sanitized_response["filters"][filter_key] = sanitize_range(remapped_filters.get(filter_key), defaults)

    sanitized_response["explanation"] = cstr(ai_response.get("explanation") or "").strip()
    return sanitized_response


def merge_structured_intent(deterministic_intent, ai_response):
    merged = copy.deepcopy(deterministic_intent)
    merged["explanation_parts"] = list(merged.get("explanation_parts") or [])
    if not ai_response:
        return merged

    if ai_response.get("query") and not merged.get("item_code_hint"):
        merged["query"] = ai_response["query"]
    if ai_response.get("sort_by") and not merged.get("sort_by"):
        merged["sort_by"] = ai_response["sort_by"]

    for filter_key in ARRAY_FILTER_KEYS:
        for value in ai_response.get("filters", {}).get(filter_key, []) or []:
            _add_filter_value(merged, filter_key, value, "ai", confidence=0.6, hard=False)

    for filter_key in BOOLEAN_FILTER_KEYS:
        if ai_response.get("filters", {}).get(filter_key):
            _set_boolean_filter(merged, filter_key, True, "ai", confidence=0.6)

    for range_key in RANGE_FILTER_DEFAULTS:
        candidate_range = ai_response.get("filters", {}).get(range_key) or {}
        defaults = RANGE_FILTER_DEFAULTS[range_key]
        if candidate_range != defaults:
            _set_range(
                merged,
                range_key,
                candidate_range.get("min"),
                candidate_range.get("max"),
                source="ai",
                confidence=0.6,
                hard=False,
            )

    if ai_response.get("explanation"):
        merged["explanation_parts"].append(ai_response["explanation"])
    return merged


def _finalize_intent(intent):
    resolved = {
        "query": cstr(intent.get("query") or "").strip(),
        "sort_by": cstr(intent.get("sort_by") or "").strip(),
        "item_code_hint": cstr(intent.get("item_code_hint") or "").strip(),
        "filters": build_default_filters(),
        "intent_class": intent.get("intent_class") or "general_search",
        "query_mode": "fast_hybrid",
        "provider": intent.get("provider") or "deterministic",
        "llm_used": bool(intent.get("llm_used")),
        "signals": list(intent.get("signals") or []),
        "confidence_map": copy.deepcopy(intent.get("confidence_map") or {}),
        "hard_constraints": {
            "item_code_hint": bool(intent.get("hard_constraints", {}).get("item_code_hint")),
            "ranges": copy.deepcopy(intent.get("hard_constraints", {}).get("ranges") or {}),
            "filters": sorted(intent.get("hard_constraints", {}).get("filters") or []),
        },
        "explanation": " ".join(
            part.strip() for part in (intent.get("explanation_parts") or []) if cstr(part).strip()
        ).strip(),
        "derived_specs": copy.deepcopy(intent.get("derived_specs") or {}),
    }

    for filter_key in ARRAY_FILTER_KEYS:
        resolved["filters"][filter_key] = list(intent.get("filters", {}).get(filter_key) or [])
    for filter_key in BOOLEAN_FILTER_KEYS:
        resolved["filters"][filter_key] = bool(intent.get("filters", {}).get(filter_key))
    for range_key, defaults in RANGE_FILTER_DEFAULTS.items():
        resolved["filters"][range_key] = sanitize_range(intent.get("filters", {}).get(range_key), defaults)

    if not resolved["query"] and not resolved["item_code_hint"]:
        resolved["query"] = ""
    return resolved


def resolve_ai_search_intent(message, page_context=None, mode="fast"):
    message = cstr(message).strip()
    if not message:
        return build_default_response("Empty message supplied for AI product search.")

    if not is_ai_product_search_enabled():
        response = build_default_response("AI product search is disabled.")
        response["resolved_intent"] = {
            "intent_class": "disabled",
            "query_mode": "disabled",
            "provider": "disabled",
            "llm_used": False,
            "signals": [],
            "confidence_map": {},
            "hard_constraints": {"item_code_hint": False, "ranges": {}, "filters": []},
        }
        return response

    start_ts = time.time()
    record_ai_metric("requests")
    vocabulary = get_ai_search_vocabulary()
    preprocessed = preprocess_user_message(message, page_context=page_context)
    deterministic_intent = extract_deterministic_intent(preprocessed, vocabulary)
    deterministic_intent["explanation_parts"].append(
        "Deterministic parsing extracted SKU/spec/sort signals."
    )
    log_ai_product_search(
        "deterministic_output",
        {
            "message": message,
            "page_context": preprocessed["page_context"],
            "intent": deterministic_intent,
        },
    )

    llm_used = False
    provider_name = "deterministic"
    if mode == "fast" and needs_model_reasoning(preprocessed, deterministic_intent):
        try:
            provider_name, raw_content, ai_response = call_ai_for_product_search(
                message,
                preprocessed["page_context"],
                vocabulary,
                deterministic_intent,
            )
            llm_used = True
            log_ai_product_search(
                "raw_output",
                {"provider": provider_name, "message": message, "raw_output": raw_content},
            )
            sanitized_ai_response = sanitize_ai_product_search_response(ai_response, vocabulary=vocabulary)
            deterministic_intent = merge_structured_intent(deterministic_intent, sanitized_ai_response)
        except Exception:
            record_ai_metric("llm_failures")
            log_ai_product_search(
                "fallback_output",
                {"message": message, "error": frappe.get_traceback()},
            )

    deterministic_intent["llm_used"] = llm_used
    deterministic_intent["provider"] = provider_name
    finalized = _finalize_intent(deterministic_intent)
    if not finalized["explanation"]:
        finalized["explanation"] = "Parsed natural-language search request into structured V2 filters."
    finalized["resolved_intent"] = {
        "intent_class": finalized["intent_class"],
        "query_mode": finalized["query_mode"],
        "provider": finalized["provider"],
        "llm_used": finalized["llm_used"],
        "signals": finalized["signals"],
        "confidence_map": finalized["confidence_map"],
        "hard_constraints": finalized["hard_constraints"],
    }
    finalized["query_debug"] = {
        "latency_ms": round((time.time() - start_ts) * 1000, 2),
        "llm_used": llm_used,
        "provider": provider_name,
        "signals": finalized["signals"],
    }
    if not llm_used:
        record_ai_metric("deterministic_only")
    return finalized


def parse_product_search_intent(message, page_context=None):
    response = resolve_ai_search_intent(message=message, page_context=page_context, mode="fast")
    return {
        "query": response.get("query", ""),
        "sort_by": response.get("sort_by", ""),
        "filters": response.get("filters", build_default_filters()),
        "explanation": response.get("explanation", ""),
        "resolved_intent": response.get("resolved_intent", {}),
        "query_debug": response.get("query_debug", {}),
    }


def _filters_are_default(filters):
    for filter_key in ARRAY_FILTER_KEYS:
        if filters.get(filter_key):
            return False
    for filter_key in BOOLEAN_FILTER_KEYS:
        if filters.get(filter_key):
            return False
    for range_key, defaults in RANGE_FILTER_DEFAULTS.items():
        if sanitize_range(filters.get(range_key), defaults) != defaults:
            return False
    return True


def _make_relaxed_intent(intent, stage):
    relaxed = copy.deepcopy(intent)
    note = None

    if stage == 1:
        removed_fields = []
        for filter_key in ARRAY_FILTER_KEYS:
            if (
                relaxed["filters"].get(filter_key)
                and filter_key not in relaxed["hard_constraints"]["filters"]
                and flt(relaxed["confidence_map"].get(filter_key)) < 0.8
            ):
                relaxed["filters"][filter_key] = []
                removed_fields.append(filter_key)
        if removed_fields:
            note = f"Relaxed low-confidence filters: {', '.join(removed_fields)}"

    if stage == 2:
        for filter_key in ARRAY_FILTER_KEYS:
            if filter_key not in relaxed["hard_constraints"]["filters"]:
                relaxed["filters"][filter_key] = []
        for range_key, defaults in RANGE_FILTER_DEFAULTS.items():
            if range_key not in relaxed["hard_constraints"]["ranges"]:
                relaxed["filters"][range_key] = copy.deepcopy(defaults)
        if relaxed.get("sort_by") and flt(relaxed["confidence_map"].get("sort_by")) < 1.0:
            relaxed["sort_by"] = ""
        note = "Fell back to strongest query and hard constraints only."

    return relaxed, note


def _parse_numeric_string(value):
    match = re.search(r"(-?\d+(?:\.\d+)?)", cstr(value or ""))
    return flt(match.group(1)) if match else 0


def _string_contains_token(value, token):
    return normalize_text(token) in normalize_text(value)


def calculate_ai_compatibility_score(document, intent):
    score = 0.0
    filters = intent.get("filters") or {}
    derived_specs = intent.get("derived_specs") or {}

    for field_name in ("mounting", "lamp_type", "material", "body_finish", "input_voltage", "output_voltage", "output_current"):
        if filters.get(field_name) and cstr(document.get(field_name)) in filters.get(field_name):
            score += 8

    if filters.get("ip_rate") and cstr(document.get("ip_rate")) in filters.get("ip_rate"):
        score += 10
    if filters.get("color_temp") and cstr(document.get("color_temp")) in filters.get("color_temp"):
        score += 10
    if filters.get("power") and cstr(document.get("power")) in filters.get("power"):
        score += 8

    for range_key, doc_field, tolerance_weight in (
        ("power_value_range", "power_value", 12),
        ("color_temp_kelvin_range", "color_temp_kelvin", 12),
        ("ip_rating_numeric_range", "ip_rating_numeric", 10),
    ):
        value_range = filters.get(range_key) or {}
        doc_value = flt(document.get(doc_field))
        if doc_value and doc_value >= flt(value_range.get("min")) and doc_value <= flt(value_range.get("max")):
            score += tolerance_weight

    if derived_specs.get("lumens_min") and derived_specs.get("lumens_max"):
        lumens_value = _parse_numeric_string(document.get("lumen_output"))
        if lumens_value and derived_specs["lumens_min"] <= lumens_value <= derived_specs["lumens_max"]:
            score += 10

    if derived_specs.get("cri_min"):
        cri_value = _parse_numeric_string(document.get("cri"))
        if cri_value and cri_value >= flt(derived_specs["cri_min"]):
            score += 8

    if derived_specs.get("dimension_text") and _string_contains_token(document.get("dimension"), derived_specs["dimension_text"]):
        score += 6

    if derived_specs.get("cut_out_text") and _string_contains_token(document.get("cut_out"), derived_specs["cut_out_text"]):
        score += 6

    if derived_specs.get("environment") == "outdoor" and flt(document.get("ip_rating_numeric")) >= 44:
        score += 6

    score += 4 if cint(document.get("in_stock")) else 0
    score += min(flt(document.get("business_score")) / 20, 5)
    return round(score, 2)


def get_query_feedback_scores(normalized_query):
    normalized_query = normalize_text(normalized_query)
    if not normalized_query or not _doctype_exists("AI Product Search Event"):
        return {}

    rows = frappe.get_all(
        "AI Product Search Event",
        filters={
            "event_type": ["in", list(FEEDBACK_WEIGHTS.keys())],
            "normalized_query": normalized_query,
        },
        fields=["selected_item_code", "event_type"],
        limit_page_length=500,
    )
    scores = defaultdict(int)
    for row in rows:
        item_code = cstr(row.selected_item_code or "").strip()
        if not item_code:
            continue
        scores[item_code] += FEEDBACK_WEIGHTS.get(row.event_type, 0)
    return dict(scores)


def rerank_hits_with_feedback(hits, normalized_query, intent):
    if not hits or intent.get("intent_class") == "sku_lookup":
        return hits, {}
    feedback_scores = get_query_feedback_scores(normalized_query)
    if not feedback_scores:
        return hits, {}

    ranked = list(hits)
    ranked.sort(
        key=lambda hit: (
            feedback_scores.get(hit.get("document", {}).get("item_code"), 0),
            hit.get("text_match") or 0,
        ),
        reverse=True,
    )
    return ranked, feedback_scores


def rerank_hits_with_compatibility(hits, intent):
    if not hits:
        return hits
    if intent.get("intent_class") not in {"spec_match", "alternatives", "stock_priority", "discount_priority"}:
        return hits

    ranked = list(hits)
    ranked.sort(
        key=lambda hit: (
            calculate_ai_compatibility_score(hit.get("document", {}), intent),
            hit.get("text_match") or 0,
        ),
        reverse=True,
    )
    return ranked


def _compute_result_quality(found, applied_relaxations, intent):
    if cint(found) <= 0:
        return "zero"
    if intent.get("intent_class") == "sku_lookup":
        return "strong" if cint(found) >= 1 else "zero"
    if cint(found) >= 5 and not applied_relaxations:
        return "strong"
    if cint(found) >= 1:
        return "weak" if applied_relaxations else "medium"
    return "zero"


def execute_intent_search(intent, page=1, page_length=20, include_inactive=0, feature_flag_override=0):
    from igh_search.igh_search.product_search_v2 import search_products_v2 as search_products_v2_impl

    return search_products_v2_impl(
        query=intent.get("query"),
        filters=intent.get("filters"),
        sort_by=intent.get("sort_by"),
        page=page,
        page_length=page_length,
        include_inactive=include_inactive,
        item_code_hint=intent.get("item_code_hint"),
        feature_flag_override=feature_flag_override,
        strict_sort=1 if intent.get("sort_by") else 0,
    )


def ai_search_products_v2(
    message=None,
    page_context=None,
    page=1,
    page_length=20,
    include_inactive=0,
    feature_flag_override=0,
):
    if getattr(frappe.local, "request", None) and frappe.local.request.method not in ("POST", "GET"):
        frappe.throw(_("AI product search only supports GET and POST requests."))

    from igh_search.igh_search.product_search_v2 import ensure_query_access

    ensure_query_access(feature_flag_override=feature_flag_override)
    intent = resolve_ai_search_intent(message=message, page_context=page_context, mode="fast")
    if not cstr(message).strip():
        return {
            "hits": [],
            "found": 0,
            "facet_counts": [],
            "resolved_intent": intent.get("resolved_intent", {}),
            "applied_filters": intent.get("filters", {}),
            "applied_sort": intent.get("sort_by", ""),
            "applied_relaxations": [],
            "explanation": intent.get("explanation", ""),
            "query_debug": intent.get("query_debug", {}),
        }

    attempts = [copy.deepcopy(intent)]
    applied_relaxations = []
    response = execute_intent_search(
        intent,
        page=page,
        page_length=page_length,
        include_inactive=include_inactive,
        feature_flag_override=feature_flag_override,
    )
    log_ai_product_search(
        "search_attempt",
        {
            "intent": intent,
            "found": response.get("found"),
            "query_debug": response.get("query_debug"),
        },
    )

    weak_results = (
        cint(response.get("found")) == 0
        or (
            intent.get("resolved_intent", {}).get("intent_class") != "sku_lookup"
            and cint(response.get("found")) < 3
            and not _filters_are_default(intent.get("filters") or {})
        )
    )

    if weak_results:
        for stage in (1, 2):
            if intent.get("resolved_intent", {}).get("hard_constraints", {}).get("item_code_hint"):
                break
            relaxed_intent, note = _make_relaxed_intent(attempts[-1], stage)
            if not note:
                continue
            record_ai_metric("search_relaxations")
            applied_relaxations.append(note)
            attempts.append(relaxed_intent)
            response = execute_intent_search(
                relaxed_intent,
                page=page,
                page_length=page_length,
                include_inactive=include_inactive,
                feature_flag_override=feature_flag_override,
            )
            log_ai_product_search(
                "relaxed_search_attempt",
                {
                    "stage": stage,
                    "relaxation": note,
                    "found": response.get("found"),
                    "intent": relaxed_intent,
                },
            )
            if cint(response.get("found")) > 0:
                intent = relaxed_intent
                break

    feedback_scores = {}
    if cint(response.get("found")) > 0:
        reranked_hits = rerank_hits_with_compatibility(response.get("hits", []), intent)
        reranked_hits, feedback_scores = rerank_hits_with_feedback(
            reranked_hits,
            intent.get("query") or intent.get("item_code_hint") or message,
            intent,
        )
        response["hits"] = reranked_hits

    if cint(response.get("found")) == 0:
        record_ai_metric("zero_results")
    else:
        record_ai_metric("final_results")

    quality_signals = {
        "result_quality": _compute_result_quality(response.get("found"), applied_relaxations, intent),
        "retry_stage": len(applied_relaxations),
        "deterministic_only": not intent.get("llm_used"),
        "feedback_reranked": bool(feedback_scores),
        "compatibility_reranked": intent.get("intent_class") in {"spec_match", "alternatives", "stock_priority", "discount_priority"},
    }
    top_item_codes = [
        hit.get("document", {}).get("item_code")
        for hit in response.get("hits", [])[:10]
        if hit.get("document", {}).get("item_code")
    ]
    search_event_id = log_ai_search_event(
        "search_issued",
        {
            "raw_message": message,
            "normalized_query": normalize_text(intent.get("query") or intent.get("item_code_hint") or message),
            "resolved_intent": {
                "intent_class": intent.get("intent_class"),
                "query_mode": intent.get("query_mode"),
                "provider": intent.get("provider"),
                "llm_used": intent.get("llm_used"),
                "signals": intent.get("signals", []),
                "confidence_map": intent.get("confidence_map", {}),
                "hard_constraints": intent.get("hard_constraints", {}),
                "derived_specs": intent.get("derived_specs", {}),
            },
            "deterministic_signals": intent.get("signals", []),
            "provider": intent.get("provider"),
            "llm_used": intent.get("llm_used"),
            "applied_sort": intent.get("sort_by"),
            "applied_filters": intent.get("filters", {}),
            "applied_relaxations": applied_relaxations,
            "result_count": response.get("found"),
            "latency_ms": (response.get("query_debug") or {}).get("latency_ms", 0),
            "top_item_codes": top_item_codes,
            "page_context": page_context,
            "outcome_status": quality_signals["result_quality"],
            "quality_signals": quality_signals,
        },
    )

    response["resolved_intent"] = {
        "intent_class": intent.get("intent_class"),
        "query_mode": intent.get("query_mode"),
        "provider": intent.get("provider"),
        "llm_used": intent.get("llm_used"),
        "signals": intent.get("signals", []),
        "confidence_map": intent.get("confidence_map", {}),
        "hard_constraints": intent.get("hard_constraints", {}),
        "derived_specs": intent.get("derived_specs", {}),
    }
    response["applied_filters"] = intent.get("filters", {})
    response["applied_sort"] = intent.get("sort_by", "")
    response["applied_relaxations"] = applied_relaxations
    response["explanation"] = intent.get("explanation", "")
    response["search_event_id"] = search_event_id
    response["quality_signals"] = quality_signals
    response["query_debug"] = {
        **(response.get("query_debug") or {}),
        "ai_provider": intent.get("provider"),
        "ai_llm_used": intent.get("llm_used"),
        "intent_class": intent.get("intent_class"),
        "applied_relaxations": applied_relaxations,
        "feedback_scores": feedback_scores,
        "ai_metrics": get_ai_search_quality_report(),
    }
    return response

# Phase 0 — Accuracy measurement harness for the Product Intelligence Layer.
#
# The existing evaluate_ai_search_benchmark() checks intent/sort/filter PARSING
# (did we build the right query). That is necessary but not sufficient: a query
# can parse correctly and still rank wrong products first. This module measures
# RESULT accuracy — do the returned documents actually satisfy the asked specs —
# plus latency, so every later phase (data cleanup, retrieval tuning, agentic
# verification) can be gated on a before/after scorecard.
#
# Two scorecards, both read-only, both runnable from `bench execute` or a dashboard:
#   run_search_scorecard()     -> the /list-style AI search (ai_search_products_v2)
#   run_assistant_scorecard()  -> the chatbot (product_assistant_chat)
#
# Metrics:
#   constraint_satisfaction@5  fraction of top-5 hits that satisfy ALL asserted
#                              specs (category/CCT/IP/power/range). THE accuracy number.
#   precision@5 / recall@k     only for cases that pin expected_item_codes.
#   zero_result_rate           share of cases that returned nothing.
#   intent/filter match rate   reused parse-level checks (regression guard).
#   latency p50/p95            warm latency per case.

import json
import re
import time

import frappe
from frappe.utils import cint, cstr, flt

from igh_search.igh_search.ai_product_search import (
    ai_search_products_v2,
    load_ai_search_benchmark_cases,
    _benchmark_filters_match,
    normalize_text,
)

# expected_filters list-key  -> the Typesense document field that carries the value
_LIST_KEY_DOC_FIELD = {
    "category_list": "category_list",
    "color_temp": "color_temp",
    "ip_rate": "ip_rate",
    "power": "power",
    "brand": "brand",
    "lamp_type": "lamp_type",
    "mounting": "mounting",
    "body_finish": "body_finish",
    "beam_angle": "beam_angle",
    "input_voltage": "input_voltage",
    "output_voltage": "output_voltage",
    "output_current": "output_current",
    "material": "material",
}
# expected_filters range-key -> document field to parse a number out of
_RANGE_KEY_DOC_FIELD = {
    "power_value_range": "power",
    "color_temp_kelvin_range": "color_temp",
    "ip_rating_numeric_range": "ip_rate",
    "rate_range": "rate",
    "stock_range": "stock",
    "lumen_output_range": "lumen_max",
}


def _num(value):
    m = re.search(r"\d+(?:\.\d+)?", cstr(value))
    return float(m.group()) if m else None


def _doc_satisfies(doc, expected_filters):
    """True if this returned document actually meets every asserted spec. None when
    the case asserts nothing checkable. Ranges get a small tolerance (the resolver
    widens single values like '10w' by ~10-20%)."""
    if not expected_filters:
        return None
    checked = False
    for key, exp in expected_filters.items():
        if isinstance(exp, dict):  # numeric range
            field = _RANGE_KEY_DOC_FIELD.get(key)
            if not field:
                continue
            val = flt(doc.get(field)) if field in ("rate", "stock") else _num(doc.get(field))
            if val is None:
                continue  # doc has no value to check -> don't penalise here
            checked = True
            if "min" in exp and val < flt(exp["min"]) * 0.95:
                return False
            if "max" in exp and val > flt(exp["max"]) * 1.05:
                return False
        elif isinstance(exp, bool):
            if key == "in_stock" and exp:
                checked = True
                if not (doc.get("in_stock") in (1, True) or flt(doc.get("stock")) > 0):
                    return False
        elif isinstance(exp, list):
            field = _LIST_KEY_DOC_FIELD.get(key)
            if not field or not exp:
                continue
            checked = True
            dv = normalize_text(doc.get(field))
            allowed = {normalize_text(v) for v in exp if v}
            # category_list / multi-token fields: accept containment either way
            if dv not in allowed and not any(a and (a in dv or dv in a) for a in allowed):
                return False
    return True if checked else None


def _llm_judge(query, docs):
    """LLM-as-judge: rate each result's appropriateness to the query 0/1/2
    (irrelevant / partial / good). Coarse spec-matching says a 3000K strip light
    'satisfies' "warm light for a hotel lobby"; a human (and this judge) does not.
    Returns (relevance@5 in [0,1], per-doc ratings). Best-effort; returns (None, [])
    if no LLM/configured or on error."""
    from igh_search.igh_search.ai_product_search import (
        get_openai_api_key, get_openai_model, OPENAI_API_URL,
    )
    import requests
    if not docs:
        return None, []
    key = get_openai_api_key()
    if not key:
        return None, []
    listing = [{
        "i": idx,
        "item_name": cstr(d.get("item_name"))[:80],
        "category": cstr(d.get("category_list")),
        "specs": {k: d.get(k) for k in ("power", "color_temp", "ip_rate", "beam_angle", "lamp_type") if d.get(k)},
    } for idx, d in enumerate(docs[:5])]
    system = (
        "You are a senior lighting sales engineer grading a product search. For the "
        "shopper's query, rate how appropriate EACH result is: 2=good match a rep would "
        "confidently offer, 1=partial/loosely related, 0=wrong (wrong product type or "
        "violates a stated spec). Judge product TYPE and fit, not just keywords (e.g. a "
        "strip light is NOT a good answer for 'light for a hotel lobby'). Return ONLY JSON "
        '{"ratings":[{"i":0,"r":2},...]}.'
    )
    user = {"query": query, "results": listing}
    try:
        r = requests.post(
            OPENAI_API_URL,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"model": get_openai_model(), "temperature": 0, "max_tokens": 300,
                  "response_format": {"type": "json_object"},
                  "messages": [{"role": "system", "content": system},
                               {"role": "user", "content": json.dumps(user, ensure_ascii=True)}]},
            timeout=30,
        )
        r.raise_for_status()
        out = json.loads(r.json()["choices"][0]["message"]["content"])
        ratings = [cint(x.get("r")) for x in (out.get("ratings") or []) if isinstance(x, dict)]
        if not ratings:
            return None, []
        return round(sum(min(2, max(0, v)) for v in ratings) / (2.0 * len(ratings)), 3), ratings
    except Exception:
        return None, []


def _percentile(values, pct):
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1))))
    return round(ordered[idx], 1)


def _score_hits(hits, case):
    """Per-case result metrics from the top-5 returned documents."""
    docs = [h.get("document", {}) for h in (hits or [])[:5]]
    expected_filters = case.get("expected_filters")
    expected_codes = [normalize_text(c) for c in (case.get("expected_item_codes") or [])]

    sat_flags = [_doc_satisfies(d, expected_filters) for d in docs]
    sat_checkable = [f for f in sat_flags if f is not None]
    constraint_sat = (sum(1 for f in sat_checkable if f) / len(sat_checkable)) if sat_checkable else None

    precision = recall = None
    if expected_codes:
        top_codes = [normalize_text(d.get("item_code")) for d in docs]
        hit_set = set(top_codes) & set(expected_codes)
        precision = round(len(hit_set) / max(1, len(top_codes)), 3)
        recall = round(len(hit_set) / len(expected_codes), 3)
    return constraint_sat, precision, recall


def run_search_scorecard(feature_flag_override=1, page_length=5, judge=0):
    """Accuracy + latency scorecard for ai_search_products_v2 over the golden set.
    judge=1 adds an LLM-as-judge relevance@5 (slower; one extra LLM call per case)."""
    judge = cint(judge)
    cases = load_ai_search_benchmark_cases()
    details, latencies = [], []
    sat_vals, prec_vals, rel_vals = [], [], []
    intent_ok = filter_ok = filter_cases = zero = low_recall = 0

    for case in cases:
        t0 = time.time()
        result = ai_search_products_v2(
            message=case.get("message"), page_context=case.get("page_context"),
            page=1, page_length=page_length, include_inactive=0,
            feature_flag_override=feature_flag_override,
        )
        dt = round((time.time() - t0) * 1000.0, 1)
        latencies.append(dt)

        found = cint(result.get("found"))
        if found <= 0:
            zero += 1
        intent_match = cstr(result.get("resolved_intent", {}).get("intent_class")) == cstr(case.get("expected_intent_class"))
        intent_ok += 1 if intent_match else 0
        fmatch = _benchmark_filters_match(case.get("expected_filters"), result.get("applied_filters"))
        if case.get("expected_filters"):
            filter_cases += 1
            filter_ok += 1 if fmatch else 0

        constraint_sat, precision, recall = _score_hits(result.get("hits"), case)
        if constraint_sat is not None:
            sat_vals.append(constraint_sat)
        if precision is not None:
            prec_vals.append(precision)
        # low recall: a broad spec query that returns < 3 items is a quality miss
        if cstr(case.get("expected_intent_class")) != "sku_lookup" and found < 3:
            low_recall += 1

        relevance = None
        if judge:
            relevance, _ratings = _llm_judge(case.get("message"), [h.get("document", {}) for h in (result.get("hits") or [])])
            if relevance is not None:
                rel_vals.append(relevance)

        details.append({
            "name": case.get("name"), "message": case.get("message"),
            "found": found, "latency_ms": dt,
            "intent_match": intent_match, "filter_match": fmatch,
            "constraint_sat@5": None if constraint_sat is None else round(constraint_sat, 3),
            "relevance@5": relevance, "precision@5": precision, "recall": recall,
        })

    n = len(cases) or 1
    scorecard = {
        "kind": "search",
        "total_cases": len(cases),
        "relevance@5_llm_judge": round(sum(rel_vals) / len(rel_vals), 3) if rel_vals else None,
        "constraint_satisfaction@5": round(sum(sat_vals) / len(sat_vals), 3) if sat_vals else None,
        "constraint_cases": len(sat_vals),
        "precision@5": round(sum(prec_vals) / len(prec_vals), 3) if prec_vals else None,
        "precision_cases": len(prec_vals),
        "intent_match_rate": round(intent_ok / n, 3),
        "filter_match_rate": round(filter_ok / filter_cases, 3) if filter_cases else None,
        "zero_result_rate": round(zero / n, 3),
        "low_recall_rate": round(low_recall / n, 3),
        "latency_p50_ms": _percentile(latencies, 50),
        "latency_p95_ms": _percentile(latencies, 95),
        "weakest": sorted(
            [d for d in details if (d["constraint_sat@5"] is not None and d["constraint_sat@5"] < 0.6)
             or (d["relevance@5"] is not None and d["relevance@5"] < 0.6) or d["found"] < 3],
            key=lambda d: (d["relevance@5"] if d["relevance@5"] is not None else (d["constraint_sat@5"] if d["constraint_sat@5"] is not None else -1)),
        )[:15],
        "details": details,
    }
    return scorecard


def _load_assistant_cases():
    path = frappe.get_app_path("igh_search", "igh_search", "data", "assistant_eval_cases.json")
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle).get("cases", [])
    except Exception:
        return []


def run_assistant_scorecard(feature_flag_override=1, judge=0):
    """Accuracy + latency scorecard for the chatbot (product_assistant_chat).
    Scores the grounded products the assistant chose against the case's specs.
    judge=1 adds the LLM-as-judge relevance@5."""
    from igh_search.igh_search.product_assistant import product_assistant_chat

    judge = cint(judge)
    cases = _load_assistant_cases()
    details, latencies, sat_vals, rel_vals = [], [], [], []
    grounded_nonzero = low_recall = 0

    for case in cases:
        t0 = time.time()
        res = product_assistant_chat(message=case.get("message"), history="[]",
                                     feature_flag_override=feature_flag_override)
        dt = round((time.time() - t0) * 1000.0, 1)
        latencies.append(dt)
        products = res.get("products") or []
        if products:
            grounded_nonzero += 1

        # reuse the same constraint check; assistant products carry the slim doc fields
        pseudo_hits = [{"document": p} for p in products]
        constraint_sat, precision, recall = _score_hits(pseudo_hits, case)
        if constraint_sat is not None:
            sat_vals.append(constraint_sat)
        # a spec/concept question that grounds on < 3 products is a recall miss
        if case.get("expected_filters") is not None and len(products) < 3:
            low_recall += 1

        relevance = None
        if judge and products:
            relevance, _r = _llm_judge(case.get("message"), products)
            if relevance is not None:
                rel_vals.append(relevance)

        reply = cstr(res.get("reply"))
        must = case.get("reply_must_contain")
        reply_ok = (normalize_text(must) in normalize_text(reply)) if must else None

        details.append({
            "name": case.get("name"), "message": case.get("message"),
            "products": len(products), "latency_ms": dt,
            "constraint_sat@5": None if constraint_sat is None else round(constraint_sat, 3),
            "relevance@5": relevance, "precision@5": precision, "reply_ok": reply_ok,
            "reply_preview": reply[:90],
        })

    n = len(cases) or 1
    return {
        "kind": "assistant",
        "total_cases": len(cases),
        "relevance@5_llm_judge": round(sum(rel_vals) / len(rel_vals), 3) if rel_vals else None,
        "constraint_satisfaction@5": round(sum(sat_vals) / len(sat_vals), 3) if sat_vals else None,
        "constraint_cases": len(sat_vals),
        "grounded_nonzero_rate": round(grounded_nonzero / n, 3),
        "low_recall_rate": round(low_recall / n, 3),
        "latency_p50_ms": _percentile(latencies, 50),
        "latency_p95_ms": _percentile(latencies, 95),
        "details": details,
    }


@frappe.whitelist()
def run_full_scorecard(feature_flag_override=1):
    """Both scorecards in one call — the before/after gate for each phase."""
    if frappe.session.user == "Guest":
        frappe.throw("Authentication required")
    return {
        "search": run_search_scorecard(feature_flag_override=cint(feature_flag_override)),
        "assistant": run_assistant_scorecard(feature_flag_override=cint(feature_flag_override)),
    }

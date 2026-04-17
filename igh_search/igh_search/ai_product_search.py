import copy
import json
import os

import frappe
import requests
from frappe.utils import cint, cstr, flt

OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
DEFAULT_OPENAI_MODEL = "gpt-4o-mini"
DEFAULT_RANGE_MAX = 1000000000
KNOWN_VALUES_CACHE_TTL = 60 * 60

SUPPORTED_SORT_VALUES = {
    "",
    "stock:desc",
    "stock:asc",
    "creation_on:desc",
    "rate:asc",
    "rate:desc",
    "sold_last_30_days:desc",
    "sold_last_30_days:asc",
    "discount_percentage:desc",
    "inventory_value:desc",
}

ARRAY_FILTER_MODEL_MAP = {
    "brand": "Brand",
    "category_list": "Category",
    "product_type": None,
    "item_group": "Item Group",
    "ip_rate": "Att IP Rate",
    "power": "Att Power",
    "color_temp_": "Att Color Temp",
    "body_finish": "Att Body Finish",
    "input": "Att Input",
    "mounting": "Att Mounting",
    "output_current": "Att Output Current",
    "output_voltage": "Att Output Voltage",
    "lamp_type": "Att Lamp Type",
    "lumen_output": "Att Lumen Output",
    "beam_angle": "Att Beam Angle",
    "material": "Att Material",
    "warranty_": "Att Warranty",
}

BOOLEAN_FILTER_KEYS = (
    "in_stock",
    "show_promotion",
    "hot_product",
    "has_variants",
    "custom_in_bundle_item",
)

RANGE_FILTER_DEFAULTS = {
    "price_range": {"min": 0, "max": DEFAULT_RANGE_MAX},
    "stock_range": {"min": 0, "max": DEFAULT_RANGE_MAX},
}

PAGE_CONTEXT_KEYS = ("route", "category", "brand", "search")


def build_default_filters():
    filters = {key: [] for key in ARRAY_FILTER_MODEL_MAP}
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
    return conf.get("ai_product_search_model") or conf.get("openai_model") or DEFAULT_OPENAI_MODEL


def log_ai_product_search(event, payload):
    try:
        frappe.logger().info(
            "AI Product Search %s: %s",
            event,
            json.dumps(payload, ensure_ascii=True, default=str),
        )
    except Exception:
        pass


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
    cache_key = "ai_product_search|known_filter_values"
    cached_value = frappe.cache().get_value(cache_key)
    if cached_value:
        return cached_value

    known_values = {}
    for filter_key, model_name in ARRAY_FILTER_MODEL_MAP.items():
        if model_name:
            known_values[filter_key] = _get_model_names(model_name)
        else:
            known_values[filter_key] = ["Listed", "Unlisted", "Obsolete"]

    frappe.cache().set_value(cache_key, known_values, expires_in_sec=KNOWN_VALUES_CACHE_TTL)
    return known_values


def _trim_known_values_for_prompt(known_values, max_values=200):
    return {key: values[:max_values] for key, values in known_values.items()}


def _build_openai_messages(message, page_context, known_values):
    response_shape = {
        "query": "string",
        "sort_by": "one of the allowed sort values",
        "filters": {
            **{key: ["string"] for key in ARRAY_FILTER_MODEL_MAP},
            **copy.deepcopy(RANGE_FILTER_DEFAULTS),
            **{key: False for key in BOOLEAN_FILTER_KEYS},
        },
        "explanation": "short explanation string",
    }

    system_prompt = f"""
You convert natural-language product discovery requests into structured search intent for an existing Typesense product listing flow.
Return ONLY valid JSON. Do not return markdown. Do not add any keys outside the required schema.

Required JSON shape:
{json.dumps(response_shape, ensure_ascii=True)}

Rules:
- Interpret user intent for product discovery only.
- Prefer existing supported filters and known values.
- Do not invent fields.
- Do not invent brands, categories, item groups, or attribute values outside the allowed values.
- If unsure, leave arrays empty, booleans false, and ranges at broad defaults.
- Keep query short and useful for product search.
- For phrases like "under 5000", set price_range.max to 5000.
- For phrases like "highest value", use sort_by = "inventory_value:desc".
- For phrases like "high stock", prefer stock-oriented sorting or stock_range if clearly implied.
- For phrases like "waterproof", map to known IP ratings only when confidence is high.
- Output must be a single JSON object only.

Allowed sort values:
{json.dumps(sorted(SUPPORTED_SORT_VALUES), ensure_ascii=True)}
""".strip()

    user_payload = {
        "message": cstr(message).strip(),
        "page_context": page_context,
        "known_filter_values": _trim_known_values_for_prompt(known_values),
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


def call_openai_for_product_search(message, page_context, known_values):
    api_key = get_openai_api_key()
    if not api_key:
        raise ValueError("OpenAI API key is not configured")

    payload = {
        "model": get_openai_model(),
        "messages": _build_openai_messages(message, page_context, known_values),
        "temperature": 0.1,
        "max_tokens": 1200,
        "response_format": {"type": "json_object"},
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    response = requests.post(OPENAI_API_URL, headers=headers, json=payload, timeout=45)
    response.raise_for_status()
    raw_content = ((response.json() or {}).get("choices") or [{}])[0].get("message", {}).get("content", "")
    return raw_content, parse_json_response(raw_content)


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
    if value in {"true", "1", "yes", "y"}:
        return True
    if value in {"false", "0", "no", "n"}:
        return False
    return False


def sanitize_string_list(values, allowed_values):
    if not isinstance(values, list):
        return []

    allowed_lookup = {cstr(value).strip().lower(): value for value in (allowed_values or []) if cstr(value).strip()}
    sanitized_values = []
    seen = set()

    for value in values:
        if not isinstance(value, str):
            continue

        cleaned_value = value.strip()
        if not cleaned_value:
            continue

        canonical_value = allowed_lookup.get(cleaned_value.lower())
        if not canonical_value:
            continue

        if canonical_value in seen:
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


def sanitize_ai_product_search_response(ai_response, known_values=None):
    known_values = known_values or get_known_filter_values()
    sanitized_response = build_default_response()

    if not isinstance(ai_response, dict):
        return sanitized_response

    sanitized_response["query"] = cstr(ai_response.get("query") or "").strip()

    sort_by = cstr(ai_response.get("sort_by") or "").strip()
    if sort_by in SUPPORTED_SORT_VALUES:
        sanitized_response["sort_by"] = sort_by

    filters = ai_response.get("filters")
    if not isinstance(filters, dict):
        filters = {}

    for filter_key in ARRAY_FILTER_MODEL_MAP:
        sanitized_response["filters"][filter_key] = sanitize_string_list(
            filters.get(filter_key),
            known_values.get(filter_key),
        )

    for filter_key in BOOLEAN_FILTER_KEYS:
        sanitized_response["filters"][filter_key] = _to_boolean(filters.get(filter_key))

    for filter_key, defaults in RANGE_FILTER_DEFAULTS.items():
        sanitized_response["filters"][filter_key] = sanitize_range(filters.get(filter_key), defaults)

    sanitized_response["explanation"] = cstr(ai_response.get("explanation") or "").strip()
    return sanitized_response


def parse_product_search_intent(message, page_context=None):
    message = cstr(message).strip()
    sanitized_page_context = parse_page_context(page_context)

    if not message:
        return build_default_response("Empty message supplied for AI product search.")

    if not is_ai_product_search_enabled():
        response = build_default_response("AI product search is disabled.")
        response["query"] = ""
        return response

    known_values = get_known_filter_values()
    log_ai_product_search(
        "request",
        {"message": message, "page_context": sanitized_page_context},
    )

    try:
        raw_content, ai_response = call_openai_for_product_search(
            message,
            sanitized_page_context,
            known_values,
        )
        log_ai_product_search("raw_output", {"message": message, "raw_output": raw_content})
        sanitized_response = sanitize_ai_product_search_response(ai_response, known_values)
        if not sanitized_response["explanation"]:
            sanitized_response["explanation"] = "Parsed natural-language search request into structured filters."
        log_ai_product_search("sanitized_output", sanitized_response)
        return sanitized_response
    except Exception:
        frappe.log_error(
            title="AI Product Search Parse Error",
            message=frappe.get_traceback(),
        )
        fallback_response = build_default_response(
            "AI parsing failed, so safe default search instructions were returned."
        )
        log_ai_product_search(
            "fallback_output",
            {
                "message": message,
                "page_context": sanitized_page_context,
                "response": fallback_response,
            },
        )
        return fallback_response

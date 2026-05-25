import copy
import re
import uuid
from collections import defaultdict

from frappe.utils import cint, cstr, flt

from igh_search.igh_search.ai_product_search import (
    ARRAY_FILTER_KEYS,
    BOOLEAN_FILTER_KEYS,
    RANGE_FILTER_DEFAULTS,
    _normalize_family_token,
    build_ai_display_filters,
    build_ai_display_query,
    execute_intent_search,
    get_known_filter_values,
    resolve_ai_search_intent,
    sanitize_range,
)
from igh_search.igh_search.search_normalization import (
    normalize_color_temp,
    normalize_ip_rate,
    normalize_text,
)

SKIP_ANSWER_RE = re.compile(
    r"^(any|anything|any one|no preference|doesn't matter|dont care|do not care|skip|all|either|whatever|show results|show options)$",
    re.I,
)
RANGE_NUMBER_RE = re.compile(r"\d+(?:\.\d+)?")
QUESTION_CANDIDATE_LIMIT = 6
CONTINUE_RESULT_THRESHOLD = 40
SOFT_CONTINUE_THRESHOLD = 12
MIN_FILTER_SCORE = 2.15
ASSISTANT_BUCKET_PREFIX = "__guided__:"
QUERY_FILTER_PROMOTION_KEYS = ("product_type", "category_list", "item_group", "lamp_type", "brand")

FILTER_REGISTRY = {
    "category_list": {
        "label": "Category",
        "question": "What category are you looking for?",
        "type": "array",
        "base_weight": 1.05,
    },
    "product_type": {
        "label": "Product Type",
        "question": "Any specific product type?",
        "type": "array",
        "base_weight": 1.1,
    },
    "brand": {
        "label": "Brand",
        "question": "Do you prefer any brand?",
        "type": "array",
        "base_weight": 0.45,
    },
    "power": {
        "label": "Power",
        "question": "What wattage or power range do you need?",
        "type": "array",
        "base_weight": 1.15,
    },
    "color_temp": {
        "label": "Color Temperature",
        "question": "What color temperature should I target?",
        "type": "array",
        "base_weight": 1.1,
    },
    "ip_rate": {
        "label": "IP Rating",
        "question": "Do you need a specific IP rating?",
        "type": "array",
        "base_weight": 0.95,
    },
    "mounting": {
        "label": "Mounting",
        "question": "Any mounting preference?",
        "type": "array",
        "base_weight": 0.8,
    },
    "beam_angle": {
        "label": "Beam Angle",
        "question": "Do you need a beam angle requirement?",
        "type": "array",
        "base_weight": 0.82,
    },
    "input_voltage": {
        "label": "Input Voltage",
        "question": "Do you need a particular input voltage?",
        "type": "array",
        "base_weight": 0.88,
    },
    "output_current": {
        "label": "Output Current",
        "question": "What output current should I target?",
        "type": "array",
        "base_weight": 0.9,
    },
    "output_voltage": {
        "label": "Output Voltage",
        "question": "What output voltage should I target?",
        "type": "array",
        "base_weight": 0.84,
    },
    "lamp_type": {
        "label": "Lamp Type",
        "question": "Do you have a lamp type in mind?",
        "type": "array",
        "base_weight": 0.55,
    },
    "material": {
        "label": "Material",
        "question": "Do you need a particular material?",
        "type": "array",
        "base_weight": 0.32,
    },
    "body_finish": {
        "label": "Body Finish",
        "question": "Do you have a preferred finish?",
        "type": "array",
        "base_weight": 0.3,
    },
    "in_stock": {
        "label": "In Stock",
        "question": "Should I keep this to in-stock products only?",
        "type": "boolean",
        "base_weight": 0.35,
    },
    "rate_range": {
        "label": "Budget",
        "question": "Do you have a budget range in mind?",
        "type": "range",
        "base_weight": 0.5,
    },
}

QUESTION_ORDER = [key for key in FILTER_REGISTRY.keys()]

RESULT_PROFILE_KEYWORDS = {
    "fixture": ["spot", "downlight", "track", "panel", "batten", "high bay", "wall light", "ceiling", "linear light"],
    "strip": ["strip", "profile", "channel", "channelume", "cob", "tape"],
    "driver": ["driver", "power supply", "constant current", "constant voltage", "dali", "triac", "dimmable"],
    "outdoor": ["outdoor", "facade", "garden", "landscape", "bollard", "flood", "wall washer", "inground"],
    "electrical": ["switch", "socket", "mcb", "rcbo", "contactor", "isolator", "sensor", "switchgear"],
}

PROFILE_FILTER_BONUSES = {
    "fixture": {"power": 0.45, "color_temp": 0.55, "beam_angle": 0.95, "mounting": 0.7, "ip_rate": 0.45, "input_voltage": -0.75, "output_current": -0.95, "output_voltage": -0.8},
    "strip": {"input_voltage": 0.8, "power": 0.65, "color_temp": 0.55, "ip_rate": 0.45, "mounting": 0.4, "beam_angle": -0.9, "output_current": -0.45},
    "driver": {"input_voltage": 0.8, "output_current": 1.0, "output_voltage": 0.95, "ip_rate": 0.35, "power": 0.2, "beam_angle": -1.1, "mounting": -0.55, "color_temp": -0.7},
    "outdoor": {"ip_rate": 1.05, "mounting": 0.6, "power": 0.45, "color_temp": 0.35, "beam_angle": 0.25, "input_voltage": -0.25, "output_current": -0.85},
    "electrical": {"product_type": 0.55, "brand": 0.3, "input_voltage": 0.35, "rate_range": 0.2, "beam_angle": -1.2, "color_temp": -0.8, "mounting": -0.35},
}

def _clean_filters(filters):
    safe_filters = {key: [] for key in ARRAY_FILTER_KEYS}
    safe_filters.update({key: False for key in BOOLEAN_FILTER_KEYS})
    safe_filters.update(copy.deepcopy(RANGE_FILTER_DEFAULTS))

    raw_filters = filters if isinstance(filters, dict) else {}

    for key in ARRAY_FILTER_KEYS:
        values = raw_filters.get(key) or []
        safe_filters[key] = [cstr(value).strip() for value in values if cstr(value).strip()]

    for key in BOOLEAN_FILTER_KEYS:
        safe_filters[key] = bool(raw_filters.get(key))

    for key, defaults in RANGE_FILTER_DEFAULTS.items():
        safe_filters[key] = sanitize_range(raw_filters.get(key), defaults)

    return safe_filters


def _is_range_active(range_value, defaults):
    sanitized = sanitize_range(range_value, defaults)
    return flt(sanitized.get("min")) != flt(defaults.get("min")) or flt(sanitized.get("max")) != flt(defaults.get("max"))


def _is_question_resolved(intent_filters, question_key):
    if question_key in ARRAY_FILTER_KEYS:
        return bool(intent_filters.get(question_key))
    if question_key in BOOLEAN_FILTER_KEYS:
        return intent_filters.get(question_key) is True
    if question_key in RANGE_FILTER_DEFAULTS:
        return _is_range_active(intent_filters.get(question_key), RANGE_FILTER_DEFAULTS[question_key])
    return False


def _resolved_filter_count(intent_filters):
    count = 0
    for question_key in FILTER_REGISTRY:
        if question_key == "in_stock":
            continue
        if _is_question_resolved(intent_filters, question_key):
            count += 1
    return count


def _normalized_text_blob(intent, source_message=None):
    parts = [
        cstr(source_message or ""),
        cstr(intent.get("query") or ""),
    ]
    for question_key in ARRAY_FILTER_KEYS:
        parts.append(" ".join(cstr(v) for v in intent.get("filters", {}).get(question_key) or []))
    return normalize_text(" ".join(part for part in parts if part).strip())


def _singularize_token(value):
    value = cstr(value or "").strip()
    if value.endswith("ies") and len(value) > 4:
        return value[:-3] + "y"
    if value.endswith("ses") and len(value) > 4:
        return value[:-2]
    if value.endswith("s") and len(value) > 3 and not value.endswith("ss"):
        return value[:-1]
    return value


def _normalized_phrase_variants(value):
    normalized = normalize_text(value)
    compact = normalized.replace(" ", "")
    variants = {normalized, compact}
    singular = _singularize_token(normalized)
    variants.add(singular)
    variants.add(singular.replace(" ", ""))
    return {variant for variant in variants if variant}


def _tokenize_normalized_text(value):
    return [token for token in normalize_text(value).split() if token]


def _singularize_tokens(tokens):
    return [_singularize_token(token) for token in tokens if token]


def _filter_value_phrase_candidates(question_key, value):
    candidates = set()
    raw = normalize_text(value)
    label = normalize_text(_normalize_option_label(question_key, value))

    for candidate in (raw, label):
        if not candidate:
            continue
        candidates.add(candidate)
        singular = normalize_text(" ".join(_singularize_tokens(candidate.split())))
        if singular:
            candidates.add(singular)

    return sorted((candidate for candidate in candidates if candidate), key=len, reverse=True)


def _remove_phrase_tokens(query_tokens, phrase_tokens):
    if not query_tokens or not phrase_tokens or len(phrase_tokens) > len(query_tokens):
        return query_tokens, False

    phrase_len = len(phrase_tokens)
    query_singular = _singularize_tokens(query_tokens)
    phrase_singular = _singularize_tokens(phrase_tokens)

    for index in range(len(query_tokens) - phrase_len + 1):
        window = query_tokens[index:index + phrase_len]
        if window == phrase_tokens:
            return query_tokens[:index] + query_tokens[index + phrase_len:], True

        if query_singular[index:index + phrase_len] == phrase_singular:
            return query_tokens[:index] + query_tokens[index + phrase_len:], True

    return query_tokens, False


def _strip_consumed_query_terms(query, intent_filters):
    query_tokens = _tokenize_normalized_text(query)
    if not query_tokens:
        return ""

    consumed = False
    covered_tokens = set()

    # Compact family tokens for the applied category / item-group values. A typed
    # family noun ("spotlights" → "spotlight") must be recognised as consumed
    # even though the category value ("SPOT LIGHT") tokenises to separate words —
    # otherwise it survives into applied_query and becomes a phantom keyword.
    family_tokens = set()
    for question_key in ("category_list", "item_group"):
        for value in intent_filters.get(question_key) or []:
            family_token = _normalize_family_token(value)
            if family_token:
                family_tokens.add(family_token)

    for question_key in ARRAY_FILTER_KEYS:
        for value in intent_filters.get(question_key) or []:
            for phrase in _filter_value_phrase_candidates(question_key, value):
                phrase_tokens = _tokenize_normalized_text(phrase)
                if not phrase_tokens:
                    continue
                for token in phrase_tokens:
                    if token and (len(token) >= 4 or any(char.isdigit() for char in token)):
                        covered_tokens.add(token)

                while True:
                    query_tokens, removed = _remove_phrase_tokens(query_tokens, phrase_tokens)
                    if not removed:
                        break
                    consumed = True

    if intent_filters.get("in_stock"):
        for phrase in ("in stock", "instock", "available now", "available"):
            phrase_tokens = _tokenize_normalized_text(phrase)
            while True:
                query_tokens, removed = _remove_phrase_tokens(query_tokens, phrase_tokens)
                if not removed:
                    break
                consumed = True

    if query_tokens and (covered_tokens or family_tokens):
        filtered_tokens = []
        for token in query_tokens:
            singular = _singularize_token(token)
            if token in covered_tokens or singular in covered_tokens:
                consumed = True
                continue
            if family_tokens and _normalize_family_token(token) in family_tokens:
                consumed = True
                continue
            filtered_tokens.append(token)
        query_tokens = filtered_tokens

    if not consumed:
        return normalize_text(query)

    return normalize_text(" ".join(query_tokens))


def _finalize_intent_query(intent, fallback_query=""):
    filters = _clean_filters(intent.get("filters") or {})
    raw_query = cstr(intent.get("query") or "").strip()
    if not raw_query and fallback_query:
        raw_query = cstr(fallback_query).strip()

    intent["filters"] = filters
    intent["query"] = _strip_consumed_query_terms(raw_query, filters) if raw_query else ""
    return intent


def _query_filter_match_score(query, candidate):
    query_variants = _normalized_phrase_variants(query)
    candidate_variants = _normalized_phrase_variants(candidate)
    if not query_variants or not candidate_variants:
        return 0.0

    if query_variants & candidate_variants:
        return 1.0

    best = 0.0
    for query_variant in query_variants:
        for candidate_variant in candidate_variants:
            if query_variant and candidate_variant and (query_variant in candidate_variant or candidate_variant in query_variant):
                best = max(best, 0.8)
            query_words = set(query_variant.split())
            candidate_words = set(candidate_variant.split())
            overlap = len(query_words & candidate_words)
            if overlap:
                best = max(best, overlap / max(len(query_words), len(candidate_words)))
    return best


def _query_words_covered(query, candidate):
    query_words = {word for word in normalize_text(query).split() if word}
    candidate_words = {word for word in normalize_text(candidate).split() if word}
    if not query_words or not candidate_words:
        return False
    if query_words.issubset(candidate_words):
        return True
    singular_query = {_singularize_token(word) for word in query_words}
    singular_candidate = {_singularize_token(word) for word in candidate_words}
    return singular_query.issubset(singular_candidate)


def _promote_query_into_filters(intent):
    query = cstr(intent.get("query") or "").strip()
    if not query or intent.get("item_code_hint"):
        return intent

    filters = _clean_filters(intent.get("filters") or {})
    if _resolved_filter_count(filters) >= 3:
        intent["filters"] = filters
        return intent

    known_values = get_known_filter_values() or {}
    best_match = None
    second_score = 0.0

    for question_key in QUERY_FILTER_PROMOTION_KEYS:
        if filters.get(question_key):
            continue
        for value in known_values.get(question_key) or []:
            score = _query_filter_match_score(query, value)
            if score > (best_match["score"] if best_match else 0):
                second_score = best_match["score"] if best_match else second_score
                best_match = {"question_key": question_key, "value": cstr(value).strip(), "score": score}
            elif score > second_score:
                second_score = score

    if best_match and best_match["score"] >= 0.8 and (best_match["score"] - second_score >= 0.15 or best_match["score"] == 1.0):
        filters[best_match["question_key"]] = [best_match["value"]]
        query_variants = _normalized_phrase_variants(query)
        value_variants = _normalized_phrase_variants(best_match["value"])
        if query_variants & value_variants or best_match["score"] >= 1.0 or _query_words_covered(query, best_match["value"]):
            intent["query"] = ""
        intent["filters"] = filters
        explanation = cstr(intent.get("explanation") or "").strip()
        extra = f" Promoted query into {best_match['question_key']} filter."
        intent["explanation"] = (explanation + extra).strip()
        return intent

    intent["filters"] = filters
    return intent


def _parse_numeric_values(answer):
    return [flt(match) for match in RANGE_NUMBER_RE.findall(cstr(answer or ""))]


def _normalize_bool_answer(answer):
    normalized = cstr(answer or "").strip().lower()
    if normalized in {"yes", "y", "true", "1", "instock", "in stock", "available", "available now"}:
        return True
    if normalized in {"no", "n", "false", "0", "show all", "all stock", "either"}:
        return False
    return None


def _is_skip_answer(answer):
    return bool(SKIP_ANSWER_RE.match(cstr(answer or "").strip()))


def _clean_label(value):
    value = cstr(value or "").replace("_", " ")
    value = re.sub(r"\s+", " ", value).strip(" ,;:-")
    return value


def _title_case_words(value):
    text = _clean_label(value)
    if not text:
        return ""
    if text.isupper() and len(text) <= 6:
        return text
    return " ".join(word.capitalize() if not re.search(r"\d", word) else word.upper() for word in text.split())


def _normalize_power_label(value):
    raw = _clean_label(value).upper().replace(" ", "")
    if not raw:
        return ""
    meter = any(token in raw for token in ["/M", "PERM", "PERMETER", "WMETER", "W/METER"])
    numbers = _parse_numeric_values(raw)
    if not numbers:
        return raw.replace("PERMETER", "/M")
    if len(numbers) >= 2 and any(token in raw for token in ["-", "TO"]):
        label = f"{_format_number(numbers[0])}W-{_format_number(numbers[1])}W"
    else:
        label = f"{_format_number(numbers[0])}W"
    if meter:
        label = f"{label}/M"
    return label


def _normalize_voltage_label(value):
    raw = _clean_label(value).upper().replace(" ", "")
    segments = [segment for segment in re.split(r"[/,;]", raw) if segment]
    voltage_segment = next((segment for segment in segments if "V" in segment), raw)
    numbers = _parse_numeric_values(voltage_segment)
    if not numbers:
        return voltage_segment or raw
    suffix = "VAC" if "VAC" in voltage_segment or ("AC" in voltage_segment and "DC" not in voltage_segment) else "VDC" if "VDC" in voltage_segment or "DC" in voltage_segment else "V"
    if len(numbers) >= 2 and any(token in voltage_segment for token in ["-", "TO"]):
        return f"{_format_number(numbers[0])}{suffix}-{_format_number(numbers[1])}{suffix}"
    return f"{_format_number(numbers[0])}{suffix}"


def _normalize_current_label(value):
    raw = _clean_label(value).upper().replace(" ", "")
    segments = [segment for segment in re.split(r"[/,;]", raw) if segment]
    current_segment = next((segment for segment in segments if segment.endswith("MA") or re.search(r"\dA$", segment)), raw)
    numbers = _parse_numeric_values(current_segment)
    if not numbers:
        return current_segment or raw
    suffix = "A" if current_segment.endswith("A") and not current_segment.endswith("MA") else "MA"
    if len(numbers) >= 2 and any(token in current_segment for token in ["-", "TO"]):
        return f"{_format_number(numbers[0])}{suffix}-{_format_number(numbers[1])}{suffix}"
    return f"{_format_number(numbers[0])}{suffix}"


def _normalize_beam_label(value):
    raw = _clean_label(value)
    compact = raw.upper().replace(" ", "")
    numbers = _parse_numeric_values(compact)
    if "ADJUST" in compact:
        if len(numbers) >= 2:
            return f"Adjustable {_format_number(numbers[0])}°-{_format_number(numbers[1])}°"
        return "Adjustable beam"
    if len(numbers) >= 2 and any(token in compact for token in ["-", "TO", "*"]):
        separator = " x " if "*" in compact else "-"
        return f"{_format_number(numbers[0])}°{separator}{_format_number(numbers[1])}°"
    if len(numbers) == 1:
        return f"{_format_number(numbers[0])}°"
    return raw.upper() if len(raw) <= 8 else raw


def _normalize_mounting_label(value):
    raw = _title_case_words(value)
    replacements = {
        "Surface Mounted": "Surface Mounted",
        "Surface": "Surface Mounted",
        "Recessed": "Recessed",
        "Suspended": "Suspended",
        "Track": "Track Mounted",
        "Wall": "Wall Mounted",
        "Ceiling": "Ceiling Mounted",
    }
    normalized = normalize_text(raw)
    for needle, label in replacements.items():
        if normalize_text(needle) in normalized:
            return label
    return raw


def _normalize_generic_label(value):
    raw = _clean_label(value)
    if not raw:
        return ""
    if re.fullmatch(r"IP\s*\d{2,3}", raw.upper()):
        return raw.upper().replace(" ", "")
    if len(raw) <= 4 and raw.upper() == raw:
        return raw
    return _title_case_words(raw)


def _normalize_option_label(question_key, value):
    if question_key == "power":
        return _normalize_power_label(value)
    if question_key == "color_temp":
        normalized = cstr(normalize_color_temp(value) or "").strip()
        return normalized or _title_case_words(value)
    if question_key == "ip_rate":
        normalized = cstr(normalize_ip_rate(value) or "").strip()
        return normalized or _clean_label(value).upper().replace(" ", "")
    if question_key in {"input_voltage", "output_voltage"}:
        return _normalize_voltage_label(value)
    if question_key == "output_current":
        return _normalize_current_label(value)
    if question_key == "beam_angle":
        return _normalize_beam_label(value)
    if question_key == "mounting":
        return _normalize_mounting_label(value)
    return _normalize_generic_label(value)


def _format_number(value):
    value = flt(value)
    if value.is_integer():
        return str(int(value))
    return ("%.2f" % value).rstrip("0").rstrip(".")


def _normalize_value_token(question_key, value):
    return normalize_text(_normalize_option_label(question_key, value))


def _build_bucket_value(question_key, bucket_key):
    return f"{ASSISTANT_BUCKET_PREFIX}{question_key}:{bucket_key}"


def _dedupe_values(values):
    seen = set()
    output = []
    for value in values or []:
        cleaned = cstr(value).strip()
        if not cleaned:
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        output.append(cleaned)
    return output


def _bucketize_ip_rate(value):
    normalized = cstr(normalize_ip_rate(value) or "").strip().upper().replace(" ", "")
    numbers = _parse_numeric_values(normalized)
    if not numbers:
        return None
    rating = cint(numbers[0])
    if rating <= 23:
        return {"token": _build_bucket_value("ip_rate", "indoor"), "label": "Indoor Use"}
    if rating <= 44:
        return {"token": _build_bucket_value("ip_rate", "splash"), "label": "Splash Resistant"}
    if rating <= 55:
        return {"token": _build_bucket_value("ip_rate", "outdoor"), "label": "Outdoor Rated"}
    return {"token": _build_bucket_value("ip_rate", "waterproof"), "label": "Waterproof / IP65+"}


def _bucketize_mounting(value):
    normalized = normalize_text(value)
    mapping = [
        ("recess", "recessed", "Recessed"),
        ("surface", "surface", "Surface Mounted"),
        ("suspend", "suspended", "Suspended"),
        ("pendant", "suspended", "Suspended"),
        ("track", "track", "Track Mounted"),
        ("wall", "wall", "Wall Mounted"),
        ("ceiling", "ceiling", "Ceiling Mounted"),
        ("spike", "ground", "Ground / Spike Mounted"),
        ("ground", "ground", "Ground / Spike Mounted"),
    ]
    for needle, bucket_key, label in mapping:
        if needle in normalized:
            return {"token": _build_bucket_value("mounting", bucket_key), "label": label}
    return None


def _bucketize_beam_angle(value):
    raw = cstr(value or "")
    normalized = normalize_text(raw)
    compact = raw.upper().replace(" ", "")
    numbers = _parse_numeric_values(compact)
    if "ADJUST" in compact:
        return {"token": _build_bucket_value("beam_angle", "adjustable"), "label": "Adjustable Beam"}
    if "*" in compact or "X" in compact or "WALLWASH" in normalized or "LINEAR" in normalized:
        return {"token": _build_bucket_value("beam_angle", "linear"), "label": "Linear / Wall Wash"}
    if not numbers:
        return None
    beam = max(numbers)
    if beam <= 24:
        return {"token": _build_bucket_value("beam_angle", "narrow"), "label": "Narrow Beam"}
    if beam <= 40:
        return {"token": _build_bucket_value("beam_angle", "medium"), "label": "Medium Beam"}
    return {"token": _build_bucket_value("beam_angle", "wide"), "label": "Wide Beam"}


def _bucketize_option(question_key, value):
    if question_key == "ip_rate":
        return _bucketize_ip_rate(value)
    if question_key == "mounting":
        return _bucketize_mounting(value)
    if question_key == "beam_angle":
        return _bucketize_beam_angle(value)
    return None


def _bucket_token_from_answer(question_key, answer):
    normalized_answer = cstr(answer or "").strip()
    if not normalized_answer:
        return ""
    if normalized_answer.startswith(ASSISTANT_BUCKET_PREFIX):
        return normalized_answer

    lowered = normalize_text(normalized_answer)
    if question_key == "ip_rate":
        if "waterproof" in lowered or "ip65" in lowered or "outdoor" in lowered:
            return _build_bucket_value("ip_rate", "waterproof" if ("ip65" in lowered or "waterproof" in lowered) else "outdoor")
        if "splash" in lowered or "ip44" in lowered or "damp" in lowered:
            return _build_bucket_value("ip_rate", "splash")
        if "indoor" in lowered or "ip20" in lowered:
            return _build_bucket_value("ip_rate", "indoor")
    if question_key == "mounting":
        bucket = _bucketize_mounting(normalized_answer)
        return bucket.get("token") if bucket else ""
    if question_key == "beam_angle":
        if "adjust" in lowered:
            return _build_bucket_value("beam_angle", "adjustable")
        if "linear" in lowered or "wall wash" in lowered:
            return _build_bucket_value("beam_angle", "linear")
        if "narrow" in lowered or "spot" in lowered:
            return _build_bucket_value("beam_angle", "narrow")
        if "medium" in lowered:
            return _build_bucket_value("beam_angle", "medium")
        if "wide" in lowered or "flood" in lowered:
            return _build_bucket_value("beam_angle", "wide")
    return ""


def _should_bucket_question(question_key, facet_counts=None):
    facet_counts = facet_counts or []
    if question_key in {"ip_rate", "mounting"}:
        return True
    if question_key == "beam_angle":
        noisy = 0
        for entry in facet_counts:
            raw_value = cstr(entry.get("value") if isinstance(entry, dict) else entry).strip().upper()
            if any(token in raw_value for token in ["ADJUST", "*", " TO ", "-", "X"]):
                noisy += 1
        return len(facet_counts) > 6 or noisy >= 2
    return False


def _build_normalized_suggestions(question_key, facet_counts=None, fallback_values=None, limit=QUESTION_CANDIDATE_LIMIT):
    groups = {}
    use_buckets = _should_bucket_question(question_key, facet_counts)

    for entry in facet_counts or []:
        raw_value = cstr(entry.get("value") if isinstance(entry, dict) else entry).strip()
        if not raw_value:
            continue
        bucket = _bucketize_option(question_key, raw_value) if use_buckets else None
        label = bucket.get("label") if bucket else _normalize_option_label(question_key, raw_value)
        token = bucket.get("token") if bucket else normalize_text(label)
        value = bucket.get("token") if bucket else raw_value
        if not token:
            continue
        count = cint(entry.get("count") or 0) if isinstance(entry, dict) else 0
        current = groups.get(token)
        if not current:
            groups[token] = {"label": label, "value": value, "count": count, "raw_values": [raw_value]}
            continue
        current["count"] += count
        current["raw_values"].append(raw_value)

    if not groups:
        for value in fallback_values or []:
            raw_value = cstr(value).strip()
            if not raw_value:
                continue
            bucket = _bucketize_option(question_key, raw_value) if question_key in {"ip_rate", "mounting"} else None
            label = bucket.get("label") if bucket else _normalize_option_label(question_key, raw_value)
            token = bucket.get("token") if bucket else normalize_text(label)
            value_to_store = bucket.get("token") if bucket else raw_value
            if token and token not in groups:
                groups[token] = {"label": label, "value": value_to_store, "count": 0, "raw_values": [raw_value]}

    suggestions = sorted(
        groups.values(),
        key=lambda row: (-cint(row.get("count") or 0), len(cstr(row.get("label") or "")), cstr(row.get("label") or "")),
    )

    normalized = []
    for row in suggestions[:limit]:
        normalized.append(
            {
                "label": cstr(row.get("label") or "").strip(),
                "value": cstr(row.get("value") or "").strip(),
                "count": cint(row.get("count") or 0) or None,
            }
        )
    return normalized


def _extract_facet_map(search_response):
    facet_map = {}
    for facet in search_response.get("facet_counts") or []:
        field_name = cstr(facet.get("field_name") or "").strip()
        if not field_name:
            continue
        facet_map[field_name] = facet.get("counts") or []
    return facet_map


def _search_snapshot(intent, feature_flag_override=0):
    response = execute_intent_search(
        intent,
        page=1,
        page_length=1,
        include_inactive=0,
        feature_flag_override=feature_flag_override,
    )
    return {
        "response": response,
        "result_count": cint(response.get("found") or 0),
        "facet_map": _extract_facet_map(response),
    }


def _profile_text_candidates(snapshot, intent_filters):
    values = []
    for question_key in ("category_list", "product_type", "item_group", "lamp_type"):
        values.extend(intent_filters.get(question_key) or [])
        for entry in (snapshot.get("facet_map", {}).get(question_key) or [])[:6]:
            if isinstance(entry, dict):
                values.append(entry.get("value"))
    return [normalize_text(value) for value in values if cstr(value).strip()]


def _infer_result_profile(snapshot, intent_filters):
    texts = _profile_text_candidates(snapshot, intent_filters)
    scores = defaultdict(float)
    for text in texts:
        for profile, keywords in RESULT_PROFILE_KEYWORDS.items():
            for keyword in keywords:
                if keyword in text:
                    scores[profile] += 1.0
    if not scores:
        return "generic"
    return sorted(scores.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _profile_bonus(profile, question_key, intent_filters):
    bonus = flt(PROFILE_FILTER_BONUSES.get(profile, {}).get(question_key) or 0)

    # As fixture searches get more specific, favor optical / installation filters earlier.
    if profile == "fixture":
        if question_key == "beam_angle" and _is_question_resolved(intent_filters, "power") and _is_question_resolved(intent_filters, "color_temp"):
            bonus += 0.45
        if question_key == "mounting" and _is_question_resolved(intent_filters, "power"):
            bonus += 0.2
        if question_key == "input_voltage" and (_is_question_resolved(intent_filters, "power") or _is_question_resolved(intent_filters, "color_temp")):
            bonus -= 0.35

    if profile == "strip" and question_key == "beam_angle":
        bonus -= 0.4

    if profile == "driver" and question_key in {"beam_angle", "mounting", "color_temp"}:
        bonus -= 0.25

    if profile == "outdoor" and question_key == "ip_rate":
        bonus += 0.35

    return bonus


def _question_signal_penalty(question_key, suggestions):
    if not suggestions:
        return 0.0
    if question_key in {"input_voltage", "output_voltage"}:
        suspicious = sum(1 for option in suggestions if "MA" in cstr(option.get("label") or "").upper())
        if suspicious:
            return min(0.8, suspicious * 0.22)
    if question_key == "output_current":
        suspicious = sum(1 for option in suggestions if cstr(option.get("label") or "").upper().endswith("V"))
        if suspicious:
            return min(0.8, suspicious * 0.22)
    return 0.0


def _score_array_filter(question_key, snapshot, known_values, intent_filters, profile):
    result_count = max(cint(snapshot.get("result_count") or 0), 1)
    facet_counts = snapshot.get("facet_map", {}).get(question_key) or []
    if not facet_counts:
        return 0.0, []

    suggestions = _build_normalized_suggestions(
        question_key,
        facet_counts=facet_counts,
        fallback_values=known_values.get(question_key) or [],
    )
    option_count = len(suggestions)
    if option_count == 0:
        return 0.0, []

    total_count = sum(cint(entry.get("count") or 0) for entry in facet_counts) or option_count
    top_count = max([cint(entry.get("count") or 0) for entry in facet_counts] or [0])
    coverage = min(1.0, flt(total_count) / flt(result_count))
    narrowing_power = 1.0 - min(1.0, flt(top_count) / flt(total_count or 1))
    distinctness = min(1.0, flt(option_count) / 6.0)
    overload_penalty = 0.0
    if option_count > QUESTION_CANDIDATE_LIMIT:
        overload_penalty += min(0.8, flt(option_count - QUESTION_CANDIDATE_LIMIT) * 0.08)
    raw_option_count = len(facet_counts)
    noise_penalty = min(0.7, max(0, raw_option_count - option_count) * 0.06)
    label_penalty = 0.0
    if suggestions:
        avg_len = sum(len(cstr(option.get("label") or "")) for option in suggestions) / float(len(suggestions))
        if avg_len > 20:
            label_penalty += min(0.6, (avg_len - 20) / 20.0)

    signal_penalty = _question_signal_penalty(question_key, suggestions)
    score = (
        FILTER_REGISTRY[question_key]["base_weight"]
        + (coverage * 1.25)
        + (narrowing_power * 1.2)
        + (distinctness * 0.9)
        + _profile_bonus(profile, question_key, intent_filters)
        - overload_penalty
        - noise_penalty
        - label_penalty
        - signal_penalty
    )
    return score, suggestions


def _score_boolean_filter(question_key, snapshot):
    result_count = cint(snapshot.get("result_count") or 0)
    score = FILTER_REGISTRY[question_key]["base_weight"]
    if result_count >= CONTINUE_RESULT_THRESHOLD:
        score += 0.3
    return score, [
        {"label": "In stock only", "value": "yes"},
        {"label": "Show all stock states", "value": "no"},
    ]


def _score_range_filter(question_key, snapshot):
    result_count = cint(snapshot.get("result_count") or 0)
    score = FILTER_REGISTRY[question_key]["base_weight"]
    if result_count >= CONTINUE_RESULT_THRESHOLD:
        score += 0.4
    elif result_count >= SOFT_CONTINUE_THRESHOLD:
        score += 0.2
    return score, []


def _pick_next_question(intent_filters, snapshot, skipped_question=None):
    known_values = get_known_filter_values() or {}
    candidates = []
    profile = _infer_result_profile(snapshot, intent_filters)

    for question_key in QUESTION_ORDER:
        if question_key == skipped_question:
            continue
        if _is_question_resolved(intent_filters, question_key):
            continue

        meta = FILTER_REGISTRY[question_key]
        if meta["type"] == "array":
            score, suggestions = _score_array_filter(question_key, snapshot, known_values, intent_filters, profile)
        elif meta["type"] == "boolean":
            score, suggestions = _score_boolean_filter(question_key, snapshot)
            score += _profile_bonus(profile, question_key, intent_filters)
        else:
            score, suggestions = _score_range_filter(question_key, snapshot)
            score += _profile_bonus(profile, question_key, intent_filters)

        if score <= 0:
            continue
        candidates.append(
            {
                "question_key": question_key,
                "score": score,
                "suggestions": suggestions,
                "profile": profile,
            }
        )

    candidates.sort(key=lambda row: (-flt(row["score"]), QUESTION_ORDER.index(row["question_key"])))
    return candidates[0] if candidates else None


def _should_finish(intent, snapshot, next_candidate):
    result_count = cint(snapshot.get("result_count") or 0)
    resolved_count = _resolved_filter_count(intent.get("filters") or {})
    exact_lookup = bool(intent.get("item_code_hint"))

    if exact_lookup and result_count > 0:
        return True
    if result_count == 0:
        return False
    if result_count <= 6 and resolved_count >= 1:
        return True
    if result_count <= 8 and resolved_count >= 2:
        return True
    if result_count <= SOFT_CONTINUE_THRESHOLD and resolved_count >= 3:
        return True
    if result_count <= 25 and resolved_count >= 4:
        return True
    if result_count > CONTINUE_RESULT_THRESHOLD and resolved_count == 0:
        return False
    if not next_candidate:
        return True
    if flt(next_candidate.get("score") or 0) < MIN_FILTER_SCORE:
        return result_count <= CONTINUE_RESULT_THRESHOLD or resolved_count >= 2
    return False


def _resolve_array_answer(question_key, answer, available_values=None):
    normalized_answer = cstr(answer or "").strip()
    if not normalized_answer:
        return []

    candidates = _dedupe_values(available_values or get_known_filter_values().get(question_key) or [])
    bucket_token = _bucket_token_from_answer(question_key, normalized_answer)
    if bucket_token:
        bucket_matches = [value for value in candidates if (_bucketize_option(question_key, value) or {}).get("token") == bucket_token]
        if bucket_matches:
            return bucket_matches

    answer_token = _normalize_value_token(question_key, normalized_answer)

    exact_matches = [value for value in candidates if _normalize_value_token(question_key, value) == answer_token]
    if exact_matches:
        return [exact_matches[0]]

    contains_matches = []
    for value in candidates:
        token = _normalize_value_token(question_key, value)
        if not token:
            continue
        if answer_token and (answer_token in token or token in answer_token):
            contains_matches.append(value)

    if contains_matches:
        return [contains_matches[0]]

    return [normalized_answer]


def _apply_answer_to_filters(intent_filters, question_key, answer, available_values=None):
    next_filters = _clean_filters(intent_filters)
    normalized_answer = cstr(answer or "").strip()

    if not question_key or not normalized_answer:
        return next_filters

    if _is_skip_answer(normalized_answer):
        return next_filters

    if question_key in ARRAY_FILTER_KEYS:
        next_filters[question_key] = _resolve_array_answer(question_key, normalized_answer, available_values=available_values)
        return next_filters

    if question_key in BOOLEAN_FILTER_KEYS:
        parsed = _normalize_bool_answer(normalized_answer)
        if parsed is not None:
            next_filters[question_key] = parsed
        return next_filters

    if question_key in RANGE_FILTER_DEFAULTS:
        defaults = RANGE_FILTER_DEFAULTS[question_key]
        values = _parse_numeric_values(normalized_answer)
        minimum = flt(defaults.get("min"))
        maximum = flt(defaults.get("max"))

        if len(values) >= 2:
            next_filters[question_key] = {
                "min": min(values[0], values[1]),
                "max": max(values[0], values[1]),
            }
            return next_filters

        if len(values) == 1:
            value = flt(values[0])
            lowered = normalized_answer.lower()
            if any(token in lowered for token in ["under", "below", "max", "upto", "up to", "less"]):
                next_filters[question_key] = {"min": minimum, "max": value}
            elif any(token in lowered for token in ["over", "above", "min", "more"]):
                next_filters[question_key] = {"min": value, "max": maximum}
            else:
                next_filters[question_key] = {"min": minimum, "max": value}

    return next_filters


def _combine_source_message(source_message, answer):
    parts = [cstr(source_message or "").strip(), cstr(answer or "").strip()]
    return " ".join(part for part in parts if part).strip()


def _merge_filters(base_filters, fresh_filters):
    merged = _clean_filters(base_filters)
    fresh = _clean_filters(fresh_filters)

    for key in ARRAY_FILTER_KEYS:
        if fresh.get(key):
            merged[key] = fresh[key]

    for key in BOOLEAN_FILTER_KEYS:
        if fresh.get(key):
            merged[key] = True

    for key, defaults in RANGE_FILTER_DEFAULTS.items():
        if _is_range_active(fresh.get(key), defaults):
            merged[key] = sanitize_range(fresh.get(key), defaults)

    return merged


def _assistant_message(result_count, next_candidate, done, skipped_question=None, zero_results=False):
    if zero_results:
        if next_candidate and next_candidate.get("suggestions"):
            return "That answer narrowed the search too much, so I removed it and kept the closest matching results. Try one of these broader options or skip this filter."
        return "That answer narrowed the search too much, so I removed it and kept the closest matching results. You can skip this filter or continue with another detail."

    if done:
        return "I have narrowed the catalog as far as is useful right now. You can browse these filtered results or ask for one more refinement."

    if skipped_question and next_candidate:
        return f"No problem, I skipped that for now. The best next filter is {FILTER_REGISTRY[next_candidate['question_key']]['label'].lower()}."

    if next_candidate:
        if cint(result_count) > 0:
            return f"I found {cint(result_count)} matching products. The best next filter is {FILTER_REGISTRY[next_candidate['question_key']]['label'].lower()}."
        return f"The best next filter is {FILTER_REGISTRY[next_candidate['question_key']]['label'].lower()}."

    return "Tell me a bit more so I can narrow the catalog."


def _build_response(intent, snapshot, session_id, next_candidate=None, skipped_question=None, zero_results=False):
    applied_filters = _clean_filters(intent.get("filters"))
    applied_sort = cstr(intent.get("sort_by") or "").strip()
    done = _should_finish(intent, snapshot, next_candidate)
    assistant_message = _assistant_message(
        result_count=snapshot.get("result_count"),
        next_candidate=next_candidate,
        done=done,
        skipped_question=skipped_question,
        zero_results=zero_results,
    )

    priority = []
    for question_key in QUESTION_ORDER:
        if not _is_question_resolved(applied_filters, question_key):
            priority.append(question_key)

    next_question_key = "" if done or not next_candidate else next_candidate["question_key"]

    return {
        "session_id": session_id or str(uuid.uuid4()),
        "assistant_message": assistant_message,
        "applied_query": cstr(intent.get("query") or "").strip(),
        "applied_filters": applied_filters,
        "applied_sort": applied_sort,
        "display_filters": build_ai_display_filters(applied_filters),
        "missing_fields": priority,
        "next_question": FILTER_REGISTRY.get(next_question_key, {}).get("question", "") if next_question_key else "",
        "question_type": "boolean" if next_question_key in BOOLEAN_FILTER_KEYS else "text",
        "question_key": next_question_key,
        "suggested_answers": [] if done or not next_candidate else next_candidate.get("suggestions") or [],
        "result_count": cint(snapshot.get("result_count") or 0),
        "done": done,
        "resolved_intent": intent.get("resolved_intent") or {},
        "display_query": build_ai_display_query(intent.get("query"), intent, applied_filters, applied_sort),
        "explanation": intent.get("explanation") or "",
        "query_debug": snapshot.get("response", {}).get("query_debug") or {},
    }


def start_guided_ai_search(message=None, page_context=None, feature_flag_override=0):
    intent = resolve_ai_search_intent(message=message, page_context=page_context, mode="fast")
    intent = _promote_query_into_filters(intent)
    intent = _finalize_intent_query(intent)
    snapshot = _search_snapshot(intent, feature_flag_override=feature_flag_override) if cstr(message).strip() or any((intent.get("filters") or {}).values()) else {"response": {}, "result_count": 0, "facet_map": {}}
    next_candidate = _pick_next_question(intent.get("filters") or {}, snapshot)
    return _build_response(intent, snapshot, str(uuid.uuid4()), next_candidate=next_candidate)


def continue_guided_ai_search(
    session_id=None,
    source_message=None,
    applied_query=None,
    current_intent=None,
    resolved_intent=None,
    answer=None,
    question_key=None,
    page_context=None,
    feature_flag_override=0,
    skip=0,
):
    answer_text = "" if cint(skip) else cstr(answer or "").strip()
    base_message = _combine_source_message(source_message or applied_query, answer_text)
    intent = resolve_ai_search_intent(message=base_message, page_context=page_context, mode="fast")
    intent = _promote_query_into_filters(intent)

    merged_filters = _merge_filters(current_intent or {}, intent.get("filters") or {})
    pre_answer_intent = copy.deepcopy(intent)
    pre_answer_intent["filters"] = _clean_filters(current_intent or {})
    if not cstr(pre_answer_intent.get("query") or "").strip():
        pre_answer_intent["query"] = cstr(applied_query or "").strip()
    available_values = []
    if question_key in ARRAY_FILTER_KEYS and (pre_answer_intent.get("query") or any(pre_answer_intent.get("filters", {}).values())):
        pre_snapshot = _search_snapshot(pre_answer_intent, feature_flag_override=feature_flag_override)
        available_values = [
            cstr(entry.get("value") or "").strip()
            for entry in (pre_snapshot.get("facet_map", {}).get(question_key) or [])
            if isinstance(entry, dict) and cstr(entry.get("value") or "").strip()
        ]
    skipped_question = question_key if cint(skip) or _is_skip_answer(answer) else ""
    if question_key and not skipped_question and answer_text:
        merged_filters = _apply_answer_to_filters(
            merged_filters,
            question_key,
            answer_text,
            available_values=available_values,
        )

    intent["filters"] = merged_filters
    intent = _finalize_intent_query(intent, fallback_query=base_message or applied_query)
    if resolved_intent and not intent.get("resolved_intent"):
        intent["resolved_intent"] = resolved_intent

    has_constraints = bool(intent.get("query") or any(intent.get("filters", {}).values()))
    snapshot = _search_snapshot(intent, feature_flag_override=feature_flag_override) if has_constraints else {"response": {}, "result_count": 0, "facet_map": {}}
    zero_results = cint(snapshot.get("result_count") or 0) == 0 and bool(answer_text) and not skipped_question

    if zero_results and question_key:
        intent["filters"] = _clean_filters(current_intent or {})
        intent = _finalize_intent_query(intent, fallback_query=source_message or applied_query)
        reverted_snapshot = _search_snapshot(intent, feature_flag_override=feature_flag_override) if (intent.get("query") or any(intent.get("filters", {}).values())) else {"response": {}, "result_count": 0, "facet_map": {}}
        next_candidate = _pick_next_question(intent.get("filters") or {}, reverted_snapshot, skipped_question=question_key)
        if next_candidate and next_candidate.get("question_key") == question_key and not next_candidate.get("suggestions"):
            next_candidate = _pick_next_question(intent.get("filters") or {}, reverted_snapshot, skipped_question=question_key)
        return _build_response(
            intent,
            reverted_snapshot,
            session_id or str(uuid.uuid4()),
            next_candidate=next_candidate,
            skipped_question=question_key,
            zero_results=True,
        )

    next_candidate = _pick_next_question(intent.get("filters") or {}, snapshot, skipped_question=skipped_question)
    return _build_response(
        intent,
        snapshot,
        session_id or str(uuid.uuid4()),
        next_candidate=next_candidate,
        skipped_question=skipped_question,
        zero_results=False,
    )

import json
import re
from functools import lru_cache

import frappe
from frappe.utils import cstr, flt


SPEC_FIELDS = (
    ("power", "Power"),
    ("color_temp", "Color Temp"),
    ("ip_rate", "IP Rating"),
    ("beam_angle", "Beam Angle"),
    ("mounting", "Mounting"),
    ("body_finish", "Body Finish"),
    ("input_voltage", "Input Voltage"),
    ("output_voltage", "Output Voltage"),
    ("output_current", "Output Current"),
    ("lamp_type", "Lamp Type"),
    ("material", "Material"),
    ("warranty", "Warranty"),
)


@lru_cache(maxsize=1)
def load_glossary():
    glossary_path = frappe.get_app_path(
        "igh_search", "igh_search", "data", "typesense_synonyms.json"
    )
    with open(glossary_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


@lru_cache(maxsize=1)
def get_alias_map():
    alias_map = {}
    for entry in load_glossary().get("entries", []):
        canonical = normalize_text(entry.get("canonical"))
        for alias in entry.get("aliases", []):
            alias_map[normalize_text(alias)] = canonical
    return alias_map


def normalize_text(value):
    value = cstr(value or "").strip().lower()
    value = re.sub(r"[_/]+", " ", value)
    value = re.sub(r"[^a-z0-9.+ -]+", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_item_code(value):
    return re.sub(r"[^A-Z0-9]+", "", cstr(value or "").upper())


def normalize_brand(value):
    return normalize_text(value)


def normalize_category(value):
    return normalize_text(value)


def normalize_color_temp(value):
    value = normalize_text(value)
    alias = get_alias_map().get(value)
    if alias:
        value = alias
    match = re.search(r"(\d{4,5})\s*k\b", value)
    if match:
        return f"{match.group(1)}K"
    if value.isdigit() and len(value) in (4, 5):
        return f"{value}K"
    return value.upper() if value else ""


def normalize_ip_rate(value):
    value = normalize_text(value)
    match = re.search(r"ip\s*([0-9]{2,3})", value)
    if match:
        return f"IP{match.group(1)}"
    return value.upper() if value else ""


def extract_numeric_specs(row):
    power_value = _extract_first_number(row.get("power"), "w")
    color_temp = normalize_color_temp(row.get("color_temp") or row.get("color_temp_"))
    color_temp_kelvin = _extract_first_number(color_temp, "k")
    ip_rate = normalize_ip_rate(row.get("ip_rate"))
    ip_rating_numeric = _extract_first_number(ip_rate, "")

    return {
        "power_value": power_value,
        "color_temp_kelvin": color_temp_kelvin,
        "ip_rating_numeric": ip_rating_numeric,
    }


def expand_search_aliases(text):
    normalized = normalize_text(text)
    if not normalized:
        return ""

    expanded = {normalized}
    alias_map = get_alias_map()
    for phrase, canonical in alias_map.items():
        if phrase and phrase in normalized:
            expanded.add(canonical)
            expanded.add(normalized.replace(phrase, canonical))
    return " ".join(sorted(expanded))


def build_search_keywords(row):
    keywords = []
    for field in (
        "item_code",
        "item_name",
        "brand",
        "item_group",
        "category_list",
        "product_type",
        "parent_item_code",
        "parent_item_name",
    ):
        value = row.get(field)
        if value:
            keywords.append(cstr(value).strip())

    for field, _label in SPEC_FIELDS:
        value = row.get(field)
        if value:
            keywords.append(cstr(value).strip())

    numeric_specs = extract_numeric_specs(row)
    if numeric_specs["color_temp_kelvin"]:
        keywords.append(f'{int(numeric_specs["color_temp_kelvin"])}K')
    if numeric_specs["ip_rating_numeric"]:
        keywords.append(f'IP{int(numeric_specs["ip_rating_numeric"])}')

    expanded = []
    for keyword in keywords:
        expanded.append(keyword)
        alias_expansion = expand_search_aliases(keyword)
        if alias_expansion:
            expanded.append(alias_expansion)

    deduped = []
    seen = set()
    for keyword in expanded:
        normalized = normalize_text(keyword)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(keyword)
    return ", ".join(deduped)


def build_spec_summary(row):
    parts = []
    for field, label in SPEC_FIELDS:
        value = row.get(field)
        if value:
            parts.append(f"{label}: {value}")
    return " | ".join(parts)


def build_searchable_text(row):
    parts = [
        cstr(row.get("item_code")),
        cstr(row.get("item_name")),
        cstr(row.get("brand")),
        cstr(row.get("item_group")),
        cstr(row.get("category_list")),
        cstr(row.get("product_type")),
        cstr(row.get("parent_item_code")),
        cstr(row.get("parent_item_name")),
        cstr(row.get("description")),
        cstr(row.get("search_keywords")),
        cstr(row.get("spec_summary")),
    ]

    expanded = [expand_search_aliases(value) for value in parts if value]
    parts.extend(expanded)
    return " ".join(part for part in parts if part).strip()


def build_similarity_signature(row):
    numeric_specs = extract_numeric_specs(row)
    power_band = _band_value(numeric_specs.get("power_value"), 10)
    color_band = _band_value(numeric_specs.get("color_temp_kelvin"), 500)
    ip_band = _band_value(numeric_specs.get("ip_rating_numeric"), 10)
    return "|".join(
        [
            normalize_category(row.get("category_list")),
            normalize_text(row.get("product_type")),
            f"power:{power_band}",
            f"color:{color_band}",
            f"ip:{ip_band}",
            normalize_text(row.get("mounting")),
            normalize_text(row.get("lamp_type")),
            normalize_text(row.get("material")),
        ]
    ).strip("|")


def compute_priority_score(row):
    score = 0
    score += 40 if flt(row.get("hot_product")) else 0
    score += 35 if flt(row.get("popular_product")) else 0
    score += 30 if flt(row.get("best_selling")) else 0
    score += 25 if flt(row.get("promotion_item")) else 0
    score += 10 if flt(row.get("new_arrival")) else 0
    return int(score)


def compute_popularity_score(row):
    return round(
        flt(row.get("sold_last_30_days"))
        + (15 if flt(row.get("best_selling")) else 0)
        + (10 if flt(row.get("popular_product")) else 0),
        2,
    )


def compute_business_score(row):
    score = compute_priority_score(row) + compute_popularity_score(row)
    score += 15 if flt(row.get("stock")) > 0 else 0
    score += 5 if flt(row.get("discount_percentage")) > 0 else 0
    return round(score, 2)


def build_stock_bucket(stock):
    stock = flt(stock)
    if stock <= 0:
        return "out_of_stock"
    if stock <= 10:
        return "low"
    if stock <= 50:
        return "medium"
    return "high"


def build_price_bucket(price):
    price = flt(price)
    if price <= 0:
        return "unknown"
    if price < 100:
        return "budget"
    if price < 1000:
        return "mid"
    return "premium"


def _extract_first_number(value, _unit_hint):
    match = re.search(r"(-?\d+(?:\.\d+)?)", cstr(value or ""))
    return round(flt(match.group(1)), 2) if match else 0.0


def _band_value(value, band_size):
    value = flt(value)
    if not value:
        return "na"
    lower = int(value // band_size) * band_size
    upper = lower + band_size
    return f"{lower}-{upper}"

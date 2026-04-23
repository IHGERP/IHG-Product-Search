import copy
import json
from datetime import datetime
from json import JSONDecodeError

import frappe
import typesense
from frappe import _
from frappe.utils import cint, cstr, flt, get_datetime, now_datetime

from igh_search.igh_search.search_normalization import (
    build_price_bucket,
    build_search_keywords,
    build_searchable_text,
    build_similarity_signature,
    build_spec_summary,
    build_stock_bucket,
    compute_business_score,
    compute_popularity_score,
    compute_priority_score,
    extract_numeric_specs,
    load_glossary,
    normalize_brand,
    normalize_category,
    normalize_color_temp,
    normalize_ip_rate,
    normalize_item_code,
    normalize_text,
)


PRODUCT_V2_COLLECTION = "product_v2"
DEFAULT_ALLOWED_ROLES = ("System Manager", "Sales Manager", "Sales User")
NUMERIC_RANGE_FILTERS = {
    "rate",
    "offer_rate",
    "discount_percentage",
    "stock",
    "sold_last_30_days",
    "inventory_value",
    "priority_score",
    "popularity_score",
    "business_score",
    "power_value",
    "color_temp_kelvin",
    "ip_rating_numeric",
}
FILTER_FIELDS = {
    "brand",
    "item_group",
    "category_list",
    "product_type",
    "power",
    "color_temp",
    "ip_rate",
    "beam_angle",
    "mounting",
    "body_finish",
    "input_voltage",
    "output_voltage",
    "output_current",
    "lamp_type",
    "material",
    "warranty",
    "is_variant",
    "variant_of",
    "is_active",
    "in_stock",
    "stock_bucket",
    "price_bucket",
}
SORT_FIELDS = {
    "discount_percentage",
    "rate",
    "offer_rate",
    "stock",
    "sold_last_30_days",
    "priority_score",
    "popularity_score",
    "business_score",
    "creation_ts",
    "modified_ts",
}
SORT_FIELD_ALIASES = {
    "creation": "creation_ts",
    "creation_on": "creation_ts",
}
FACET_FIELDS = [
    "brand",
    "item_group",
    "category_list",
    "product_type",
    "power",
    "color_temp",
    "ip_rate",
    "beam_angle",
    "mounting",
    "body_finish",
    "input_voltage",
    "output_voltage",
    "output_current",
    "lamp_type",
    "material",
    "warranty",
    "is_variant",
    "variant_of",
    "is_active",
    "in_stock",
    "stock_bucket",
    "price_bucket",
]
SEARCH_RESULT_FIELDS = (
    "item_code",
    "item_name",
    "brand",
    "category_list",
    "item_group",
    "product_type",
    "image",
    "stock_uom",
    "rate",
    "offer_rate",
    "discount_percentage",
    "stock",
    "in_stock",
    "priority_score",
    "popularity_score",
    "business_score",
    "is_active",
    "variant_of",
    "parent_item_code",
    "parent_item_name",
    "power",
    "color_temp",
    "ip_rate",
    "beam_angle",
    "mounting",
    "lamp_type",
    "material",
    "body_finish",
    "warranty",
    "input_voltage",
    "output_voltage",
    "output_current",
    "spec_summary",
    "manual_alternative_codes",
    "manual_related_codes",
)

PRODUCT_V2_SCHEMA = {
    "name": PRODUCT_V2_COLLECTION,
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "item_code", "type": "string", "infix": True},
        {"name": "item_code_normalized", "type": "string", "infix": True},
        {"name": "item_name", "type": "string"},
        {"name": "item_name_normalized", "type": "string"},
        {"name": "is_active", "type": "int32", "facet": True},
        {"name": "is_deleted", "type": "int32", "facet": True},
        {"name": "disabled", "type": "int32", "facet": True},
        {"name": "modified_ts", "type": "int64"},
        {"name": "creation_ts", "type": "int64"},
        {"name": "is_variant", "type": "int32", "facet": True},
        {"name": "variant_of", "type": "string", "facet": True},
        {"name": "parent_item_code", "type": "string"},
        {"name": "parent_item_name", "type": "string"},
        {"name": "parent_item_code_normalized", "type": "string"},
        {"name": "parent_item_name_normalized", "type": "string"},
        {"name": "description", "type": "string"},
        {"name": "brand", "type": "string", "facet": True},
        {"name": "category_list", "type": "string", "facet": True},
        {"name": "item_group", "type": "string", "facet": True},
        {"name": "stock_uom", "type": "string", "optional": True},
        {"name": "series", "type": "string", "optional": True},
        {"name": "image", "type": "string", "optional": True},
        {"name": "is_stock_item", "type": "bool", "optional": True},
        {"name": "has_variants", "type": "bool", "optional": True},
        {"name": "product_type", "type": "string", "facet": True},
        {"name": "height", "type": "string", "optional": True},
        {"name": "width", "type": "string", "optional": True},
        {"name": "depth", "type": "float", "optional": True},
        {"name": "dimension", "type": "string", "optional": True},
        {"name": "custom_moq", "type": "string", "optional": True},
        {"name": "range", "type": "string", "optional": True},
        {"name": "lamp_qty", "type": "string", "optional": True},
        {"name": "safety_class", "type": "string", "optional": True},
        {"name": "eec", "type": "string", "optional": True},
        {"name": "power", "type": "string", "facet": True},
        {"name": "color_temp", "type": "string", "facet": True},
        {"name": "ip_rate", "type": "string", "facet": True},
        {"name": "beam_angle", "type": "string", "facet": True},
        {"name": "lumen_output", "type": "string", "optional": True},
        {"name": "reflector", "type": "string", "optional": True},
        {"name": "mounting", "type": "string", "facet": True},
        {"name": "att_heat_sink", "type": "string", "optional": True},
        {"name": "output_signal", "type": "string", "optional": True},
        {"name": "power_factor", "type": "string", "optional": True},
        {"name": "working_temp", "type": "string", "optional": True},
        {"name": "life_time", "type": "string", "optional": True},
        {"name": "body_finish", "type": "string", "facet": True},
        {"name": "input_voltage", "type": "string", "facet": True},
        {"name": "output_voltage", "type": "string", "facet": True},
        {"name": "output_current", "type": "string", "facet": True},
        {"name": "light_intensity", "type": "string", "optional": True},
        {"name": "light_source", "type": "string", "optional": True},
        {"name": "lamp_type", "type": "string", "facet": True},
        {"name": "cri", "type": "string", "optional": True},
        {"name": "efficacy", "type": "string", "optional": True},
        {"name": "operating_frequency", "type": "string", "optional": True},
        {"name": "input_signal", "type": "string", "optional": True},
        {"name": "function", "type": "string", "optional": True},
        {"name": "cut_out", "type": "string", "optional": True},
        {"name": "material", "type": "string", "facet": True},
        {"name": "shade_material", "type": "string", "optional": True},
        {"name": "shade_finish", "type": "string", "optional": True},
        {"name": "pole_dimension", "type": "string", "optional": True},
        {"name": "suspended_length", "type": "string", "optional": True},
        {"name": "warranty_type", "type": "string", "optional": True},
        {"name": "warranty", "type": "string", "facet": True},
        {"name": "warranty_in_yrs", "type": "float", "optional": True},
        {"name": "diffuser", "type": "string", "optional": True},
        {"name": "custom_esma_certified", "type": "bool", "optional": True},
        {"name": "primary_material", "type": "string", "optional": True},
        {"name": "secondary_material", "type": "string", "optional": True},
        {"name": "capacity", "type": "string", "optional": True},
        {"name": "country_of_orgin", "type": "string", "optional": True},
        {"name": "number_of_pieces", "type": "string", "optional": True},
        {"name": "leather_finish", "type": "string", "optional": True},
        {"name": "fabric_finish", "type": "string", "optional": True},
        {"name": "primary_color", "type": "string", "optional": True},
        {"name": "secondary_color", "type": "string", "optional": True},
        {"name": "remarks", "type": "string", "optional": True},
        {"name": "search_keywords", "type": "string"},
        {"name": "spec_summary", "type": "string"},
        {"name": "searchable_text", "type": "string"},
        {"name": "in_stock", "type": "int32", "facet": True},
        {"name": "rate", "type": "float", "facet": True},
        {"name": "offer_rate", "type": "float", "facet": True},
        {"name": "discount_percentage", "type": "float", "facet": True},
        {"name": "stock", "type": "float", "facet": True},
        {"name": "sold_last_30_days", "type": "float", "facet": True},
        {"name": "inventory_value", "type": "float"},
        {"name": "priority_score", "type": "float"},
        {"name": "popularity_score", "type": "float"},
        {"name": "business_score", "type": "float"},
        {"name": "power_value", "type": "float", "facet": True},
        {"name": "color_temp_kelvin", "type": "float", "facet": True},
        {"name": "ip_rating_numeric", "type": "float", "facet": True},
        {"name": "stock_bucket", "type": "string", "facet": True},
        {"name": "price_bucket", "type": "string", "facet": True},
        {"name": "manual_related_codes", "type": "string[]", "optional": True},
        {"name": "manual_alternative_codes", "type": "string[]", "optional": True},
        {"name": "manual_bought_together_codes", "type": "string[]", "optional": True},
        {"name": "bought_together", "type": "string[]", "optional": True},
        {"name": "similar_range", "type": "string[]", "optional": True},
        {"name": "related_products", "type": "string[]", "optional": True},
        {"name": "accessories", "type": "string[]", "optional": True},
        {"name": "must_use", "type": "string[]", "optional": True},
        {"name": "similarity_signature", "type": "string"},
    ],
}


def get_product_v2_schema():
    return copy.deepcopy(PRODUCT_V2_SCHEMA)


def create_typesense_client():
    client_details = frappe.get_doc("Typesense Settings")
    return typesense.Client(
        {
            "nodes": [
                {
                    "host": client_details.host,
                    "port": client_details.port,
                    "protocol": client_details.protocol,
                }
            ],
            "api_key": client_details.get_password("api_key"),
            "connection_timeout_seconds": 120,
        }
    )


def get_v2_config():
    conf = frappe.conf or {}
    return {
        "dual_write": cint(conf.get("igh_search_v2_dual_write", 1)),
        "query_enabled": cint(conf.get("igh_search_v2_query_enabled", 0)),
        "default_collection": cstr(
            conf.get("igh_search_v2_default_collection", PRODUCT_V2_COLLECTION)
        ).strip()
        or PRODUCT_V2_COLLECTION,
        "query_roles": tuple(
            conf.get("igh_search_v2_query_roles", DEFAULT_ALLOWED_ROLES)
        ),
        "max_retry_count": cint(conf.get("igh_search_v2_max_retry_count", 3)),
    }


def is_dual_write_enabled():
    return bool(get_v2_config()["dual_write"])


def is_query_enabled():
    return bool(get_v2_config()["query_enabled"])


def get_default_collection():
    return get_v2_config()["default_collection"]


def ensure_query_access(feature_flag_override=0):
    if frappe.session.user == "Guest":
        frappe.throw(_("Authentication required"))

    roles = set(frappe.get_roles())
    allowed_roles = set(get_v2_config()["query_roles"])
    if not roles.intersection(allowed_roles):
        frappe.throw(_("You are not permitted to access product search V2"))

    if not is_query_enabled() and not (
        cint(feature_flag_override) and "System Manager" in roles
    ):
        frappe.throw(_("Product search V2 is not enabled"))


def create_sync_log(trigger_type, source_doctype, source_docname, collection_name, item_codes):
    if not frappe.db.exists("DocType", "Typesense Sync Log"):
        return None

    log = frappe.get_doc(
        {
            "doctype": "Typesense Sync Log",
            "trigger_type": trigger_type,
            "source_doctype": source_doctype,
            "source_docname": source_docname,
            "collection_name": collection_name,
            "status": "Queued",
            "retry_count": 0,
            "affected_item_codes": json.dumps(sorted(item_codes or [])),
            "queued_at": now_datetime(),
        }
    )
    log.insert(ignore_permissions=True)
    return log.name


def update_sync_log(log_name, status, retry_count=None, failure_reason=None, started=False, finished=False):
    if not log_name or not frappe.db.exists("Typesense Sync Log", log_name):
        return

    updates = {"status": status}
    if retry_count is not None:
        updates["retry_count"] = retry_count
    if failure_reason is not None:
        updates["failure_reason"] = cstr(failure_reason)[:100000]
    if started:
        updates["started_at"] = now_datetime()
    if finished:
        updates["finished_at"] = now_datetime()
    frappe.db.set_value("Typesense Sync Log", log_name, updates, update_modified=False)


def get_sync_health_summary():
    if not frappe.db.exists("DocType", "Typesense Sync Log"):
        return {
            "last_successful_sync": None,
            "failed_item_list": [],
            "dead_letter_count": 0,
            "backlog_count": 0,
        }

    last_successful_sync = frappe.db.get_value(
        "Typesense Sync Log",
        {"status": "Success"},
        "finished_at",
        order_by="finished_at desc",
    )
    failed_logs = frappe.get_all(
        "Typesense Sync Log",
        filters={"status": ["in", ["Failed", "Dead Letter"]]},
        fields=["affected_item_codes", "status", "name"],
        order_by="modified desc",
        limit=20,
    )
    failed_item_list = []
    for log in failed_logs:
        failed_item_list.extend(json.loads(log.affected_item_codes or "[]"))
    return {
        "last_successful_sync": last_successful_sync,
        "failed_item_list": list(dict.fromkeys(failed_item_list)),
        "dead_letter_count": frappe.db.count(
            "Typesense Sync Log", {"status": "Dead Letter"}
        ),
        "backlog_count": frappe.db.count(
            "Typesense Sync Log", {"status": ["in", ["Queued", "Running", "Retrying"]]}
        ),
    }


def sync_typesense_synonyms(client, collection_name=PRODUCT_V2_COLLECTION):
    collection = client.collections[collection_name]
    for entry in load_glossary().get("entries", []):
        synonyms = [entry.get("canonical")] + list(entry.get("aliases", []))
        collection.synonyms.upsert(
            entry["id"],
            {
                "synonyms": [value for value in synonyms if value],
                "root": entry.get("canonical"),
            },
        )


def build_related_item_map(item_codes=None):
    if item_codes:
        rows = frappe.db.sql(
            """
            SELECT item_1, item_2, type, relate_both_ways
            FROM `tabRelated Items`
            WHERE item_1 IN %(item_codes)s OR item_2 IN %(item_codes)s
            """,
            {"item_codes": tuple(item_codes)},
            as_dict=1,
        )
    else:
        rows = frappe.get_all(
            "Related Items",
            fields=["item_1", "item_2", "type", "relate_both_ways"],
        )

    related_map = {}
    for row in rows:
        _append_relation(related_map, row.item_1, row.item_2, row.type)
        if cint(row.relate_both_ways):
            _append_relation(related_map, row.item_2, row.item_1, row.type)
    return related_map


def compute_product_v2_document(row, related_map=None):
    related_map = related_map or {}
    document = {
        "id": row["item_code"],
        "item_code": row["item_code"],
        "item_code_normalized": normalize_item_code(row["item_code"]),
        "item_name": cstr(row.get("item_name")),
        "item_name_normalized": normalize_text(row.get("item_name")),
        "is_active": 0 if cint(row.get("disabled")) or cint(row.get("item_group_disabled")) else 1,
        "is_deleted": 0,
        "disabled": cint(row.get("disabled")),
        "modified_ts": _to_timestamp(row.get("modified")),
        "creation_ts": _to_timestamp(row.get("creation_raw")),
        "is_variant": 1 if cstr(row.get("variant_of")) else 0,
        "variant_of": cstr(row.get("variant_of")),
        "parent_item_code": cstr(row.get("variant_of")),
        "parent_item_name": cstr(row.get("parent_item_name")),
        "parent_item_code_normalized": normalize_item_code(row.get("variant_of")),
        "parent_item_name_normalized": normalize_text(row.get("parent_item_name")),
        "description": cstr(
            row.get("description") or row.get("full_description") or row.get("item_description")
        ),
        "stock_uom": cstr(row.get("stock_uom")),
        "series": cstr(row.get("series")),
        "image": cstr(row.get("image") or row.get("website_image_url")),
        "brand": cstr(row.get("brand")),
        "category_list": cstr(row.get("category_list")),
        "item_group": cstr(row.get("item_group")),
        "is_stock_item": bool(row.get("is_stock_item")),
        "has_variants": bool(row.get("has_variants")),
        "product_type": cstr(row.get("product_type")),
        "height": cstr(row.get("height")),
        "width": cstr(row.get("width")),
        "depth": flt(row.get("depth")),
        "dimension": cstr(row.get("dimension")),
        "custom_moq": cstr(row.get("custom_moq")),
        "range": cstr(row.get("range")),
        "lamp_qty": cstr(row.get("lamp_qty")),
        "safety_class": cstr(row.get("safety_class")),
        "eec": cstr(row.get("eec")),
        "power": cstr(row.get("power")),
        "color_temp": normalize_color_temp(row.get("color_temp") or row.get("color_temp_")),
        "ip_rate": normalize_ip_rate(row.get("ip_rate")),
        "beam_angle": cstr(row.get("beam_angle")),
        "lumen_output": cstr(row.get("lumen_output")),
        "reflector": cstr(row.get("reflector")),
        "mounting": cstr(row.get("mounting")),
        "att_heat_sink": cstr(row.get("att_heat_sink")),
        "output_signal": cstr(row.get("output_signal")),
        "power_factor": cstr(row.get("power_factor")),
        "working_temp": cstr(row.get("working_temp")),
        "life_time": cstr(row.get("life_time")),
        "body_finish": cstr(row.get("body_finish")),
        "input_voltage": cstr(row.get("input_voltage") or row.get("input")),
        "output_voltage": cstr(row.get("output_voltage")),
        "output_current": cstr(row.get("output_current")),
        "light_intensity": cstr(row.get("light_intensity")),
        "light_source": cstr(row.get("light_source")),
        "lamp_type": cstr(row.get("lamp_type")),
        "cri": cstr(row.get("cri")),
        "efficacy": cstr(row.get("efficacy")),
        "operating_frequency": cstr(row.get("operating_frequency")),
        "input_signal": cstr(row.get("input_signal")),
        "function": cstr(row.get("function")),
        "cut_out": cstr(row.get("cut_out")),
        "material": cstr(row.get("material")),
        "shade_material": cstr(row.get("shade_material")),
        "shade_finish": cstr(row.get("shade_finish")),
        "pole_dimension": cstr(row.get("pole_dimension")),
        "suspended_length": cstr(row.get("suspended_length")),
        "warranty_type": cstr(row.get("warranty_type") or row.get("warranty_type_")),
        "warranty": cstr(row.get("warranty") or row.get("warranty_")),
        "warranty_in_yrs": flt(row.get("warranty_in_yrs")),
        "diffuser": cstr(row.get("diffuser")),
        "custom_esma_certified": bool(row.get("custom_esma_certified")),
        "primary_material": cstr(row.get("primary_material")),
        "secondary_material": cstr(row.get("secondary_material")),
        "capacity": cstr(row.get("capacity")),
        "country_of_orgin": cstr(row.get("country_of_orgin")),
        "number_of_pieces": cstr(row.get("number_of_pieces")),
        "leather_finish": cstr(row.get("leather_finish")),
        "fabric_finish": cstr(row.get("fabric_finish")),
        "primary_color": cstr(row.get("primary_color")),
        "secondary_color": cstr(row.get("secondary_color")),
        "remarks": cstr(row.get("remarks")),
        "rate": flt(row.get("rate")),
        "offer_rate": flt(row.get("offer_rate")),
        "discount_percentage": flt(row.get("discount_percentage")),
        "stock": flt(row.get("stock")),
        "sold_last_30_days": flt(row.get("sold_last_30_days")),
        "inventory_value": flt(row.get("inventory_value")),
    }
    document["search_keywords"] = build_search_keywords(document)
    document["spec_summary"] = build_spec_summary(document)
    document["searchable_text"] = build_searchable_text(document)
    document["in_stock"] = 1 if document["stock"] > 0 else 0
    document["priority_score"] = compute_priority_score(row)
    document["popularity_score"] = compute_popularity_score(row)
    document["business_score"] = compute_business_score(
        {
            **row,
            "stock": document["stock"],
            "discount_percentage": document["discount_percentage"],
        }
    )
    document.update(extract_numeric_specs(document))
    document["stock_bucket"] = build_stock_bucket(document["stock"])
    document["price_bucket"] = build_price_bucket(
        document["offer_rate"] or document["rate"]
    )

    manual_relationships = related_map.get(document["item_code"], {})
    document["manual_related_codes"] = manual_relationships.get("related", [])
    document["manual_alternative_codes"] = manual_relationships.get("alternative", [])
    document["manual_bought_together_codes"] = manual_relationships.get(
        "bought_together", []
    )
    document["bought_together"] = _unique_strings(
        row.get("bought_together") or manual_relationships.get("bought_together", [])
    )
    document["similar_range"] = _unique_strings(row.get("similar_range"))
    document["related_products"] = _unique_strings(
        row.get("related_products") or manual_relationships.get("related", [])
    )
    document["accessories"] = _unique_strings(row.get("accessories"))
    document["must_use"] = _unique_strings(row.get("must_use"))
    document["similarity_signature"] = build_similarity_signature(document)
    return document


def delete_typesense_documents(client, collection_name, item_codes):
    if not item_codes:
        return
    item_codes = [code for code in item_codes if code]
    if not item_codes:
        return
    filters = ",".join(f'"{code}"' for code in item_codes)
    try:
        client.collections[collection_name].documents.delete(
            {"filter_by": f"item_code:=[{filters}]"}
        )
    except typesense.exceptions.ObjectNotFound:
        return


def build_filter_by(filters=None, include_inactive=0):
    filters = _coerce_json(filters) or {}
    clauses = []
    if not cint(include_inactive):
        clauses.append("is_active:=1")

    for key, value in filters.items():
        if key in FILTER_FIELDS:
            clauses.extend(_build_filter_clause(key, value))
            continue

        field_name = key[:-6] if key.endswith("_range") else key
        if field_name in NUMERIC_RANGE_FILTERS:
            clauses.extend(_build_numeric_range_clauses(field_name, value))

    return " && ".join(clause for clause in clauses if clause)


def search_products_v2(
    query=None,
    filters=None,
    sort_by=None,
    page=1,
    page_length=20,
    include_inactive=0,
    item_code_hint=None,
    feature_flag_override=0,
):
    ensure_query_access(feature_flag_override=feature_flag_override)

    client = create_typesense_client()
    parsed_filters = parse_search_filters(filters)
    query_resolution = resolve_effective_query(query=query, item_code_hint=item_code_hint)
    query_text = query_resolution["effective_query"]
    sku_like = query_resolution["sku_like"]
    sort_resolution = resolve_sort_by(sort_by, sku_like=sku_like)

    search_parameters = {
        "q": query_text,
        "query_by": "item_code_normalized,item_code,item_name_normalized,item_name,searchable_text,brand,category_list,parent_item_code,parent_item_name",
        "query_by_weights": "12,10,8,6,4,2,2,2,2",
        "facet_by": ",".join(FACET_FIELDS),
        "filter_by": build_filter_by(filters=parsed_filters, include_inactive=include_inactive),
        "page": max(cint(page), 1),
        "per_page": max(min(cint(page_length), 100), 1),
        "sort_by": sort_resolution["final_sort"],
        "include_fields": ",".join(SEARCH_RESULT_FIELDS),
    }
    if sku_like:
        search_parameters["prefix"] = "true,true,false,false,false,false,false,false,false"
        search_parameters["num_typos"] = "0,0,1,1,1,1,1,1,1"

    log_search_request(
        "request",
        {
            "query": cstr(query or ""),
            "item_code_hint": cstr(item_code_hint or ""),
            "effective_query": query_text,
            "normalized_query": query_resolution["normalized_query"],
            "requested_sort": cstr(sort_by or ""),
            "aliased_sort": sort_resolution["aliased_sort"],
            "final_sort": sort_resolution["final_sort"],
            "sku_like": sku_like,
            "parsed_filters": parsed_filters,
            "fallback_reasons": sort_resolution["fallback_reasons"],
            "search_parameters": search_parameters,
        },
    )

    response = client.collections[get_default_collection()].documents.search(
        search_parameters
    )
    if sort_resolution["should_rerank"]:
        response["hits"] = rank_search_hits(response.get("hits", []), query_text)
    response["applied_filters"] = parsed_filters
    response["query_debug"] = {
        "normalized_query": query_resolution["normalized_query"],
        "effective_query": query_text,
        "sku_like": sku_like,
        "requested_sort": cstr(sort_by or ""),
        "aliased_sort": sort_resolution["aliased_sort"],
        "applied_sort": sort_resolution["final_sort"],
        "fallback_reasons": sort_resolution["fallback_reasons"],
        "search_parameters": search_parameters,
    }
    return response


def suggest_products_v2(query=None, limit=10, feature_flag_override=0):
    response = search_products_v2(
        query=query,
        page=1,
        page_length=limit,
        feature_flag_override=feature_flag_override,
    )
    suggestions = []
    for hit in response.get("hits", []):
        document = hit.get("document", {})
        suggestions.append(
            {
                "item_code": document.get("item_code"),
                "item_name": document.get("item_name"),
                "brand": document.get("brand"),
            }
        )
    return {"suggestions": suggestions, "query_debug": response.get("query_debug")}


def get_similar_products_v2(item_code, limit=10, include_manual=1, feature_flag_override=0):
    ensure_query_access(feature_flag_override=feature_flag_override)
    source_document = get_product_document(item_code, include_inactive=1)
    if not source_document:
        frappe.throw(_("Item not indexed in product_v2"))

    results = []
    seen = {item_code}
    if cint(include_manual):
        manual_codes = (
            source_document.get("manual_alternative_codes", [])
            + source_document.get("manual_related_codes", [])
        )
        manual_hits = get_documents_by_codes(manual_codes, include_inactive=0)
        for hit in manual_hits:
            code = hit.get("item_code")
            if code in seen:
                continue
            seen.add(code)
            results.append({"reason": "manual", "score": 100, "document": hit})
            if len(results) >= cint(limit):
                return {"item_code": item_code, "results": results}

    client = create_typesense_client()
    filter_clauses = [f'item_code:!={item_code}', "is_active:=1"]
    if source_document.get("category_list"):
        filter_clauses.append(
            f'category_list:="{_escape_filter_value(source_document.get("category_list"))}"'
        )
    if source_document.get("product_type"):
        filter_clauses.append(
            f'product_type:="{_escape_filter_value(source_document.get("product_type"))}"'
        )
    candidate_response = client.collections[get_default_collection()].documents.search(
        {
            "q": "*",
            "query_by": "searchable_text",
            "filter_by": " && ".join(filter_clauses),
            "per_page": max(cint(limit) * 5, 20),
            "page": 1,
            "sort_by": "in_stock:desc,business_score:desc,stock:desc",
        }
    )
    for hit in candidate_response.get("hits", []):
        document = hit.get("document", {})
        code = document.get("item_code")
        if code in seen:
            continue
        similarity_score = calculate_similarity_score(source_document, document)
        if similarity_score <= 0:
            continue
        seen.add(code)
        results.append(
            {"reason": "computed_similarity", "score": similarity_score, "document": document}
        )

    results.sort(key=lambda item: item["score"], reverse=True)
    return {"item_code": item_code, "results": results[: cint(limit)]}


def get_documents_by_codes(item_codes, include_inactive=0):
    item_codes = [code for code in item_codes if code]
    if not item_codes:
        return []
    client = create_typesense_client()
    joined_codes = ",".join(f'"{code}"' for code in item_codes)
    filters = [f"item_code:=[{joined_codes}]"]
    if not cint(include_inactive):
        filters.append("is_active:=1")
    response = client.collections[get_default_collection()].documents.search(
        {
            "q": "*",
            "query_by": "searchable_text",
            "filter_by": " && ".join(filters),
            "per_page": len(item_codes),
            "page": 1,
        }
    )
    return [hit.get("document", {}) for hit in response.get("hits", [])]


def get_product_document(item_code, include_inactive=0):
    documents = get_documents_by_codes([item_code], include_inactive=include_inactive)
    return documents[0] if documents else None


def resolve_effective_query(query=None, item_code_hint=None):
    normalized_query = normalize_text(query)
    normalized_hint = normalize_text(item_code_hint)
    effective_query = normalized_query or normalized_hint or "*"
    return {
        "normalized_query": normalized_query,
        "normalized_item_code_hint": normalized_hint,
        "effective_query": effective_query,
        "sku_like": is_sku_like(query or item_code_hint),
    }


def rank_search_hits(hits, query_text):
    normalized_query = normalize_text(query_text)

    def sort_key(hit):
        document = hit.get("document", {})
        item_code = document.get("item_code") or ""
        normalized_code = document.get("item_code_normalized") or normalize_item_code(
            item_code
        )
        item_name = normalize_text(document.get("item_name"))
        exact_sku = normalized_code == normalize_item_code(normalized_query)
        prefix_sku = normalized_code.startswith(normalize_item_code(normalized_query))
        exact_name = item_name == normalized_query
        spec_match = 1 if normalized_query and normalized_query in normalize_text(document.get("spec_summary")) else 0
        text_match = hit.get("text_match") or 0
        return (
            1 if exact_sku else 0,
            1 if prefix_sku else 0,
            1 if exact_name else 0,
            cint(document.get("in_stock")),
            spec_match,
            flt(document.get("priority_score")),
            flt(document.get("popularity_score")),
            text_match,
        )

    return sorted(hits, key=sort_key, reverse=True)


def sanitize_sort_by(sort_by, sku_like=False):
    value = cstr(sort_by or "").strip()
    if not value:
        return "_text_match:desc,in_stock:desc,business_score:desc"

    parts = value.split(":")
    field_name = SORT_FIELD_ALIASES.get(parts[0], parts[0])
    direction = parts[1] if len(parts) > 1 else "desc"
    if field_name not in SORT_FIELDS or direction not in {"asc", "desc"}:
        return "_text_match:desc,in_stock:desc,business_score:desc"
    return f"_text_match:desc,{field_name}:{direction},in_stock:desc"


def resolve_sort_by(sort_by, sku_like=False):
    requested_sort = cstr(sort_by or "").strip()
    fallback_reasons = []
    aliased_sort = requested_sort
    should_rerank = False

    if not requested_sort:
        should_rerank = True
        return {
            "requested_sort": requested_sort,
            "aliased_sort": aliased_sort,
            "final_sort": sanitize_sort_by(""),
            "should_rerank": should_rerank,
            "fallback_reasons": fallback_reasons,
        }

    parts = requested_sort.split(":")
    raw_field_name = parts[0]
    field_name = SORT_FIELD_ALIASES.get(raw_field_name, raw_field_name)
    direction = parts[1] if len(parts) > 1 else "desc"
    aliased_sort = f"{field_name}:{direction}"
    if raw_field_name in SORT_FIELD_ALIASES:
        fallback_reasons.append(
            f"sort_alias:{raw_field_name}->{SORT_FIELD_ALIASES[raw_field_name]}"
        )

    if field_name not in SORT_FIELDS or direction not in {"asc", "desc"}:
        should_rerank = True
        fallback_reasons.append("unsupported_sort:fallback_to_relevance")
        return {
            "requested_sort": requested_sort,
            "aliased_sort": aliased_sort,
            "final_sort": sanitize_sort_by(""),
            "should_rerank": should_rerank,
            "fallback_reasons": fallback_reasons,
        }

    return {
        "requested_sort": requested_sort,
        "aliased_sort": aliased_sort,
        "final_sort": sanitize_sort_by(aliased_sort, sku_like=sku_like),
        "should_rerank": False,
        "fallback_reasons": fallback_reasons,
    }


def is_sku_like(value):
    normalized = normalize_item_code(value)
    return bool(normalized) and len(normalized) >= 3 and any(char.isdigit() for char in normalized)


def calculate_similarity_score(source_document, candidate_document):
    score = 0
    if source_document.get("category_list") == candidate_document.get("category_list"):
        score += 25
    if source_document.get("product_type") == candidate_document.get("product_type"):
        score += 20
    if _within_band(source_document.get("power_value"), candidate_document.get("power_value"), 0.1):
        score += 15
    if _within_delta(
        source_document.get("color_temp_kelvin"),
        candidate_document.get("color_temp_kelvin"),
        500,
    ):
        score += 15
    if _within_delta(
        source_document.get("ip_rating_numeric"),
        candidate_document.get("ip_rating_numeric"),
        10,
    ):
        score += 10
    for field in ("mounting", "lamp_type", "material"):
        if normalize_text(source_document.get(field)) and normalize_text(
            source_document.get(field)
        ) == normalize_text(candidate_document.get(field)):
            score += 5
    score += 5 if cint(candidate_document.get("in_stock")) else 0
    return score


def _append_relation(related_map, source_code, target_code, relation_type):
    if not source_code or not target_code:
        return
    buckets = related_map.setdefault(
        source_code,
        {"related": [], "alternative": [], "bought_together": []},
    )
    if target_code not in buckets["related"]:
        buckets["related"].append(target_code)

    relation_type = cstr(relation_type)
    if relation_type == "Bought Together":
        if target_code not in buckets["bought_together"]:
            buckets["bought_together"].append(target_code)
        return

    if target_code not in buckets["alternative"]:
        buckets["alternative"].append(target_code)


def _to_timestamp(value):
    if not value:
        return 0
    if isinstance(value, datetime):
        return int(value.timestamp())
    return int(get_datetime(value).timestamp())


def _coerce_json(value):
    if value is None or value == "":
        return {}
    if isinstance(value, dict):
        return value
    return json.loads(value)


def parse_search_filters(filters):
    try:
        return _coerce_json(filters) or {}
    except (TypeError, ValueError, JSONDecodeError):
        log_search_request(
            "invalid_filters",
            {
                "filters": filters,
                "error": "Invalid filters payload supplied to search_products_v2",
            },
        )
        frappe.throw(_("Invalid filters payload for product search V2"))


def _build_filter_clause(field_name, value):
    if isinstance(value, list):
        joined = ",".join(f'"{_escape_filter_value(item)}"' for item in value if item not in (None, ""))
        return [f"{field_name}:=[{joined}]"] if joined else []
    if value in (None, ""):
        return []
    if field_name in {"is_variant", "is_active", "in_stock"}:
        return [f"{field_name}:={cint(value)}"]
    return [f'{field_name}:="{_escape_filter_value(value)}"']


def _build_numeric_range_clauses(field_name, value):
    if not isinstance(value, dict):
        return []
    clauses = []
    if value.get("min") not in (None, ""):
        clauses.append(f"{field_name}:>={flt(value.get('min'))}")
    if value.get("max") not in (None, ""):
        clauses.append(f"{field_name}:<={flt(value.get('max'))}")
    return clauses


def _within_band(source_value, candidate_value, tolerance_fraction):
    source_value = flt(source_value)
    candidate_value = flt(candidate_value)
    if not source_value or not candidate_value:
        return False
    tolerance = source_value * tolerance_fraction
    return source_value - tolerance <= candidate_value <= source_value + tolerance


def _within_delta(source_value, candidate_value, delta):
    source_value = flt(source_value)
    candidate_value = flt(candidate_value)
    if not source_value or not candidate_value:
        return False
    return abs(source_value - candidate_value) <= delta


def _escape_filter_value(value):
    return cstr(value).replace('"', '\\"')


def _unique_strings(values):
    cleaned_values = []
    seen = set()
    for value in values or []:
        cleaned_value = cstr(value).strip()
        if not cleaned_value or cleaned_value in seen:
            continue
        seen.add(cleaned_value)
        cleaned_values.append(cleaned_value)
    return cleaned_values


def log_search_request(event, payload):
    try:
        frappe.logger().info(
            "V2 Product Search %s: %s",
            event,
            json.dumps(payload, ensure_ascii=True, default=str),
        )
    except Exception:
        pass

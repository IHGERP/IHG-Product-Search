import copy
import json
import time
from datetime import datetime
from json import JSONDecodeError

import frappe
import typesense
from frappe import _
from frappe.utils import cint, cstr, flt, get_datetime, now_datetime

from igh_search.igh_search.lumen_normalization import build_lumen_overlap_filter, normalize_lumen_fields
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
    "lumen_min",
    "lumen_max",
    "product_star_rating",
    "customer_count",
    "total_sold_qty_lifetime",
}
FILTER_FIELDS = {
    "brand",
    "item_group",
    "category_list",
    "series",
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
    "lumen_unit",
    "is_variant",
    "variant_of",
    "is_active",
    "in_stock",
    "stock_bucket",
    "price_bucket",
    "is_manufactured_item",
    "lumen_output",
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
    "series",
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
    "lumen_unit",
    "is_variant",
    "variant_of",
    "is_active",
    "in_stock",
    "stock_bucket",
    "price_bucket",
    "is_manufactured_item",
    "product_star_rating",
    "customer_count",
]
SEARCH_RESULT_FIELDS = (
    "item_code",
    "item_name",
    "brand",
    "category_list",
    "series",
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
    "lumen_raw",
    "lumen_unit",
    "lumen_min",
    "lumen_max",
    "lumen_parse_status",
    "spec_summary",
    "manual_alternative_codes",
    "manual_related_codes",
    "product_star_rating",
    "customer_count",
    "total_sold_qty_lifetime",
    "is_manufactured_item",
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
        {"name": "series", "type": "string", "optional": True, "facet": True},
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
        {"name": "lumen_raw", "type": "string", "optional": True},
        {"name": "lumen_min", "type": "float", "facet": True, "optional": True},
        {"name": "lumen_max", "type": "float", "facet": True, "optional": True},
        {"name": "lumen_unit", "type": "string", "facet": True, "optional": True},
        {"name": "lumen_values", "type": "float[]", "optional": True},
        {"name": "lumen_parse_status", "type": "string", "facet": True, "optional": True},
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
        {"name": "product_star_rating", "type": "float", "facet": True, "optional": True},
        {"name": "customer_count", "type": "int32", "facet": True, "optional": True},
        {"name": "total_sold_qty_lifetime", "type": "float", "optional": True},
        {"name": "is_manufactured_item", "type": "int32", "facet": True, "optional": True},
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
                    "protocol": client_details.protocol if client_details.protocol in ("http", "https") else "https",
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
        # Phase 3 — hybrid (keyword + semantic vector) search. OFF by default;
        # reads a parallel auto-embedding collection so the live product_v2 and
        # the /list page are untouched. Flip igh_search_hybrid_enabled to roll out.
        "hybrid_enabled": cint(conf.get("igh_search_hybrid_enabled", 0)),
        "hybrid_collection": cstr(
            conf.get("igh_search_hybrid_collection", "product_v2_hybrid")
        ).strip()
        or "product_v2_hybrid",
        "hybrid_alpha": flt(conf.get("igh_search_hybrid_alpha", 0.7)),
    }


def is_dual_write_enabled():
    return bool(get_v2_config()["dual_write"])


def is_hybrid_enabled():
    return bool(get_v2_config()["hybrid_enabled"])


def get_hybrid_collection():
    return get_v2_config()["hybrid_collection"]


def is_query_enabled():
    return bool(get_v2_config()["query_enabled"])


def get_default_collection():
    return get_v2_config()["default_collection"]


def ensure_query_access(feature_flag_override=0):
    if frappe.session.user == "Guest":
        frappe.throw(_("Authentication required"))

    if not is_query_enabled() and not cint(feature_flag_override):
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
        "product_star_rating": flt(row.get("product_star_rating") or 3.5),
        "customer_count": cint(row.get("customer_count") or 0),
        "total_sold_qty_lifetime": flt(row.get("total_sold_qty_lifetime") or 0),
        "is_manufactured_item": cint(row.get("is_manufactured_item") or 0),
    }

    lumen_fields = normalize_lumen_fields(row.get("lumen_output"))
    document.update(lumen_fields)
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

    clauses.extend(_build_lumen_clauses(filters))

    return " && ".join(clause for clause in clauses if clause)


SOFT_RANGE_SENTINEL_MAX = 1000000000
SOFT_BOOST_STRING_FIELDS = (
    "ip_rate", "color_temp", "power", "beam_angle", "mounting",
    "body_finish", "material", "input_voltage", "output_current",
    "output_voltage", "lamp_type", "lumen_output",
)
# Maps a soft *_range filter key to the indexed numeric field it scores against.
SOFT_BOOST_RANGE_FIELDS = {
    "power_value_range": "power_value",
    "color_temp_kelvin_range": "color_temp_kelvin",
    "ip_rating_numeric_range": "ip_rating_numeric",
}


def _eval_quote(value):
    # Backtick-quote a filter literal for use inside _eval(); strip embedded
    # backticks so the expression can't be broken.
    return "`" + cstr(value).replace("`", "") + "`"


# Priority order for spec matches. Typesense `_eval([(f):w, ...])` is FIRST-MATCH-
# WINS, not additive, so conditions are emitted in strict priority order with
# strictly-descending weights: an item is scored by the single most important spec
# it matches. The numeric range specs (case-free, reliable) outrank string specs;
# sellability is the lowest tier so a precise spec match always beats a merely
# sellable but non-matching item. The Python compatibility reranker
# (calculate_ai_compatibility_score) then does the fine-grained additive ordering
# within the fetched page.
SOFT_BOOST_PRIORITY_RANGE = ("power_value_range", "color_temp_kelvin_range", "ip_rating_numeric_range")
SOFT_BOOST_PRIORITY_STRING = (
    "power", "color_temp", "ip_rate", "output_current", "output_voltage",
    "input_voltage", "lamp_type", "beam_angle", "mounting", "body_finish",
    "material", "lumen_output",
)


def _soft_range_expr(field, rng):
    if not isinstance(rng, dict):
        return ""
    parts = []
    low = rng.get("min")
    high = rng.get("max")
    if low not in (None, "") and flt(low) > 0:
        parts.append(f"{field}:>={flt(low)}")
    if high not in (None, "") and flt(high) < SOFT_RANGE_SENTINEL_MAX:
        parts.append(f"{field}:<={flt(high)}")
    return " && ".join(parts)


def build_soft_boost_clause(soft_boosts):
    """Build a Typesense `_eval([...])` sort expression that BOOSTS (never filters)
    docs matching the user's spec preferences, plus a final sellability tier.
    Fields absent on a doc simply don't match -> no boost, doc is kept. Conditions
    are ordered by importance with descending weights because `_eval` is
    first-match-wins (see note above)."""
    if not isinstance(soft_boosts, dict):
        return ""

    # Collect active spec conditions in priority order.
    range_conditions = []
    for range_key in SOFT_BOOST_PRIORITY_RANGE:
        expr = _soft_range_expr(SOFT_BOOST_RANGE_FIELDS[range_key], soft_boosts.get(range_key))
        if expr:
            range_conditions.append(expr)

    string_conditions = []
    for field in SOFT_BOOST_PRIORITY_STRING:
        values = _unique_strings(soft_boosts.get(field) or [])
        if values:
            joined = ",".join(_eval_quote(value) for value in values)
            string_conditions.append(f"{field}:=[{joined}]")

    tiers = []  # (expr, weight)
    # Top tier: an item matching ALL numeric specs at once (the strongest signal).
    if len(range_conditions) >= 2:
        tiers.append(("(" + " && ".join(range_conditions) + ")", 60))

    weight = 50
    for expr in range_conditions:
        tiers.append((f"({expr})", weight))
        weight -= 4
    for expr in string_conditions:
        tiers.append((f"({expr})", max(weight, 6)))
        weight -= 4
    # Sellability is the lowest tier: only decides order among items that match no
    # spec at all (so junk with rate=0 & stock=0 sinks, nothing is hidden).
    tiers.append(("(rate:>0 || stock:>0)", 2))

    body = ", ".join(f"{expr}:{w}" for expr, w in tiers)
    return f"_eval([{body}]):desc"


def search_products_v2(
    query=None,
    filters=None,
    sort_by=None,
    page=1,
    page_length=20,
    per_page=None,
    include_inactive=0,
    item_code_hint=None,
    feature_flag_override=0,
    strict_sort=0,
    soft_boosts=None,
    use_hybrid=None,
):
    ensure_query_access(feature_flag_override=feature_flag_override)

    started_at = time.perf_counter()
    client = create_typesense_client()
    parsed_filters = parse_search_filters(filters)
    query_resolution = resolve_effective_query(query=query, item_code_hint=item_code_hint)
    query_text = query_resolution["effective_query"]
    sku_like = query_resolution["sku_like"]
    sort_resolution = resolve_sort_by(sort_by, sku_like=sku_like, strict_sort=strict_sort)

    if per_page not in (None, ""):
        page_length = per_page

    if per_page not in (None, ):
        page_length = per_page

    search_parameters = {
        "q": query_text,
        "query_by": "item_code_normalized,item_code,item_name_normalized,item_name,searchable_text,brand,category_list,series,parent_item_code,parent_item_name",
        "query_by_weights": "12,10,8,6,4,2,2,2,2,2",
        "facet_by": ",".join(FACET_FIELDS),
        "filter_by": build_filter_by(filters=parsed_filters, include_inactive=include_inactive),
        "page": max(cint(page), 1),
        "per_page": max(min(cint(page_length), 100), 1),
        "sort_by": sort_resolution["final_sort"],
        "include_fields": ",".join(SEARCH_RESULT_FIELDS),
    }

    # Soft spec/sellability boosts: prepend an _eval(...) scoring clause so that
    # items matching the user's spec preferences (and sellable items with a price
    # or stock) rank first, WITHOUT excluding anything. _eval counts as one of
    # Typesense's max 3 sort fields, so keep it + the 2 strongest existing
    # tiebreakers.
    boost_clause = build_soft_boost_clause(soft_boosts)
    if boost_clause:
        tail = [s for s in cstr(search_parameters["sort_by"]).split(",") if s.strip()][:2]
        search_parameters["sort_by"] = ",".join([boost_clause] + tail)

    if sku_like:
        search_parameters["prefix"] = "true,true,false,false,false,false,false,false,false,false"
        search_parameters["num_typos"] = "0,0,1,1,1,1,1,1,1,1"

    # Phase 3 — hybrid (keyword + semantic vector) search. Only for real free-text
    # queries (not "*"/SKU browse) and only when the flag + caller opt-in are on.
    # Prepend the auto-embedding vector field so Typesense embeds `q` server-side
    # and rank-fuses keyword + semantic; keep positional param strings aligned.
    target_collection = get_default_collection()
    if (
        cint(use_hybrid)
        and is_hybrid_enabled()
        and query_text not in ("", "*")
        and not sku_like
    ):
        target_collection = get_hybrid_collection()
        search_parameters["query_by"] = "embedding," + search_parameters["query_by"]
        search_parameters["query_by_weights"] = "1," + search_parameters["query_by_weights"]
        search_parameters["exclude_fields"] = "embedding"
        if "prefix" in search_parameters:
            search_parameters["prefix"] = "false," + search_parameters["prefix"]
        if "num_typos" in search_parameters:
            search_parameters["num_typos"] = "0," + search_parameters["num_typos"]

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

    response = client.collections[target_collection].documents.search(
        search_parameters
    )

    stock_reconcile = {
        "freshness_source": "index",
        "checked_count": 0,
        "reconciled_count": 0,
    }
    try:
        from igh_search.igh_search.product_stock_freshness import reconcile_search_hits_stock

        # Reconcile the FULL returned page (single batched SQL via
        # get_authoritative_stock_snapshot + Redis cache), not just the top 8.
        # Lets us drop the redundant per-item stock fan-out in the Next.js
        # proxy and frontend. A positive site_config value still caps it.
        configured_top_n = cint((frappe.conf or {}).get("stock_runtime_reconcile_top_n", 0))
        top_n = configured_top_n if configured_top_n > 0 else len(response.get("hits", []))
        stock_reconcile = reconcile_search_hits_stock(
            response.get("hits", []),
            query=query,
            item_code_hint=item_code_hint,
            top_n=top_n,
        )
        response["hits"] = stock_reconcile.get("hits", response.get("hits", []))
    except Exception:
        frappe.log_error(
            frappe.get_traceback(),
            "IGH Search V2: stock runtime reconcile failed",
        )

    if sort_resolution["should_rerank"]:
        response["hits"] = rank_search_hits(response.get("hits", []), query_text)

    latency_ms = int((time.perf_counter() - started_at) * 1000)
    try:
        from igh_search.igh_search.product_stock_freshness import record_search_latency

        record_search_latency(latency_ms)
    except Exception:
        pass

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
        "latency_ms": latency_ms,
        "stock_reconcile": {
            "freshness_source": stock_reconcile.get("freshness_source", "index"),
            "checked_count": cint(stock_reconcile.get("checked_count") or 0),
            "reconciled_count": cint(stock_reconcile.get("reconciled_count") or 0),
        },
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


# Complementary category map for cross-sell: what naturally sells WITH what. Used
# only to seed companion-category candidates; manual related codes always win.
CROSS_SELL_COMPANION_CATEGORIES = {
    "STRIP LIGHT": ["LED DRIVERS", "ALUMINIUM PROFILE", "PROFILE", "LIGHTING ACCESSORIES"],
    "NEON FLEX": ["LED DRIVERS", "LIGHTING ACCESSORIES"],
    "LED DRIVERS": ["STRIP LIGHT", "NEON FLEX", "LIGHTING ACCESSORIES"],
    "ALUMINIUM PROFILE": ["STRIP LIGHT", "LED DRIVERS", "LIGHTING ACCESSORIES"],
    "TRACK LIGHT": ["LIGHTING ACCESSORIES", "LED DRIVERS"],
    "SPOT LIGHT": ["LED DRIVERS", "LIGHTING ACCESSORIES"],
    "DOWN LIGHT": ["LED DRIVERS", "LIGHTING ACCESSORIES"],
    "PENDANT LIGHT": ["BULBS & LAMPS", "LIGHTING ACCESSORIES"],
    "CHANDELIERS": ["BULBS & LAMPS", "LIGHTING ACCESSORIES"],
}


def get_product_alternatives_v2(item_code, mode="alternatives", limit=8, in_stock_only=1, feature_flag_override=0):
    """Sales-facing alternatives & cross-sell.

    mode="alternatives": closest EQUIVALENT products in the SAME category (for
      "this is out of stock / too expensive, what else fits?"). Semantic neighbours
      via hybrid (when enabled) + spec-closeness, optionally in-stock only.
    mode="cross_sell": COMPLEMENTARY products (driver for a strip, diffuser for a
      profile). Manual related/bought-together codes first, then semantic
      neighbours from companion categories.
    """
    ensure_query_access(feature_flag_override=feature_flag_override)
    source = get_product_document(item_code, include_inactive=1)
    if not source:
        frappe.throw(_("Item not indexed in product_v2"))

    mode = cstr(mode or "alternatives").strip().lower()
    limit = max(min(cint(limit) or 8, 30), 1)
    client = create_typesense_client()
    results = []
    seen = {item_code}

    # Manual relationships always lead (curated by the product team).
    manual_codes = (
        source.get("manual_alternative_codes", []) if mode == "alternatives"
        else source.get("manual_related_codes", [])
    )
    for hit in get_documents_by_codes(manual_codes, include_inactive=0):
        code = hit.get("item_code")
        if code and code not in seen:
            seen.add(code)
            results.append({"reason": "manual", "score": 100, "document": hit})

    # Build the candidate filter by mode.
    filter_clauses = [f'item_code:!={item_code}', "is_active:=1"]
    if cint(in_stock_only) and mode == "alternatives":
        filter_clauses.append("in_stock:=1")

    source_category = cstr(source.get("category_list") or "")
    if mode == "alternatives":
        if source_category:
            filter_clauses.append(f'category_list:="{_escape_filter_value(source_category)}"')
    else:  # cross_sell -> companion categories, never the same category
        companions = CROSS_SELL_COMPANION_CATEGORIES.get(source_category.upper(), [])
        manual_companions = [c for c in companions if c]
        if manual_companions:
            joined = ",".join(f'`{c}`' for c in manual_companions)
            filter_clauses.append(f"category_list:=[{joined}]")
        elif source_category:
            filter_clauses.append(f'category_list:!="{_escape_filter_value(source_category)}"')

    # Semantic candidates: use the source's own descriptive text as the query so
    # hybrid surfaces meaning-similar items; fall back to keyword when hybrid off.
    seed_text = cstr(source.get("searchable_text") or source.get("item_name") or "*")[:400]
    search_params = {
        "q": seed_text,
        "query_by": "searchable_text,item_name,category_list",
        "filter_by": " && ".join(filter_clauses),
        "per_page": max(limit * 5, 25),
        "page": 1,
        "sort_by": "_eval([(in_stock:=1 && rate:>0):2]):desc,_text_match:desc,business_score:desc",
        "include_fields": ",".join(SEARCH_RESULT_FIELDS),
    }
    use_hybrid = (
        is_hybrid_enabled()
        and seed_text not in ("", "*")
    )
    target_collection = get_default_collection()
    if use_hybrid:
        target_collection = get_hybrid_collection()
        search_params["query_by"] = "embedding," + search_params["query_by"]
        search_params["query_by_weights"] = "2,3,2,1"
        search_params["exclude_fields"] = "embedding"

    try:
        response = client.collections[target_collection].documents.search(search_params)
    except Exception:
        # Hybrid hiccup -> fall back to keyword on the main collection.
        search_params.pop("query_by_weights", None)
        search_params.pop("exclude_fields", None)
        search_params["query_by"] = "searchable_text,item_name,category_list"
        response = client.collections[get_default_collection()].documents.search(search_params)

    for hit in response.get("hits", []):
        document = hit.get("document", {})
        code = document.get("item_code")
        if not code or code in seen:
            continue
        if mode == "alternatives":
            score = calculate_similarity_score(source, document)
            if score <= 0:
                continue
            reason = "similar_spec"
        else:
            score = 50 + (10 if cint(document.get("in_stock")) else 0)
            reason = "complementary"
        seen.add(code)
        results.append({"reason": reason, "score": score, "document": document})

    results.sort(key=lambda item: item["score"], reverse=True)
    return {"item_code": item_code, "mode": mode, "results": results[:limit]}


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


def sanitize_sort_by(sort_by, sku_like=False, strict_sort=False):
    value = cstr(sort_by or "").strip()
    if not value:
        return "_text_match:desc,in_stock:desc,business_score:desc"

    parts = value.split(":")
    field_name = SORT_FIELD_ALIASES.get(parts[0], parts[0])
    direction = parts[1] if len(parts) > 1 else "desc"
    if field_name not in SORT_FIELDS or direction not in {"asc", "desc"}:
        return "_text_match:desc,in_stock:desc,business_score:desc"
    if cint(strict_sort):
        return f"{field_name}:{direction},in_stock:desc,business_score:desc"
    return f"_text_match:desc,{field_name}:{direction},in_stock:desc"


def resolve_sort_by(sort_by, sku_like=False, strict_sort=False):
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
        "final_sort": sanitize_sort_by(
            aliased_sort, sku_like=sku_like, strict_sort=strict_sort
        ),
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


def _normalize_filter_list_values(values):
    normalized = []
    for value in values or []:
        if isinstance(value, dict):
            candidate = value.get("value")
            if candidate in (None, ""):
                candidate = value.get("label")
            value = candidate
        if value in (None, ""):
            continue
        normalized.append(value)
    return normalized


def parse_search_filters(filters):
    try:
        parsed = _coerce_json(filters) or {}
    except (TypeError, ValueError, JSONDecodeError):
        log_search_request(
            "invalid_filters",
            {
                "filters": filters,
                "error": "Invalid filters payload supplied to search_products_v2",
            },
        )
        frappe.throw(_("Invalid filters payload for product search V2"))

    if not isinstance(parsed, dict):
        return {}

    legacy_key_map = {
        "input": "input_voltage",
        "output": "output_voltage",
        "current": "output_current",
    }
    for old_key, new_key in legacy_key_map.items():
        if old_key in parsed and new_key not in parsed:
            parsed[new_key] = parsed.pop(old_key)

    for key in list(parsed.keys()):
        value = parsed.get(key)
        if isinstance(value, list):
            parsed[key] = _normalize_filter_list_values(value)

    return parsed


def _build_filter_clause(field_name, value):
    if isinstance(value, list):
        numeric_list_fields = {"is_manufactured_item", "customer_count", "product_star_rating", "total_sold_qty_lifetime"}
        if field_name in numeric_list_fields:
            int_fields = {"is_manufactured_item", "customer_count"}
            if field_name in int_fields:
                nums = [str(cint(item)) for item in value if item not in (None, "")]
            else:
                nums = [str(flt(item)) for item in value if item not in (None, "")]
            joined = ",".join(nums)
            return [f"{field_name}:=[{joined}]"] if joined else []

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
    int_fields = {"customer_count", "is_manufactured_item"}
    caster = cint if field_name in int_fields else flt
    if value.get("min") not in (None, ""):
        clauses.append(f"{field_name}:>={caster(value.get('min'))}")
    if value.get("max") not in (None, ""):
        clauses.append(f"{field_name}:<={caster(value.get('max'))}")
    return clauses

def _build_lumen_clauses(filters):
    clauses = []

    explicit_clause = build_lumen_overlap_filter(
        filters.get("lumen_unit"),
        filters.get("lumen_min"),
        filters.get("lumen_max"),
    )
    if explicit_clause:
        clauses.append(explicit_clause)

    lumen_ranges = filters.get("lumen_ranges")
    if isinstance(lumen_ranges, dict):
        for unit_key, range_data in lumen_ranges.items():
            if not isinstance(range_data, dict):
                continue
            unit_clause = build_lumen_overlap_filter(
                unit_key, range_data.get("min"), range_data.get("max")
            )
            if unit_clause:
                clauses.append(unit_clause)

    if clauses:
        return [f"({' || '.join(clauses)})"]
    return []


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

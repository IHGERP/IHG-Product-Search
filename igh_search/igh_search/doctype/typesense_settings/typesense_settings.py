# Copyright (c) 2025, Aerele and contributors
# For license information, please see license.txt

import copy

import frappe
import typesense
from frappe.custom.doctype.custom_field.custom_field import create_custom_fields
from frappe.model.document import Document
from frappe.utils import cint, cstr, flt

from igh_search.igh_search.product_search_v2 import (
    PRODUCT_V2_COLLECTION,
    build_related_item_map,
    compute_product_v2_document,
    create_sync_log,
    create_typesense_client,
    delete_typesense_documents,
    get_product_v2_schema,
    get_v2_config,
    is_dual_write_enabled,
    sync_typesense_synonyms,
    update_sync_log,
)


product_schema = {
    "name": "product",
    "fields": [
        {"name": "item_code", "type": "string", "infix": True},
        {"name": "item_name", "type": "string", "infix": True},
        {"name": "item_group", "type": "string", "facet": True},
        {"name": "item_description", "type": "string"},
        {"name": "full_description", "type": "string"},
        {"name": "stock_uom", "type": "string"},
        {"name": "website_image_url", "type": "string"},
        {"name": "sold_last_30_days", "type": "float"},
        {"name": "offer_rate", "type": "float", "facet": True},
        {"name": "brand", "type": "string", "facet": True},
        {"name": "rate", "type": "float", "facet": True},
        {"name": "best_selling", "type": "int32", "facet": True},
        {"name": "hot_product", "type": "int32", "facet": True},
        {"name": "is_bundle_item", "type": "int32", "facet": True},
        {"name": "popular_product", "type": "int32", "facet": True},
        {"name": "frequently_bought_together", "type": "string"},
        {"name": "has_variants", "type": "int32", "facet": True},
        {"name": "stock", "type": "float", "facet": True},
        {"name": "inventory_value", "type": "float"},
        {"name": "product_type", "type": "string", "facet": True},
        {"name": "category_list", "type": "string", "facet": True},
        {"name": "beam_angle", "type": "string", "facet": True},
        {"name": "lumen_output", "type": "string", "facet": True},
        {"name": "mounting", "type": "string", "facet": True},
        {"name": "ip_rate", "type": "string", "facet": True},
        {"name": "lamp_type", "type": "string", "facet": True},
        {"name": "power", "type": "string", "facet": True},
        {"name": "input", "type": "string", "facet": True},
        {"name": "dimension", "type": "string", "facet": True},
        {"name": "material", "type": "string", "facet": True},
        {"name": "body_finish", "type": "string", "facet": True},
        {"name": "warranty_", "type": "string", "facet": True},
        {"name": "output_voltage", "type": "string", "facet": True},
        {"name": "output_current", "type": "string", "facet": True},
        {"name": "color_temp_", "type": "string", "facet": True},
        {"name": "promotion_item", "type": "int32", "facet": True},
        {"name": "new_arrival", "type": "int32", "facet": True},
        {"name": "creation", "type": "string", "facet": True},
        {"name": "creation_on", "type": "float", "facet": True},
        {"name": "barcode", "type": "string"},
        {"name": "last_sold", "type": "int32", "facet": True},
        {"name": "last_brought", "type": "int32", "facet": True},
        {"name": "discount_percentage", "type": "float", "facet": True},
    ],
}


class TypesenseSettings(Document):
    pass


BASE_PRICE_LIST = "RRP"
PROMO_PRICE_LIST = "Promo"
ITEM_EXTRA_FIELD_MAP = {
    "series": "series",
    "image": "image",
    "is_stock_item": "is_stock_item",
    "height": "height",
    "width": "width",
    "depth": "depth",
    "custom_moq": "custom_moq",
    "range": "range",
    "lamp_qty": "lamp_qty",
    "safety_class": "safety_class",
    "eec": "eec",
    "reflector": "reflector",
    "att_heat_sink": "att_heat_sink",
    "output_signal": "output_signal",
    "power_factor": "power_factor",
    "working_temp": "working_temp",
    "life_time": "life_time",
    "light_intensity": "light_intensity",
    "light_source": "light_source",
    "cri": "cri",
    "efficacy": "efficacy",
    "operating_frequency": "operating_frequency",
    "input_signal": "input_signal",
    "function": "function",
    "cut_out": "cut_out",
    "shade_material": "shade_material",
    "shade_finish": "shade_finish",
    "pole_dimension": "pole_dimension",
    "suspended_length": "suspended_length",
    "warranty_type": "warranty_type_",
    "warranty_in_yrs": "warranty_in_yrs",
    "diffuser": "diffuser",
    "custom_esma_certified": "custom_esma_certified",
    "primary_material": "primary_material",
    "secondary_material": "secondary_material",
    "capacity": "capacity",
    "country_of_orgin": "country_of_orgin",
    "number_of_pieces": "_number_of_pieces",
    "leather_finish": "leather_finish",
    "fabric_finish": "fabric_finish",
    "primary_color": "primary_color",
    "secondary_color": "secondary_color",
    "remarks": "remarks",
}
ITEM_MULTISELECT_FIELDS = (
    "bought_together",
    "similar_range",
    "related_products",
    "accessories",
    "must_use",
)


def calculate_inventory_value(stock=None, rate=None):
    return flt(stock) * flt(rate)


def create_client():
    return create_typesense_client()


def sync_items_to_typesense(client):
    log_name = create_sync_log(
        trigger_type="full_sync",
        source_doctype="Typesense Settings",
        source_docname="Typesense Settings",
        collection_name=_get_sync_collections_label(),
        item_codes=[],
    )
    frappe.enqueue(
        get_product_schema_data_qr_job,
        timeout=10000,
        queue="long",
        job_name="get_product_schema_data",
        enqueue_after_commit=True,
        log_name=log_name,
    )


def get_product_schema_data_qr_job(client=None, retry_count=0, log_name=None):
    client = client or create_client()
    update_sync_log(log_name, "Running", retry_count=retry_count, started=True)

    try:
        collections = [(product_schema["name"], product_schema)]
        if is_dual_write_enabled():
            collections.append((PRODUCT_V2_COLLECTION, get_product_v2_schema()))

        for collection_name, schema in collections:
            recreate_collection(client, collection_name, schema)

        payload = get_product_schema_data(version="both")
        import_documents_in_batches(client, product_schema["name"], payload["v1"])
        if is_dual_write_enabled():
            import_documents_in_batches(client, PRODUCT_V2_COLLECTION, payload["v2"])
            sync_typesense_synonyms(client, PRODUCT_V2_COLLECTION)

        frappe.db.set_value(
            "Typesense Settings", "Typesense Settings", "is_sync", 0, update_modified=False
        )
        update_sync_log(log_name, "Success", retry_count=retry_count, finished=True)
    except Exception:
        frappe.db.set_value(
            "Typesense Settings", "Typesense Settings", "is_sync", 0, update_modified=False
        )
        _retry_job(
            job_method=get_product_schema_data_qr_job,
            job_kwargs={"log_name": log_name},
            retry_count=retry_count,
            log_name=log_name,
            title="get_product_schema_data_qr_job",
        )


@frappe.whitelist()
def initialize_syncing_item_group(self, method):
    initialize_syncing_items()


@frappe.whitelist()
def initialize_syncing_items():
    frappe.db.set_value(
        "Typesense Settings", "Typesense Settings", "is_sync", 1, update_modified=False
    )
    client = create_client()
    sync_items_to_typesense(client)


def item_custom_fields():
    field = {
        "Item": [
            dict(
                label="Best Selling",
                fieldname="best_selling",
                fieldtype="Check",
                insert_after="new_arrival",
            ),
            dict(
                label="Hot Product",
                fieldname="hot_product",
                fieldtype="Check",
                insert_after="best_selling",
            ),
            dict(
                label="Popular Product",
                fieldname="popular_product",
                fieldtype="Check",
                insert_after="hot_product",
            ),
            dict(
                label="Is Bundle Item",
                fieldname="is_bundle_item",
                fieldtype="Check",
                insert_after="popular_product",
            ),
        ]
    }
    create_custom_fields(field)


def get_product_schema_data(item_code=None, version="v1"):
    rows = fetch_item_base_data(item_code)
    related_map = build_related_item_map([row["item_code"] for row in rows]) if rows else {}
    v1_payload = build_v1_documents(rows)
    v2_payload = [compute_product_v2_document(row, related_map=related_map) for row in rows]

    if version == "v1":
        return v1_payload
    if version == "v2":
        return v2_payload
    if version == "both":
        return {"v1": v1_payload, "v2": v2_payload}

    frappe.throw("Unsupported product schema version")


def fetch_item_base_data(item_codes=None):
    company = frappe.db.get_value("E Commerce Settings", "E Commerce Settings", "company")
    item_codes = normalize_item_codes(item_codes)
    item_code_filter_item = get_item_filter_sql(item_codes, alias="it")
    item_code_filter_price = get_item_filter_sql(item_codes, alias="i")

    item_price_list_data = get_item_wise__price_list(
        BASE_PRICE_LIST, PROMO_PRICE_LIST, item_code=item_code_filter_price
    )
    sold_last_30_days = get_wise_sold_last_30_days(company, item_code=item_code_filter_price)
    item_wise_stock = get_item_wise_stock(company, item_code=item_code_filter_price)

    rows = frappe.db.sql(
        f"""
        SELECT
            it.name AS item_code,
            it.name AS id,
            COALESCE(it.item_name, "") AS item_name,
            COALESCE(it.item_group, "") AS item_group,
            COALESCE(ig.disable, 0) AS item_group_disabled,
            COALESCE(it.disabled, 0) AS disabled,
            COALESCE(it.variant_of, "") AS variant_of,
            COALESCE(parent.item_name, "") AS parent_item_name,
            COALESCE(it.has_variants, 0) AS has_variants,
            COALESCE(it.best_selling, 0) AS best_selling,
            COALESCE(it.hot_product, 0) AS hot_product,
            COALESCE(it.popular_product, 0) AS popular_product,
            COALESCE(it.custom_is_bundle_item, 0) AS is_bundle_item,
            COALESCE(it.short_descrition, "") AS item_description,
            COALESCE(it.description, "") AS full_description,
            COALESCE(it.stock_uom, "") AS stock_uom,
            COALESCE(it.product_type, "") AS product_type,
            COALESCE(it.category_list, "") AS category_list,
            COALESCE(it.beam_angle, "") AS beam_angle,
            COALESCE(it.lumen_output, "") AS lumen_output,
            COALESCE(it.mounting, "") AS mounting,
            COALESCE(it.ip_rate, "") AS ip_rate,
            COALESCE(it.lamp_type, "") AS lamp_type,
            COALESCE(it.power, "") AS power,
            COALESCE(it.input, "") AS input,
            COALESCE(it.dimension, "") AS dimension,
            COALESCE(it.material, "") AS material,
            COALESCE(it.body_finish, "") AS body_finish,
            COALESCE(CAST(it.warranty_ AS CHAR), "") AS warranty_,
            COALESCE(it.output_voltage, "") AS output_voltage,
            COALESCE(it.output_current, "") AS output_current,
            COALESCE(it.color_temp_, "") AS color_temp_,
            COALESCE(it.image, "") AS website_image_url,
            COALESCE(it.brand, "") AS brand,
            COALESCE(it.new_arrival, 0) AS new_arrival,
            COALESCE(it.promotion_item, 0) AS promotion_item,
            DATE_FORMAT(it.creation, '%Y-%m-%d') AS creation,
            it.creation AS creation_raw,
            it.modified AS modified,
            COALESCE(
                (
                    SELECT GROUP_CONCAT(b.barcode ORDER BY b.barcode SEPARATOR ', ')
                    FROM `tabItem Barcode` AS b
                    WHERE b.parent = it.name
                ),
                ""
            ) AS barcode,
            COALESCE(
                (
                    SELECT DATEDIFF(CURDATE(), si.posting_date)
                    FROM `tabSales Invoice` AS si
                    JOIN `tabSales Invoice Item` AS sii ON sii.parent = si.name
                    WHERE si.docstatus = 1
                        AND sii.item_code = it.name
                        AND si.is_return = 0
                    ORDER BY si.posting_date DESC
                    LIMIT 1
                ),
                -1
            ) AS last_sold,
            COALESCE(
                (
                    SELECT DATEDIFF(CURDATE(), pr.posting_date)
                    FROM `tabPurchase Receipt` AS pr
                    JOIN `tabPurchase Receipt Item` AS pri ON pri.parent = pr.name
                    WHERE pr.docstatus = 1
                        AND pri.item_code = it.name
                        AND pr.is_return = 0
                    ORDER BY pr.posting_date DESC
                    LIMIT 1
                ),
                -1
            ) AS last_brought
        FROM `tabItem` AS it
        LEFT JOIN `tabItem Group` AS ig ON it.item_group = ig.name
        LEFT JOIN `tabItem` AS parent ON parent.name = it.variant_of
        WHERE 1 = 1
        {item_code_filter_item}
        """,
        as_dict=1,
    )

    for row in rows:
        rate_offer_rate = item_price_list_data.get(row["item_code"], {})
        row["rate"] = rate_offer_rate.get("price_list_rate") or 0
        row["offer_rate"] = rate_offer_rate.get("offer_rate") or 0
        row["discount_percentage"] = 0
        if row["rate"] and row["offer_rate"] and row["rate"] > row["offer_rate"]:
            row["discount_percentage"] = round(
                ((row["rate"] - row["offer_rate"]) / row["rate"]) * 100, 2
            )
        row["sold_last_30_days"] = sold_last_30_days.get(row["item_code"]) or 0
        row["stock"] = item_wise_stock.get(row["item_code"]) or 0
        row["inventory_value"] = calculate_inventory_value(row.get("stock"), row.get("rate"))
        row["frequently_bought_together"] = ""

    enrich_rows_with_item_metadata(rows)
    return rows


def build_v1_documents(rows):
    payload = []
    for row in rows:
        if cint(row.get("disabled")) or cint(row.get("item_group_disabled")):
            continue
        value = copy.deepcopy(row)
        value["creation_on"] = _to_timestamp(value.get("creation_raw"))
        payload.append(
            {
                "item_code": value["item_code"],
                "id": value["id"],
                "item_name": value["item_name"],
                "item_group": value["item_group"],
                "has_variants": cint(value["has_variants"]),
                "best_selling": cint(value["best_selling"]),
                "hot_product": cint(value["hot_product"]),
                "popular_product": cint(value["popular_product"]),
                "is_bundle_item": cint(value["is_bundle_item"]),
                "item_description": value["item_description"],
                "full_description": value["full_description"],
                "stock_uom": value["stock_uom"],
                "product_type": value["product_type"],
                "category_list": value["category_list"],
                "beam_angle": value["beam_angle"],
                "lumen_output": value["lumen_output"],
                "mounting": value["mounting"],
                "ip_rate": value["ip_rate"],
                "lamp_type": value["lamp_type"],
                "power": value["power"],
                "input": value["input"],
                "dimension": value["dimension"],
                "material": value["material"],
                "body_finish": value["body_finish"],
                "warranty_": value["warranty_"],
                "output_voltage": value["output_voltage"],
                "output_current": value["output_current"],
                "color_temp_": value["color_temp_"],
                "website_image_url": value["website_image_url"],
                "brand": value["brand"],
                "new_arrival": cint(value["new_arrival"]),
                "creation": value["creation"],
                "creation_on": value["creation_on"],
                "promotion_item": cint(value["promotion_item"]),
                "frequently_bought_together": value["frequently_bought_together"],
                "barcode": value["barcode"],
                "last_sold": cint(value["last_sold"]),
                "last_brought": cint(value["last_brought"]),
                "rate": flt(value["rate"]),
                "offer_rate": flt(value["offer_rate"]),
                "discount_percentage": flt(value["discount_percentage"]),
                "sold_last_30_days": flt(value["sold_last_30_days"]),
                "stock": flt(value["stock"]),
                "inventory_value": flt(value["inventory_value"]),
            }
        )
    return payload


def enrich_rows_with_item_metadata(rows):
    item_codes = [row.get("item_code") for row in rows if row.get("item_code")]
    if not item_codes:
        return

    scalar_values = get_item_scalar_field_values(item_codes)
    multiselect_values = get_item_multiselect_field_values(item_codes)

    for row in rows:
        item_code = row.get("item_code")
        row.update(scalar_values.get(item_code, {}))
        row.update(multiselect_values.get(item_code, {}))
        row["image"] = row.get("image") or row.get("website_image_url") or ""
        row["description"] = (
            row.get("description")
            or row.get("full_description")
            or row.get("item_description")
            or ""
        )
        row["input_voltage"] = row.get("input_voltage") or row.get("input") or ""
        row["warranty"] = row.get("warranty") or row.get("warranty_") or ""
        row["warranty_type"] = row.get("warranty_type") or row.get("warranty_type_") or ""
        row["color_temp"] = row.get("color_temp") or row.get("color_temp_") or ""


def get_item_scalar_field_values(item_codes):
    available_columns = set(frappe.db.get_table_columns("Item"))
    fields = ["name"]
    alias_map = {}

    for target_field, source_field in ITEM_EXTRA_FIELD_MAP.items():
        if source_field in available_columns:
            fields.append(source_field)
            alias_map[target_field] = source_field

    if len(fields) == 1:
        return {}

    item_rows = frappe.get_all(
        "Item",
        filters={"name": ["in", item_codes]},
        fields=fields,
        limit_page_length=len(item_codes),
    )
    return {
        item_row["name"]: {
            target_field: item_row.get(source_field)
            for target_field, source_field in alias_map.items()
        }
        for item_row in item_rows
    }


def get_item_multiselect_field_values(item_codes):
    item_meta = frappe.get_meta("Item")
    values_by_item = {
        item_code: {fieldname: [] for fieldname in ITEM_MULTISELECT_FIELDS}
        for item_code in item_codes
    }

    for fieldname in ITEM_MULTISELECT_FIELDS:
        docfield = item_meta.get_field(fieldname)
        if not docfield or docfield.fieldtype != "Table MultiSelect" or not docfield.options:
            continue

        child_doctype = docfield.options
        if not frappe.db.table_exists(child_doctype):
            continue

        value_field = get_multiselect_value_field(child_doctype)
        if not value_field:
            continue

        child_rows = frappe.get_all(
            child_doctype,
            filters={
                "parent": ["in", item_codes],
                "parenttype": "Item",
                "parentfield": fieldname,
            },
            fields=["parent", value_field],
            order_by="idx asc",
            limit_page_length=0,
        )
        for child_row in child_rows:
            value = child_row.get(value_field)
            if value:
                values_by_item.setdefault(
                    child_row["parent"],
                    {name: [] for name in ITEM_MULTISELECT_FIELDS},
                )[fieldname].append(value)

    return values_by_item


def get_multiselect_value_field(child_doctype):
    child_columns = set(frappe.db.get_table_columns(child_doctype))
    if "link_name" in child_columns:
        return "link_name"

    child_meta = frappe.get_meta(child_doctype)
    ignored_fields = {
        "name",
        "parent",
        "parentfield",
        "parenttype",
        "idx",
        "doctype",
        "owner",
        "modified_by",
        "creation",
        "modified",
        "docstatus",
    }
    for docfield in child_meta.fields:
        if docfield.fieldname in ignored_fields:
            continue
        if docfield.fieldtype in {"Link", "Dynamic Link", "Data", "Autocomplete"}:
            return docfield.fieldname

    return None


def get_item_wise__price_list(price_list, offer_price_list, item_code=""):
    item_price_list_data = frappe.db.sql(
        f"""
        SELECT DISTINCT
            i.name AS id,
            ip.price_list_rate AS price_list_rate,
            ip2.price_list_rate AS offer_rate
        FROM `tabItem` i
        LEFT JOIN `tabItem Price` AS ip ON (
            i.name = ip.item_code
            AND ip.price_list = '{price_list}'
            AND ip.selling = 1
            AND IF(ip.valid_from, IF(ip.valid_from <= CURDATE(), 1, 0), 1) = 1
            AND IF(ip.valid_upto, IF(ip.valid_upto >= CURDATE(), 1, 0), 1) = 1
        )
        LEFT JOIN `tabItem Price` AS ip2 ON (
            i.name = ip2.item_code
            AND ip2.price_list = '{offer_price_list}'
            AND ip2.selling = 1
            AND IF(ip2.valid_from, IF(ip2.valid_from <= CURDATE(), 1, 0), 1) = 1
            AND IF(ip2.valid_upto, IF(ip2.valid_upto >= CURDATE(), 1, 0), 1) = 1
        )
        WHERE 1 = 1
        {item_code}
        GROUP BY i.name
        """,
        as_dict=1,
    )
    return {
        item["id"]: {
            "price_list_rate": item["price_list_rate"],
            "offer_rate": item["offer_rate"],
        }
        for item in item_price_list_data
    }


def get_wise_sold_last_30_days(company, item_code=""):
    sold_last_30_days = frappe.db.sql(
        f"""
        SELECT DISTINCT sii.item_code AS id, SUM(sii.stock_qty) AS sold_qty
        FROM `tabSales Invoice` AS si
        JOIN `tabSales Invoice Item` AS sii ON sii.parent = si.name
        JOIN `tabItem` AS i ON i.name = sii.item_code
        WHERE si.docstatus = 1
            AND si.is_return = 0
            AND si.posting_date BETWEEN (CURDATE() - INTERVAL 30 DAY) AND CURDATE()
            {item_code}
        GROUP BY sii.item_code
        """,
        as_dict=1,
    )
    return {item["id"]: item["sold_qty"] for item in sold_last_30_days}


def get_item_wise_stock(company, item_code=""):
    item_wise_stock = frappe.db.sql(
        f"""
        SELECT DISTINCT bin.item_code AS id, COALESCE(SUM(bin.actual_qty), 0) AS stock
        FROM `tabBin` AS bin
        JOIN `tabItem` AS i ON i.name = bin.item_code
        JOIN `tabWarehouse` AS warehouse ON bin.warehouse = warehouse.name
        WHERE LOWER(warehouse.name) NOT LIKE '%damage%'
            AND LOWER(warehouse.name) NOT LIKE '%missing%'
            {item_code}
        GROUP BY bin.item_code
        """,
        as_dict=1,
    )
    return {item["id"]: item["stock"] for item in item_wise_stock}


def update_value_item_wise(updated_data, client=None, collection_name=None):
    if not updated_data:
        return
    client = client or create_client()
    collection_name = collection_name or product_schema["name"]
    client.collections[collection_name].documents.import_(updated_data, {"action": "upsert"})


def update_product_schema_data(self, method):
    if self.get("doctype") == "Item Price" and not cint(self.get("selling") or 0):
        return

    self_data = extract_doc_event_payload(self, method)
    log_name = create_sync_log(
        trigger_type=f"incremental:{self_data.get('doctype')}:{method}",
        source_doctype=self_data.get("doctype"),
        source_docname=self_data.get("name"),
        collection_name=_get_sync_collections_label(),
        item_codes=get_affected_item_codes(self_data),
    )
    frappe.enqueue(
        update_product_schema_data_qr_job,
        timeout=10000,
        queue="long",
        job_name="update_product_schema_data_qr_job",
        enqueue_after_commit=True,
        self_data=self_data,
        retry_count=0,
        log_name=log_name,
    )


def extract_doc_event_payload(doc, method):
    payload = doc.as_dict()
    payload["event_method"] = method
    if payload.get("doctype") == "Item":
        previous_doc = None
        if hasattr(doc, "get_doc_before_save"):
            previous_doc = doc.get_doc_before_save()
        if previous_doc:
            payload["_previous_variant_of"] = previous_doc.get("variant_of")
            payload["_previous_item_group"] = previous_doc.get("item_group")
    return payload


def get_item_codes_for_typesense_update(self_data):
    return get_affected_item_codes(self_data)


def get_affected_item_codes(self_data):
    doctype = self_data.get("doctype")
    affected = set()

    if doctype == "Item":
        item_code = self_data.get("item_code") or self_data.get("name")
        if item_code:
            affected.add(item_code)
            affected.update(get_variant_codes_for_parent(item_code))
        for parent_code in (
            self_data.get("variant_of"),
            self_data.get("_previous_variant_of"),
        ):
            if parent_code:
                affected.add(parent_code)
                affected.update(get_variant_codes_for_parent(parent_code))
    elif doctype == "Item Price":
        if self_data.get("item_code"):
            affected.add(self_data.get("item_code"))
    elif doctype == "Related Items":
        affected.update([self_data.get("item_1"), self_data.get("item_2")])
    elif doctype == "Item Group":
        group_name = self_data.get("name")
        if group_name:
            affected.update(
                frappe.get_all("Item", filters={"item_group": group_name}, pluck="name")
            )

    for row in self_data.get("items") or []:
        if row.get("item_code"):
            affected.add(row.get("item_code"))

    return sorted(code for code in affected if code)


def get_variant_codes_for_parent(parent_code):
    if not parent_code:
        return []
    return frappe.get_all("Item", filters={"variant_of": parent_code}, pluck="name")


def update_product_schema_data_qr_job(self_data, retry_count=0, log_name=None):
    update_sync_log(log_name, "Running", retry_count=retry_count, started=True)
    client = create_client()

    try:
        doctype = self_data.get("doctype")
        event_method = self_data.get("event_method")
        affected_item_codes = get_affected_item_codes(self_data)
        if doctype == "Item" and event_method == "on_trash":
            deleted_code = self_data.get("item_code") or self_data.get("name")
            delete_typesense_documents(client, product_schema["name"], [deleted_code])
            if is_dual_write_enabled():
                delete_typesense_documents(client, PRODUCT_V2_COLLECTION, [deleted_code])
            if affected_item_codes:
                affected_item_codes = [
                    item_code for item_code in affected_item_codes if item_code != deleted_code
                ]

        payload = get_product_schema_data(affected_item_codes, version="both")
        active_v1_codes = {doc["item_code"] for doc in payload["v1"]}
        missing_v1_codes = [
            item_code for item_code in affected_item_codes if item_code not in active_v1_codes
        ]
        if missing_v1_codes:
            delete_typesense_documents(client, product_schema["name"], missing_v1_codes)

        update_value_item_wise(payload["v1"], client=client, collection_name=product_schema["name"])

        if is_dual_write_enabled():
            active_v2_codes = {doc["item_code"] for doc in payload["v2"]}
            if doctype == "Item" and event_method == "on_trash":
                delete_typesense_documents(
                    client,
                    PRODUCT_V2_COLLECTION,
                    [item_code for item_code in affected_item_codes if item_code not in active_v2_codes],
                )
            update_value_item_wise(
                payload["v2"], client=client, collection_name=PRODUCT_V2_COLLECTION
            )

        update_sync_log(log_name, "Success", retry_count=retry_count, finished=True)
    except Exception:
        _retry_job(
            job_method=update_product_schema_data_qr_job,
            job_kwargs={"self_data": self_data, "log_name": log_name},
            retry_count=retry_count,
            log_name=log_name,
            title="update_product_schema_data_qr_job",
        )


def recreate_collection(client, collection_name, schema):
    try:
        client.collections[collection_name].delete()
    except typesense.exceptions.ObjectNotFound:
        pass
    client.collections.create(schema)


def import_documents_in_batches(client, collection_name, documents, batch_size=5000):
    for index in range(0, len(documents), batch_size):
        batch = documents[index : index + batch_size]
        if batch:
            client.collections[collection_name].documents.import_(batch, {"action": "upsert"})


def normalize_item_codes(item_codes):
    if not item_codes:
        return []
    if isinstance(item_codes, str):
        return [item_codes]
    if isinstance(item_codes, tuple):
        return list(item_codes)
    return [item_code for item_code in item_codes if item_code]


def get_item_filter_sql(item_codes, alias):
    item_codes = normalize_item_codes(item_codes)
    if not item_codes:
        return ""
    if len(item_codes) == 1:
        return f" AND {alias}.name = {frappe.db.escape(item_codes[0])}"
    values = ", ".join(frappe.db.escape(item_code) for item_code in item_codes)
    return f" AND {alias}.name IN ({values})"


def _retry_job(job_method, job_kwargs, retry_count, log_name, title):
    max_retry_count = get_v2_config()["max_retry_count"]
    if retry_count < max_retry_count:
        update_sync_log(log_name, "Retrying", retry_count=retry_count + 1)
        frappe.enqueue(
            job_method,
            timeout=10000,
            queue="long",
            job_name=title,
            retry_count=retry_count + 1,
            **job_kwargs,
        )
        return

    update_sync_log(log_name, "Dead Letter", retry_count=retry_count, failure_reason=frappe.get_traceback(), finished=True)
    frappe.log_error(title=title, message=frappe.get_traceback())


def _to_timestamp(value):
    if not value:
        return 0
    return int(frappe.utils.get_datetime(value).timestamp())


def _get_sync_collections_label():
    return "product,product_v2" if is_dual_write_enabled() else "product"

# Copyright (c) 2025, Aerele and contributors
# For license information, please see license.txt

import frappe
import typesense
from frappe.model.document import Document
from frappe.custom.doctype.custom_field.custom_field import create_custom_fields
from datetime import datetime

product_schema = {
    "name": "product",
    "fields": [
        {"name": "item_code", "type": "string"},
        {"name": "item_name", "type": "string"},
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


def create_client():
    client_details = frappe.get_doc("Typesense Settings")
    client = typesense.Client(
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

    return client


def sync_items_to_typesense(client):
    try:
        client.collections["product"].delete()
    except typesense.exceptions.ObjectNotFound as e:
        if e.args[0] == 404:
            frappe.msgprint(
                "Collection 'product' does not exist. Creating a new one..."
            )
        else:
            frappe.db.set_value(
                "Typesense Settings", "Typesense Settings", "is_sync", 0
            )
            raise e
    frappe.enqueue(
        get_product_schema_data_qr_job,
        timeout=10000,
        queue="long",
        job_name="get_product_schema_data",
    )


def get_product_schema_data_qr_job(client=None):
    try:
        if not client:
            client = create_client()
        client.collections.create(product_schema)
        transfer_items = get_product_schema_data()
        BATCH_SIZE = 5000  # Adjust as needed
        for i in range(0, len(transfer_items), BATCH_SIZE):
            batch = transfer_items[i : i + BATCH_SIZE]
            client.collections["product"].documents.import_(batch, {"action": "upsert"})

    except Exception:
        frappe.db.set_value("Typesense Settings", "Typesense Settings", "is_sync", 0)
        frappe.log_error(
            title="get_product_schema_data_qr_job", message=frappe.get_traceback()
        )


@frappe.whitelist()
def initialize_syncing_items():
    frappe.db.set_value("Typesense Settings", "Typesense Settings", "is_sync", 1)
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


def get_product_schema_data(item_code=None):
    company, price_list = frappe.db.get_value(
        "E Commerce Settings", "E Commerce Settings", ["company", "price_list"]
    )
    item_code_main_filter = ""
    item_code_filter = ""
    if item_code:
        if str(type(item_code)) == "<class 'list'>" and len(item_code) == 1:
            item_code = item_code[0]
            item_code_filter = f" And i.name = '{item_code}' "
            item_code_main_filter = f" And it.name = '{item_code}'"
        else:
            item_code = tuple(item_code)
            item_code_filter = f" And i.name in {item_code} "
            item_code_main_filter = f" And it.name in {item_code}"
    offer_price_list = "Promo"
    item_price_list_data = get_item_wise__price_list(
        price_list, offer_price_list, item_code=item_code_filter
    )
    sold_last_30_days = get_wise_sold_last_30_days(company, item_code=item_code_filter)
    item_wise_stock = get_item_wise_stock(company, item_code=item_code_filter)

    data = frappe.db.sql(
        f""" SELECT it.name as item_code ,
	it.name as id,
	COALESCE(it.item_name,"") as item_name,
	COALESCE(it.item_group, '') AS item_group,
	it.has_variants,
	it.best_selling,
	it.hot_product,
	it.popular_product,
	it.is_bundle_item,
	COALESCE(it.short_descrition,"") AS item_description,
    COALESCE(it.description,"") AS full_description,
	COALESCE(it.stock_uom,"") as stock_uom,
	COALESCE(it.product_type, '') AS product_type,
	COALESCE(it.category_list, '') AS category_list,
	COALESCE(it.beam_angle, '') AS beam_angle,
	COALESCE(it.lumen_output, '') AS lumen_output,
	COALESCE(it.mounting, '') AS mounting,
	COALESCE(it.ip_rate, '') AS ip_rate,
	COALESCE(it.lamp_type, '') AS lamp_type,
	COALESCE(it.power, '') AS power,
	COALESCE(it.input, '') AS input,
	COALESCE(it.dimension, '') AS dimension,
	COALESCE(it.material, '') AS material,
	COALESCE(it.body_finish, '') AS body_finish,
	COALESCE(CAST(it.warranty_ AS CHAR), '') AS warranty_,
	COALESCE(it.output_voltage, '') AS output_voltage,
	COALESCE(it.output_current, '') AS output_current,
	COALESCE(it.color_temp_, '') AS color_temp_,
	COALESCE(it.image, '') AS website_image_url,
	COALESCE(it.brand, '') AS brand,
	it.new_arrival,
	DATE_FORMAT(it.creation, '%Y-%m-%d') AS creation,
    it.creation  AS creation_on,
	it.promotion_item,
	"" AS frequently_bought_together,
	(COALESCE(
		   (SELECT GROUP_CONCAT(b.barcode ORDER BY b.barcode SEPARATOR ', ') 
			FROM `tabItem Barcode` AS b 
			WHERE b.parent = it.name),
		   ""
	   ) )AS barcode,
    COALESCE( 
    (SELECT DATEDIFF(CURDATE(), si.posting_date) AS last_sold
     FROM `tabSales Invoice` AS si
     JOIN `tabSales Invoice Item` AS sii ON sii.parent = si.name
     WHERE si.docstatus = 1
       AND sii.item_code = it.name
       AND si.is_return = 0
     ORDER BY si.posting_date DESC 
     LIMIT 1
    ), 
-1 ) AS last_sold,
    COALESCE( 
    (SELECT DATEDIFF(CURDATE(), si.posting_date) AS last_brought
     FROM `tabPurchase Receipt` AS si
     JOIN `tabPurchase Receipt Item` AS sii ON sii.parent = si.name
     WHERE si.docstatus = 1
       AND sii.item_code = it.name
       AND si.is_return = 0
     ORDER BY si.posting_date DESC 
     LIMIT 1
    ), 
-1 ) AS last_brought
    
From `tabItem` AS it
where it.disabled =0
{item_code_main_filter}
	""",
        as_dict=1,
    )
    for value in data:
        creation_on = datetime.strptime(
            str(value["creation_on"]), "%Y-%m-%d %H:%M:%S.%f"
        )
        creation_on = round(creation_on.timestamp(), 2)
        value["creation_on"] = creation_on
        rate_offer_rate = item_price_list_data.get(value["item_code"])
        value["rate"] = 0
        value["offer_rate"] = 0
        value["discount_percentage"] = 0
        if rate_offer_rate:
            value["rate"] = rate_offer_rate.get("price_list_rate") or 0
            value["offer_rate"] = rate_offer_rate.get("offer_rate") or 0
            if (
                value["rate"]
                and value["offer_rate"]
                and value["rate"] > value["offer_rate"]
            ):
                value["discount_percentage"] = round(
                    ((value["rate"] - value["offer_rate"]) / value["rate"]) * 100, 2
                )
        value["sold_last_30_days"] = sold_last_30_days.get(value["item_code"]) or 0
        value["stock"] = item_wise_stock.get(value["item_code"]) or 0
        if value.get("frequently_bought_together"):
            value["frequently_bought_together"] = str(
                value["frequently_bought_together"]
            )
        else:
            value["frequently_bought_together"] = ""

    return data


def get_item_wise__price_list(price_list, offer_price_list, item_code=""):
    item_price_list_data = frappe.db.sql(
        f""" SELECT DISTINCT 
            i.name as id,                                                                                                                                                                
            ip.price_list_rate as price_list_rate,                                                                                                                                                          
            ip2.price_list_rate as offer_rate                                                                                                                                                               
      FROM `tabItem` i                                                                                                                                                                                
      left join `tabItem Price` as ip on (i.name = ip.item_code and ip.price_list = '{price_list}' and ip.selling = 1 and IF(ip.valid_from, IF(ip.valid_from <= CURDATE(), 1, 0), 1) = 1 and IF(ip.valid_upto, IF(ip.valid_upto >= CURDATE(), 1, 0), 1) = 1)                                                                                                                                                             
      left join `tabItem Price` as ip2 on (i.name = ip2.item_code and ip2.price_list = '{offer_price_list}' and ip2.selling = 1 and IF(ip2.valid_from, IF(ip2.valid_from <= CURDATE(), 1, 0), 1) = 1 and IF(ip2.valid_upto, IF(ip2.valid_upto >= CURDATE(), 1, 0), 1) = 1)                                                                                                                                                   
      where
      i.disabled = 0
      {item_code}                                                                                                                                                                              
       GROUP BY i.name """,
        as_dict=1,
    )
    item_price_list_data = {
        item["id"]: {
            "price_list_rate": item["price_list_rate"],
            "offer_rate": item["offer_rate"],
        }
        for item in item_price_list_data
    }
    return item_price_list_data


def get_wise_sold_last_30_days(company, item_code=""):
    sold_last_30_days = frappe.db.sql(
        f"""
				SELECT DISTINCT sii.item_code as id ,SUM(sii.stock_qty) as sold_qty
				FROM `tabSales Invoice` AS si
				JOIN `tabSales Invoice Item` AS sii ON sii.parent = si.name
				join `tabItem` as i on i.name = sii.item_code
				WHERE si.docstatus = 1
				AND si.is_return = 0
				AND si.posting_date BETWEEN (CURDATE() - INTERVAL 30 DAY) AND CURDATE()
				AND si.company = '{company}'
				and i.disabled = 0
                {item_code}
				GROUP BY sii.item_code
			""",
        as_dict=1,
    )
    sold_last_30_days = {item["id"]: item["sold_qty"] for item in sold_last_30_days}
    return sold_last_30_days


def get_item_wise_stock(company, item_code=""):
    item_wise_stock = frappe.db.sql(
        f"""
				SELECT DISTINCT bin.item_code as id , COALESCE(SUM(bin.actual_qty),0)  as stock
				FROM `tabBin` AS bin
				join `tabItem` as i on i.name = bin.item_code
				JOIN `tabWarehouse` AS warehouse ON bin.warehouse = warehouse.name
				WHERE warehouse.company = '{company}'
				and i.disabled = 0
				{item_code}
				GROUP BY bin.item_code
			""",
        as_dict=1,
    )
    item_wise_stock = {item["id"]: item["stock"] for item in item_wise_stock}
    return item_wise_stock


def update_value_item_wise(updated_data, client=None):
    if not client:
        client = create_client()
    client.collections[product_schema.get("name")].documents.import_(
        updated_data, {"action": "upsert"}
    )


def update_product_schema_data(self, method):
    frappe.enqueue(
        update_product_schema_data_qr_job,
        timeout=10000,
        queue="long",
        job_name="update_product_schema_data_qr_job",
        self_data=(self.as_dict()),
    )


def update_product_schema_data_qr_job(self_data):
    try:
        doctype = self_data.get("doctype")
        item_code = []
        if doctype == "Item Price" and self_data.get("selling") == 1:
            item_code = [self_data.get("item_code")]
        elif doctype == "Item" and self_data.get("disabled") == 0:
            item_code = [self_data.get("item_code")]
        elif self_data.get("items"):
            item_code = [
                item_code_vlaue.get("item_code")
                for item_code_vlaue in self_data.get("items")
            ]
        get_product_schema_data_value = get_product_schema_data(item_code)
        update_value_item_wise(get_product_schema_data_value)
    except Exception:
        frappe.log_error(
            title="update_product_schema_data_qr_job", message=frappe.get_traceback()
        )
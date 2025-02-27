import frappe
from frappe.core.doctype.user.user import generate_keys
from frappe.sessions import delete_session

MASTER_DICT = {
    "item_group": "Item Group",
    "ip_rate": "Att IP Rate",
    "category_list": "Category",
    "target_category": "Target Category",
    "brand": "Brand",
    "sub_brand": "Sub Brand",
    "buying_uom": "UOM",
    "range": "Att Range",
    "eec": "Att Eec",
    "lamp_qty": "Att Lamp Qty",
    "safety_class": "Att Safety Class",
    "beam_angle": "Att Beam Angle",
    "lumen_output": "Att Lumen Output",
    "reflector": "Att Reflector",
    "mounting": "Att Mounting",
    "att_heat_sink": "Att Heat Sink",
    "output_signal": "Att Output Signal",
    "power_factor": "Att Power Factor",
    "working_temp": "Att Working Temp",
    "life_time": "Att Life Time",
    "output_current": "Att Output Current",
    "output_voltage": "Att Output Voltage",
    "light_intensity": "Att Light Intensity",
    "color_temp_": "Att Color Temp",
    "light_source": "Att Light Source",
    "lamp_type": "Att Lamp Type",
    "cri": "Att CRI",
    "power": "Att Power",
    "input": "Att Input",
    "efficacy": "Att Efficacy",
    "operating_frequency": "Att Operating Frequency",
    "input_signal": "Att Input Signal",
    "function": "Att Function",
    "cut_out": "Att Cut Out",
    "material": "Att Material",
    "body_finish": "Att Body Finish",
    "shade_material": "Att Shade Material",
    "shade_finish": "Att Shade Finish",
    "pole_dimension": "Att Pole Dimension",
    "suspended_length": "Att Suspended Length",
    "warranty_type_": "Att Warranty Type",
    "warranty_": "Att Warranty",
    "diffuser": "Att Diffuser",
}


@frappe.whitelist()
def get_all_masters():
    try:
        master_data = {}
        for master in MASTER_DICT:
            if MASTER_DICT[master]!="Item Group":
                master_data[master] = frappe.get_list(MASTER_DICT[master], pluck="name")
            else:
                master_data[master] = frappe.get_list(MASTER_DICT[master], pluck="name",filters={"disable":0,"name":("!=","All Item Groups")})
        master_data["product_type"] = ["Listed", "Unlisted", "Obsolete"]
        return master_data
    except Exception as e:
        frappe.log_error(title="get_all_masters", message=frappe.get_traceback())
        frappe.throw(e)


@frappe.whitelist()
def get_product_info(item_code):
    company = frappe.get_cached_value(
        "E Commerce Settings", "E Commerce Settings", "company"
    )
    product_info = {
        "stock": warehouse_wise_stock(item_code, company),
        "related_products": get_related_products(item_code),
    }
    return product_info


def warehouse_wise_stock(item_code, company):
    stock = frappe.db.sql(
        f"""
             SELECT warehouse.name as warehouse ,bin.actual_qty
                FROM `tabBin` AS bin
                JOIN `tabWarehouse` AS warehouse ON bin.warehouse = warehouse.name
                WHERE warehouse.company = '{company}'
                AND bin.item_code = '{item_code}'
            """,
        as_dict=1,
    )
    return stock


def get_related_products(item_code):
    related_products = {}
    types = ["Bought Together", "Must Use", "Add On"]
    for type_value in types:
        related_items_bought_together = frappe.db.sql(
            f""" 
        SELECT COALESCE(GROUP_CONCAT(
            CASE 
                WHEN relate_both_ways = 1 THEN 
                    CASE 
                        WHEN item_1 = '{item_code}' THEN item_2 
                        WHEN item_2 = '{item_code}' THEN item_1 
                        ELSE NULL 
                    END
                ELSE 
                    CASE 
                        WHEN item_1 = '{item_code}' THEN item_2 
                        ELSE NULL 
                    END
            END
        ), NULL) AS item_codes
        FROM `tabRelated Items`
        WHERE (item_1 = '{item_code}' OR item_2 = '{item_code}')
        and type = '{type_value}'
        limit 10 """,
            as_list=1,
        )
        if related_items_bought_together and related_items_bought_together[0][0]:
            related_products[type_value] = related_items_bought_together[0][0].split(
                ","
            )
        else:
            related_products[type_value] = []
    category_list = frappe.get_cached_value(
        "Item", {"name": item_code}, "category_list"
    )
    if category_list:
        related_products_group = frappe.get_list(
            "Item",
            filters={
                "disabled": 0,
                "category_list": category_list,
                "name": ["!=", item_code],
            },
            fields=["name"],
            order_by="modified DESC",
            limit=10,
            pluck="name",
        )
        related_products["category_list"] = related_products_group
    return related_products


@frappe.whitelist(allow_guest=True)
def get_user_credentials(email, pwd):
    try:
        login_manager = frappe.auth.LoginManager()
        login_manager.authenticate(user=email, pwd=pwd)
        login_manager.post_login()
    except frappe.exceptions.AuthenticationError:
        return {"key": 0, "message": "Incorrect Username or Password"}
    frappe.set_user("Administrator")
    user = frappe.get_doc("User", email)
    if not user.api_key:
        api_key = frappe.generate_hash(length=15)
        user.api_key = api_key
        user.save(ignore_permissions=True)
    api_generate = generate_keys(email)
    frappe.db.commit()
    delete_session(frappe.session.sid, "Administrator")
    return {
        "key": 1,
        "message": "Success",
        "api_key": user.api_key,
        "api_secret": api_generate["api_secret"],
        "name": user.full_name,
        "dob": user.birth_date,
        "mobile_no": user.mobile_no,
        "email": user.email,
    }

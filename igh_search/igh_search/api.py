import frappe
from frappe.core.doctype.user.user import generate_keys
from frappe.rate_limiter import rate_limit
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

def get_model_names(model_name):
    key = f"get_all_masters|{model_name}"
    cached_value = frappe.cache().get_value(key)
    if cached_value: 
        return cached_value
    result = frappe.get_list(model_name, pluck="name")
    if model_name == "Item Group":
        result = frappe.get_list(model_name, pluck="name",filters={"disable":0,"name":("!=","All Item Groups")})
    frappe.cache().set_value(key, result, expires_in_sec=3600)
    return result

@frappe.whitelist()
def get_all_masters():
    try:
        master_data_dict = {key: get_model_names(value) for key, value in MASTER_DICT.items()}
        master_data_dict["product_type"] = ["Listed", "Unlisted", "Obsolete"]
        return master_data_dict
    except Exception as e:
        # Log the error with a traceback for debugging
        frappe.log_error(title="get_all_masters", message=frappe.get_traceback())
        # Re-raise the exception to inform the caller
        frappe.throw(str(e))
# @frappe.whitelist()
# def get_all_masters():
#     try:
#         master_data = {}
#         for master in MASTER_DICT:
#             if MASTER_DICT[master]!="Item Group":
#                 master_data[master] = frappe.get_list(MASTER_DICT[master], pluck="name")
#             else:
#                 master_data[master] = frappe.get_list(MASTER_DICT[master], pluck="name",filters={"disable":0,"name":("!=","All Item Groups")})
#         master_data["product_type"] = ["Listed", "Unlisted", "Obsolete"]
#         return master_data
#     except Exception as e:
#         frappe.log_error(title="get_all_masters", message=frappe.get_traceback())
#         frappe.throw(e)


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
                WHERE bin.item_code = '{item_code}'
                AND LOWER(warehouse.name) NOT LIKE '%damage%'
                AND LOWER(warehouse.name) NOT LIKE '%missing%'
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
    
        # frappe.log_error("session_user",frappe.session.user)
        user_email = frappe.session.user
        if frappe.db.exists("User",email):
            user_email = email
        else:
            user_email = frappe.db.get_value("User",{"username":email})
            email = user_email
        user = frappe.get_doc("User", user_email)
        frappe.set_user("Administrator")
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
    except frappe.exceptions.AuthenticationError:
        return {"key": 0, "message": "Incorrect Username or Password"}
    except:
        frappe.log_error(title="get_user_credentials", message=frappe.get_traceback())


def _get_ai_product_search_rate_limit():
    from igh_search.igh_search.ai_product_search import get_ai_product_search_rate_limit

    return get_ai_product_search_rate_limit()


@frappe.whitelist(allow_guest=True)
@rate_limit(limit=_get_ai_product_search_rate_limit, seconds=60, methods="POST")
def ai_product_search(message=None, page_context=None):
    if getattr(frappe.local, "request", None) and frappe.local.request.method != "POST":
        frappe.throw("AI product search only supports POST requests.")

    from igh_search.igh_search.ai_product_search import parse_product_search_intent

    return parse_product_search_intent(message=message, page_context=page_context)

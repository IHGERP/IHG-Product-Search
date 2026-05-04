import contextlib
import json

import frappe
from frappe.rate_limiter import rate_limit
from frappe.utils import flt
from igh_search.igh_search.product_search_v2 import (
    get_similar_products_v2 as get_similar_products_v2_impl,
    get_sync_health_summary,
    search_products_v2 as search_products_v2_impl,
    suggest_products_v2 as suggest_products_v2_impl,
)

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

@frappe.whitelist(methods=["GET", "POST"])
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


@frappe.whitelist(methods=["GET", "POST"])
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


@frappe.whitelist(allow_guest=True, methods=["POST"])
def get_user_credentials(email, pwd):
    try:
        login_manager = getattr(frappe.local, "login_manager", None) or frappe.auth.LoginManager()
        login_manager.authenticate(user=email, pwd=pwd)
        login_manager.post_login()

        user_email = login_manager.user or frappe.session.user
        if not user_email or user_email == "Guest":
            frappe.throw("Failed to establish authenticated session")

        user = frappe.get_doc("User", user_email)
        if not user.enabled:
            frappe.throw(f"User {user_email} is disabled. Please contact your System Manager.")

        if not user.api_key:
            user.api_key = frappe.generate_hash(length=15)

        api_secret = frappe.generate_hash(length=15)
        user.api_secret = api_secret

        # Preserve the logged-in user's session; only elevate briefly for saving the key fields.
        with contextlib.suppress(Exception):
            frappe.clear_cache(user=user_email)

        current_user = frappe.session.user
        try:
            frappe.set_user("Administrator")
            user.save(ignore_permissions=True)
        finally:
            frappe.set_user(current_user)

        frappe.db.commit()

        if not frappe.session.user or frappe.session.user in ("Guest", "Administrator"):
            frappe.set_user(user_email)

        return {
            "key": 1,
            "message": "Success",
            "api_key": user.api_key,
            "api_secret": api_secret,
            "sid": frappe.session.sid,
            "name": user.full_name,
            "dob": user.birth_date,
            "mobile_no": user.mobile_no,
            "email": user.email,
        }
    except frappe.exceptions.AuthenticationError:
        return {"key": 0, "message": "Incorrect Username or Password"}
    except Exception:
        frappe.log_error(title="get_user_credentials", message=frappe.get_traceback())
        raise


def _get_ai_product_search_rate_limit():
    from igh_search.igh_search.ai_product_search import get_ai_product_search_rate_limit

    return get_ai_product_search_rate_limit()


@frappe.whitelist(allow_guest=True, methods=["POST"])
@rate_limit(limit=_get_ai_product_search_rate_limit, seconds=60, methods="POST")
def ai_product_search(message=None, page_context=None):
    from igh_search.igh_search.ai_product_search import parse_product_search_intent

    return parse_product_search_intent(message=message, page_context=page_context)


@frappe.whitelist(methods=["GET", "POST"])
@rate_limit(limit=_get_ai_product_search_rate_limit, seconds=60, methods="POST")
def ai_search_products_v2(
    message=None,
    page_context=None,
    page=1,
    page_length=20,
    include_inactive=0,
    feature_flag_override=0,
):
    from igh_search.igh_search.ai_product_search import ai_search_products_v2 as ai_search_products_v2_impl

    return ai_search_products_v2_impl(
        message=message,
        page_context=page_context,
        page=page,
        page_length=page_length,
        include_inactive=include_inactive,
        feature_flag_override=feature_flag_override,
    )


@frappe.whitelist(methods=["GET", "POST"])
def get_ai_product_search_quality_report():
    if "System Manager" not in frappe.get_roles():
        frappe.throw("Only System Manager can view AI product search quality.")

    from igh_search.igh_search.ai_product_search import get_ai_search_quality_report

    return get_ai_search_quality_report()


@frappe.whitelist(methods=["POST"])
def track_ai_search_click(search_event_id, item_code):
    if frappe.session.user == "Guest":
        frappe.throw("Authentication required")

    from igh_search.igh_search.ai_product_search import track_ai_search_outcome

    return track_ai_search_outcome(
        "search_click",
        search_event_id=search_event_id,
        item_code=item_code,
    )


@frappe.whitelist(methods=["POST"])
def track_ai_search_shortlist(search_event_id, item_code):
    if frappe.session.user == "Guest":
        frappe.throw("Authentication required")

    from igh_search.igh_search.ai_product_search import track_ai_search_outcome

    return track_ai_search_outcome(
        "shortlist",
        search_event_id=search_event_id,
        item_code=item_code,
    )


@frappe.whitelist(methods=["POST"])
def track_ai_search_quotation(search_event_id, item_code, quotation=None):
    if frappe.session.user == "Guest":
        frappe.throw("Authentication required")

    from igh_search.igh_search.ai_product_search import track_ai_search_outcome

    return track_ai_search_outcome(
        "quotation_created",
        search_event_id=search_event_id,
        item_code=item_code,
        related_item_code=quotation,
    )


@frappe.whitelist(methods=["POST"])
def track_ai_search_reformulation(search_event_id, reformulated_message, page_context=None):
    if frappe.session.user == "Guest":
        frappe.throw("Authentication required")

    from igh_search.igh_search.ai_product_search import track_ai_search_outcome

    return track_ai_search_outcome(
        "reformulated_query",
        search_event_id=search_event_id,
        reformulated_message=reformulated_message,
        page_context=page_context,
    )


@frappe.whitelist(methods=["POST"])
def evaluate_ai_product_search_benchmark(feature_flag_override=0):
    if "System Manager" not in frappe.get_roles():
        frappe.throw("Only System Manager can run AI search benchmark.")

    from igh_search.igh_search.ai_product_search import evaluate_ai_search_benchmark

    return evaluate_ai_search_benchmark(feature_flag_override=feature_flag_override)


@frappe.whitelist(methods=["GET", "POST"])
def search_products_v2(
    query=None,
    filters=None,
    sort_by=None,
    page=1,
    page_length=20,
    include_inactive=0,
    item_code_hint=None,
    feature_flag_override=0,
    strict_sort=0,
):
    return search_products_v2_impl(
        query=query,
        filters=filters,
        sort_by=sort_by,
        page=page,
        page_length=page_length,
        include_inactive=include_inactive,
        item_code_hint=item_code_hint,
        feature_flag_override=feature_flag_override,
        strict_sort=strict_sort,
    )


@frappe.whitelist(methods=["GET", "POST"])
def suggest_products_v2(query=None, limit=10, feature_flag_override=0):
    return suggest_products_v2_impl(
        query=query, limit=limit, feature_flag_override=feature_flag_override
    )


@frappe.whitelist(methods=["GET", "POST"])
def get_similar_products_v2(
    item_code, limit=10, include_manual=1, feature_flag_override=0
):
    return get_similar_products_v2_impl(
        item_code=item_code,
        limit=limit,
        include_manual=include_manual,
        feature_flag_override=feature_flag_override,
    )


@frappe.whitelist(methods=["GET", "POST"])
def get_typesense_sync_health():
    if "System Manager" not in frappe.get_roles():
        frappe.throw("Only System Manager can view sync health.")
    return get_sync_health_summary()


def _ensure_authenticated_user():
    if frappe.session.user == "Guest":
        frappe.throw("Authentication required")

    return frappe.session.user


def _serialize_safe_form_subset():
    safe_keys = {
        "item_code",
        "qty",
        "name",
        "cart_id",
        "cmd",
        "doctype",
        "docname",
    }
    form_dict = getattr(frappe, "form_dict", {}) or {}
    output = {}
    for key in sorted(form_dict.keys()):
        if key not in safe_keys:
            continue
        value = form_dict.get(key)
        if value is None:
            output[key] = None
            continue
        value_str = str(value)
        output[key] = value_str[:120] + ("..." if len(value_str) > 120 else "")
    return output


def _log_cart_mutation_request(endpoint_name):
    request = getattr(frappe.local, "request", None)
    if not request:
        return

    headers = getattr(request, "headers", {}) or {}
    args = getattr(request, "args", None)
    query_string = ""
    if args:
        with contextlib.suppress(Exception):
            query_string = args.to_dict(flat=False)

    frappe.logger("igh_search").warning(
        "cart mutation request | endpoint=%s | method=%s | path=%s | query=%s | form_keys=%s | form_subset=%s | host=%s | referer=%s | origin=%s | user_agent=%s | x_forwarded_for=%s | user=%s | roles=%s",
        endpoint_name,
        getattr(request, "method", ""),
        getattr(request, "path", ""),
        query_string,
        sorted(list((getattr(frappe, "form_dict", {}) or {}).keys())),
        _serialize_safe_form_subset(),
        headers.get("Host"),
        headers.get("Referer"),
        headers.get("Origin"),
        headers.get("User-Agent"),
        headers.get("X-Forwarded-For"),
        getattr(frappe.session, "user", "Guest"),
        ",".join(frappe.get_roles() or []),
    )


def _log_cart_endpoint_context(endpoint_name):
    request = getattr(frappe.local, "request", None)
    if not request:
        return

    frappe.logger("igh_search").warning(
        "cart endpoint hit: endpoint=%s method=%s path=%s user=%s content_type=%s",
        endpoint_name,
        getattr(request, "method", ""),
        getattr(request, "path", ""),
        getattr(frappe.session, "user", "Guest"),
        getattr(request, "content_type", ""),
    )


def _log_update_cartitem_request():
    request = getattr(frappe.local, "request", None)
    if not request:
        return

    headers = getattr(request, "headers", {}) or {}
    host = headers.get("Host")
    forwarded_proto = headers.get("X-Forwarded-Proto")
    forwarded_host = headers.get("X-Forwarded-Host")
    referer = headers.get("Referer")
    origin = headers.get("Origin")
    user_agent = headers.get("User-Agent")
    full_url = getattr(request, "url", "")
    form_keys = sorted(list((getattr(frappe, "form_dict", {}) or {}).keys()))

    frappe.logger("igh_search").warning(
        "update_cartitem request capture | method=%s | path=%s | url=%s | host=%s | x_forwarded_proto=%s | x_forwarded_host=%s | referer=%s | origin=%s | user_agent=%s | form_keys=%s",
        getattr(request, "method", ""),
        getattr(request, "path", ""),
        full_url,
        host,
        forwarded_proto,
        forwarded_host,
        referer,
        origin,
        user_agent,
        ",".join(form_keys),
    )


def _parse_json_payload(value, fieldname):
    if value is None:
        return None

    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None

        try:
            return json.loads(value)
        except json.JSONDecodeError:
            frappe.throw(f"Invalid JSON payload for {fieldname}")

    return value


def _get_portal_cart_cache_key():
    user = _ensure_authenticated_user()
    return f"igh_search_portal_cart:{user}"


def _get_portal_cart_items():
    items = frappe.cache().get_value(_get_portal_cart_cache_key())
    if not items:
        return []

    if isinstance(items, str):
        try:
            items = json.loads(items)
        except json.JSONDecodeError:
            return []

    return items if isinstance(items, list) else []


def _set_portal_cart_items(items):
    normalized_items = []
    for item in items or []:
        item_code = (item or {}).get("item_code")
        qty = flt((item or {}).get("qty"))
        if item_code and qty > 0:
            normalized_items.append(
                {
                    "cart_id": (item or {}).get("cart_id") or frappe.generate_hash(length=10),
                    "item_code": item_code,
                    "qty": qty,
                }
            )

    frappe.cache().set_value(_get_portal_cart_cache_key(), normalized_items)
    return normalized_items


def _get_default_company():
    return (
        frappe.defaults.get_user_default("Company")
        or frappe.db.get_single_value("Global Defaults", "default_company")
        or frappe.get_all("Company", pluck="name", limit=1)[0]
    )


def _get_default_selling_price_list():
    price_list = frappe.db.get_single_value("Selling Settings", "selling_price_list")
    if price_list:
        return price_list

    rows = frappe.get_all(
        "Price List",
        filters={"selling": 1, "enabled": 1},
        pluck="name",
        limit=1,
    )
    if rows:
        return rows[0]

    rows = frappe.get_all("Price List", filters={"selling": 1}, pluck="name", limit=1)
    return rows[0] if rows else None


def _get_linked_customer_for_user(user=None):
    user = user or _ensure_authenticated_user()
    contact_name = frappe.db.get_value("Contact", {"email_id": user}, "name")
    if not contact_name:
        return None

    customer = frappe.db.get_value(
        "Dynamic Link",
        {
            "parenttype": "Contact",
            "parent": contact_name,
            "link_doctype": "Customer",
        },
        "link_name",
    )
    return customer


def _resolve_customer_from_session_user():
    user = _ensure_authenticated_user()
    customer = frappe.db.get_value("Customer", {"email_id": user}, "name")
    if customer:
        return customer

    # Fallback to contact-linked customer mapping used elsewhere in this module.
    return _get_linked_customer_for_user(user)


def _build_cart_item_summary(cart_item, company, selling_price_list, customer=None):
    item_code = cart_item.get("item_code")
    qty = flt(cart_item.get("qty"))

    item_doc = frappe.get_cached_doc("Item", item_code)
    summary = {
        "cart_id": cart_item.get("cart_id"),
        "item_code": item_code,
        "item_name": item_doc.item_name,
        "description": item_doc.description,
        "image": item_doc.image,
        "stock_uom": item_doc.stock_uom,
        "qty": qty,
        "rate": 0,
        "amount": 0,
    }

    if not selling_price_list:
        return summary

    from erpnext import get_company_currency
    from erpnext.stock.get_item_details import get_item_details

    args = {
        "doctype": "Quotation",
        "item_code": item_code,
        "company": company,
        "currency": get_company_currency(company),
        "conversion_rate": 1,
        "selling_price_list": selling_price_list,
        "qty": qty,
        "price_list_currency": get_company_currency(company),
        "plc_conversion_rate": 1,
    }
    if customer:
        args["customer"] = customer

    details = get_item_details(args)
    rate = flt(details.get("rate") or details.get("price_list_rate") or 0)

    summary.update(
        {
            "rate": rate,
            "amount": flt(rate * qty),
            "uom": details.get("uom") or item_doc.stock_uom,
        }
    )
    return summary


def _build_cart_response():
    _ensure_authenticated_user()

    cart_items = _get_portal_cart_items()
    if cart_items and any(not item.get("cart_id") for item in cart_items):
        cart_items = _set_portal_cart_items(cart_items)
    company = _get_default_company()
    selling_price_list = _get_default_selling_price_list()
    customer = _get_linked_customer_for_user()

    marketplace_items = []
    valid_cart_items = []
    for item in cart_items:
        try:
            marketplace_items.append(
                _build_cart_item_summary(item, company, selling_price_list, customer=customer)
            )
            valid_cart_items.append(item)
        except frappe.DoesNotExistError:
            # Skip stale cart rows whose item master no longer exists.
            continue

    # Persist cleaned cart rows so stale item references are pruned from cache.
    if len(valid_cart_items) != len(cart_items):
        _set_portal_cart_items(valid_cart_items)

    total = sum(flt(item.get("amount")) for item in marketplace_items)

    return {
        "status": "success",
        "cart": {"marketplace_items": marketplace_items},
        "total": total,
        "grand_total": total,
    }


def _get_open_opportunity_filters():
    filters = {"docstatus": ["!=", 2]}
    if frappe.db.has_column("Opportunity", "status"):
        filters["status"] = ["not in", ["Lost", "Closed", "Converted"]]
    return filters


def _get_accessible_opportunity(opportunity_name):
    _ensure_authenticated_user()
    opportunity = frappe.db.get_value(
        "Opportunity",
        opportunity_name,
        ["name", "owner", "party_name", "customer_name", "title", "opportunity_from", "status"],
        as_dict=True,
    )
    if not opportunity:
        frappe.throw("Opportunity not found")

    return opportunity


@frappe.whitelist(allow_guest=True, methods=["GET", "POST"])
def get_user_opportunities():
    _ensure_authenticated_user()

    fields = ["name", "title", "party_name", "customer_name", "opportunity_from", "owner"]
    opportunities = frappe.get_all(
        "Opportunity",
        filters=_get_open_opportunity_filters(),
        fields=fields,
        order_by="modified desc",
    )

    data = []
    for opportunity in opportunities:
        customer = opportunity.get("customer_name")
        if opportunity.get("opportunity_from") == "Customer":
            customer = opportunity.get("party_name") or customer

        data.append(
            {
                "name": opportunity.get("name"),
                "title": opportunity.get("title"),
                "customer": customer,
            }
        )

    return {"status": "success", "data": data}


@frappe.whitelist(allow_guest=True, methods=["GET", "POST"])
def search_opportunities(search=None, customer_id=None):
    _ensure_authenticated_user()

    request = getattr(frappe.local, "request", None)
    payload = {}
    if request:
        with contextlib.suppress(Exception):
            payload = request.get_json(silent=True) or {}

    search_text = (
        search or payload.get("search") or (getattr(frappe, "form_dict", {}) or {}).get("search") or ""
    ).strip()
    customer_filter = (
        customer_id
        or payload.get("customer_id")
        or (getattr(frappe, "form_dict", {}) or {}).get("customer_id")
        or ""
    ).strip()

    if not search_text:
        return {"status": "success", "data": []}

    if len(search_text) < 2:
        return {"status": "error", "message": "Search query too short"}

    fields = ["name", "party_name", "customer_name", "title", "status", "owner", "opportunity_from"]

    filters = _get_open_opportunity_filters()
    if customer_filter:
        filters["party_name"] = customer_filter

    like_pattern = f"%{search_text}%"
    opportunities = frappe.get_all(
        "Opportunity",
        filters=filters,
        or_filters={
            "name": ["like", like_pattern],
            "customer_name": ["like", like_pattern],
            "party_name": ["like", like_pattern],
            "title": ["like", like_pattern],
        },
        fields=fields,
        order_by="modified desc",
        limit=20,
    )

    data = []
    for opportunity in opportunities:
        customer_value = opportunity.get("party_name") if opportunity.get("opportunity_from") == "Customer" else None
        customer_value = customer_value or opportunity.get("party_name")
        if not customer_value:
            continue

        data.append(
            {
                "name": opportunity.get("name"),
                "customer": customer_value,
                "customer_name": opportunity.get("customer_name"),
                "title": opportunity.get("title"),
                "status": opportunity.get("status"),
            }
        )

    return {"status": "success", "data": data}


@frappe.whitelist(allow_guest=True, methods=["POST"])
def create_quotation_from_portal(opportunity=None, items=None):
    _ensure_authenticated_user()

    payload_items = _parse_json_payload(items, "items")
    if isinstance(opportunity, str) and opportunity.startswith("{"):
        payload = _parse_json_payload(opportunity, "payload") or {}
        opportunity = payload.get("opportunity")
        payload_items = payload.get("items")

    if not opportunity:
        frappe.throw("Opportunity is required")

    if not isinstance(payload_items, list) or not payload_items:
        frappe.throw("At least one item is required")

    accessible_opportunity = _get_accessible_opportunity(opportunity)

    from erpnext.crm.doctype.opportunity.opportunity import make_quotation

    # Elevate to Administrator for the entire quotation creation pipeline.
    # The scope must cover make_quotation(), get_doc(), set_missing_values(),
    # calculate_taxes_and_totals(), and insert() — all of which read related
    # ERPNext documents (price lists, tax templates, customer records) that the
    # portal user does not have read permission for.
    saved_user = frappe.session.user
    try:
        frappe.set_user("Administrator")
        quotation_dict = make_quotation(opportunity)

        quotation = frappe.get_doc(quotation_dict)
        quotation.set("items", [])
        quotation.opportunity = accessible_opportunity.name

        for row in payload_items:
            item_code = (row or {}).get("item_code")
            qty = flt((row or {}).get("qty"))
            rate = (row or {}).get("rate")
            description = (row or {}).get("description") or ""
            if not item_code:
                frappe.throw("Each item requires an item_code")
            if qty <= 0:
                frappe.throw(f"Quantity must be greater than zero for item {item_code}")

            item = {"item_code": item_code, "qty": qty}
            if rate not in (None, ""):
                item["rate"] = flt(rate)
            if description:
                item["description"] = description

            quotation.append("items", item)

        quotation.flags.ignore_permissions = True
        quotation.run_method("set_missing_values")
        quotation.run_method("calculate_taxes_and_totals")
        quotation.insert()
        frappe.db.commit()
        quotation_name = quotation.name
    finally:
        frappe.set_user(saved_user)

    return {"status": "success", "quotation": quotation_name}


@frappe.whitelist(allow_guest=True, methods=["GET", "POST"])
def get_cart_items(customer_id=None):
    if not customer_id:
        customer_id = _resolve_customer_from_session_user()

    cart_response = _build_cart_response()
    marketplace_items = (cart_response.get("cart") or {}).get("marketplace_items") or []
    normalized_items = []
    for item in marketplace_items:
        row = dict(item or {})
        row["name"] = row.get("name") or row.get("cart_id")
        row["quantity"] = row.get("quantity") if row.get("quantity") is not None else row.get("qty")
        normalized_items.append(row)

    return {
        "status": "success",
        "cart": {"marketplace_items": normalized_items},
        "total": cart_response.get("total", 0),
        "grand_total": cart_response.get("grand_total", 0),
        "you_may_like": [],
    }


@frappe.whitelist(methods=["POST"])
def update_cart_item(item_code=None, qty=None):
    _ensure_authenticated_user()

    if isinstance(item_code, str) and item_code.startswith("{"):
        payload = _parse_json_payload(item_code, "payload") or {}
        item_code = payload.get("item_code")
        qty = payload.get("qty")

    if not item_code:
        frappe.throw("item_code is required")

    qty = flt(qty)
    items = _get_portal_cart_items()
    updated = False
    next_items = []

    for item in items:
        if item.get("item_code") == item_code:
            updated = True
            if qty > 0:
                next_items.append({"item_code": item_code, "qty": qty})
        else:
            next_items.append(item)

    if not updated and qty > 0:
        next_items.append({"item_code": item_code, "qty": qty})

    next_items = _set_portal_cart_items(next_items)
    return {"status": "success", "data": next_items}


@frappe.whitelist(methods=["POST"])
def clear_cart():
    frappe.cache().delete_value(_get_portal_cart_cache_key())
    return {"status": "success", "data": []}


@frappe.whitelist(methods=["POST"])
def get_recent_quotations():
    _ensure_authenticated_user()
    quotations = frappe.get_all(
        "Quotation",
        filters={"owner": frappe.session.user, "docstatus": ["!=", 2]},
        fields=["name", "customer_name", "opportunity", "status", "grand_total", "creation"],
        order_by="creation desc",
        limit=20,
    )
    return {"status": "success", "data": quotations}


@frappe.whitelist(allow_guest=True, methods=["POST"])
def update_cartitem(item_code=None, qty=None):
    _log_cart_mutation_request("update_cartitem")
    _log_update_cartitem_request()
    _log_cart_endpoint_context("update_cartitem")
    update_cart_item(item_code=item_code, qty=qty)
    return {"status": "success", "message": "Updated"}


@frappe.whitelist(allow_guest=True, methods=["POST"])
def insert_cart_items(item_code=None, qty=None, customer=None):
    _log_cart_mutation_request("insert_cart_items")
    _ensure_authenticated_user()

    if isinstance(item_code, str) and item_code.startswith("{"):
        payload = _parse_json_payload(item_code, "payload") or {}
        item_code = payload.get("item_code")
        qty = payload.get("qty")
        customer = payload.get("customer") or payload.get("customer_id") or customer

    if not customer:
        customer = _resolve_customer_from_session_user()

    if not item_code:
        frappe.throw("item_code is required")

    qty = flt(qty or 1)
    if qty <= 0:
        frappe.throw("qty must be greater than zero")

    items = _get_portal_cart_items()
    for item in items:
        if item.get("item_code") == item_code:
            item["qty"] = flt(item.get("qty")) + qty
            _set_portal_cart_items(items)
            return {"status": "success", "message": "Updated", "customer": customer}

    items.append({"cart_id": frappe.generate_hash(length=10), "item_code": item_code, "qty": qty})
    _set_portal_cart_items(items)
    return {"status": "success", "message": "Updated", "customer": customer}


@frappe.whitelist(allow_guest=True, methods=["POST"])
def delete_cart_items(cart_id=None):
    _log_cart_mutation_request("delete_cart_items")
    _ensure_authenticated_user()

    if isinstance(cart_id, str) and cart_id.startswith("{"):
        payload = _parse_json_payload(cart_id, "payload") or {}
        cart_id = payload.get("cart_id")

    if not cart_id:
        frappe.throw("cart_id is required")

    items = [item for item in _get_portal_cart_items() if item.get("cart_id") != cart_id]
    _set_portal_cart_items(items)
    return {"status": "success", "message": "Updated"}


@frappe.whitelist(allow_guest=True, methods=["GET", "POST"])
def get_customer_info():
    user = _ensure_authenticated_user()

    user_doc = frappe.get_doc("User", user)
    customer = _get_linked_customer_for_user(user)
    customer_doc = frappe.get_doc("Customer", customer) if customer else None

    return {
        "status": "success",
        "data": {
            "user": user,
            "full_name": user_doc.full_name,
            "email": user_doc.email,
            "mobile_no": user_doc.mobile_no,
            "customer": customer,
            "customer_name": customer_doc.customer_name if customer_doc else None,
        },
    }


@frappe.whitelist(allow_guest=True, methods=["GET", "POST"])
def get_product_details(item_code=None):
    if not item_code:
        frappe.throw("item_code is required")

    item = frappe.get_doc("Item", item_code)
    return {
        "status": "success",
        "data": {
            "item_code": item.name,
            "item_name": item.item_name,
            "description": item.description,
            "image": item.image,
            "stock_uom": item.stock_uom,
            "disabled": item.disabled,
            "is_stock_item": item.is_stock_item,
        },
    }


@frappe.whitelist(allow_guest=True, methods=["POST"])
def move_item_to_cart(item_code=None, qty=None):
    _log_cart_mutation_request("move_item_to_cart")
    return insert_cart_items(item_code=item_code, qty=qty)


@frappe.whitelist(allow_guest=True, methods=["POST"])
def move_all_tocart(items=None):
    _log_cart_mutation_request("move_all_tocart")
    _ensure_authenticated_user()

    payload_items = _parse_json_payload(items, "items")
    if payload_items is None:
        payload_items = frappe.form_dict.get("items")
        payload_items = _parse_json_payload(payload_items, "items")

    if not isinstance(payload_items, list) or not payload_items:
        frappe.throw("items must be a non-empty list")

    for row in payload_items:
        row = row or {}
        insert_cart_items(item_code=row.get("item_code") or row.get("name"), qty=row.get("qty") or 1)

    return {"status": "success", "message": "Updated"}


@frappe.whitelist(allow_guest=True, methods=["POST"])
def clear_cartitem(cart_id=None):
    _log_cart_mutation_request("clear_cartitem")
    return delete_cart_items(cart_id=cart_id)

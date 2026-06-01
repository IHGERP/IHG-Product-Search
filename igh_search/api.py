from importlib import import_module

import frappe


def _impl():
    return import_module('igh_search.igh_search.api')


def __getattr__(name):
    return getattr(_impl(), name)


def _sanitize_framework_kwargs(kwargs):
    cleaned = dict(kwargs or {})
    for key in ('cmd', 'data'):
        cleaned.pop(key, None)
    return cleaned


@frappe.whitelist(allow_guest=True)
def get_cart_items(*args, **kwargs):
    return _impl().get_cart_items(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def update_cartitem(*args, **kwargs):
    return _impl().update_cartitem(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def insert_cart_items(*args, **kwargs):
    return _impl().insert_cart_items(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def delete_cart_items(*args, **kwargs):
    return _impl().delete_cart_items(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def get_user_opportunities(*args, **kwargs):
    return _impl().get_user_opportunities(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def search_opportunities(*args, **kwargs):
    return _impl().search_opportunities(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def create_quotation_from_portal(*args, **kwargs):
    return _impl().create_quotation_from_portal(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def get_customer_info(*args, **kwargs):
    return _impl().get_customer_info(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def get_product_details(*args, **kwargs):
    return _impl().get_product_details(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def move_item_to_cart(*args, **kwargs):
    return _impl().move_item_to_cart(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def move_all_tocart(*args, **kwargs):
    return _impl().move_all_tocart(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def clear_cartitem(*args, **kwargs):
    return _impl().clear_cartitem(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(methods=['POST'])
def clear_cart(*args, **kwargs):
    return _impl().clear_cart(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(methods=['POST'])
def get_recent_quotations(*args, **kwargs):
    return _impl().get_recent_quotations(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(methods=['POST'])
def get_sales_dashboard_reports(*args, **kwargs):
    return _impl().get_sales_dashboard_reports(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def get_all_masters(*args, **kwargs):
    return _impl().get_all_masters(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def search_products_v2(*args, **kwargs):
    return _impl().search_products_v2(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def suggest_products_v2(*args, **kwargs):
    return _impl().suggest_products_v2(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def get_similar_products_v2(*args, **kwargs):
    return _impl().get_similar_products_v2(*args, **_sanitize_framework_kwargs(kwargs))



@frappe.whitelist(allow_guest=True)
def get_product_alternatives_v2(*args, **kwargs):
    return _impl().get_product_alternatives_v2(*args, **_sanitize_framework_kwargs(kwargs))

@frappe.whitelist(allow_guest=True)
def ai_search_products_v2(*args, **kwargs):
    return _impl().ai_search_products_v2(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def get_typesense_sync_health(*args, **kwargs):
    return _impl().get_typesense_sync_health(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist()
def sync_product_intelligence_to_typesense(*args, **kwargs):
    return _impl().sync_product_intelligence_to_typesense(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def list_product_data_issues(*args, **kwargs):
    return _impl().list_product_data_issues(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def create_product_data_issue(*args, **kwargs):
    return _impl().create_product_data_issue(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def update_product_data_issue(*args, **kwargs):
    return _impl().update_product_data_issue(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist()
def get_search_freshness_health(*args, **kwargs):
    return _impl().get_search_freshness_health(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist()
def run_stock_drift_repair(*args, **kwargs):
    return _impl().run_stock_drift_repair(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist()
def reindex_stock_for_items(*args, **kwargs):
    return _impl().reindex_stock_for_items(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist()
def backfill_item_specs(*args, **kwargs):
    return _impl().backfill_item_specs(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist()
def scan_data_quality(*args, **kwargs):
    return _impl().scan_data_quality(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist()
def product_assistant_chat(*args, **kwargs):
    return _impl().product_assistant_chat(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def analyze_driver_requirement(*args, **kwargs):
    return _impl().analyze_driver_requirement(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True)
def find_suitable_drivers(*args, **kwargs):
    return _impl().find_suitable_drivers(*args, **_sanitize_framework_kwargs(kwargs))

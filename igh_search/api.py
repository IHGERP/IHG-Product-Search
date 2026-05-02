from importlib import import_module

import frappe


def _impl():
    return import_module("igh_search.igh_search.api")


def __getattr__(name):
    return getattr(_impl(), name)


def _sanitize_framework_kwargs(kwargs):
    cleaned = dict(kwargs or {})
    for key in ("cmd", "data"):
        cleaned.pop(key, None)
    return cleaned


@frappe.whitelist(allow_guest=True, methods=["GET", "POST"])
def get_cart_items(*args, **kwargs):
    return _impl().get_cart_items(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True, methods=["POST"])
def update_cartitem(*args, **kwargs):
    return _impl().update_cartitem(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True, methods=["POST"])
def insert_cart_items(*args, **kwargs):
    return _impl().insert_cart_items(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True, methods=["POST"])
def delete_cart_items(*args, **kwargs):
    return _impl().delete_cart_items(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True, methods=["GET", "POST"])
def get_user_opportunities(*args, **kwargs):
    return _impl().get_user_opportunities(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True, methods=["GET", "POST"])
def search_opportunities(*args, **kwargs):
    return _impl().search_opportunities(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True, methods=["POST"])
def create_quotation_from_portal(*args, **kwargs):
    return _impl().create_quotation_from_portal(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True, methods=["GET", "POST"])
def get_customer_info(*args, **kwargs):
    return _impl().get_customer_info(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True, methods=["GET", "POST"])
def get_product_details(*args, **kwargs):
    return _impl().get_product_details(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True, methods=["POST"])
def move_item_to_cart(*args, **kwargs):
    return _impl().move_item_to_cart(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True, methods=["POST"])
def move_all_tocart(*args, **kwargs):
    return _impl().move_all_tocart(*args, **_sanitize_framework_kwargs(kwargs))


@frappe.whitelist(allow_guest=True, methods=["POST"])
def clear_cartitem(*args, **kwargs):
    return _impl().clear_cartitem(*args, **_sanitize_framework_kwargs(kwargs))

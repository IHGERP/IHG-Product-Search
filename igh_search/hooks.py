app_name = "igh_search"
app_title = "IGH Search"
app_publisher = "Aerele"
app_description = "Integration with Typesense"
app_email = "hello@aerele.in"
app_license = "mit"

# Apps
# ------------------

# required_apps = []

# Each item in the list will be shown as an app in the apps page
# add_to_apps_screen = [
# 	{
# 		"name": "igh_search",
# 		"logo": "/assets/igh_search/logo.png",
# 		"title": "IGH Search",
# 		"route": "/igh_search",
# 		"has_permission": "igh_search.api.permission.has_app_permission"
# 	}
# ]

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
app_include_css = "/assets/igh_search/css/igh_search.css"
# app_include_js = "/assets/igh_search/js/igh_search.js"

# include js, css files in header of web template
# web_include_css = "/assets/igh_search/css/igh_search.css"
# web_include_js = "/assets/igh_search/js/igh_search.js"

# include custom scss in every website theme (without file extension ".scss")
# website_theme_scss = "igh_search/public/scss/website"

# include js, css files in header of web form
# webform_include_js = {"doctype": "public/js/doctype.js"}
# webform_include_css = {"doctype": "public/css/doctype.css"}

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
# doctype_js = {"doctype" : "public/js/doctype.js"}
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

# Svg Icons
# ------------------
# include app icons in desk
# app_include_icons = "igh_search/public/icons.svg"

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
# 	"Role": "home_page"
# }

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# Jinja
# ----------

# add methods and filters to jinja environment
# jinja = {
# 	"methods": "igh_search.utils.jinja_methods",
# 	"filters": "igh_search.utils.jinja_filters"
# }

# Installation
# ------------

# before_install = "igh_search.install.before_install"
# after_install = "igh_search.install.after_install"

# Uninstallation
# ------------

# before_uninstall = "igh_search.uninstall.before_uninstall"
# after_uninstall = "igh_search.uninstall.after_uninstall"

# Integration Setup
# ------------------
# To set up dependencies/integrations with other apps
# Name of the app being installed is passed as an argument

# before_app_install = "igh_search.utils.before_app_install"
# after_app_install = "igh_search.utils.after_app_install"

# Integration Cleanup
# -------------------
# To clean up dependencies/integrations with other apps
# Name of the app being uninstalled is passed as an argument

# before_app_uninstall = "igh_search.utils.before_app_uninstall"
# after_app_uninstall = "igh_search.utils.after_app_uninstall"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "igh_search.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# permission_query_conditions = {
# 	"Event": "frappe.desk.doctype.event.event.get_permission_query_conditions",
# }
#
# has_permission = {
# 	"Event": "frappe.desk.doctype.event.event.has_permission",
# }

# DocType Class
# ---------------
# Override standard doctype classes

# override_doctype_class = {
# 	"ToDo": "custom_app.overrides.CustomToDo"
# }

# Document Events
# ---------------
# Hook on document methods and events

typesense_update_doctype = [
    "Stock Entry",
    "Purchase Receipt",
    "Purchase Invoice",
    "Sales Invoice",
    "Delivery Note",
    "Stock Reconciliation",
]
doc_events = {
    doctype: {
        "on_submit": "igh_search.igh_search.doctype.typesense_settings.typesense_settings.update_product_schema_data",
        "on_cancel": "igh_search.igh_search.doctype.typesense_settings.typesense_settings.update_product_schema_data",
    }
    for doctype in typesense_update_doctype
}
doc_events.update(
    {
        "Item Price": {
            "on_update": "igh_search.igh_search.doctype.typesense_settings.typesense_settings.update_product_schema_data",
            "on_trash": "igh_search.igh_search.doctype.typesense_settings.typesense_settings.update_product_schema_data",
        },
        "Item": {
            "on_update": "igh_search.igh_search.doctype.typesense_settings.typesense_settings.update_product_schema_data",
            "on_trash": "igh_search.igh_search.doctype.typesense_settings.typesense_settings.update_product_schema_data",
        },
         # Incremental, like every other doctype here. This used to call
         # initialize_syncing_item_group -> a FULL catalogue resync, so editing
         # any item group rebuilt all ~181k documents. get_affected_item_codes()
         # already expands an Item Group to its member items, so the normal
         # incremental path covers this correctly.
         "Item Group": {
             "on_update": "igh_search.igh_search.doctype.typesense_settings.typesense_settings.update_product_schema_data",
             "on_trash": "igh_search.igh_search.doctype.typesense_settings.typesense_settings.update_product_schema_data",
         },
         "Related Items": {
             "on_update": "igh_search.igh_search.doctype.typesense_settings.typesense_settings.update_product_schema_data",
             "on_trash": "igh_search.igh_search.doctype.typesense_settings.typesense_settings.update_product_schema_data",
         }
    }
)


doc_events.update(
    {
        "Bin": {
            "on_update": "igh_search.igh_search.product_stock_freshness.on_bin_change",
            "after_insert": "igh_search.igh_search.product_stock_freshness.on_bin_change",
        },
        "Stock Ledger Entry": {
            "on_update": "igh_search.igh_search.product_stock_freshness.on_stock_ledger_entry_change",
            "after_insert": "igh_search.igh_search.product_stock_freshness.on_stock_ledger_entry_change",
        },
        # Editing a commission rate must re-index just that item. on_trash
        # matters as much as on_update: deleting the rate has to push the item
        # back to zero commission in the index, not leave the old value there.
        "Product Based Commission": {
            "on_update": "igh_search.igh_search.doctype.typesense_settings.typesense_settings.update_product_schema_data",
            "on_trash": "igh_search.igh_search.doctype.typesense_settings.typesense_settings.update_product_schema_data",
        },
    }
)

# Commission logging rides ALONGSIDE the Typesense resync on Sales Invoice.
#
# This has to be a merge, not a doc_events.update({"Sales Invoice": {...}}):
# a plain update() replaces the whole "Sales Invoice" entry that the dict
# comprehension above built, which would silently stop the catalogue from
# re-syncing when an invoice is submitted. Frappe's append_hook() extends
# list-valued handlers (frappe/__init__.py), so a list runs both in order.
_COMMISSION_HOOKS = "igh_search.igh_search.commission.sales_invoice_hooks"
doc_events["Sales Invoice"] = {
    "on_submit": [
        doc_events["Sales Invoice"]["on_submit"],
        f"{_COMMISSION_HOOKS}.on_submit",
    ],
    "on_cancel": [
        doc_events["Sales Invoice"]["on_cancel"],
        f"{_COMMISSION_HOOKS}.on_cancel",
    ],
    # Every Sales Team field is allow_on_submit, so attribution can change
    # after submission without firing on_submit again.
    "on_update_after_submit": f"{_COMMISSION_HOOKS}.on_update_after_submit",
}

# Permissions
# -----------
# Row-level visibility for the commission ledger. Returning "" from these means
# "no restriction", so the handlers fail closed with "1=0" for a user who
# cannot be resolved to a Sales Person.

permission_query_conditions = {
    "Sales Commission Entry": "igh_search.igh_search.commission.permissions.commission_entry_query_conditions",
    "Commission Payout": "igh_search.igh_search.commission.permissions.commission_payout_query_conditions",
}

has_permission = {
    "Sales Commission Entry": "igh_search.igh_search.commission.permissions.commission_entry_has_permission",
    "Commission Payout": "igh_search.igh_search.commission.permissions.commission_payout_has_permission",
}

# Scheduled Tasks
# ---------------

scheduler_events = {
    "daily": [
        "igh_search.igh_search.doctype.typesense_settings.typesense_settings.initialize_syncing_items",
        # Safety net: the Sales Invoice commission hooks swallow their errors so
        # a bug can never block invoicing, and frappe.db.set_value writes to a
        # sales team fire no document event at all. This re-runs the same
        # idempotent sync over recent invoices to catch both.
        "igh_search.igh_search.commission.engine.reconcile_recent_invoices",
    ],
    "cron": {
        "*/1 * * * *": [
            "igh_search.igh_search.product_stock_freshness.process_pending_stock_sync_batch"
        ],
        "*/30 * * * *": [
            "igh_search.igh_search.product_stock_freshness.run_stock_drift_repair"
        ],
        # Refresh the filter-panel masters well before their 6h TTL expires, so
        # the ~1.2s rebuild never lands on a user's request.
        "0 */2 * * *": [
            "igh_search.igh_search.api.warm_all_masters_cache"
        ],
    },
}

after_install = [
    "igh_search.igh_search.doctype.typesense_settings.typesense_settings.item_custom_fields"
]

# Testing
# -------

# before_tests = "igh_search.install.before_tests"

# Overriding Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "igh_search.event.get_events"
# }
#
# each overriding function accepts a `data` argument;
# generated from the base implementation of the doctype dashboard,
# along with any modifications made in other Frappe apps
# override_doctype_dashboards = {
# 	"Task": "igh_search.task.get_dashboard_data"
# }

# exempt linked doctypes from being automatically cancelled
#
# auto_cancel_exempted_doctypes = ["Auto Repeat"]

# Ignore links to specified DocTypes when deleting documents
# -----------------------------------------------------------

# ignore_links_on_delete = ["Communication", "ToDo"]

# Request Events
# ----------------
# before_request = ["igh_search.utils.before_request"]
# after_request = ["igh_search.utils.after_request"]

# Job Events
# ----------
# before_job = ["igh_search.utils.before_job"]
# after_job = ["igh_search.utils.after_job"]

# User Data Protection
# --------------------

# user_data_fields = [
# 	{
# 		"doctype": "{doctype_1}",
# 		"filter_by": "{filter_by}",
# 		"redact_fields": ["{field_1}", "{field_2}"],
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_2}",
# 		"filter_by": "{filter_by}",
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_3}",
# 		"strict": False,
# 	},
# 	{
# 		"doctype": "{doctype_4}"
# 	}
# ]

# Authentication and authorization
# --------------------------------

# auth_hooks = [
# 	"igh_search.auth.validate"
# ]

# Automatically update python controller files with type annotations for this app.
# export_python_type_annotations = True

# default_log_clearing_doctypes = {
# 	"Logging DocType Name": 30  # days to retain logs
# }
fixtures = [
     {
        "doctype": "Custom Field",
        "filters": {
            "name": [
                "in",
                [
                    "Item Group-custom_disable"
                ],
            ]
        },
    },
]

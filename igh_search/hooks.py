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
# app_include_css = "/assets/igh_search/css/igh_search.css"
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
         "Item Group": {
             "on_update": "igh_search.igh_search.doctype.typesense_settings.typesense_settings.initialize_syncing_items",
         }
    }
)

# Scheduled Tasks
# ---------------

scheduler_events = {
    "daily": [
        "igh_search.igh_search.doctype.typesense_settings.typesense_settings.initialize_syncing_items"
    ]
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
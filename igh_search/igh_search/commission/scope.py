# Copyright (c) 2026, IHG and contributors
# For license information, please see license.txt

"""Who is allowed to see whose commission.

Implemented natively here rather than imported from another app, so igh_search
carries no cross-app dependency. The User -> Employee.user_id -> Sales
Person.employee traversal is stock ERPNext; ERPNext enforces one Sales Person
per Employee (SalesPerson.validate_employee_id), so the mapping is 1:1.
"""

import frappe

MANAGEMENT_ROLES = ("System Manager", "Managing Director", "CEO", "Sales Manager")
DIVISIONAL_ROLES = ("Divisional Manager", "Regional Manager")
TEAM_HEAD_ROLES = ("Sales Team Head", "Team Lead")


def get_user_role_level(user=None):
	roles = frappe.get_roles(user or frappe.session.user)
	if any(r in roles for r in MANAGEMENT_ROLES):
		return "management"
	if any(r in roles for r in DIVISIONAL_ROLES):
		return "divisional"
	if any(r in roles for r in TEAM_HEAD_ROLES):
		return "team_head"
	return "salesperson"


def get_linked_sales_person(user=None):
	"""Return the Sales Person linked to a user, or None."""
	user = user or frappe.session.user
	employee = frappe.db.get_value("Employee", {"user_id": user}, "name")
	if not employee:
		return None
	return frappe.db.get_value("Sales Person", {"employee": employee}, "name")


def visible_sales_persons(user=None):
	"""Sales Persons this user may see.

	Returns None for "unrestricted". Returns a list otherwise -- and an EMPTY
	list means "nothing", not "everything". Callers must fail closed on []:
	a user with no linked Sales Person record must never fall through to
	seeing the whole company's commission.
	"""
	user = user or frappe.session.user

	if get_user_role_level(user) == "management":
		return None

	me = get_linked_sales_person(user)
	if not me:
		return []

	if get_user_role_level(user) == "salesperson":
		return [me]

	# Divisional / team head: own subtree. Sales Person is a nested set with an
	# index on (lft, rgt), so this is cheap.
	bounds = frappe.db.get_value("Sales Person", me, ["lft", "rgt"], as_dict=True)
	if not bounds or bounds.lft is None or bounds.rgt is None:
		return [me]

	return frappe.get_all(
		"Sales Person",
		filters={"lft": (">=", bounds.lft), "rgt": ("<=", bounds.rgt)},
		pluck="name",
	) or [me]

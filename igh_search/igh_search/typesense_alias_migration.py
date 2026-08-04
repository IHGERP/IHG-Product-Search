"""Reporting for the alias-fronted Typesense collections.

Zero-downtime reindexing needs the names the app queries (``product_v2``,
``product``, …) to be *aliases* pointing at timestamped generations rather than
real collections. Typesense will not let an alias and a collection share a name,
so a site that has never used aliases has to give the name up once.

There is deliberately **no data-copying migration here**. An earlier version
cloned each collection to a generation name first; that meant exporting ~1.6GB
of documents and re-importing them, which fails outright (HTTP 413) and holds
the whole catalogue in memory — the exact problem the streaming sync exists to
avoid.

Instead the transition is folded into the first alias-based full sync
(``swap_alias`` in typesense_settings.py): the new generation is built and
verified first, then the old real collection is dropped and the alias claims the
name. The gap is one API call (~1s) instead of the ~11 minutes the old
delete-then-refill sync was dark for, and it happens once per collection.

So: to migrate, just run a full sync. Use ``status()`` here to check the result.

    bench --site <site> execute \
        igh_search.igh_search.typesense_alias_migration.status
"""

import frappe
from frappe.utils import cint

from igh_search.igh_search.product_search_v2 import create_typesense_client


def _tracked_collections():
    """The collections this site owns — i.e. the ones its full sync rebuilds.

    Derived from the sync write-targets rather than a hardcoded list, so a
    development site never reports on (or touches) production's collections.
    """
    from igh_search.igh_search.doctype.typesense_settings.typesense_settings import (
        _get_v2_sync_collections,
        get_legacy_collection_name,
    )

    names = list(_get_v2_sync_collections()) + [get_legacy_collection_name()]
    seen = []
    for name in names:
        name = (name or "").strip()
        if name and name not in seen:
            seen.append(name)
    return seen


def _is_alias(client, name):
    try:
        client.aliases[name].retrieve()
        return True
    except Exception:
        return False


def status():
    """Report which of this site's collections are alias-fronted yet."""
    client = create_typesense_client()
    existing = {c["name"] for c in client.collections.retrieve()}
    out = []

    for name in _tracked_collections():
        if _is_alias(client, name):
            target = client.aliases[name].retrieve().get("collection_name")
            out.append(
                {
                    "name": name,
                    "type": "alias",
                    "points_to": target,
                    "documents": cint(
                        client.collections[target].retrieve().get("num_documents")
                    ),
                    "zero_downtime": True,
                }
            )
        elif name in existing:
            out.append(
                {
                    "name": name,
                    "type": "plain collection",
                    "documents": cint(
                        client.collections[name].retrieve().get("num_documents")
                    ),
                    "zero_downtime": False,
                    "note": "next full sync converts this to an alias (~1s gap, once)",
                }
            )
        else:
            out.append({"name": name, "type": "missing"})

    for row in out:
        frappe.logger("igh_search").info(f"typesense_alias_status: {row}")
    return out


def generations(alias_name):
    """List the retained generations behind an alias, newest first.

    Useful for rolling back: point the alias at the previous generation with
    ``client.aliases.upsert(alias_name, {"collection_name": <generation>})``.
    """
    client = create_typesense_client()
    prefix = f"{alias_name}_"
    live = None
    if _is_alias(client, alias_name):
        live = client.aliases[alias_name].retrieve().get("collection_name")

    rows = []
    for col in client.collections.retrieve():
        name = col["name"]
        if name.startswith(prefix) and name[len(prefix):].isdigit():
            rows.append(
                {
                    "collection": name,
                    "documents": col["num_documents"],
                    "live": name == live,
                }
            )
    return sorted(rows, key=lambda r: r["collection"], reverse=True)

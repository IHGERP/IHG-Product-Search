"""Deterministic literal catalogue matching, independent of Frappe."""
import re


def is_compact_query(value):
    return bool(re.fullmatch(r"[A-Za-z0-9._/-]{3,}", str(value or "").strip()))


def literal_parameters(query, field_count=10):
    if not is_compact_query(query):
        return {}
    return {
        "prefix": ",".join(["true"] * field_count),
        "infix": ",".join(["always", "always"] + ["off"] * (field_count - 2)),
        "num_typos": ",".join(["0"] * field_count),
        "exhaustive_search": True,
        "drop_tokens_threshold": 0,
        "prioritize_exact_match": True,
    }

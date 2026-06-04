# Phase 4 — Product Assistant (grounded, agentic chatbot).
# A senior lighting/electrical sales engineer that answers using IHG's LIVE catalog
# via tools (never inventing SKUs) plus domain knowledge. Multi-turn: the frontend
# passes the running message list back each turn.
#
# Grounding contract: any product/stock/price/spec claim must come from a tool
# result. The system prompt forbids fabricating item codes or specs.
#
# Tools (all reuse existing, tested code):
#   search_products(query, in_stock_only)      -> hybrid semantic search
#   get_product(item_code)                      -> one product's full record
#   find_alternatives(item_code, mode)          -> equivalents / cross-sell
#   check_driver_requirement(item_code)         -> is an external driver needed?
#   find_driver(item_code)                      -> suitable LED drivers for a fixture
# Provider: OpenAI (tool calling) with Groq fallback for plain (toolless) turns.

import copy
import json

import frappe
import requests
from frappe.utils import cint, cstr

from igh_search.igh_search.ai_product_search import (
    get_openai_api_key, get_openai_model, OPENAI_API_URL,
)

MAX_TOOL_ROUNDS = 4
DEFAULT_RESULT_FIELDS = (
    "item_code", "item_name", "brand", "category_list", "power", "color_temp",
    "ip_rate", "beam_angle", "lumen_output", "rate", "stock", "in_stock",
)
SEARCH_CANDIDATES = 10  # pool fetched per search; top 8 are returned to the model

SYSTEM_PROMPT = """You are the IHG Product Assistant — a senior lighting and electrical sales engineer for an internal sales team in the UAE. You help reps find products, compare options, size drivers/accessories, and answer technical lighting/electrical questions.

GROUNDING RULES (critical):
- For anything about IHG's actual products, stock, price, or specs, you MUST call a tool. NEVER invent an item code, price, stock figure, or spec.
- Quote item_code and item_name exactly as returned by tools. If a tool returns nothing, say so plainly and suggest how to refine — do not fabricate.
- Prices are in AED. Treat stock figures as point-in-time.
- SEARCHING: keep the search query short and spec-focused (e.g. "15W 3000K IP65 spotlight"), not a full sentence. Do NOT set in_stock_only unless the rep explicitly asks for in-stock / available items — otherwise good matches that are temporarily out of stock are needlessly hidden. If the rep asks for the cheapest/lowest price, just search normally; the catalog already ranks sensibly.

DOMAIN KNOWLEDGE (use to advise, clearly separated from catalog facts):
- CCT: 2700-3000K warm (hospitality, residential), 4000K neutral (office/retail), 5000-6500K cool/daylight (industrial, task).
- IP: IP20 indoor/dry, IP44 splash, IP65 outdoor/wet, IP67/68 submersible. Outdoor/garden/facade => IP65+.
- Driver sizing: driver wattage must exceed total LED load (~20% headroom); match constant-voltage (strips, e.g. 12V/24V) vs constant-current (mA, many fixtures); match dimming protocol (TRIAC/0-10V/DALI).
- DRIVER QUESTIONS: for "is a driver required for X", "find/suggest a driver for X", or "what driver fits X", ALWAYS call check_driver_requirement / find_driver with the item_code — never judge it yourself. If the tool says driver_required is "unknown", tell the rep the spec is missing and to verify the datasheet; do not invent a driver. Fixtures with an integrated driver (name says "with driver" or code ends .DR) need none.
- Beam: narrow (10-24°) accent/spot, medium (36°) general, wide (60°+) wash/flood.
- Lumens vs watts: prefer lm for brightness; modern LED ~ 100-130 lm/W.

ACTIONS:
- The rep can add products to a cart (which becomes a quotation). When they say "add X", "put 10 of Y in the cart", or "quote these", call add_to_cart with the exact item_code(s) and quantities. If quantity isn't stated, default to 1. Briefly confirm what you added.
- ENQUIRY PASTE: if the rep pastes a multi-line customer enquiry (several products/specs at once), treat each line as a separate need — search per line, then summarise which line matched which item_code and offer to add them all to a quote.

STYLE: concise, practical, sales-oriented. The UI renders the matched products as visual CARDS below your message (with item_code, name, price, stock), so DO NOT repeat a long numbered list of those same details — it is redundant. Instead give a 1-2 sentence summary (e.g. "Here are 3 compatible 220mA drivers in stock — the Lumitronix is the closest match.") and call out only the single best pick or any caveat. Keep formatting light. Stock and price come live from the catalog at query time — state that when relevant. End with a brief next step (add to quote, alternatives)."""

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_products",
            "description": "Search IHG's live catalog for products matching a natural-language need or specs. Returns matching products with specs, price and stock.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Natural language need or spec string, e.g. '15W 3000K IP65 outdoor spotlight' or 'warm light for a hotel lobby'."},
                    "in_stock_only": {"type": "boolean", "description": "Set true ONLY if the rep explicitly asks for in-stock / available / ready-to-ship items. Default false — leave unset for normal searches so good out-of-stock matches still show (down-ranked)."},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_product",
            "description": "Get the full record (specs, price, stock) for one product by its exact item_code.",
            "parameters": {
                "type": "object",
                "properties": {"item_code": {"type": "string"}},
                "required": ["item_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "find_alternatives",
            "description": "Given an item_code, find either equivalent alternatives (same category, e.g. for an out-of-stock item) or complementary cross-sell items (drivers, profiles, accessories).",
            "parameters": {
                "type": "object",
                "properties": {
                    "item_code": {"type": "string"},
                    "mode": {"type": "string", "enum": ["alternatives", "cross_sell"]},
                },
                "required": ["item_code", "mode"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "check_driver_requirement",
            "description": "Determine whether a fixture needs an EXTERNAL LED driver. Returns driver_required (true/false/unknown), the parsed electrical load (mA / voltage), and the reason. Use for 'is a driver required for <code>?'.",
            "parameters": {
                "type": "object",
                "properties": {"item_code": {"type": "string"}},
                "required": ["item_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "find_driver",
            "description": "Find LED drivers compatible with a fixture's electrical load (matches output current mA / voltage / power headroom). Use for 'find/suggest a suitable driver for <code>'. Returns no drivers if the fixture has an integrated driver or its load spec is unknown.",
            "parameters": {
                "type": "object",
                "properties": {"item_code": {"type": "string"}},
                "required": ["item_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "add_to_cart",
            "description": "Add one or more products to the rep's cart (the basis for a quotation). Use when the rep says 'add X', 'put 10 of Y in the cart', 'quote these'. Confirm the exact item_code(s) first if ambiguous.",
            "parameters": {
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "description": "Products to add.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "item_code": {"type": "string"},
                                "qty": {"type": "number", "description": "Quantity (default 1)."},
                            },
                            "required": ["item_code"],
                        },
                    }
                },
                "required": ["items"],
            },
        },
    },
]


def _slim(doc):
    return {k: doc.get(k) for k in DEFAULT_RESULT_FIELDS if doc.get(k) not in (None, "")}


# Real category vocabulary (from the live facet audit) — the extractor may only
# pick a category from this set, so a generic word like "light" never becomes a
# bogus category lock the way the deterministic parser did.
_EXTRACT_CATEGORIES = [
    "SPOT LIGHT", "DOWN LIGHT", "CEILING RECESSED LIGHT", "PANEL LIGHT", "TRACK LIGHT",
    "LINEAR LIGHT", "WALL LIGHT", "WALL WASHER", "FLOOD LIGHT", "GARDEN LIGHT",
    "BOLLARD LIGHT", "INGROUND LIGHT", "STRIP LIGHT", "NEON FLEX", "PENDANT LIGHT",
    "CHANDELIERS", "CEILING LIGHT", "SUSPENDED LAMP", "TABLE LAMP", "FLOOR LAMP",
    "LED DRIVERS", "ALUMINUM PROFILES", "BULBS & LAMPS", "MAGNETIC LIGHT", "STREET LIGHT",
    "UNDERWATER LIGHT", "PROJECTOR LIGHT", "BRICK LIGHT", "SWITCHES AND SOCKETS",
    "LIGHTING ACCESSORIES", "STREET LIGHT", "TASK LIGHT", "INDUSTRIAL LIGHT",
]


def _extract_query_constraints(query):
    """Cheap, cached LLM spec-extractor. One small call (cheap model) → structured
    constraints. Cached 24h by normalized query. Returns None on any failure so the
    caller falls back to the deterministic parser. This is the fast two-tier 'deep'
    understanding without the heavy resolve_ai_search_intent(mode='fast') pipeline."""
    q = cstr(query).strip()
    if not q:
        return None
    cache = frappe.cache()
    ck = "ai_assist_extract|" + q.lower()[:180]
    cached = cache.get_value(ck)
    if cached is not None:
        try:
            return json.loads(cached)
        except Exception:
            pass
    key = get_openai_api_key()
    if not key:
        return None
    system = (
        "Extract structured lighting/electrical search constraints from a sales query. "
        "Return ONLY JSON with keys: category (one of " + json.dumps(_EXTRACT_CATEGORIES) +
        " or null), color_temp (e.g. \"3000K\"; warm=3000K, neutral=4000K, cool/daylight=6000K, or null), "
        "ip_rate (e.g. \"IP65\"; outdoor/wet/garden/facade => IP65, or null), power (e.g. \"15W\" or null), "
        "clean_query (the core product noun phrase for semantic search). "
        "Rules: pick a category ONLY if the specific product type is clear — never map a generic word "
        "like 'light' to a category. Only include values clearly implied by the query."
    )
    try:
        r = requests.post(
            OPENAI_API_URL,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"model": get_openai_model(), "temperature": 0, "max_tokens": 160,
                  "response_format": {"type": "json_object"},
                  "messages": [{"role": "system", "content": system},
                               {"role": "user", "content": q}]},
            timeout=15,
        )
        r.raise_for_status()
        out = json.loads(r.json()["choices"][0]["message"]["content"])
        if not isinstance(out, dict):
            return None
        cache.set_value(ck, json.dumps(out), expires_in_sec=86400)
        return out
    except Exception:
        return None


def _intent_from_extractor(query):
    """Build a deterministic-shaped intent, then correct it with the cheap extractor:
    category comes ONLY from the real vocabulary (kills the bogus 'LIGHT' lock);
    specs are added as SOFT boosts (rank, don't exclude) to preserve recall."""
    ex = _extract_query_constraints(query)
    if not isinstance(ex, dict):
        return None
    from igh_search.igh_search.ai_product_search import resolve_ai_search_intent
    base = resolve_ai_search_intent(message=query, mode="deterministic")
    filters = base.get("filters") or {}
    cat = cstr(ex.get("category") or "").strip().upper()
    filters["category_list"] = [cat] if cat in {c.upper() for c in _EXTRACT_CATEGORIES} else []
    hard = ["category_list"] if filters["category_list"] else []
    for fld, key in (("color_temp", "color_temp"), ("ip_rate", "ip_rate"), ("power", "power")):
        val = cstr(ex.get(key) or "").strip()
        if val:
            filters[fld] = [val]
            hard.append(fld)  # user EXPLICITLY stated it -> hard filter (precision)
    base["filters"] = filters
    # explicit category + specs are hard (precision); tier-2 fallback in _lean_search
    # clears these to recover recall when an over-constrained query returns 0.
    base.setdefault("hard_constraints", {})["filters"] = hard
    cq = cstr(ex.get("clean_query") or "").strip()
    if cq:
        base["query"] = cq
    return base


def _lean_search(query, in_stock_only=False):
    """Fast catalog search for the chat. The assistant LLM has already turned the
    user's words into a clean spec query, so we DON'T re-run the intent LLM
    (mode='deterministic') and we DON'T run the heavy 3-stage relaxation loop that
    ai_search_products_v2 uses. Just: deterministic intent -> one hybrid search;
    if it's empty, ONE relaxed retry that demotes the hard spec filters to soft
    boosts (recall). Hybrid (semantic) is on, so conceptual queries work too."""
    from igh_search.igh_search.ai_product_search import (
        resolve_ai_search_intent, execute_intent_search,
    )
    from igh_search.igh_search.product_search_v2 import search_products_v2

    # Lightweight cached LLM spec-extractor (cheap model, tiny prompt) replaces the
    # crude deterministic parser (which invented a bogus "LIGHT" category) WITHOUT
    # the heavy resolve_ai_search_intent(mode="fast") pipeline (~5s). Falls back to
    # deterministic if extraction is unavailable. See _extract_query_constraints.
    intent = _intent_from_extractor(query) or resolve_ai_search_intent(message=query, mode="deterministic")
    r = execute_intent_search(intent, page_length=SEARCH_CANDIDATES, feature_flag_override=1)
    # Tier 2: demote the hard spec filters to soft boosts (recall on over-constrained specs).
    if cint(r.get("found")) == 0:
        try:
            relaxed = copy.deepcopy(intent)
            relaxed.setdefault("hard_constraints", {})["filters"] = []
            r = execute_intent_search(relaxed, page_length=SEARCH_CANDIDATES, feature_flag_override=1)
        except Exception:
            pass
    # Tier 3: pure semantic — query only, NO filters. Bypasses a bogus category/spec
    # lock from the crude deterministic parser (e.g. "light" -> category "LIGHT") and
    # lets the hybrid vector index answer conceptual queries ("warm light for a lobby").
    if cint(r.get("found")) == 0:
        try:
            r = search_products_v2(query=query, filters={}, page_length=SEARCH_CANDIDATES,
                                   feature_flag_override=1, use_hybrid=1)
        except Exception:
            pass
    hits = [_slim(h.get("document", {})) for h in (r.get("hits") or [])]
    if in_stock_only:
        in_stock = [h for h in hits if h.get("in_stock")]
        hits = in_stock or hits  # prefer in-stock, but never hide all matches (down-rank, don't exclude)
    return {"found": cint(r.get("found")), "products": hits[:8]}


def _run_tool(name, args):
    """Execute a tool call against the existing search/product code. Returns a
    compact JSON-able result (token-budget friendly)."""
    try:
        if name == "search_products":
            return _lean_search(cstr(args.get("query")), cint(args.get("in_stock_only")))

        if name == "get_product":
            from igh_search.igh_search.product_search_v2 import get_product_document
            doc = get_product_document(cstr(args.get("item_code")), include_inactive=1)
            return {"product": _slim(doc)} if doc else {"error": "not found"}

        if name == "find_alternatives":
            from igh_search.igh_search.product_search_v2 import get_product_alternatives_v2
            r = get_product_alternatives_v2(
                cstr(args.get("item_code")),
                mode=cstr(args.get("mode") or "alternatives"),
                limit=6, feature_flag_override=1,
            )
            return {"mode": r.get("mode"),
                    "results": [{"reason": x.get("reason"), **_slim(x.get("document", {}))}
                                for x in (r.get("results") or [])[:6]]}

        if name == "check_driver_requirement":
            from igh_search.igh_search.product_search_v2 import analyze_driver_requirement
            r = analyze_driver_requirement(cstr(args.get("item_code")), feature_flag_override=1)
            return {"driver_required": r.get("driver_required"), "reason": r.get("reason"),
                    "load": r.get("load")}

        if name == "find_driver":
            from igh_search.igh_search.product_search_v2 import find_suitable_drivers
            r = find_suitable_drivers(cstr(args.get("item_code")), limit=6, feature_flag_override=1)
            return {"driver_required": r.get("driver_required"), "reason": r.get("reason"),
                    "load": r.get("load"),
                    "drivers": [{"match_reason": d.get("match_reason"), **_slim(d.get("document", {}))}
                                for d in (r.get("drivers") or [])[:6]]}

        if name == "add_to_cart":
            from igh_search.igh_search.api import insert_cart_items
            requested = args.get("items") or []
            added, failed = [], []
            last = None
            for entry in requested:
                code = cstr(entry.get("item_code")).strip()
                qty = entry.get("qty") or 1
                if not code:
                    continue
                res = insert_cart_items(item_code=code, qty=qty)
                if (res or {}).get("status") == "success":
                    added.append({"item_code": code, "qty": qty})
                    last = res
                else:
                    failed.append({"item_code": code, "error": (res or {}).get("message", "failed")})
            return {"added": added, "failed": failed,
                    "cart_total": (last or {}).get("grand_total"),
                    "message": f"Added {len(added)} item(s) to the cart." if added else "Nothing added."}

        return {"error": f"unknown tool {name}"}
    except Exception as exc:
        return {"error": cstr(exc)[:200]}


def _openai_chat(messages, with_tools=True):
    key = get_openai_api_key()
    if not key:
        raise ValueError("OpenAI API key is not configured")
    body = {"model": get_openai_model(), "messages": messages, "temperature": 0.2, "max_tokens": 700}
    if with_tools:
        body["tools"] = TOOLS
        body["tool_choice"] = "auto"
    r = requests.post(
        OPENAI_API_URL,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json=body, timeout=40,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]


@frappe.whitelist()
def product_assistant_chat(message=None, history=None, feature_flag_override=0):
    """One assistant turn. `message` = the new user message. `history` = prior
    turns as a JSON list of {role, content}. Returns the reply plus the products
    the assistant grounded on (for the UI to render as cards)."""
    if frappe.session.user == "Guest":
        frappe.throw("Authentication required")
    message = cstr(message).strip()
    if not message:
        return {"reply": "How can I help you find a product today?", "products": []}

    try:
        prior = json.loads(history) if isinstance(history, str) else (history or [])
    except Exception:
        prior = []

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for turn in prior[-10:]:  # cap context
        role = turn.get("role")
        content = cstr(turn.get("content"))
        if role in ("user", "assistant") and content:
            messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": message})

    grounded_products = []
    tool_trace = []
    cart_added = []  # items the assistant added to the cart this turn (for a UI toast)
    for _round in range(MAX_TOOL_ROUNDS):
        reply = _openai_chat(messages, with_tools=True)
        tool_calls = reply.get("tool_calls") or []
        if not tool_calls:
            return {
                "reply": cstr(reply.get("content")).strip(),
                "products": grounded_products[:12],
                "tool_trace": tool_trace,
                "cart_added": cart_added,
            }
        # assistant turn that requested tools
        messages.append({"role": "assistant", "content": reply.get("content") or "",
                         "tool_calls": tool_calls})
        for call in tool_calls:
            fn = call.get("function", {})
            name = fn.get("name")
            try:
                args = json.loads(fn.get("arguments") or "{}")
            except Exception:
                args = {}
            result = _run_tool(name, args)
            tool_trace.append({"tool": name, "args": args})
            for key in ("products", "results", "drivers"):
                for p in result.get(key, []) or []:
                    if p.get("item_code"):
                        grounded_products.append(p)
            if isinstance(result, dict) and result.get("product"):
                grounded_products.append(result["product"])
            if name == "add_to_cart" and result.get("added"):
                cart_added.extend(result["added"])
            messages.append({"role": "tool", "tool_call_id": call.get("id"),
                             "content": json.dumps(result, ensure_ascii=True)})

    # Tool-round budget exhausted: ask for a final answer without more tools.
    final = _openai_chat(messages, with_tools=False)
    return {"reply": cstr(final.get("content")).strip(),
            "products": grounded_products[:12], "tool_trace": tool_trace,
            "cart_added": cart_added}

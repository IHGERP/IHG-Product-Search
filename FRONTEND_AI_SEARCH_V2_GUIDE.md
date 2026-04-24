# IGH Search V2 Frontend Integration Guide

This document is the frontend handoff for the current `igh_search` backend, including:

- standard V2 product listing
- AI-powered V2 search
- search analytics / feedback tracking
- result quality diagnostics

The goal is to help the frontend team wire the backend correctly without reintroducing old assumptions from V1 or early V2.

## 1. Core Principles

### 1.1 Use backend proxy APIs only
Do not call Typesense directly from the frontend for V2.

All reads must go through backend APIs in `igh_search`.

### 1.2 Treat search results as lean list payloads
The Typesense index stores many fields, but `search_products_v2` and `ai_search_products_v2` should be treated as list/search APIs, not full detail APIs.

Render compact list results from the returned hit document. If any screen needs extra data later, load it lazily from an appropriate backend endpoint.

### 1.3 AI search is a smart retrieval flow, not just intent parsing
There are now two AI-related APIs:

- `ai_product_search`
  - intent-only
  - returns parsed query / filters / sort
  - useful for debugging or advanced UX flows

- `ai_search_products_v2`
  - primary AI search API
  - parses intent
  - runs V2 search
  - may relax weak queries
  - reranks results
  - returns final hits + diagnostics + `search_event_id`

For the main AI search experience, use `ai_search_products_v2`.

## 2. Backend APIs

## 2.1 Standard V2 search
Endpoint:

`igh_search.igh_search.api.search_products_v2`

Use for:

- normal search box queries
- filtering
- sorting
- pagination
- autocomplete result pages

Request params:

- `query`
- `filters`
- `sort_by`
- `page`
- `page_length`
- `include_inactive`
- `item_code_hint`
- `feature_flag_override`

Notes:

- `filters` should be sent as a JSON stringified object.
- `item_code_hint` is now supported correctly when `query` is empty.
- explicit sorts are honored by backend.

## 2.2 Suggest API
Endpoint:

`igh_search.igh_search.api.suggest_products_v2`

Use for:

- autocomplete suggestions
- typeahead dropdown

Params:

- `query`
- `limit`
- `feature_flag_override`

## 2.3 Similar / alternatives API
Endpoint:

`igh_search.igh_search.api.get_similar_products_v2`

Use for:

- similar products drawer
- alternatives panel

Params:

- `item_code`
- `limit`
- `include_manual`
- `feature_flag_override`

## 2.4 AI intent-only API
Endpoint:

`igh_search.igh_search.api.ai_product_search`

Use for:

- optional “understand my request” flow
- intent debugging
- developer tooling

Do not use this as the final user-facing AI retrieval endpoint unless you intentionally want a two-step flow.

## 2.5 AI smart retrieval API
Endpoint:

`igh_search.igh_search.api.ai_search_products_v2`

Use for:

- AI search entry
- “search by description / search by need”
- natural-language product finding

Params:

- `message`
- `page_context`
- `page`
- `page_length`
- `include_inactive`
- `feature_flag_override`

This API is now the primary AI search API.

## 2.6 AI quality report API
Endpoint:

`igh_search.igh_search.api.get_ai_product_search_quality_report`

Use for:

- admin/internal QA dashboard
- AI quality monitoring

Restricted to `System Manager`.

## 2.7 AI benchmark API
Endpoint:

`igh_search.igh_search.api.evaluate_ai_product_search_benchmark`

Use for:

- internal QA only
- release validation

Restricted to `System Manager`.

## 2.8 AI search tracking APIs
These are important if you want the AI system to get smarter from real usage.

Endpoints:

- `igh_search.igh_search.api.track_ai_search_click`
- `igh_search.igh_search.api.track_ai_search_shortlist`
- `igh_search.igh_search.api.track_ai_search_quotation`
- `igh_search.igh_search.api.track_ai_search_reformulation`

These APIs attach outcome signals to a prior AI search using `search_event_id`.

Without wiring these, feedback-based reranking will stay weak.

## 3. Standard Search Request Contract

Recommended request shape for `search_products_v2`:

```json
{
  "query": "ip65 3000k downlight",
  "filters": "{\"brand\":[\"LUMIBRIGHT\"],\"mounting\":[\"Surface\"],\"stock_range\":{\"min\":1}}",
  "sort_by": "",
  "page": 1,
  "page_length": 20,
  "include_inactive": 0,
  "item_code_hint": "",
  "feature_flag_override": 0
}
```

### 3.1 Supported sort values
Frontend should only send these:

- `""`
- `creation:asc`
- `creation:desc`
- `rate:asc`
- `rate:desc`
- `offer_rate:asc`
- `offer_rate:desc`
- `stock:asc`
- `stock:desc`
- `sold_last_30_days:asc`
- `sold_last_30_days:desc`
- `discount_percentage:asc`
- `discount_percentage:desc`
- `priority_score:asc`
- `priority_score:desc`
- `popularity_score:asc`
- `popularity_score:desc`
- `business_score:asc`
- `business_score:desc`
- `modified_ts:asc`
- `modified_ts:desc`

Do not send old sort aliases like:

- `creation_on:desc`
- `creation_on:asc`

### 3.2 Supported filter fields
Use exact V2 field names only:

- `brand`
- `item_group`
- `category_list`
- `product_type`
- `power`
- `color_temp`
- `ip_rate`
- `beam_angle`
- `mounting`
- `body_finish`
- `input_voltage`
- `output_voltage`
- `output_current`
- `lamp_type`
- `material`
- `warranty`
- `variant_of`
- `in_stock`

Range filters:

- `rate_range`
- `offer_rate_range`
- `discount_percentage_range`
- `stock_range`
- `sold_last_30_days_range`
- `inventory_value_range`
- `priority_score_range`
- `popularity_score_range`
- `business_score_range`
- `power_value_range`
- `color_temp_kelvin_range`
- `ip_rating_numeric_range`

Do not send legacy keys:

- `color_temp_`
- `input`
- `warranty_`
- `price_range`

Use:

- `color_temp`
- `input_voltage`
- `warranty`
- `rate_range`

### 3.3 Filter semantics
Backend behavior:

- different filter fields combine with `AND`
- multiple values in one field combine with `OR`
- numeric ranges are inclusive

Example:

```json
{
  "brand": ["LUMIBRIGHT", "ACME"],
  "mounting": ["Surface"],
  "stock_range": { "min": 1, "max": 100 }
}
```

Meaning:

- brand is LUMIBRIGHT or ACME
- and mounting is Surface
- and stock is between 1 and 100 inclusive

## 4. Standard Search Response Contract

Frontend should expect:

- `hits`
- `found`
- `facet_counts`
- `applied_filters`
- `query_debug`

Each `hit` contains `document`.

Treat `facet_counts` as raw Typesense facet output.

## 4.1 Lean list-result fields
The result document is intentionally lean. Use it for result cards / table rows.

Frontend should rely on these fields for standard list rendering:

- `item_code`
- `item_name`
- `brand`
- `category_list`
- `item_group`
- `product_type`
- `image`
- `stock_uom`
- `rate`
- `offer_rate`
- `discount_percentage`
- `stock`
- `in_stock`
- `priority_score`
- `popularity_score`
- `business_score`
- `is_active`
- `variant_of`
- `parent_item_code`
- `parent_item_name`
- `power`
- `color_temp`
- `ip_rate`
- `beam_angle`
- `mounting`
- `lamp_type`
- `material`
- `body_finish`
- `warranty`
- `input_voltage`
- `output_voltage`
- `output_current`
- `spec_summary`
- `manual_alternative_codes`
- `manual_related_codes`

Do not assume the full indexed Typesense document is returned in every hit.

## 4.2 Pricing rules
Backend pricing logic:

- `rate` = `Item Price` from price list `RRP`
- `offer_rate` = `Item Price` from price list `Promo` if present
- `discount_percentage` is computed from `RRP` vs `Promo`

Frontend rendering rule:

- if `offer_rate > 0` and `offer_rate < rate`
  - show `offer_rate` as active price
  - show `rate` as struck-through price
  - show `discount_percentage`
- otherwise
  - show only `rate`

## 4.3 Stock rules
Use:

- `stock`
- `in_stock`
- `stock_uom`

Examples:

- `In stock: 24 Nos`
- `Out of stock`

If the user chooses `stock:desc` or `stock:asc`, the backend now supports strict field-first sort for AI-originated searches and explicit sort handling for V2.

## 4.4 Image rules
Use `document.image`.

Do not use `website_image_url` in V2.

If the image field is:

- empty: show placeholder
- `/files/...`: render directly or prefix the ERP origin when needed
- `/private/files/...`: treat as private and use the appropriate authenticated flow if required

## 5. AI Search Response Contract

`ai_search_products_v2` returns normal search result fields plus AI-specific fields.

Frontend should expect:

- `hits`
- `found`
- `facet_counts`
- `resolved_intent`
- `applied_filters`
- `applied_sort`
- `applied_relaxations`
- `explanation`
- `query_debug`
- `search_event_id`
- `quality_signals`

## 5.1 `resolved_intent`
Current structure:

```json
{
  "intent_class": "spec_match",
  "query_mode": "fast_hybrid",
  "provider": "openai",
  "llm_used": true,
  "signals": ["sku", "sort:phrase"],
  "confidence_map": {},
  "hard_constraints": {
    "item_code_hint": false,
    "ranges": {},
    "filters": []
  },
  "derived_specs": {}
}
```

Intent classes currently used:

- `sku_lookup`
- `spec_match`
- `stock_priority`
- `discount_priority`
- `recent_products`
- `alternatives`
- `general_search`

Frontend can use `intent_class` for badges, telemetry, or UI hints, but should not hard-block behavior on it.

## 5.2 `applied_relaxations`
This is a list of backend recovery steps when the AI search was too strict.

Example:

```json
[
  "Relaxed low-confidence filters: brand, mounting"
]
```

Recommended frontend behavior:

- do not show by default in the main UI
- expose in developer/debug mode
- optionally show a subtle note like:
  - `Search broadened to improve results`

## 5.3 `quality_signals`
Current structure includes fields like:

- `result_quality`
- `retry_stage`
- `deterministic_only`
- `feedback_reranked`
- `compatibility_reranked`

Recommended usage:

- admin/debug panel
- QA visibility
- not primary end-user UI

## 5.4 `search_event_id`
This is critical for learning and feedback.

Whenever the frontend uses `ai_search_products_v2`, preserve the returned `search_event_id`.

You should pass that ID into follow-up tracking APIs when the user:

- clicks a result
- shortlists an item
- creates a quotation from the result
- reformulates the query

Without `search_event_id`, the backend cannot associate outcomes to the originating AI search.

## 6. AI Search Frontend Flow

Recommended AI search flow:

1. User types a natural-language request
2. Frontend calls `ai_search_products_v2`
3. Render returned hits immediately
4. Preserve:
   - `search_event_id`
   - `resolved_intent`
   - `quality_signals`
5. On user action:
   - click -> `track_ai_search_click`
   - shortlist -> `track_ai_search_shortlist`
   - quotation -> `track_ai_search_quotation`
   - reformulation -> `track_ai_search_reformulation`

## 6.1 Example AI search request

```json
{
  "message": "high stock surface lights under 500",
  "page_context": {
    "route": "/list",
    "category": "",
    "brand": "",
    "search": ""
  },
  "page": 1,
  "page_length": 20,
  "include_inactive": 0,
  "feature_flag_override": 0
}
```

## 6.2 Example AI search response shape

```json
{
  "hits": [],
  "found": 12,
  "facet_counts": [],
  "resolved_intent": {
    "intent_class": "stock_priority",
    "provider": "openai",
    "llm_used": true
  },
  "applied_filters": {},
  "applied_sort": "stock:desc",
  "applied_relaxations": [],
  "explanation": "Parsed natural-language request into structured V2 filters.",
  "search_event_id": "AIQEV-00001",
  "quality_signals": {
    "result_quality": "strong",
    "retry_stage": 0,
    "deterministic_only": false,
    "feedback_reranked": false,
    "compatibility_reranked": true
  },
  "query_debug": {}
}
```

## 7. Tracking API Integration

These APIs should usually be called in the background after a successful UI action.

## 7.1 Track click

Call when the user opens or selects a result from AI search.

Endpoint:

`igh_search.igh_search.api.track_ai_search_click`

Payload:

```json
{
  "search_event_id": "AIQEV-00001",
  "item_code": "DL-100"
}
```

## 7.2 Track shortlist

Call when the user adds an AI result to quotation shortlist.

Endpoint:

`igh_search.igh_search.api.track_ai_search_shortlist`

Payload:

```json
{
  "search_event_id": "AIQEV-00001",
  "item_code": "DL-100"
}
```

## 7.3 Track quotation

Call when the user creates or confirms quotation flow from the AI result.

Endpoint:

`igh_search.igh_search.api.track_ai_search_quotation`

Payload:

```json
{
  "search_event_id": "AIQEV-00001",
  "item_code": "DL-100",
  "quotation": "QTN-00045"
}
```

## 7.4 Track reformulation

Call when the user searches again after a weak or zero-result AI search.

Endpoint:

`igh_search.igh_search.api.track_ai_search_reformulation`

Payload:

```json
{
  "search_event_id": "AIQEV-00001",
  "reformulated_message": "ip65 3000k surface downlight"
}
```

Recommended trigger:

- user submits a new AI message after the previous one
- previous search had weak or zero results
- or previous search returned `applied_relaxations`

## 8. UI Recommendations

## 8.1 Standard V2 listing
Keep the standard `/list?search_v2=1` results:

- dense
- text-first
- SKU-focused
- fast

Do not turn it into a heavy card grid.

Recommended visible row fields:

- SKU
- name
- brand
- category
- price / promo price
- stock
- compact spec chips
- similar / alternatives action
- shortlist action

## 8.2 AI search UX
Recommended options:

- separate “AI Search” input mode
- or a toggle/tab above the search input

Suggested UX:

- standard search stays deterministic/manual
- AI search handles natural-language requests
- both lead to the same result list component

## 8.3 Debug / QA mode
For internal QA, add an expandable debug block showing:

- `resolved_intent.intent_class`
- `resolved_intent.provider`
- `resolved_intent.llm_used`
- `applied_sort`
- `applied_relaxations`
- `quality_signals`

Do not show this by default to all users.

## 8.4 Similar products
Continue to fetch similar products on demand only, not preloaded for every row.

This keeps the listing fast.

## 8.5 Inactive products
Keep hidden by default.

Only expose `include_inactive` if you intentionally support it for admin roles.

## 9. Error Handling

## 9.1 Standard search
Handle:

- invalid filters payload
- unauthorized V2 access
- empty results
- timeout/network error

Recommended messaging:

- empty: `No matching SKUs/products found`
- invalid filters: `Search filters could not be applied`
- backend disabled: `V2 search unavailable`

## 9.2 AI search
Handle:

- no results
- weak results
- backend relaxation
- provider fallback

Recommended behavior:

- if results are weak but non-zero, show results normally
- optionally display:
  - `Search broadened to improve results`
- if AI endpoint fails, allow fallback to standard `search_products_v2`

## 10. Frontend State Management

Persist standard V2 listing state in URL where possible:

- `q`
- `sort_by`
- `page`
- `page_length`
- filter params
- `search_v2`

For AI search:

- you may keep the raw `message` in UI state
- do not need to serialize all AI diagnostics to URL
- keep `search_event_id` in component state or a request-local store

## 11. Implementation Priorities

Recommended order for the frontend team:

1. Ensure standard V2 list integration uses the latest sort/filter contract
2. Add AI search mode using `ai_search_products_v2`
3. Preserve and propagate `search_event_id`
4. Wire click / shortlist / quotation / reformulation tracking
5. Add QA/debug panel for internal testing
6. Add admin metrics view using `get_ai_product_search_quality_report`

## 12. Acceptance Checklist

The frontend implementation is complete when:

- standard V2 search uses only current supported sort values
- standard V2 search uses only normalized V2 filter keys
- AI search uses `ai_search_products_v2` instead of chaining `ai_product_search` manually
- `search_event_id` is preserved after every AI search
- click tracking is wired
- shortlist tracking is wired
- quotation tracking is wired
- reformulation tracking is wired
- result rows still use lean payloads
- price rendering follows `RRP` / `Promo` rules
- image rendering uses `document.image`
- debug/QA information is available for internal testing

## 13. Important Do-Not-Do List

Do not:

- call Typesense directly from frontend
- assume full product detail is present in every search hit
- send legacy filter keys
- send unsupported sort keys
- drop `search_event_id`
- skip outcome tracking for AI searches
- depend on `ai_product_search` alone for final AI retrieval UX

## 14. Suggested Backend Contracts to Use in Code Comments

Use these short summaries in frontend code comments or service wrappers:

### Standard search
`search_products_v2` = lean Typesense-backed product listing API

### AI search
`ai_search_products_v2` = natural-language product retrieval API with intent parsing, search execution, relaxation, reranking, and tracking ID output

### AI tracking
`track_ai_search_*` = feedback loop endpoints that make future AI ranking smarter

## 15. File References

Backend implementation references:

- [ai_product_search.py](/home/erp-ihg/frappe-bench/apps/igh_search/igh_search/igh_search/ai_product_search.py:1)
- [api.py](/home/erp-ihg/frappe-bench/apps/igh_search/igh_search/igh_search/api.py:221)
- [AI Product Search Event DocType](/home/erp-ihg/frappe-bench/apps/igh_search/igh_search/igh_search/doctype/ai_product_search_event/ai_product_search_event.json:1)
- [AI Benchmark Data](/home/erp-ihg/frappe-bench/apps/igh_search/igh_search/igh_search/data/ai_product_search_benchmark.json:1)


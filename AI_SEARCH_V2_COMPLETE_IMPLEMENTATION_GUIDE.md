# AI Search V2 Complete Implementation Guide

This document is the complete project-level guide for the current `igh_search` V2 and AI search system.

It is intended for:

- frontend developers
- backend developers
- QA
- product owners
- technical reviewers

It captures the full strategy behind the work already implemented and the way the system should be used going forward.

## 1. Purpose

The objective of this system is not to build a chatbot.

The objective is to build a fast internal product-finding engine that:

- finds exact SKUs quickly
- handles structured filtering correctly
- understands natural-language product requests
- recovers from weak searches
- gets smarter from real user behavior
- stays operationally measurable

The system is designed for internal ERPNext/Frappe users, especially sales teams.

## 2. High-Level Architecture

The current system has four major layers:

1. Product indexing layer
2. Standard V2 search layer
3. AI orchestration layer
4. Feedback and evaluation layer

### 2.1 Product indexing layer
This layer builds the Typesense `product_v2` collection from ERPNext `Item` and related data.

It includes:

- searchable fields
- filterable fields
- normalized fields
- computed fields
- stock and pricing data
- compatibility-friendly fields

This is the retrieval foundation. If indexing is poor, AI will also perform poorly.

### 2.2 Standard V2 search layer
This is the backend proxy around Typesense.

Its job is:

- query execution
- filter handling
- sort handling
- pagination
- facet return
- access control
- stable frontend contract

This layer is used by `/list?search_v2=1`.

### 2.3 AI orchestration layer
This sits above standard V2 search.

Its job is:

- preprocess natural-language messages
- do deterministic extraction first
- optionally call LLM provider
- normalize output to the V2 contract
- execute V2 search
- retry with controlled relaxation
- rerank with compatibility and behavior feedback
- return final hits and diagnostics

### 2.4 Feedback and evaluation layer
This layer records what users do after searching and uses that data to make the system more useful over time.

It includes:

- AI search event tracking
- click tracking
- shortlist tracking
- quotation tracking
- reformulation tracking
- benchmark execution
- quality reporting

## 3. Why This System Is Efficient

The system was intentionally designed to avoid the common failure mode of “send everything to the LLM and hope for the best.”

Efficiency comes from these decisions:

### 3.1 Deterministic parsing first
The system tries to extract obvious structure without an LLM for things like:

- SKU
- IP rating
- CCT / Kelvin
- wattage
- voltage/current
- beam angle
- stock intent
- price intent
- discount intent
- latest/recent intent

This makes the system:

- faster
- cheaper
- more predictable
- easier to debug

### 3.2 LLM only for ambiguity
The LLM is not the first step. It is a gap-filling layer.

This keeps the model from doing work that regex, normalization, and business rules can already do better.

### 3.3 Typesense stays the retrieval engine
The AI does not replace search.

Typesense remains the fast retrieval system. AI improves:

- parsing
- query formulation
- relaxation
- reranking

### 3.4 Lean list responses
The search APIs return lean search-hit payloads rather than full product-detail documents.

That keeps:

- response size down
- parse cost lower
- frontend rendering faster

### 3.5 Controlled retry and relaxation
Instead of returning bad zero-result searches immediately, the AI layer can relax low-confidence filters in a controlled way.

This improves practical usefulness without making search noisy.

### 3.6 Feedback-aware reranking
The system can improve based on actual user behavior, not only prompt design.

This is the key path from “AI-assisted” to “genuinely smarter over time.”

## 4. Product Retrieval Strategy

The retrieval logic is based on a hybrid model:

- exactness for SKU queries
- structured matching for spec-heavy queries
- quantity sorting for stock-driven intent
- business-aware ranking for normal search
- compatibility-aware reranking for alternatives/spec requests

### 4.1 SKU retrieval
If the request is SKU-like:

- treat it as a hard signal
- prefer exact item code
- then prefix item code
- avoid over-relaxing the query

### 4.2 Spec retrieval
For spec-heavy queries such as:

- `ip65 3000k downlight`
- `24v 350ma driver`

The system extracts hard constraints and uses them as filters or numeric ranges.

### 4.3 Stock intent
For stock-priority intent such as:

- `high stock`
- `stock high to low`
- `quantity wise`

The search should use true quantity-wise sort semantics, not relevance-first sort.

### 4.4 Discount intent
For discount-priority intent such as:

- `highest discount`
- `best offer`

The search should sort by `discount_percentage`.

### 4.5 Alternatives intent
For requests like:

- `alternative for black pendant 2700k`
- `similar to this`
- `equivalent replacement`

The system should:

- preserve known structured specs
- bias compatibility
- favor manual alternatives when available

## 5. Backend Components

## 5.1 `product_search_v2.py`
This file owns the standard V2 search behavior.

Main responsibilities:

- search query construction
- filter parsing
- sort resolution
- access control
- lean result output

Important behavior:

- explicit sorts are supported
- `item_code_hint` is respected
- malformed filters are rejected cleanly
- debug info is returned in `query_debug`

## 5.2 `ai_product_search.py`
This file owns the AI search behavior.

Main responsibilities:

- deterministic extraction
- optional provider call
- V2 intent normalization
- search execution
- result relaxation
- compatibility rerank
- feedback rerank
- analytics logging
- benchmark execution

## 5.3 `api.py`
This file exposes whitelisted endpoints for frontend use.

## 5.4 `AI Product Search Event`
This DocType stores persistent AI query/outcome events.

It is part of the learning and observability layer.

## 5.5 Benchmark data
The benchmark dataset gives the team a measurable quality baseline.

## 6. Current Public APIs

### 6.1 Standard listing
- `search_products_v2`
- `suggest_products_v2`
- `get_similar_products_v2`

### 6.2 AI endpoints
- `ai_product_search`
- `ai_search_products_v2`
- `get_ai_product_search_quality_report`
- `evaluate_ai_product_search_benchmark`

### 6.3 AI tracking endpoints
- `track_ai_search_click`
- `track_ai_search_shortlist`
- `track_ai_search_quotation`
- `track_ai_search_reformulation`

## 7. AI Intent Model

The AI layer should normalize requests into the V2 contract only.

### 7.1 Allowed V2 filter keys
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

### 7.2 Allowed V2 range keys
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

### 7.3 Allowed sort values
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

### 7.4 Legacy keys that must not survive
- `color_temp_`
- `input`
- `warranty_`
- `price_range`
- `creation_on:desc`
- `creation_on:asc`

These may still be accepted as compatibility aliases in some paths, but frontend and AI output should always use the normalized V2 form.

## 8. Deterministic Extraction Rules

The deterministic parser should continue to be the primary intelligence layer.

### 8.1 Signals currently extracted
- SKU/item code
- IP rating
- color temperature and white-tone phrases
- wattage
- voltage
- current
- beam angle
- price under/over/between
- stock high/low intent
- discount intent
- latest/recent intent
- lumens
- CRI
- dimension
- cut-out
- indoor/outdoor hints

### 8.2 Why this matters
This avoids spending LLM calls on obvious requests and makes the system much more robust.

### 8.3 Rule of thumb
If a request can be reliably interpreted without an LLM, keep it deterministic.

## 9. AI Provider Strategy

The system currently uses:

- OpenAI as primary
- Groq as fallback
- deterministic fallback if model calls fail

### 9.1 Why multi-provider matters
- resilience
- reduced outage impact
- graceful degradation

### 9.2 What the frontend should assume
The frontend should not care which provider was used for normal operation.

It may surface provider info in internal debug mode only.

## 10. Relaxation Strategy

The AI layer can retry weak searches.

### 10.1 Current rules
- do not relax exact SKU constraints
- preserve hard numeric constraints
- remove low-confidence categorical filters first
- then fall back to strongest query + hard constraints

### 10.2 Why this matters
Natural-language search often overconstrains the query. Controlled relaxation improves real-world usefulness.

## 11. Compatibility and Substitute Intelligence

The current system has the first step of compatibility reranking.

It uses:

- matching mounting
- lamp type
- material
- finish
- voltage/current compatibility
- power tolerance
- CCT tolerance
- IP rating tolerance
- stock/business signals

### 11.1 What this means
For spec-heavy and alternative searches, backend can bias results toward technically closer products.

### 11.2 What it does not yet mean
This is not yet a full engineering-grade compatibility engine.

It is a compatibility-aware reranker built on currently indexed attributes.

## 12. Learning and Feedback Model

The AI becomes smarter only if the frontend actually sends outcome events.

### 12.1 Required tracked outcomes
- search issued
- result clicked
- item shortlisted
- quotation created
- reformulated query

### 12.2 Why this is essential
Without these feedback signals:

- top results cannot learn from user behavior
- query-specific reranking stays weak
- quality reporting remains shallow

### 12.3 `search_event_id`
This is the key that ties everything together.

Whenever frontend uses `ai_search_products_v2`, it must retain `search_event_id` and reuse it in outcome calls.

## 13. Frontend Integration Model

The frontend should implement two paths:

### 13.1 Standard V2 listing path
Use:

- `search_products_v2`
- `suggest_products_v2`
- `get_similar_products_v2`

Use this for:

- standard listing
- structured search
- filter-and-sort workflows

### 13.2 AI listing path
Use:

- `ai_search_products_v2`

Use this for:

- natural-language product search
- “find me…” style queries
- needs-based product discovery

### 13.3 Shared rendering
Both standard V2 and AI search should render through the same core result-row/list component when possible.

That reduces divergence and bugs.

## 14. Frontend Responsibilities

Frontend must:

- use normalized sort values
- use normalized filter keys
- keep list rendering lean
- show correct `RRP` / `Promo` pricing logic
- use `document.image`
- preserve `search_event_id`
- call tracking APIs after AI search outcomes
- expose debug data only in internal/debug mode

## 15. Pricing and Stock Rules

### 15.1 Pricing
Current pricing contract:

- `rate` = `RRP`
- `offer_rate` = `Promo`

Frontend display rule:

- if promo is valid, show promo + struck-through RRP
- otherwise show RRP only

### 15.2 Stock
Stock sorting should mean quantity sorting.

That means:

- `stock:desc` = high quantity to low quantity
- `stock:asc` = low quantity to high quantity

Not relevance-first stock ordering.

## 16. Observability and QA

This system is only maintainable if the team uses the quality and benchmark tools.

### 16.1 Quality report
Use `get_ai_product_search_quality_report` to inspect:

- request counts
- provider usage
- zero-result rate
- relaxation rate
- top failed queries
- top reformulated queries
- top clicked results

### 16.2 Benchmark
Use `evaluate_ai_product_search_benchmark` before major prompt/ranking changes.

This should become a release habit.

### 16.3 Debug visibility
For internal QA, expose:

- intent class
- provider
- llm_used
- applied sort
- applied relaxations
- quality signals

## 17. Current Operational Workflow

Recommended workflow after backend changes:

1. deploy code
2. run site migration if DocTypes changed
3. verify V2 search
4. verify AI search
5. verify event tracking
6. verify quality report
7. verify benchmark

## 18. What Makes the System “Smart”

This system becomes smarter from five things working together:

1. better indexing
2. better deterministic extraction
3. correct V2 query execution
4. controlled recovery when intent is weak
5. learning from actual user behavior

The last one is the most important long-term differentiator.

## 19. What Still Needs To Be Done Later

The current implementation is strong, but there is still room to improve.

Future improvements should be considered in this order:

### 19.1 Frontend tracking completion
The frontend must fully wire:

- click tracking
- shortlist tracking
- quotation tracking
- reformulation tracking

Without this, the feedback loop is incomplete.

### 19.2 Stronger evaluation dataset
The benchmark file should grow from a small seed set to a real business dataset with 30-100 useful queries.

### 19.3 Better compatibility features
Add more product intelligence fields for:

- accessory compatibility
- driver-to-light compatibility
- must-use relationships
- substitute policies

### 19.4 Usage analytics dashboards
Add admin views or reports so non-developers can inspect:

- top weak AI searches
- search behavior trends
- result quality metrics

### 19.5 Optional semantic reranking
If needed later, add semantic reranking on a small candidate set, not on the full catalog.

## 20. Non-Goals

This system is not intended to be:

- a general chat assistant
- a full product expert with unlimited reasoning
- a frontend-only AI experience
- a replacement for structured search

It is a search intelligence layer.

## 21. Implementation References

Key backend files:

- [ai_product_search.py](/home/erp-ihg/frappe-bench/apps/igh_search/igh_search/igh_search/ai_product_search.py:1)
- [product_search_v2.py](/home/erp-ihg/frappe-bench/apps/igh_search/igh_search/igh_search/product_search_v2.py:1)
- [api.py](/home/erp-ihg/frappe-bench/apps/igh_search/igh_search/igh_search/api.py:221)
- [AI Product Search Event DocType](/home/erp-ihg/frappe-bench/apps/igh_search/igh_search/igh_search/doctype/ai_product_search_event/ai_product_search_event.json:1)
- [AI Benchmark Dataset](/home/erp-ihg/frappe-bench/apps/igh_search/igh_search/igh_search/data/ai_product_search_benchmark.json:1)
- [Frontend Handoff Guide](/home/erp-ihg/frappe-bench/apps/igh_search/FRONTEND_AI_SEARCH_V2_GUIDE.md:1)

## 22. Final Recommended Team Usage

For backend developers:

- improve deterministic extraction first
- only then expand model prompting
- keep Typesense as retrieval backbone
- use benchmark before changing ranking logic

For frontend developers:

- use the dedicated APIs correctly
- keep list rendering lean
- wire the feedback loop completely
- expose debug data only for internal QA

For QA:

- test exact SKU
- test spec-heavy searches
- test stock and discount sorting
- test zero-result recovery
- test AI event tracking
- test benchmark output

For product owners:

- judge the system on retrieval quality, speed, and business usefulness
- not on how conversational the AI sounds


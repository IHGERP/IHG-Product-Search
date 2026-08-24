# Frontend build prompt — IHG-Store (stock age + per-item commission)

The `igh_search` backend has shipped two features. Both are live in the Typesense
`product_v2` index and in new whitelisted APIs. **No backend work is needed — do
not modify the Frappe apps.** This is purely IHG-Store (Next.js) work.

Work only in the V2 search stack (`components/Search/v2/**`, `libs/ighSearchV2.js`).
Do **not** touch the legacy v1 filter UI in `components/Product/filters/**`.

---

## Part 1 — Six new Typesense document fields

Every product document in `product_v2` now carries these. All are **optional** —
a document may lack them entirely, so default every read to `0` / `"none"` /
`"unknown"` rather than assuming presence.

### Stock age (how old the stock is)
| field | type | meaning |
|---|---|---|
| `stock_age_days` | float | Days since the item's most recent submitted, **non-intercompany** Purchase Invoice. `-1` = no qualifying purchase exists. |
| `stock_age_bucket` | string | One of `unknown`, `lt_1yr`, `1_2yr`, `2_3yr`, `3_4yr`, `gt_4yr` |

Why non-intercompany matters, so you label it correctly: if Company A bought an
item from a real supplier 4 years ago and later transferred it to Company B, a
naive "last purchase" date would make 4-year-old stock look new. Only external
supplier purchases count, so this is genuine age.

`unknown` means no qualifying external purchase was ever found (e.g. stock that
only ever arrived via intercompany transfer or opening balance). Keep it visually
distinct from `lt_1yr` — it is "we don't know", not "it's new".

### Commission (what a salesperson earns)
| field | type | meaning |
|---|---|---|
| `commission_percentage` | float | Raw percent. `2.5` means 2.5%. `0` = item earns no commission. |
| `commission_per_unit` | float | Currency (AED). Already computed as `(offer_rate or rate) × percentage / 100`. |
| `has_commission` | int32 | `1` / `0`. Cheap boolean for "does this earn anything". |
| `commission_bucket` | string | One of `none`, `lt_2`, `2_5`, `5_10`, `gte_10` |

**`commission_per_unit` is an ESTIMATE and must be labelled as one.** Actual
commission is charged on the invoice line's `net_amount` after invoice-level
discounts, whereas this uses the list/offer price. Do not present it as a
guaranteed payout. Wording like "≈ AED 12.40/unit" or "est. AED 12.40 per unit"
is right; "You earn AED 12.40" is not.

---

## Part 2 — Product card

Per the agreed design, show **percentage + per-unit amount** together, e.g.
`2.5% · ≈AED 12.40/unit`. Render the commission badge only when
`has_commission === 1`; render nothing (not "0%") otherwise.

Also surface stock age on the card. `gt_4yr` / `3_4yr` are the commercially
interesting ones (aged stock to move), so those deserve visual weight;
`unknown` should be muted.

`ProductCard.jsx` currently reads fields straight off the `document` object, so
these are available with no plumbing changes.

## Part 3 — Filters

`libs/ighSearchV2.js`:
- Add to `V2_FILTER_KEYS`: `"stock_age_bucket"`, `"has_commission"`, `"commission_bucket"`
- Add to `V2_RANGE_KEYS`: `"commission_percentage_range"`, `"commission_per_unit_range"`
- Add matching entries to `DEFAULT_V2_STATE.filters` — arrays (`[]`) for the
  three filter keys, `{ min: "", max: "" }` for the two range keys.

That is all the plumbing needed: every consumer of those two arrays iterates
them generically (`.forEach`), and the backend's `build_filter_by` /
`_build_numeric_range_clauses` already handle any registered `<field>_range`
key with no new backend code.

`components/Search/v2/constants.js`:
- `VISIBLE_FILTERS`: add `{ key: "stock_age_bucket", label: "Stock Age" }` and
  `{ key: "commission_bucket", label: "Commission" }`
- `FILTER_LABEL_MAP`: matching labels, plus `has_commission: "Earns Commission"`,
  `commission_percentage_range: "Commission %"`,
  `commission_per_unit_range: "Commission per Unit"`

**Facet-count availability differs per field — this matters:**
- `stock_age_bucket` → facet counts **are** returned. It renders through the
  existing generic multi-select checkbox section automatically once it is in
  `VISIBLE_FILTERS`.
- `has_commission` → facet counts **are** returned. Best rendered as a simple
  toggle ("Earns commission only"), like the existing In Stock / Promo toggles.
- `commission_bucket` → **filterable but NO facet counts** (it was deliberately
  left out of the backend's `SEARCH_FACET_FIELDS`, because each extra facet has
  a measured latency cost: ~21ms → ~205ms across the full facet list). So render
  it as a fixed list of the five known bucket values **without** counts, or as
  pill buttons. Do not expect counts to appear; do not add a "(0)" suffix.
  If you decide counts are essential, ask the backend team first — it is a
  one-line change with a real performance cost.

Raw bucket values are not user-facing strings. Map them:

```js
const STOCK_AGE_BUCKET_LABELS = {
  unknown: "Unknown", lt_1yr: "Less than 1 year", "1_2yr": "1 - 2 years",
  "2_3yr": "2 - 3 years", "3_4yr": "3 - 4 years", gt_4yr: "Above 4 years",
};
const COMMISSION_BUCKET_LABELS = {
  none: "No commission", lt_2: "Under 2%", "2_5": "2 - 5%",
  "5_10": "5 - 10%", gte_10: "10% and above",
};
```

There is an existing precedent for relabelling a facet's values in
`V2SearchPage.jsx`'s `visibleFilterOptions` memo — the `is_manufactured_item`
special case that maps `"1"`/`"0"` to `"Manufactured"`/`"Non-manufactured"`.
Follow that shape.

`FilterPanel.jsx`: add icons in the `ICONS` map (a clock for stock age, a coin /
percent for commission), following the existing inline-SVG component style. Add
both keys to `preliminaryOrder` in the `groupedSections` memo so they surface
near the top rather than among the spec filters.

## Part 4 — Sorting

Two new sort values are accepted by the backend: `commission_percentage:desc`
and `commission_per_unit:desc` (asc also works). Add them to the sort dropdown —
"Highest commission %" and "Highest commission per unit" are the useful ones.

---

## Part 5 — Commission dashboards (new pages)

Two views are required: a personal one and a shared one.

All endpoints are plain Frappe whitelisted methods reached through the existing
same-origin proxy. **Note the module path differs from the search API** — the
search SDK uses `.../api/method/igh_search.igh_search.api.`, but these live at:

```
/api/erp/api/method/igh_search.igh_search.commission.api.<function>
```

None are guest-accessible; all require an authenticated session (the existing
proxy already forwards the `sid` cookie).

### `get_commission_scope_info()`
Returns `{ role_level, sales_person, unrestricted }`.
Call this first to decide what to render. `role_level` is one of `management`,
`divisional`, `team_head`, `salesperson`. `sales_person` is `null` if the logged-in
user is not linked to a Sales Person record — in that case show an explanatory
empty state ("Your user isn't linked to a Sales Person yet — ask HR"), **not** an
error and **not** an empty zero-value dashboard.

### `get_my_commission_summary(from_date?, to_date?, sales_person?)`
Returns:
```json
{ "sales_person": "...", "unpaid_total": 0.0, "unpaid_count": 0,
  "paid_total": 0.0, "entry_count": 0,
  "by_item": [{ "item_code","item_name","brand","qty","base_amount","commission_amount" }],
  "by_month": [{ "month": "2026-08", "commission_amount": 0.0 }] }
```
`unpaid_total` is the headline number: what they have earned but not yet been paid.

### `get_my_commission_entries(status?, from_date?, to_date?, sales_person?, limit?, offset?)`
`status` is `unpaid` (default) | `paid` | `all`. Returns line-level rows
(posting_date, sales_invoice, item, qty, net_amount, allocated_percentage,
commission_rate, base_amount, commission_amount, is_return, source, payout).
Supports paging via `limit`/`offset`.

### `get_payout_history(sales_person?, limit?)`
Submitted payouts with totals and payment reference.

### `get_commission_leaderboard(status?, from_date?, to_date?, company?, limit?)`
The shared "common area" board: every salesperson's totals, ordered by
commission. Returns `sales_person`, `branch`, `entry_count`, `base_amount`,
`commission_amount` — **totals only, deliberately no line-level detail**.

### Behaviour you must implement correctly

**The "reset" model.** When a Commission Payout is submitted, the covered
entries are stamped as paid. The default dashboard shows **unpaid only**, so it
visibly resets to zero after a payout — that is intended, not a bug. Nothing is
deleted: `status: "paid"` and `get_payout_history` still show everything. Make
this legible in the UI ("Paid out on <date>", with history reachable in a tab or
toggle) so nobody thinks their earnings vanished.

**Negative rows are normal.** Returns/credit notes produce negative
`commission_amount` rows, and a return that arrives after a payout appears as a
negative *unpaid* row that reduces the next payout. `unpaid_total` can therefore
legitimately be negative. Render negatives clearly (and don't `Math.max(0, …)`
them away).

**Row-level visibility is enforced server-side.** A salesperson sees only their
own rows; managers see their subtree; the leaderboard is the one shared view.
Don't build client-side filtering on top of this and don't pass a
`sales_person` the user isn't entitled to — the API throws if you do.

---

## Hard dependency — coordinate on timing

The six Typesense fields only become **filterable, sortable and facetable** after
the backend team runs a full catalogue resync (~11 minutes). Before that, the
fields are stored but not indexed, and any `filter_by` / `sort_by` / `facet_by`
against them will throw. `include_fields` (reading the values to display) is safe
immediately.

So: card display can ship first; filters and sorting must wait for confirmation
that the resync has run. Ask the backend team for that go-ahead date.

Commission values will read `0` for every product until commission rates are
loaded into the `Product Based Commission` doctype — an empty commission filter
early on means "no rates loaded yet", not a bug.

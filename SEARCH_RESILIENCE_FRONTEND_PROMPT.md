# Frontend prompt — IHG-Store search resilience + updated error contract

Context: a backend bug made some V2 search requests fail hard (Typesense 404 on
a facet field a collection was missing). That bug is **fixed backend-side** — you
do not need to change anything to make search work again.

But the way it presented in the UI is itself worth fixing. When those requests
failed, the page **hung on a spinner** until the user refreshed, instead of
showing an error. That is a frontend robustness gap, and it will happen again on
any transient backend/network failure. This prompt is about closing it, plus two
small contract updates.

Scope: `components/Search/v2/**` and `libs/ighSearchV2.js`. No backend changes.

---

## 1. Never leave a hanging spinner (the important one)

The search call needs three things it currently appears to lack:

**a) Abort superseded requests.** Filtering, sorting, paging and changing page
size all fire a new search. Without an `AbortController`, rapid interaction
leaves several in flight, and a slow *older* response can land after a newer one
and overwrite correct results with stale ones. Keep a controller per in-flight
search; abort the previous before starting the next.

```js
const inflight = useRef(null);

async function runSearch(params) {
  inflight.current?.abort();
  const ctrl = new AbortController();
  inflight.current = ctrl;

  const timeout = setTimeout(() => ctrl.abort(), 20000);
  try {
    const res = await fetch(url, { signal: ctrl.signal, ... });
    ...
  } catch (err) {
    if (err.name === "AbortError") return;   // superseded or timed out: not an error state
    setError(err);                            // <- must always land somewhere
  } finally {
    clearTimeout(timeout);
    setLoading(false);                        // <- must run on EVERY path
  }
}
```

**b) A hard timeout.** 20s is generous; a page-1 search is normally ~300-500ms.
Without one, a stalled connection spins forever.

**c) `setLoading(false)` in a `finally`.** The hang means at least one failure
path leaves the loading flag set. Every exit — success, HTTP error, network
error, timeout — must clear it and either render results or render an error.

**d) An error state with a retry button.** Today a failed search is
indistinguishable from a slow one. Show the message and let the user retry
without a full page reload.

## 2. Handle HTTP 417 as an actionable message

The backend now returns a clear, human-readable error when a search asks for a
field the index does not have yet (index behind the app schema), instead of a
raw Typesense 500. Frappe returns these as:

- **HTTP 417**, with the text in the `_server_messages` field of the JSON body
  (a JSON-encoded array of JSON-encoded objects, each with a `message` key —
  Frappe's usual double-encoding).

Treat 417 as "show this message to the user", not as a generic crash. Everything
else (500, network) can use the generic error state.

```js
function extractFrappeMessage(body) {
  try {
    const msgs = JSON.parse(body?._server_messages || "[]");
    return msgs.map(m => JSON.parse(m).message).join(" ");
  } catch { return null; }
}
```

## 3. Tolerate missing facet counts

The backend now *drops* facets that the collection being queried cannot serve,
rather than failing the whole search. So `facet_counts` may legitimately omit a
field you asked for — most likely `stock_age_bucket` or `has_commission` during
a rollout window.

Your facet handling must not assume every requested facet comes back. Render the
filter with no counts (or hide that section) rather than throwing on
`undefined`. Check `adaptFacetCounts` and anything reading
`visibleFilterOptions[key]` for this assumption.

## 4. Surface the new diagnostics (small, saves future debugging)

Every search response now carries two extra keys in `query_debug`:

- `query_debug.collection` — which Typesense collection actually served the query
- `query_debug.degraded` — `null` normally; otherwise an object explaining what
  was downgraded, e.g. `{"hybrid_disabled_missing_fields": ["has_commission"]}`
  or `{"facets_dropped": ["stock_age_bucket"]}`

Log these to the console (or your error reporter) when `degraded` is non-null.
When semantic/hybrid search silently falls back to keyword search, this is the
only signal — without it, "results feel worse today" is undiagnosable.

Do not show these to end users.

---

## What NOT to change

- The request contract is unchanged. Filter keys, range keys, sort values and
  pagination all work exactly as documented in
  `COMMISSION_STOCK_AGE_FRONTEND_PROMPT.md`.
- Do not add client-side retry loops on 417 — that error means the index needs
  work, and retrying will just fail again.
- Do not work around missing facets by hiding the filters permanently; they come
  back on their own once the index catches up.

"""Run with bench's Python from sites/; prints no credentials and changes no Items."""
import json
import time
import frappe
frappe.init(site='site1.local', sites_path='.')
frappe.connect(); frappe.set_user('Administrator')
from igh_search.igh_search.product_search_v2 import search_products_v2
expected = {r[0] for r in frappe.db.sql('SELECT item_code FROM tabItem WHERE item_code LIKE %s AND disabled=0', ('%lbmg%',))}
sets=[]; timings=[]
for query in ['lbmg', 'LBMG', 'LbMg']:
    codes=set(); page=1
    while True:
        started=time.monotonic()
        r=search_products_v2(query=query,page=page,page_length=100,include_facets=0)
        timings.append(round((time.monotonic()-started)*1000))
        batch=[h['document']['item_code'] for h in r['hits']]
        assert not codes.intersection(batch), 'Duplicated pagination'
        codes.update(batch)
        if page*100>=r['found']:break
        page+=1
    assert not expected-codes, 'Missing ERP code matches'
    sets.append(codes)
    print(json.dumps({'query':query,'found':r['found'],'unique':len(codes),'erp_code_matches':len(expected),'missing':len(expected-codes)}))
assert sets[0]==sets[1]==sets[2], 'Case variants differ'
print(json.dumps({'requests':len(timings),'p95_ms':sorted(timings)[int(len(timings)*.95)-1],'max_ms':max(timings)}))
frappe.db.rollback();frappe.destroy()

import unittest
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / 'igh_search' / 'igh_search'
def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / (name + '.py'))
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod); return mod
policy = load('assistant_policy'); search = load('search_policy')

class SearchPolicyTests(unittest.TestCase):
    def test_case_invariant_complete_literal_matching(self):
        parameters = [search.literal_parameters(q) for q in ['lbmg', 'LBMG', 'LbMg']]
        self.assertTrue(all(p == parameters[0] for p in parameters))
        self.assertTrue(parameters[0]['exhaustive_search'])
        self.assertEqual(parameters[0]['infix'].split(',')[:2], ['always', 'always'])
        self.assertTrue(all(n == '0' for n in parameters[0]['num_typos'].split(',')))
    def test_browse_and_prose_are_not_compact_codes(self):
        for q in ['', '*', 'lb', 'warm outdoor light', 'ഇളം വെളിച്ചം']:
            self.assertEqual(search.literal_parameters(q), {})
    def test_punctuation_code_supported(self):
        self.assertTrue(search.literal_parameters('CUS-LBMG322.W.835'))

class AssistantPolicyTests(unittest.TestCase):
    def setUp(self):
        self.current = {'query':'spotlight', 'filters': {'ip_rate':['IP65'], 'rate_range':{'max':100}, 'in_stock':True}, 'sort_by':'', 'page':3}
    def test_refinement_preserves_hard_constraints(self):
        result = policy.merge_search(self.current, {'sort_by':'rate:asc', 'filters':{'color_temp':['4000K']}})
        self.assertEqual(result['filters']['ip_rate'], ['IP65'])
        self.assertTrue(result['filters']['in_stock'])
        self.assertEqual(result['filters']['rate_range'], {'max':100})
        self.assertEqual(result['page'], 1)
        self.assertNotIn('color_temp', self.current['filters'])
    def test_explicit_removal_only(self):
        result = policy.merge_search(self.current, {'filters':{'ip_rate':None}})
        self.assertNotIn('ip_rate', result['filters'])
        self.assertTrue(result['filters']['in_stock'])
    def test_bad_quantities_and_duplicates(self):
        for qty in [0,-1,float('nan'),float('inf'),1000001]:
            with self.assertRaises(ValueError): policy.validate_items([{'item_code':'LBMG','qty':qty}])
        with self.assertRaises(ValueError): policy.validate_items([{'item_code':'X'},{'item_code':'X'}])
    def test_price_and_stock_changes_require_new_preview(self):
        old=[{'item_code':'X','qty':2,'rate':10,'currency':'AED','available_qty':4}]
        policy.validate_preview(old, old)
        for change in [{'rate':11},{'available_qty':1},{'currency':'USD'}]:
            with self.assertRaises(ValueError): policy.validate_preview(old,[{**old[0],**change}])

if __name__ == '__main__': unittest.main()

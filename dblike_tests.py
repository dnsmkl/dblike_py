import unittest
from dblike import TableDef, DBSchema, DBTable, DBRow, DBValue


class DBLikeTestCase(unittest.TestCase):
    def test_column_deref(self):
        s = DBSchema(schema_def=[
                    TableDef('items','item_id'),
                    TableDef('cmp','cmp_id') # cmp = comparison
                ])
        s.items.save_row({'item_id':11, 'name':'chair'})
        s.items.save_row({'item_id':22, 'name':'house'})
        s.cmp.save_row({'cmp_id':1, 'smaller_item_id':11, 'larger_item_id':22})

        self.assertEquals(s.cmp[1].smaller_item_id.deref_in('items').name.value, 'chair')
        self.assertEquals(s.cmp[1].larger_item_id.deref_in('items').name.value, 'house')

    def test_find_refs(self):
        s = DBSchema(schema_def=[
                    TableDef('items','item_id'),
                    TableDef('owners','owner_id') # cmp = comparison
                ])
        s.owners.save_row({'owner_id':1, 'owner_name':'Tom'})
        s.items.save_row({'item_id':1, 'name':'chair', 'owner_id':1})
        s.items.save_row({'item_id':2, 'name':'house', 'owner_id':1})

        owned_items = s.owners[1].find_refs('items', 'owner_id')
        self.assertEquals(len(owned_items), 2)
        self.assertEquals(owned_items[0].name.value, 'chair')
        self.assertEquals(owned_items[1].name.value, 'house')


class DBSchemaTestCase(unittest.TestCase):
    def test_simple_set_get(self):
        s = DBSchema(schema_def=[TableDef('table1','column1')])
        s.table1.save_row({'column1':1, 'some_value':'ThisIsValue'})
        self.assertEquals(s.table1[1].some_value.value, 'ThisIsValue')


class DBTableTestCase(unittest.TestCase):
    def test(self):
        x = DBTable(parent_schema=None, pk='row_id')
        x.save_row({'row_id':1, 'some_value':'ThisIsValue'})
        self.assertEquals(x[1].some_value.value, 'ThisIsValue')


class DBRowTestCase(unittest.TestCase):
    def test(self):
        x = DBRow(parent_schema=None, pk='a', value_dict={'a':'this is a', 'b':'this is not a'})
        self.assertEquals(x.a.value, 'this is a')
        self.assertEquals(x.b.value, 'this is not a')


class DBValueTestCase(unittest.TestCase):
    def test(self):
        x = DBValue(parent_schema=None, value='ThisIsValue')
        self.assertEquals(x.value, 'ThisIsValue')


def run_tests():
    unittest.main(__name__)


if __name__ == '__main__':
    run_tests()

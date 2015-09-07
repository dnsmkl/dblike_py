import unittest
from dblike import TableDef, DBSchema, DBTable, DBRow, DBValue


class DBLikeTestCase(unittest.TestCase):
    '''\
    Integration tests of DBLike module
    (tests that are not isolated to separate classes)
    '''
    def setUp(self):
        '''Define simple schema for query testing purposes'''
        s = DBSchema(schema_def=[
                    TableDef(name='items', pk='item_id'),
                    TableDef(name='owners', pk='owner_id')
                ])
        s.owners.add_row({'owner_id':1, 'owner_name':'Tom'})
        s.items.add_row({'item_id':1, 'name':'chair', 'owner_id':1})
        s.items.add_row({'item_id':2, 'name':'house', 'owner_id':1})
        self.s = s

    def test_value_deref(self):
        s = self.s
        self.assertEquals(s.items[1].owner_id.deref_in('owners').owner_name.value, 'Tom')

    def test_row_find_refs(self):
        s = self.s
        owned_items = s.owners[1].find_refs('items', 'owner_id')
        self.assertEquals(len(owned_items), 2)
        self.assertEquals(owned_items[0].name.value, 'chair')
        self.assertEquals(owned_items[1].name.value, 'house')


class DBSchemaTestCase(unittest.TestCase):
    def test_getattr(self):
        s = DBSchema(schema_def=[TableDef('table1','column1')])
        s.table1.add_row({'column1':1, 'some_value':'ThisIsValue'})
        self.assertEquals(s.table1[1].some_value.value, 'ThisIsValue')


class DBTableTestCase(unittest.TestCase):
    def test_getitem(self):
        x = DBTable(parent_schema=None, pk='row_id')
        x.add_row({'row_id':1, 'some_value':'ThisIsValue'})
        self.assertEquals(x[1].some_value.value, 'ThisIsValue')

    def test_find_row(self):
        x = DBTable(parent_schema=None, pk='row_id')
        x.add_row({'row_id':1, 'some_value':'value1'})
        x.add_row({'row_id':2, 'some_value':'value2'})
        x.add_row({'row_id':3, 'some_value':'valueX'})
        x.add_row({'row_id':4, 'some_value':'valueX'})

        rows = x.find_rows('some_value', 'value2')
        self.assertEquals(len(rows), 1)
        self.assertEquals(rows[0].row_id.value, 2)

        rows = x.find_rows('some_value', 'valueX')
        self.assertEquals(len(rows), 2)
        self.assertEquals(rows[0].row_id.value, 3)
        self.assertEquals(rows[1].row_id.value, 4)


class DBRowTestCase(unittest.TestCase):
    def test_getattr(self):
        x = DBRow(parent_schema=None, pk='a', value_dict={'a':'this is a', 'b':'this is not a'})
        self.assertEquals(x.a.value, 'this is a')
        self.assertEquals(x.b.value, 'this is not a')


class DBValueTestCase(unittest.TestCase):
    def test_value(self):
        x = DBValue(parent_schema=None, value='ThisIsValue')
        self.assertEquals(x.value, 'ThisIsValue')


def run_tests():
    unittest.main(__name__)


if __name__ == '__main__':
    run_tests()

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
        self.assertEquals(s.items[1].owner_id.deref('owners').owner_name.value, 'Tom')

    def test_row_find_refs(self):
        s = self.s
        owned_items = s.owners[1].find_refs('items', 'owner_id')
        self.assertEquals(len(owned_items), 2)
        self.assertItemsEqual([v.name.value for v in owned_items], ['chair','house'])


class DBSchemaTestCase(unittest.TestCase):
    def test_getattr(self):
        s = DBSchema(schema_def=[TableDef(name='test_table', pk='k')])
        s.test_table.add_row({'k':1, 'val':'valueX'})
        self.assertEquals(s.test_table[1].val.value, 'valueX')


class DBTableTestCase(unittest.TestCase):
    def test_getitem(self):
        x = DBTable(parent_schema=None, pk='row_id')
        x.add_row({'row_id':1, 'val':'valueX'})
        self.assertEquals(x[1].val.value, 'valueX')

    def test_getitem_multivalued_pk(self):
        x = DBTable(parent_schema=None, pk='row_id id_modif')
        x.add_row({'row_id':1, 'id_modif':100, 'val':'valueX'})
        self.assertEquals(x[1,100].val.value, 'valueX')

    def test_find_rows(self):
        x = DBTable(parent_schema=None, pk='row_id')
        x.add_row({'row_id':1, 'val':'value1'})
        x.add_row({'row_id':2, 'val':'value2'})
        x.add_row({'row_id':3, 'val':'valueX'})
        x.add_row({'row_id':4, 'val':'valueX'})

        # Find one row, based on one column
        rows = x.find_rows(['val'], ['value2'])
        self.assertEquals(len(rows), 1)
        self.assertEquals(rows[0].row_id.value, 2)

        # Find more then one row, based on one column
        rows = x.find_rows(['val'], ['valueX'])
        self.assertEquals(len(rows), 2)
        self.assertItemsEqual([r.row_id.value for r in rows], [3,4])

        # Find one row, based on two columns
        rows = x.find_rows(['val', 'row_id'], ['valueX', 3])
        self.assertEquals(len(rows), 1)
        self.assertEqual(rows[0].row_id.value, 3)


class DBRowTestCase(unittest.TestCase):
    def test_getattr(self):
        x = DBRow(parent_schema=None, pk='k', value_dict={'k':'a1', 'b':'b1'})
        self.assertEquals(x.k.value, 'a1')
        self.assertEquals(x.b.value, 'b1')

    def test_column_values(self):
        x = DBRow(parent_schema=None,
                    pk=('k'),
                    value_dict={'k':'a1', 'b':'b1', 'c': 'c1', 'd': 'd1'}
                )
        # Specify columns by different types:
        self.assertEqual(x.column_values(('b', 'c')), tuple(['b1', 'c1'])) # tuple
        self.assertEqual(x.column_values(('b', 'c')), tuple(['b1', 'c1'])) # list
        self.assertEqual(x.column_values('b c'), tuple(['b1', 'c1'])) # string
        # Change column order:
        self.assertEqual(x.column_values('k b c d'), tuple(['a1', 'b1', 'c1', 'd1']))
        self.assertEqual(x.column_values('b k c d'), tuple(['b1', 'a1', 'c1', 'd1']))
        self.assertEqual(x.column_values('d b c k'), tuple(['d1', 'b1', 'c1', 'a1']))
        self.assertEqual(x.column_values('d c b k'), tuple(['d1', 'c1', 'b1', 'a1']))
        # Repeat columns:
        self.assertEqual(x.column_values('d d d'), tuple(['d1', 'd1', 'd1']))
        self.assertEqual(x.column_values('b c b'), tuple(['b1', 'c1', 'b1']))

    def test_multivalued_pk(self):
        x = DBRow(parent_schema=None, pk='a b', value_dict={'a':'a1', 'b':'b1', 'c':'c1'})
        self.assertEquals(x.a.value, 'a1')
        self.assertEquals(x.b.value, 'b1')
        self.assertEquals(x.c.value, 'c1')


class DBValueTestCase(unittest.TestCase):
    def test_value(self):
        x = DBValue(parent_schema=None, value='ThisIsValue')
        self.assertEquals(x.value, 'ThisIsValue')


def run_tests():
    unittest.main(__name__)


if __name__ == '__main__':
    run_tests()

'''\
Storage system that mimics some aspects of DB.

This module provides classes, that looseley correspond to database concepts:
- DBSchema
- DBTable
- DBRow
- DBValue

Situation, when it can be useful:
- Needed data is in DB dump (e.g. XML file).
- There is no possibility to load it to actual DB
- It is more convenient/quicker, to parse the DB dump,
    load data into DBLIKE system and use its API.
'''


from collections import namedtuple
import unittest


TableDef = namedtuple("TableDef", "name pk")


class DBSchema(object):
    'Contains dict of DBTable'
    def __init__(self, schema_def):
        self._tables = dict()
        assert isinstance(schema_def, list)
        for name, pk in schema_def:
            self._tables[name] = DBTable(self, pk)

    def __getattr__(self, table_name):
        return self._tables[table_name]


class DBTable(object):
    'Contains dict of DBRow'
    def __init__(self, parent_schema, pk):
        self._schema = parent_schema
        self._pk = pk
        self._rows = dict()

    def save_row(self, value_dict):
        new_row = DBRow(self._schema, self._pk, value_dict)
        self._rows[new_row.key] = new_row

    def __getitem__(self, id):
        return self._rows[id]


class DBRow(object):
    'Contains dict of DBValue'
    def __init__(self, parent_schema, pk, value_dict):
        self._schema = parent_schema
        self._pk = pk
        self._dbvalue_dict = dict()
        for i, val in value_dict.iteritems():
            self._dbvalue_dict[i] = DBValue(self._schema, val)

    @property
    def key(self):
        return self._dbvalue_dict[self._pk].value

    def __getattr__(self, column_name):
        return self._dbvalue_dict[column_name]


class DBValue(object):
    'Contains value'
    def __init__(self, parent_schema, value):
        self._schema = parent_schema
        self._value = value

    @property
    def value(self):
        return self._value

    def deref_in(self, table_name):
        'Retrieve row, by using this value as key in the supplied table'
        return getattr(self._schema, table_name)[self.value]


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


if __name__ == '__main__':
    unittest.main()

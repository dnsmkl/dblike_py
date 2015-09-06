'''\
Storage system that mimics some aspects of DB.

This module provides classes, that loosely correspond to database concepts:
- DBSchema
- DBTable
- DBRow
- DBValue

Situation, when it can be useful:
- Needed data is in DB dump (e.g. XML file).
- There is no possibility to load it to actual DB
- It is more convenient/quicker, to parse the DB dump,
    load data into DBLIKE system and use its API.

It will not be useful if real database is needed.
(no data consistency checks, no data types, no transactions)
'''


from collections import namedtuple


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
        assert column_name in self._dbvalue_dict, str(self._dbvalue_dict)
        return self._dbvalue_dict[column_name]

    def find_refs(self, table_name, column_name):
        '''Find rows in other table, that refer to this row'''
        table = getattr(self._schema, table_name)
        return [r for k, r in table._rows.items()
            if getattr(r, column_name).value == self.key]


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


if __name__ == '__main__':
    import dblike_tests
    dblike_tests.run_tests()

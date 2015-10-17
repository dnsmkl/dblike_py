'''\
Storage system that mimics some aspects of DB.

In the end it is ORM like interface to some nested dictionaries.

This module provides classes, that loosely correspond to database concepts:
- DBSchema - dict of tables
- DBTable  - dict of rows
- DBRow    - dict of values
- DBValue

Specific situation, when it can be useful:
- Data is structured similarly as DB (e.g. XML dump of database file).
- There is no possibility to actually load data to DB.
- It is inconvenient/inefficient to use existing API (e.g. XPath).
- It is easy/quick to load data into dblike.

It will not be useful if real database is needed.
- No data consistency checks
- No data types
- No transactions
- No optimizations
'''

__version__ = "1.2.0"

from collections import namedtuple


TableDef = namedtuple("TableDef", "name pk")


class DBSchema(object):
    '''Contains dict of DBTable'''
    def __init__(self, schema_def):
        self._tables = dict()
        assert isinstance(schema_def, list)
        for name, pk in schema_def:
            self._tables[name] = DBTable(self, pk)

    def __getattr__(self, table_name):
        return self._tables[table_name]

    def __getitem__(self, table_name):
        return self._tables[table_name]


class DBTable(object):
    '''Contains dict of DBRow'''
    def __init__(self, parent_schema, pk):
        self._schema = parent_schema # needed for find_refs() and deref()
        self._pk = pk
        self._rows = dict()
        self._indexes = dict()

    def add_row(self, value_dict):
        new_row = DBRow(self._schema, self._pk, value_dict)
        self._rows[new_row.pk_value] = new_row

    def find_rows(self, column_names, column_values):
        return [row for row in self._rows.values()
            if list(row.column_values(column_names)) == list(column_values)]

    def _index_create(self, column_names):
        '''Create index, to make finding rows more efficient

        Index is not updated/maintained once created,
        so any changes in rows will make index invalid.
        '''
        if isinstance(column_names, str):
            column_names = tuple(column_names.split())
        new_index = dict()
        for row in self._rows.values():
            idx_key = row.column_values(column_names)
            if idx_key not in new_index:
                new_index[idx_key] = set()
            new_index[idx_key].add(row)
        self._indexes[column_names] = new_index

    def _index_find_rows(self, column_names, column_values):
        '''Find rows by using index'''
        column_names = _tupleize(column_names)
        column_values = _tupleize(column_values)
        assert column_names in self._indexes, 'Index on "{}" does not exist'.format(column_names)
        index = self._indexes[column_names]
        if column_values in index:
            return index[tuple(column_values)]
        else:
            return None

    def __getitem__(self, id):
        assert not(isinstance(id, tuple) and len(id) == 1)
        if not isinstance(id, tuple):
            id = tuple([id])
        return self._rows[id]

    def __contains__(self, id):
        if not isinstance(id, tuple):
            id = tuple([id])
        return id in self._rows

    def iteritems(self):
        for k, v in self._rows.iteritems():
            assert isinstance(k, tuple)
            if len(k) == 1:
                yield (k[0], v)
            else:
                yield (k, v)

    def values(self):
        return self._rows.values()


class DBRow(object):
    '''Contains dict of DBValue'''
    def __init__(self, parent_schema, pk, value_dict):
        self._schema = parent_schema # needed for find_refs() and deref()
        self._pk = pk
        self._dbvalue_dict = dict()
        for i, val in value_dict.iteritems():
            self._dbvalue_dict[i] = DBValue(self._schema, val)

    @property
    def pk_value(self):
        return self.column_values(self._pk)

    def __getattr__(self, column_name):
        assert column_name in self._dbvalue_dict, str(self._dbvalue_dict)
        return self._dbvalue_dict[column_name]

    def find_refs(self, table_name, column_names):
        '''Find rows in other table, that refer to this row'''
        column_names = _tupleize(column_names)
        table = getattr(self._schema, table_name)
        return table.find_rows(column_names, self.pk_value)

    def column_values(self, column_names):
        column_names = _tupleize(column_names)
        return tuple([self._dbvalue_dict[name].value for name in column_names])


class DBValue(object):
    '''Contains value'''
    def __init__(self, parent_schema, value):
        self._schema = parent_schema # needed for deref()
        self._value = value

    @property
    def value(self):
        return self._value

    def deref(self, table_name):
        '''Retrieve row, by using this value as key in the supplied table'''
        return getattr(self._schema, table_name)[self.value]


def _tupleize(str_or_list):
    '''Make a tuple by modules standard rules

    It should be used for column names and filtering values
    '''
    if isinstance(str_or_list, str):
        return tuple(str_or_list.split())
    else:
        return tuple(str_or_list)


if __name__ == '__main__':
    import dblike_tests
    dblike_tests.run_tests()

"""\
Storage system that mimics some aspects of DB.

In the end it is ORM like interface to some nested dictionaries.

This module provides classes, that loosely correspond to database concepts:
- DBSchema - dict of tables
- _DBTable  - dict of rows
- _DBRow    - dict of values
- _DBValue

Specific situation, when it can be useful:
- Data is structured similarly as DB (e.g. XML dump of database schema).
- There is no possibility to actually load data to DB.
- It is inefficient to use existing API (e.g. thousands of XPath queries).
- It is easy/quick to load data into dblike.

It will not be useful if real database is needed.
- No data consistency checks
- No data types
- No transactions
- No optimizations
"""

__version__ = '2.1.4'

from collections import namedtuple


class DuplicateRowException(Exception):
    """Duplicate row exception."""

    def __init__(self, existing_row, new_row):
        super(DuplicateRowException, self).__init__(
            'Existing: {}; New: {}'.format(existing_row, new_row)
        )
        self.existing_row = existing_row
        self.new_row = new_row


# Table definition used by DBSchema.
# Contains name (string) and primary key (list or string that is later split)
TableDef = namedtuple('TableDef', 'name pk')


class DBSchema(object):
    """Contains dict of _DBTable."""

    def __init__(self, schema_def):
        """Create DBSchema from list of TableDef."""
        self._tables = dict()
        for name, pk in schema_def:
            self._tables[name] = _DBTable(self, pk)

    # Forward some methods to internal dict.
    def __getattr__(self, table_name): return self._tables[table_name]
    def __getitem__(self, table_name): return self._tables[table_name]
    def __contains__(self, table_name): return table_name in self._tables


class _DBTable(object):
    """Contains dict of _DBRow.

    Public methods of this class, belong to the interface of the module,
    but class it self should be instantiated only by `DBSchema`.
    """

    def __init__(self, parent_schema, pk):
        """Construct empty DBTable.

        :param parent_schema:
            Parent schema used by `_DBRow.find_refs()` and `_DBValue.deref()`.
        :param pk: Primary key column names.
        :type parent_schema: `DBSchema`.
        :type pk: `tuple`, `list` or `str`. (`str` is processed by `str.split`)
        """
        self._schema = parent_schema # needed for find_refs() and deref()
        self._pk = pk
        self._rows = dict()
        self._indexes = dict()

    def add_row(self, value_dict):
        """Add a row into the table.

        :param value_dict: Values to be stored. Names->value map of columns.
        :type value_dict: `dict`.
        :raises DuplicateRowException: In case pk for new row is already taken.
        """
        self._index_clear_all()
        new_row = _DBRow(self._schema, self._pk, value_dict)
        new_pk = new_row._pk_value
        if new_pk in self._rows:
            raise DuplicateRowException(self._rows[new_pk], new_row)
        self._rows[new_pk] = new_row

    def find_rows(self, column_names, column_values, skip_index=False):
        """Find rows based on column value filter.

        It is possible to filter by more then one column,
        where column conditions are joined by using ``and``.

        :optimization:
            By default create index before searching, if it does not exist yet.
            This can be disabled by `skip_index`.

        :param column_names: Column names to be filtered on.
        :param column_values: Column values (corresponding to `column_names`) to be searched.
        :param skip_index: Disables index building optimization.

        :type column_names:
            `tuple`, `list` or `str`. (`str` is processed by `str.split`)
        :type column_values:
            `tuple`, `list` or `str`. (`str` is processed by `str.split`)
        :type skip_index: `bool`.

        :returns: Rows where all columns were matched.
        :rtype: `set` of `_DBRow`.
        """
        if not skip_index:
            if not self._index_exists(column_names):
                self._index_create(column_names)
            return self._index_find_rows(column_names, column_values)
        else:
            return self._walking_find_rows(column_names, column_values)

    def _walking_find_rows(self, column_names, column_values):
        """Find rows by iterating through all elements."""
        return set([row for row in self._rows.values()
            if list(row.column_values(column_names)) == list(column_values)])

    def _index_create(self, column_names):
        """Create index, to make finding rows more efficient."""
        column_names = _tupleize(column_names)
        new_index = dict()
        for row in self._rows.values():
            idx_key = row.column_values(column_names)
            if idx_key not in new_index:
                new_index[idx_key] = set()
            new_index[idx_key].add(row)
        self._indexes[column_names] = new_index

    def _index_find_rows(self, column_names, column_values):
        """Find rows by using index."""
        column_names = _tupleize(column_names)
        column_values = _tupleize(column_values)
        assert column_names in self._indexes,\
            'Index on "{}" does not exist'.format(column_names)
        index = self._indexes[column_names]
        if column_values in index:
            return index[tuple(column_values)]
        else:
            return set()

    def _index_exists(self, column_names):
        """Check if index exists."""
        column_names = _tupleize(column_names)
        return column_names in self._indexes

    def _index_clear_all(self):
        for index in self._indexes.values():
            index.clear()
        self._indexes.clear()

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


class _DBRow(object):
    """Contains dict of _DBValue.

    Public methods of this class, belong to the interface of the module,
    but class it self should be instantiated only by `_DBTable`.
    """

    def __init__(self, parent_schema, pk, value_dict):
        """Construct _DBRow with supplied values.

        :param parent_schema:
            Parent schema used by `_DBRow.find_refs()` and `_DBValue.deref()`.
        :param pk: Primary key column names.
        :param value_dict: Values to be stored. Names->value map of columns.
        :type pk: `tuple`, `list` or `str`. (`str` is processed by `str.split`)
        :type value_dict: `dict`.
        """
        self._schema = parent_schema # needed for find_refs() and deref()
        self._pk = pk
        self._columns = dict() # column values of the row
        for i, val in value_dict.iteritems():
            self._columns[i] = _DBValue(self._schema, val)

    @property
    def _pk_value(self):
        """Get primary key column values

        Should be called only from :class:`_DBTable`.
        """
        return self.column_values(self._pk)

    def __getattr__(self, column_name):
        assert column_name in self._columns,\
            '{} not in {}'.format(column_name, self._columns)
        return self._columns[column_name]

    def find_refs(self, table_name, column_names):
        """Find rows in other table, that refer to this row.

        :param table_name: Table to be search.
        :param column_names:
            Columns to be compared for match with `self._pk_value`.
        """
        column_names = _tupleize(column_names)
        table = self._schema[table_name]
        return table.find_rows(column_names, self._pk_value)

    def column_values(self, column_names):
        """Return plain column values (without _DBValue wrappers)."""
        column_names = _tupleize(column_names)
        return tuple([self._columns[name].value for name in column_names])

    def __repr__(self):
        return '_DBRow({0._schema!r}, {0._pk!r}, {0._columns!r})'.format(self)


class _DBValue(object):
    """Contains value.

    Public methods of this class, belong to the interface of the module,
    but class it self should be instantiated only by `_DBRow`.
    """

    def __init__(self, parent_schema, value):
        self._schema = parent_schema # needed for deref()
        self._value = value

    @property
    def value(self):
        return self._value

    def deref(self, table_name):
        """Retrieve row, by using this value as key in the supplied table"""
        assert self.value
        table = self._schema[table_name]
        return table[self.value]

    def __nonzero__(self): return self.__bool__()
    def __bool__(self): return bool(self._value)

    def __repr__(self):
        return '_DBValue({0._schema!r}, {0._value!r})'.format(self)


def _tupleize(str_or_list):
    """Make a tuple by modules standard rules.

    It should be used for column names and filtering values.
    """
    if isinstance(str_or_list, str):
        return tuple(str_or_list.split())
    else:
        return tuple(str_or_list)


if __name__ == '__main__':
    import dblike_tests
    dblike_tests.run_tests()

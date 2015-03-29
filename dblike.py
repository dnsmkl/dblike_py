'''\
Storage system that mimics some aspects of DB.

This module provides classes, that looseley correspond to database concepts:
(currently only TODO list)
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


from __future__ import print_function
import unittest


class DBTable(object):
    def __init__(self, pk):
        self._pk = pk
        self._rows = dict()

    def save_row(self, value_dict):
        new_row = DBRow(self._pk, value_dict)
        self._rows[new_row.key] = new_row

    def __getitem__(self, id):
        return self._rows[id]


class DBRow(object):
    def __init__(self, pk, value_dict):
        self._pk = pk
        self._value_dict = dict()
        for i, val in value_dict.iteritems():
            self._value_dict[i] = DBValue(val)

    @property
    def key(self):
        return self._value_dict[self._pk].value

    def __getitem__(self, id):
        return self._value_dict[id]


class DBValue(object):
    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value


class DBTableTestCase(unittest.TestCase):
    def test(self):
        x = DBTable('row_id')
        x.save_row({'row_id':1, 'some_value':'ThisIsValue'})
        self.assertEquals(x[1]['some_value'].value, 'ThisIsValue')


class DBRowTestCase(unittest.TestCase):
    def test(self):
        x = DBRow('a', {'a':'this is a', 'b':'this is not a'})
        self.assertEquals(x['a'].value, 'this is a')
        self.assertEquals(x['b'].value, 'this is not a')


class DBValueTestCase(unittest.TestCase):
    def test(self):
        x = DBValue('ThisIsValue')
        self.assertEquals(x.value, 'ThisIsValue')


if __name__ == '__main__':
    unittest.main()

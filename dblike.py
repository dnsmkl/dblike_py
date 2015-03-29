from __future__ import print_function
import unittest


class DBTable(object):
    def __init__(self):
        self._rows = dict()

    def __setitem__(self, id, val):
        self._rows[id] = val

    def __getitem__(self, id):
        return self._rows[id]


class DBRow(object):
    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value


class DBTableTestCase(unittest.TestCase):
    def test(self):
        x = DBTable()
        x[1] = 'ThisIsValue'
        self.assertEquals(x[1], 'ThisIsValue')


class DBRowTestCase(unittest.TestCase):
    def test(self):
        x = DBRow('ThisIsValue')
        self.assertEquals(x.value, 'ThisIsValue')


if __name__ == '__main__':
    unittest.main()

import errno
import os
from json import loads, dumps, dump, load
from collections import defaultdict
from itertools import chain
from contextlib import contextmanager
from itertools import repeat
import sqlite3

try:
    import psycopg2
except:
    pass


import space



class SqliteBackend():

    def __init__(self, path=None):
        if path is None:
            self.connection = sqlite3.connect(':memory:')
        else:
            self.connection = sqlite3.connect(path)
        self.cursor = self.connection.cursor()

    def register(self, space):
        space.set_db(self)
        for dim in space._dimensions:
            name = '%s_%s' % (space._name, dim)
            self.cursor.execute(
                'CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY,'
                    'parent INTEGER, name TEXT)' % name)

        # TODO test dimension type to adapt db Scheme
        cols = ','.join(chain(
                ('%s TEXT NOT NULL' % i for i in space._dimensions),
                ('%s REAL NOT NULL' % i for i in space._measures)
                ))
        query = 'CREATE TABLE IF NOT EXISTS %s (%s)' % (space._name, cols)
        self.cursor.execute(query)

        self.cursor.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS %s_dim_index on %s (%s)' % (
                space._name, space._name, ','.join(space._dimensions)
                )
            )

    def load_coordinates(self, dim):
        name = '%s_%s' % (dim._spc._name, dim._name)
        return self.cursor.execute('SELECT id, parent, name from %s' % name)

    def create_coordinate(self, dim, name, parent_id):
        table = "%s_%s" % (dim._spc._name, dim._name)
        self.cursor.execute(
            'INSERT into %s (name, parent) VALUES (?, ?)' % table,
            (name, parent_id)
            )

        select = 'SELECT id from %s where name %s ? and parent %s ?'
        parent_op = parent_id is None and 'is' or '='
        name_op = name is None and 'is' or '='

        res = self.cursor.execute(select % (table, name_op, parent_op), (
                name, parent_id
                ))
        return res.next()[0]

    def get_child_coordinates(self, dim, parent_id):
        table = "%s_%s" % (dim._spc._name, dim._name)
        select = 'SELECT name from %s where parent %s ?'
        parent_op = parent_id is None and 'is' or '='

        res = self.cursor.execute(select % (table, parent_op), (parent_id,))
        return res

    def get(self, space, key):
        select = ','.join(space._measures)
        clause = lambda x: ('%s is ?' if x[1] is None else '%s = ?')
        where = ' and '.join(map(clause, zip(space._dimensions, key)))

        stm = ('SELECT %s FROM %s WHERE %s' %
                (select, space._name, where)) % tuple(space._dimensions)

        res = self.cursor.execute(stm, key).fetchall()
        return res and res[0] or None

    def set(self, space, key_values):
        # XXX prepare statement ?
        fields = tuple(chain(space._dimensions, space._measures))
        values = ','.join('?' for f in fields)
        fields = ','.join(fields)
        stm = 'INSERT OR REPLACE INTO %s (%s) values (%s)' % (
            space._name, fields, values)
        data = list(key + values for key, values in key_values)
        self.cursor.executemany(
            stm, data
            )

    def commit(self):
        self.connection.commit()


def get_backend(name, uri):
    if name == 'sqlite':
        db = SqliteBackend(uri)
    else:
        raise Exception('Backend %s not known' % backend)
    return db


@contextmanager
def connect(backend='sqlite', uri=None):
    backend = get_backend(backend, uri)
    for spc in space.SPACES.itervalues():
        backend.register(spc)
    yield
    for spc in space.SPACES.itervalues():
        spc.flush()

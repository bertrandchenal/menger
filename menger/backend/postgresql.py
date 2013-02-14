from cStringIO import StringIO
from itertools import chain, imap
from sql import SqlBackend
import psycopg2

class PGBackend(SqlBackend):

    def __init__(self, connection_string):
        self.space = None
        self.connection = psycopg2.connect(connection_string)
        self.cursor = self.connection.cursor()
        self.write_buffer = {}
        self.read_cache = {}

    def close(self):
        super(PGBackend, self).close()
        self.connection.close()

    def register(self, space):
        self.space = space
        create_idx = 'CREATE UNIQUE INDEX %s_id_parent_index on %s (id, parent)'
        for dim in space._dimensions:
            name = '%s_%s' % (space._name, dim)
            self.cursor.execute(
                'CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY,'
                    'parent INTEGER, name varchar)' % name)

            self.cursor.execute(
                "SELECT 1 FROM pg_indexes "\
                    "WHERE tablename = %s AND indexname = %s",
                (name, name + '_id_parent_index',))

            if not self.cursor.fetchall():
                self.cursor.execute(create_idx % (name, name))

        cols = ','.join(chain(
                ('%s INTEGER NOT NULL references %s_%s (id)' % (
                        i, space._name, i
                        ) for i in space._dimensions),
                ('%s REAL NOT NULL' % i for i in space._measures)
                ))
        query = 'CREATE TABLE IF NOT EXISTS %s (%s)' % (space._name, cols)
        self.cursor.execute(query)

        self.cursor.execute(
            "SELECT 1 FROM pg_indexes "\
                "WHERE tablename = %s AND indexname = %s",
            (space._name, space._name + '_dim_index',))

        if self.cursor.fetchall():
            return

        self.cursor.execute(
            'CREATE UNIQUE INDEX %s_dim_index on %s (%s)' % (
                space._name, space._name, ','.join(space._dimensions)
                )
            )

    def load_coordinates(self, dim):
        name = '%s_%s' % (dim._spc._name, dim._name)
        self.cursor.execute('SELECT id, parent, name from %s' % name)
        return self.cursor

    def create_coordinate(self, dim, name, parent_id):
        table = "%s_%s" % (dim._spc._name, dim._name)
        self.cursor.execute(
            'INSERT into %s (name, parent) VALUES (%%s, %%s)' % table,
            (name, parent_id)
            )

        select = 'SELECT id from %s where name %s %%s and parent %s %%s'
        parent_op = parent_id is None and 'is' or '='
        name_op = name is None and 'is' or '='

        self.cursor.execute(select % (table, name_op, parent_op), (
                name, parent_id
                ))
        return self.cursor.next()[0]

    def get_child_coordinates(self, dim, parent_id):
        table = "%s_%s" % (dim._spc._name, dim._name)
        select = 'SELECT name from %s where parent %s %%s'
        parent_op = parent_id is None and 'is' or '='

        self.cursor.execute(select % (table, parent_op), (parent_id,))
        return self.cursor

    def get(self, keys, flushing=False):
        if not flushing and len(self.read_cache) > self.space.MAX_CACHE:
            self.flush()

        select = ','.join(chain(self.space._dimensions, self.space._measures))
        where_dim = ','.join(self.space._dimensions)
        where_in_part = ','.join('%s' for d in self.space._dimensions)
        where_in = ','.join('(%s)' % where_in_part for k in keys \
                                if k not in self.read_cache)

        stm = ('SELECT %s FROM %s WHERE (%s) IN (%s)' % (
                select, self.space._name, where_dim, where_in))

        args = tuple(chain(*(k for k in keys if k not in self.read_cache)))

        if len(args) == 0:
            for k in keys:
                yield k, self.read_cache[k]
            return

        self.cursor.execute(stm, args)

        nbd = len(self.space._dimensions)
        res = self.cursor.fetchmany()
        while res:
            for item in res:
                self.read_cache[item[:nbd]] = item[nbd:]
            res = self.cursor.fetchmany()

        for k in keys:
            if k not in self.read_cache:
                self.read_cache[k] = None
            yield k, self.read_cache[k]

    def update(self, values):
        # TODO write lock on table
        set_stm = ','.join('%s = %%s' % m for m in self.space._measures)
        clause =  ' and '.join('%s = %%s' % d for d in self.space._dimensions)
        update_stm = 'UPDATE %s SET %s WHERE %s' % (
            self.space._name, set_stm, clause)

        args = (tuple(chain(v, k)) for k, v in values.iteritems())
        self.cursor.executemany(update_stm, args)

    def insert(self, values):
        fields = tuple(chain(self.space._measures, self.space._dimensions))

        data = StringIO( '\n'.join(
                '\t'.join(imap(str, chain(k, v)))
                for v, k in values.iteritems()
                ))
        self.cursor.copy_from(data, self.space._name, columns=fields)
        data.close()

    def get_columns_info(self, name):
        stm = 'SELECT column_name, data_type '\
            'from information_schema.columns where table_name=%s'
        self.cursor.execute(stm, (name,))
        return self.cursor.fetchall()

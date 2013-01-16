from itertools import chain
import sqlite3

class SqliteBackend():

    def __init__(self, path):
        self.connection = sqlite3.connect(path)
        self.cursor = self.connection.cursor()
        self.cursor.execute('PRAGMA journal_mode=WAL')

    def register(self, space):
        space.set_db(self)
        for dim in space._dimensions:
            name = '%s_%s' % (space._name, dim)
            # TODO put idx on dim table
            self.cursor.execute(
                'CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY,'
                    'parent INTEGER, name TEXT)' % name)

        # TODO declare foreign key
        cols = ','.join(chain(
                ('%s INTEGER NOT NULL' % i for i in space._dimensions),
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

    def update(self, space, values):
        set_stm = ','.join('%s = ?' % m for m in space._measures)
        clause =  ' and '.join('%s = ?' % d for d in space._dimensions)
        update_stm = 'UPDATE %s SET %s WHERE %s' % (
            space._name, set_stm, clause)

        args = (tuple(chain(v, k)) for k, v in values.iteritems())
        self.cursor.executemany(update_stm, args)

    def insert(self, space, values):
        fields = tuple(chain(space._measures, space._dimensions))
        val_stm = ','.join('?' for f in fields)
        field_stm = ','.join(fields)
        insert_stm = 'INSERT INTO %s (%s) VALUES (%s)' % (
            space._name, field_stm, val_stm)

        args = (tuple(chain(v, k)) for k, v in values.iteritems())
        self.cursor.executemany(insert_stm, args)

    def commit(self):
        self.connection.commit()

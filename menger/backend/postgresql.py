from itertools import chain
import psycopg2

class PGBackend():

    def __init__(self, connection_string):
        self.connection = psycopg2.connect(connection_string)
        self.cursor = self.connection.cursor()

    def register(self, space):
        space.set_db(self)
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

        # TODO declare foreign key
        cols = ','.join(chain(
                ('%s INTEGER NOT NULL' % i for i in space._dimensions),
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

    def get(self, space, key):
        select = ','.join(space._measures)
        clause = lambda x: ('%s is %%s' if x[1] is None else '%s = %%s')
        where = ' and '.join(map(clause, zip(space._dimensions, key)))

        stm = ('SELECT %s FROM %s WHERE %s' %
                (select, space._name, where)) % tuple(space._dimensions)

        self.cursor.execute(stm, key)
        res = self.cursor.fetchall()
        return res and res[0] or None

    def update(self, space, values):
        # TODO write lock on table
        set_stm = ','.join('%s = %%s' % m for m in space._measures)
        clause =  ' and '.join('%s = %%s' % d for d in space._dimensions)
        update_stm = 'UPDATE %s SET %s WHERE %s' % (
            space._name, set_stm, clause)

        args = (tuple(chain(v, k)) for k, v in values.iteritems())
        self.cursor.executemany(update_stm, args)

    def insert(self, space, values):
        fields = tuple(chain(space._measures, space._dimensions))
        val_stm = ','.join('%s' for f in fields)
        field_stm = ','.join(fields)
        insert_stm = 'INSERT INTO %s (%s) VALUES (%s)' % (
            space._name, field_stm, val_stm)

        args = (tuple(chain(v, k)) for k, v in values.iteritems())
        self.cursor.executemany(insert_stm, args)

    def commit(self):
        self.connection.commit()

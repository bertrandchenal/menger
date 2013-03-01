from itertools import chain, imap
import sqlite3

from sql import SqlBackend


class SqliteBackend(SqlBackend):

    def __init__(self, path):
        self.space = None
        self.connection = sqlite3.connect(path)
        self.cursor = self.connection.cursor()
        self.cursor.execute('PRAGMA journal_mode=WAL')
        self.write_buffer = {}
        self.read_cache = {}
        self.old_read_cache = {}

    def close(self):
        super(SqliteBackend, self).close()
        self.connection.close()

    def register(self, space):
        self.space = space
        for dim_name, dim in space._dimensions:
            name = '%s_%s' % (space._name, dim_name)
            self.cursor.execute(
                'CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY,'
                    'parent INTEGER, name %s)' % (name, dim.type))

        # TODO add unique index on (parent, name)
        # TODO declare foreign key
        cols = ','.join(chain(
                ('%s INTEGER NOT NULL references %s_%s (id)' % (
                        i, space._name, i
                        ) for i, _ in space._dimensions),
                ('%s REAL NOT NULL' % i for i, _ in space._measures)
                ))
        query = 'CREATE TABLE IF NOT EXISTS %s (%s)' % (space._name, cols)
        self.cursor.execute(query)

        self.cursor.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS %s_dim_index on %s (%s)' % (
                space._name,
                space._name,
                ','.join(d for d, _ in space._dimensions)
                )
            )

    def load_coordinates(self, dim):
        name = '%s_%s' % (self.space._name, dim._name)
        return self.cursor.execute('SELECT id, parent, name from %s' % name)

    def create_coordinate(self, dim, name, parent_id):
        table = "%s_%s" % (self.space._name, dim._name)
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
        table = "%s_%s" % (self.space._name, dim._name)
        select = 'SELECT name from %s where parent %s ?'
        parent_op = parent_id is None and 'is' or '='

        res = self.cursor.execute(select % (table, parent_op), (parent_id,))
        return res

    def get(self, keys, flushing=False):
        if not flushing and len(self.read_cache) > self.space.MAX_CACHE:
            self.flush()

        select = ','.join(m for m, _ in self.space._measures)
        clause = lambda x: ('%s is ?' if x[1] is None else '%s = ?') % x[0]
        stm = 'SELECT %s FROM %s WHERE ' % (select, self.space._name)

        for key in keys:
            if key in self.read_cache:
                yield key, self.read_cache[key]
                continue

            where = ' and '.join(imap(clause, zip(
                        (d for d, _ in self.space._dimensions),
                        key
                        )))
            values = self.cursor.execute(stm + where, key).fetchone()

            self.read_cache[key] = values
            yield key, values

    def update(self, values):
        set_stm = ','.join('%s = ?' % m for m, _ in self.space._measures)
        clause =  ' and '.join('%s = ?' % d for d, _ in self.space._dimensions)
        update_stm = 'UPDATE %s SET %s WHERE %s' % (
            self.space._name, set_stm, clause)

        args = (tuple(chain(v, k)) for k, v in values.iteritems())
        self.cursor.executemany(update_stm, args)

    def insert(self, values):
        fields = tuple(chain(
                (m for m, _ in self.space._measures),
                (d for d, _ in self.space._dimensions)
                ))
        val_stm = ','.join('?' for f in fields)
        field_stm = ','.join(fields)
        insert_stm = 'INSERT INTO %s (%s) VALUES (%s)' % (
            self.space._name, field_stm, val_stm)

        args = (tuple(chain(v, k)) for k, v in values.iteritems())
        self.cursor.executemany(insert_stm, args)


    def get_columns_info(self, name):
        # FIXME user  PRAGMA foreign_key_list(table);
        stm = 'PRAGMA table_info(%s)'
        self.cursor.execute(stm % name)
        for space_info in list(self.cursor):
            col_name = space_info[1]
            col_type = space_info[2].lower()
            if col_type == 'integer':
                table = '%s_%s' % (name, col_name)
                self.cursor.execute(stm % table)
                for dim_info in self.cursor:
                    dim_col = dim_info[1]
                    dim_type = dim_info[2].lower()
                    if dim_col == 'name':
                        yield col_name, col_type, dim_type
                        break
            else:
                yield col_name, col_type, None

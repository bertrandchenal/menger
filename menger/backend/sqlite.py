from itertools import chain
import sqlite3

from sql import SqlBackend


class SqliteBackend(SqlBackend):

    def __init__(self, path):
        self.connection = sqlite3.connect(path)
        self.cursor = self.connection.cursor()
        self.cursor.execute('PRAGMA journal_mode=WAL')
        super(SqlBackend, self).__init__()

    def close(self):
        super(SqliteBackend, self).close()
        self.connection.close()

    def register(self, space):
        self.space = space
        self.cursor.execute('PRAGMA foreign_keys=1')
        for dim_name, dim in space._dimensions:
            dim_table = '%s_%s' % (space._name, dim_name)

            # Dimension table
            self.cursor.execute(
                'CREATE TABLE IF NOT EXISTS %s ( '
                'id INTEGER PRIMARY KEY, '
                'name %s)' % (dim_table, dim.type))

            # Closure table for the dimension
            cls_table = dim_table + '_closure'
            self.cursor.execute(
                'CREATE TABLE IF NOT EXISTS %s ('
                'parent INTEGER  references %s (id), '
                'child INTEGER  references %s (id), '
                'depth INTEGER)' % (cls_table, dim_table, dim_table))

            self.cursor.execute(
                'CREATE INDEX IF NOT EXISTS %s_cls_index '
                'ON %s (parent, child)' % (cls_table, cls_table))

        # Space (main) table
        cols = ','.join(chain(
            ('%s INTEGER references %s_%s (id) NOT NULL' % (
                i, space._name, i
            ) for i, _ in space._dimensions),
            ('%s %s NOT NULL' % (i, m.type) for i, m in space._measures)
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

    def create_coordinate(self, dim, name, parent_id):
        table = "%s_%s" % (self.space._name, dim._name)
        closure = table + '_closure'

        # Fill dimension table
        self.cursor.execute(
            'INSERT into %s (name) VALUES (?)' % table, (name,))
        last_id = self.cursor.lastrowid

        # Fetch parent depth + 1 as last_id from the closure table ...
        self.cursor.execute(
            'SELECT parent, ? as child, depth+1 FROM %s '
            'WHERE child = ?' % closure, (last_id, parent_id))

        # ... and insert them
        stm = 'INSERT INTO %s (parent, child, depth) '\
            ' VALUES (?, ?, ?)' % closure
        self.cursor.executemany(stm, self.cursor.fetchall())
        self.cursor.execute(stm, (last_id, last_id, 0))

        return last_id

    def get_childs(self, dim, parent_id):
        table = "%s_%s" % (self.space._name, dim._name)
        closure = table + '_closure'

        if parent_id is None:
            stm = "SELECT name, id from %s where name is null" % table
            args = tuple()
        else:
            stm = 'SELECT d.name, d.id ' \
                'FROM %s AS c JOIN %s AS d ON (c.child = d.id) '\
                'WHERE c.depth = 1 AND c.parent = ?' % (closure, table)
            args = (parent_id,)
        return self.cursor.execute(stm, args)

    def get(self, keys, flushing=False):
        """
        Return values for all measures for given keys.
        The keys must share the same form (same columns must be set).
        """

        if not flushing and len(self.read_cache) > self.space.MAX_CACHE:
            self.flush()

        select = ','.join(
            'coalesce(sum(%s), 0)' % m for m, _ in self.space._measures)
        table = self.space._name
        tail = ''

        if not flushing:
            first = next(keys)
            wheres = []
            for coord, (dname, dim) in zip(first, self.space._dimensions):
                if coord == dim.key(tuple()):
                    wheres.append("%s = ?" % dname)
                else:
                    tail += self.build_join(table, dname)
            if wheres:
                tail += ' WHERE ' + ' and '.join(wheres)
            keys = chain((first,), keys)

        else:
            tail = 'WHERE ' + ' and '.join(
                "%s = ?" % d for d, _ in self.space._dimensions)

        stm = 'SELECT %s FROM %s %s' % (select, table, tail)

        for key in keys:
            if key in self.read_cache:
                yield key, self.read_cache[key]
                continue
            values = self.cursor.execute(stm, key).fetchone()

            self.read_cache[key] = values
            yield key, values

    def build_join(self, spc, dname):
        #TODO tester les perfs si on a une colonne "leaf BOOLEAN" pour
        #booster les subselect
        cls = "%s_%s_closure" % (spc, dname)
        return " JOIN %s ON (%s.%s = %s.child and %s.parent = ?)" \
            % (cls, spc, dname, cls, cls)

    def update(self, values):
        set_stm = ','.join('%s = ?' % m for m, _ in self.space._measures)
        clause = ' and '.join('%s = ?' % d for d, _ in self.space._dimensions)
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
        stm = 'PRAGMA foreign_key_list(%s)'
        fk = set(x[3] for x in self.cursor.execute(stm % name))

        stm = 'PRAGMA table_info(%s)'
        self.cursor.execute(stm % name)
        for space_info in list(self.cursor):
            col_name = space_info[1]
            col_type = space_info[2].lower()
            if col_name in fk:
                table = '%s_%s' % (name, col_name)
                self.cursor.execute(stm % table)
                for dim_info in self.cursor:
                    dim_col = dim_info[1]
                    dim_type = dim_info[2].lower()
                    if dim_col == 'name':
                        yield col_name, 'dimension', dim_type
                        break
            else:
                yield col_name, 'measure', col_type

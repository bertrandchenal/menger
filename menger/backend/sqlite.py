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
        self.clean_cache()
        self.connection.commit()
        self.connection.close()

    def register(self, space):
        self.space = space
        self.cursor.execute('PRAGMA foreign_keys=1')
        space_table = space._name
        cache_table = space_table + '__cache'
        for dim_name, dim in space._dimensions:
            dim_table = '%s_%s' % (space_table, dim_name)

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
                i, space_table, i
            ) for i, _ in space._dimensions),
            ('%s %s NOT NULL' % (i, m.type) for i, m in space._measures)
        ))


        for table in (space_table, cache_table):
            extra_col = ''
            if table == cache_table:
                extra_col = ",last_read INT(4) DEFAULT (strftime('%s','now'))"
            query = 'CREATE TABLE IF NOT EXISTS %s (%s)' % (
                table, cols + extra_col)

            self.cursor.execute(query)

            self.cursor.execute(
                'CREATE UNIQUE INDEX IF NOT EXISTS %s_dim_index on %s (%s)' % (
                    table,
                    table,
                    ','.join(d for d, _ in space._dimensions)
                    )
                )

        measures = [m[0] for m in self.space._measures]
        dimensions = [d[0] for d in self.space._dimensions]

        # exist_stm
        dim_where = 'WHERE ' + ' and '.join("%s = ?" % d for d in dimensions)
        self.exist_stm = 'SELECT 1 FROM %s %s' % (space_table, dim_where)

        # get_cache_stm
        msr_select = ','.join(measures)
        self.get_cache_stm = 'SELECT %s FROM %s %s' % (
            msr_select, cache_table, dim_where)

        # insert_cache_stm
        fields = list(chain(measures, dimensions))
        values = ','.join('?' for f in fields)
        self.insert_cache_stm = 'INSERT INTO %s (%s) VALUES (%s)' % (
            cache_table, ','.join(fields), values)

        # update_stm
        set_stm = ','.join('%s = ?' % m for m in measures)
        clause = ' and '.join('%s = ?' % d for d in dimensions)
        self.update_stm = 'UPDATE %s SET %s WHERE %s' % (
            space_table, set_stm, clause)

        #insert_stm
        fields = tuple(chain(dimensions, measures))

        val_stm = ','.join('?' for f in fields)
        field_stm = ','.join(fields)
        self.insert_stm = 'INSERT INTO %s (%s) VALUES (%s)' % (
            space_table, field_stm, val_stm)

        # update_cache_stm
        set_values = ','.join('%s = %s + ?' % (m, m) for m in measures)
        conds = [self.parent_cond(space_table, d) for d in dimensions]
        where = ' AND '.join(conds)
        self.update_cache_stm = 'UPDATE %s SET %s WHERE %s' % (
            cache_table, set_values, where)

        # set_cache_timestamp_stm
        where = ' AND '.join('%s = ?' % d for d in dimensions)
        self.set_cache_timestamp_stm = "UPDATE %s SET "\
            "last_read = strftime('%%s','now') WHERE %s" % (cache_table, where)

        # test_cache_stm
        self.test_cache_stm = 'SELECT count(1) FROM %s' % cache_table

        # delete_cache_stm
        self.delete_cache_stm = 'DELETE FROM %s '\
            'WHERE last_read in (SELECT min(last_read) FROM %s)' % (
            cache_table, cache_table)

    def clean_cache(self):
        while True:
            self.cursor.execute(self.test_cache_stm)
            if self.cursor.fetchone()[0] > self.space.MAX_CACHE:
                self.cursor.execute(self.delete_cache_stm)
            else:
                break

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

    def exist(self, key):
        values = self.cursor.execute(self.exist_stm, key).fetchone()
        return values

    def get_cache(self, key):
        values = self.cursor.execute(self.get_cache_stm, key).fetchone()
        if values:
            self.cursor.execute(self.set_cache_timestamp_stm, key)
        return values

    def insert_cache(self, key, values):
        self.cursor.execute(self.insert_cache_stm, values + key)

    def update_cache(self, key, values):
        self.cursor.execute(self.update_cache_stm, values + key)

    def parent_cond(self, spc, dname):
        cls = "%s_%s_closure" % (spc, dname)
        return " (%s in ("\
            "SELECT parent FROM %s WHERE child = :%s AND depth > 0)) " \
            % (dname, cls, dname)

    def get(self, key):
        table = self.space._name
        select = ','.join(
            'coalesce(sum(%s), 0)' % m for m, _ in self.space._measures)
        tail = ''
        for coord, (dname, dim) in zip(key, self.space._dimensions):
            if coord != dim.key(tuple()):
                tail += self.child_join(table, dname)

        stm = 'SELECT %s FROM %s %s' % (select, table, tail)
        params = dict(zip((d[0] for d in self.space._dimensions), key))
        return self.cursor.execute(stm, params).fetchone()

    def child_join(self, spc, dname):
        #TODO tester les perfs si on a une colonne "leaf BOOLEAN" pour
        #booster les subselect
        cls = "%s_%s_closure" % (spc, dname)
        return " JOIN %s ON (%s.%s = %s.child and %s.parent = :%s)" \
            % (cls, spc, dname, cls, cls, dname)


    def update(self, k, v):
        self.cursor.execute(self.update_stm, v + k)

    def insert(self, k, v):
        self.cursor.execute(self.insert_stm, k + v)

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

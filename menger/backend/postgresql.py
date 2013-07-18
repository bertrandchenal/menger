from cStringIO import StringIO
from itertools import chain, imap
from sql import SqlBackend
import psycopg2


class PGBackend(SqlBackend):

    def __init__(self, connection_string):
        self.space = None
        self.connection = psycopg2.connect(connection_string)
        self.cursor = self.connection.cursor()
        super(PGBackend, self).__init__()

    def close(self):
        self.clean_cache()
        self.connection.commit()
        self.connection.close()

    def register(self, space):
        self.space = space
        space_table = space._name
        cache_table = space_table + '__cache'

        create_idx = 'CREATE UNIQUE INDEX %s_parent_child_index'\
            ' on %s (parent, child)'
        for dim_name, dim in space._dimensions:
            dim_table = '%s_%s' % (space_table, dim_name)

            # Dimension table
            self.cursor.execute(
                'CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY,'
                'name %s)' % (dim_table, dim.type))

            # Closure table for the dimension
            cls_table = dim_table + '_closure'
            self.cursor.execute(
                'CREATE TABLE IF NOT EXISTS %s ('
                'parent INTEGER references %s (id), '
                'child INTEGER references %s (id), '
                'depth INTEGER)' % (cls_table, dim_table, dim_table))

            self.cursor.execute(
                "SELECT 1 FROM pg_indexes "
                "WHERE tablename = %s AND indexname = %s",
                (cls_table, cls_table + '_parent_child_index',))

            if not self.cursor.fetchall():
                self.cursor.execute(create_idx % (cls_table, cls_table))

        # Space (main) table
        cols = ','.join(chain(
            ('"%s" INTEGER NOT NULL references %s_%s (id)' % (
                i, space_table, i
            ) for i, _ in space._dimensions),
            ('"%s" %s NOT NULL' % (i, m.type) for i, m in space._measures)
        ))

        for table in (space_table, cache_table):
            extra_col = ''
            if table == cache_table:
                extra_col = ",last_read INTEGER "\
                    "DEFAULT extract(epoch from now())::int"
            query = 'CREATE TABLE IF NOT EXISTS %s (%s)' % (
                table, cols + extra_col)


            self.cursor.execute(query)

            self.cursor.execute(
                "SELECT 1 FROM pg_indexes "
                "WHERE tablename = %s AND indexname = %s",
                (table, table + '_dim_index',))

            if not self.cursor.fetchall():
                self.cursor.execute(
                    'CREATE UNIQUE INDEX %s_dim_index on %s (%s)' % (
                        table, table,
                        ','.join('"%s"' % d for d, _ in space._dimensions)
                        )
                    )

        measures = [m[0] for m in self.space._measures]
        dimensions = [d[0] for d in self.space._dimensions]

        # exist_stm
        dim_where = 'WHERE ' + ' and '.join("%s = %%s" % d for d in dimensions)
        self.exist_stm = 'SELECT 1 FROM %s %s' % (space_table, dim_where)

        # get_cache_stm
        msr_select = ','.join(measures)
        self.get_cache_stm = 'SELECT %s FROM %s %s' % (
            msr_select, cache_table, dim_where)

        # insert_cache_stm
        fields = list(chain(measures, dimensions))
        values = ','.join('%s' for f in fields)
        self.insert_cache_stm = 'INSERT INTO %s (%s) VALUES (%s)' % (
            cache_table, ','.join(fields), values)

        # update_stm
        set_stm = ','.join('%s = %%s' % m for m in measures)
        clause = ' and '.join('%s = %%s' % d for d in dimensions)
        self.update_stm = 'UPDATE %s SET %s WHERE %s' % (
            space_table, set_stm, clause)

        #insert_stm
        fields = tuple(chain(dimensions, measures))

        val_stm = ','.join('%s' for f in fields)
        field_stm = ','.join(fields)
        self.insert_stm = 'INSERT INTO %s (%s) VALUES (%s)' % (
            space_table, field_stm, val_stm)

        # update_cache_stm
        set_values = ','.join('%s = %s + %%s' % (m, m) for m in measures)
        conds = [self.parent_cond(space_table, d) for d in dimensions]
        where = ' AND '.join(conds)
        self.update_cache_stm = 'UPDATE %s SET %s WHERE %s' % (
            cache_table, set_values, where)

        # set_cache_timestamp_stm
        where = ' AND '.join('%s = %%s' % d for d in dimensions)
        self.set_cache_timestamp_stm = "UPDATE %s SET "\
            "last_read = extract(epoch from now())::int "\
            "WHERE %s" % (cache_table, where)

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
            'INSERT into %s (name) VALUES (%%s) RETURNING id' % table, (name,))

        last_id = self.cursor.next()[0]

        # Update closure table
        self.cursor.execute(
            'INSERT INTO %s (parent, child, depth) '
            'SELECT parent, %%s, depth+1 FROM %s '
            'WHERE child = %%s' % (closure, closure), (last_id, parent_id))

        # Add new coordinate
        self.cursor.execute('INSERT INTO %s (parent, child, depth) '
                            'VALUES (%%s, %%s, %%s)' % closure,
                            (last_id, last_id, 0))
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
                'WHERE c.depth = 1 AND c.parent = %%s' % (closure, table)
            args = (parent_id,)

        self.cursor.execute(stm, args)
        return self.cursor

    def exist(self, key):
        self.cursor.execute(self.exist_stm, key)
        return self.cursor.fetchone()

    def get_cache(self, key):
        self.cursor.execute(self.get_cache_stm, key)
        values = self.cursor.fetchone()
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
            "SELECT parent FROM %s WHERE child = %%s AND depth > 0)) " \
            % (dname, cls)

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
        self.cursor.execute(stm, params)
        return self.cursor.fetchone()

    def child_join(self, spc, dname):
        #TODO tester les perfs si on a une colonne "leaf BOOLEAN" pour
        #booster les subselect
        cls = "%s_%s_closure" % (spc, dname)
        return " JOIN %s ON (%s.%s = %s.child and %s.parent = %%(%s)s)" \
            % (cls, spc, dname, cls, cls, dname)


    def update(self, k, v):
        self.cursor.execute(self.update_stm, v + k)

    def insert(self, k, v):
        self.cursor.execute(self.insert_stm, k + v)

    def get_columns_info(self, name):
        stm = "SELECT constraint_name " \
            "from information_schema.table_constraints" \
            " where table_name = '%s' and constraint_type = 'FOREIGN KEY'"
        self.cursor.execute(stm % name)
        fk = set(x[0] for x in self.cursor.fetchall())

        stm = 'SELECT column_name, data_type '\
            "from information_schema.columns where table_name=%s"
        self.cursor.execute(stm, (name,))

        for col_name, col_type in self.cursor.fetchall():
            if '%s_%s_fkey' % (name, col_name) in fk:
                table = '%s_%s' % (name, col_name)
                self.cursor.execute(stm, (table,))
                for dim_col, dim_type in self.cursor.fetchall():
                    if dim_col == 'name':
                        yield col_name, 'dimension', dim_type
                        break
            else:
                yield col_name, 'measure', col_type

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
        super(PGBackend, self).close()
        self.connection.close()

    def register(self, space):
        self.space = space
        create_idx = 'CREATE UNIQUE INDEX %s_parent_child_index'\
            ' on %s (parent, child)'
        for dim_name, dim in space._dimensions:
            dim_table = '%s_%s' % (space._name, dim_name)

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
                i, space._name, i
            ) for i, _ in space._dimensions),
            ('"%s" %s NOT NULL' % (i, m.type) for i, m in space._measures)
        ))
        query = 'CREATE TABLE IF NOT EXISTS %s (%s)' % (space._name, cols)

        self.cursor.execute(query)

        self.cursor.execute(
            "SELECT 1 FROM pg_indexes "
            "WHERE tablename = %s AND indexname = %s",
            (space._name, space._name + '_dim_index',))

        if not self.cursor.fetchall():
            self.cursor.execute(
                'CREATE UNIQUE INDEX %s_dim_index on %s (%s)' % (
                    space._name, space._name,
                    ','.join('"%s"' % d for d, _ in space._dimensions)
                )
            )

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
            stm = "SELECT id, name from %s where name is null" % table
            args = tuple()
        else:
            stm = 'SELECT d.id, d.name ' \
                'FROM %s AS c JOIN %s AS d ON (c.child = d.id) '\
                'WHERE c.depth = 1 AND c.parent = %%s' % (closure, table)
            args = (parent_id,)

        self.cursor.execute(stm, args)
        return self.cursor

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
        stm = 'SELECT %s FROM %s WHERE ' % (select, table)
        if not flushing:
            first = next(keys)
            stm += ' and '.join(map(
                self.build_clause,
                zip(first, self.space._dimensions)
            ))
            keys = chain((first,), keys)
        else:
            stm += ' and '.join(
                "%s = %%s" % d for d, _ in self.space._dimensions)

        for key in keys:
            if key in self.read_cache:
                yield key, self.read_cache[key]
                continue
            self.cursor.execute(stm, key)
            values = self.cursor.fetchone()

            self.read_cache[key] = values
            yield key, values

    def build_clause(self, args):
        key, (dname, dim) = args
        if key == dim.key(tuple()):
            return "%s = %%s" % dname

        #TODO comparer les perfs avec un join
        #TODO tester les perfs si on a une colonne "leaf BOOLEAN" pour
        #booster les subselect

        table = dim._spc._name
        return "%s in (SELECT child from %s_%s_closure WHERE parent = %%s"\
            ")" % (dname, table, dname)

    def update(self, values):
        # TODO write lock on table
        set_stm = ','.join('"%s" = %%s' % m for m, _ in self.space._measures)
        clause = ' and '.join('"%s" = %%s' %
                              d for d, _ in self.space._dimensions)
        update_stm = 'UPDATE %s SET %s WHERE %s' % (
            self.space._name, set_stm, clause)

        args = (tuple(chain(v, k)) for k, v in values.iteritems())
        self.cursor.executemany(update_stm, args)

    def insert(self, values):
        fields = tuple(chain(
            ('"%s"' % m for m, _ in self.space._measures),
            ('"%s"' % d for d, _ in self.space._dimensions)
        ))

        data = StringIO('\n'.join(
            '\t'.join(imap(str, chain(k, v)))
            for v, k in values.iteritems()
        ))
        self.cursor.copy_from(data, self.space._name, columns=fields)
        data.close()

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

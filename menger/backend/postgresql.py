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

    def register(self, space):
        self.space = space
        space_table = space._name

        create_idx = 'CREATE UNIQUE INDEX %s_parent_child_index'\
            ' on %s (parent, child)'
        for dim in space._dimensions:
            dim_table = '%s_%s' % (space_table, dim.name)

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
                d.name, space_table, d.name
            ) for d in space._dimensions),
            ('"%s" %s NOT NULL' % (m.name, m.type) for m in space._measures)
        ))

        query = 'CREATE TABLE IF NOT EXISTS %s (%s)' % (space_table, cols)
        self.cursor.execute(query)

        self.cursor.execute(
            "SELECT indexname FROM pg_indexes WHERE tablename = %s",
            (space_table,))
        all_idx = [x[0] for x in self.cursor.fetchall()]

        # Create one index per dimension
        for d in space._dimensions:
            idx_name = '%s_%s_index' % (space_table, d.name)
            if idx_name not in all_idx:
                self.cursor.execute(
                    'CREATE INDEX %s on %s ("%s")' % (
                        idx_name, space_table, d.name)
                    )

        measures = [m.name for m in self.space._measures]
        dimensions = [d.name for d in self.space._dimensions]

        # get_stm
        select = ', '.join(measures)
        dim_where = 'WHERE ' \
            + ' AND '.join('"%s" = %%s' % d for d in dimensions)
        self.get_stm = 'SELECT %s FROM %s %s' % (
            select, space_table, dim_where)

        # update_stm
        set_stm = ','.join('"%s" = %%s' % m for m in measures)
        clause = ' and '.join('"%s" = %%s' % d for d in dimensions)
        self.update_stm = 'UPDATE %s SET %s WHERE %s' % (
            space_table, set_stm, clause)

        #insert_stm
        fields = tuple(chain(dimensions, measures))
        field_stm = ','.join('"%s"' % f for f in fields)
        val_stm = ','.join('%s' for f in fields)
        self.insert_stm = 'INSERT INTO %s (%s) VALUES (%s)' % (
            space_table, field_stm, val_stm)

    def create_coordinate(self, dim, name, parent_id):
        table = "%s_%s" % (self.space._name, dim.name)
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
        table = "%s_%s" % (self.space._name, dim.name)
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


    def get_parents(self, dim):
        dim_table = "%s_%s" % (self.space._name, dim.name)
        cls_table = dim_table + '_closure'
        stm = "SELECT id, name, parent FROM %s"\
            " JOIN %s ON (child = id) WHERE depth = 1"\
            %(dim_table, cls_table)
        self.cursor.execute(stm)
        return self.cursor.fetchall()

    def get(self, key):
        self.cursor.execute(self.get_stm, key)
        return self.cursor.fetchone()

    def dice(self, cube, msrs):
        table = self.space._name
        select = []
        joins = []
        where = []
        group_by = []
        params = {}

        for dim, coord, depth in cube:
            params[dim.name] = coord

            joins.append(self.child_join(table, dim))
            f = '%s_%s_closure.parent'% (table, dim.name)
            select.append(f)
            group_by.append(f)
            params[dim.name + '_depth'] = depth

        select.extend('sum(%s)' % m.name for m in msrs)
        stm = 'SELECT %s FROM %s' % (', '.join(select), table)

        if joins:
            stm += ' ' + ' '.join(joins)
        if where:
            stm += ' WHERE ' +  ' AND '.join(where)
        if group_by:
            stm += ' GROUP BY ' + ', '.join(group_by)

        self.cursor.execute(stm, params)
        return self.cursor.fetchall()


    def child_join(self, spc, dim):
        cls = "%s_%s_closure" % (spc, dim.name)
        dim_table = "%s_%s" % (spc, dim.name)
        join = "JOIN %s ON (%s.child = %s.%s" \
            " AND %s.parent IN (SELECT child from %s WHERE parent = %%(%s)s" \
            " AND depth = %%(%s)s))" \
            % (cls, cls, spc, dim.name, cls, cls, dim.name,
               dim.name + '_depth')
        return join

    def update(self, k, v):
        self.cursor.execute(self.update_stm, v + k)

    def insert(self, k, v):
        self.cursor.execute(self.insert_stm, k + v)

    def close(self):
        self.connection.commit()
        self.connection.close()

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

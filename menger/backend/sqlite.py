from itertools import chain, repeat
import sqlite3

from sql import SqlBackend


class SqliteBackend(SqlBackend):

    def __init__(self, path):
        self.connection = sqlite3.connect(path)
        self.cursor = self.connection.cursor()
        self.cursor.execute('PRAGMA journal_mode=WAL')
        super(SqliteBackend, self).__init__()

    def register(self, space):
        self.cursor.execute('PRAGMA foreign_keys=1')

        for dim in space._dimensions:
            # Dimension table
            self.cursor.execute(
                'CREATE TABLE IF NOT EXISTS "%s" ( '
                'id INTEGER PRIMARY KEY, '
                'name %s)' % (dim.table, dim.sql_type))

            # Closure table for the dimension
            self.cursor.execute(
                'CREATE TABLE IF NOT EXISTS "%s" ('
                'parent INTEGER references "%s" (id), '
                'child INTEGER references "%s" (id), '
                'depth INTEGER)' % (dim.closure_table, dim.table,
                                    dim.table))

        # Space (main) table
        cols = ', '.join(chain(
            ('"%s" INTEGER references %s (id) NOT NULL' % (
                dim.name,  dim.table
            ) for dim in space._dimensions),
            ('"%s" %s NOT NULL' % (msr.name, msr.sql_type) \
             for msr in space._measures)
        ))
        query = 'CREATE TABLE IF NOT EXISTS "%s" (%s)' % (
            space._table, cols)
        self.cursor.execute(query)

        # Create index covering all dimensions
        self.cursor.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS %s_dim_index on "%s" (%s)' % (
                space._table,
                space._table,
                ' ,'.join(d.name for d in space._dimensions)
                )
            )

        # Create one index per dimension
        for d in space._dimensions:
            self.cursor.execute(
                'CREATE INDEX IF NOT EXISTS %s_%s_index on "%s" (%s)' % (
                    space._table,
                    d.name,
                    space._table,
                    d.name
                    )
                )

        measures = [m.name for m in space._measures]
        dimensions = [d.name for d in space._dimensions]

        # get_stm
        select = ', '.join(measures)
        dim_where = 'WHERE ' + ' AND '.join('"%s" = ?' % d for d in dimensions)
        self.get_stm[space._name] = 'SELECT %s FROM "%s" %s' % (
            select, space._table, dim_where)

        # update_stm
        set_stm = ', '.join('"%s" = ?' % m for m in measures)
        clause = ' and '.join('"%s" = ?' % d for d in dimensions)
        self.update_stm[space._name] = 'UPDATE "%s" SET %s WHERE %s' % (
            space._table, set_stm, clause)

        #insert_stm
        fields = tuple(chain(dimensions, measures))

        val_stm = ', '.join('?' for f in fields)
        field_stm = ', '.join(fields)
        self.insert_stm[space._name] = 'INSERT INTO "%s" (%s) VALUES (%s)' % (
            space._table, field_stm, val_stm)

    def load(self, space, keys_vals):
        # TODO check for equivalent in postgresql
        res = super(SqliteBackend, self).load(space, keys_vals)
        self.cursor.execute('ANALYZE')
        return res

    def create_coordinate(self, dim, name, parent_id):
        # Fill dimension table
        self.cursor.execute(
            'INSERT into %s (name) VALUES (?)' % dim.table, (name,))
        last_id = self.cursor.lastrowid

        # Fetch parent depth + 1 as last_id from the closure table ...
        self.cursor.execute(
            'SELECT parent, ? as child, depth+1 FROM "%s" '
            'WHERE child = ?' % dim.closure_table, (last_id, parent_id))

        # ... and insert them
        stm = 'INSERT INTO %s (parent, child, depth) '\
            ' VALUES (?, ?, ?)' % dim.closure_table
        self.cursor.executemany(stm, self.cursor.fetchall())
        self.cursor.execute(stm, (last_id, last_id, 0))
        return last_id

    def get_childs(self, dim, parent_id, depth=1):
        if parent_id is None:
            stm = 'SELECT name, id from "%s" where name is null' % dim.table
            args = tuple()
        else:
            stm = 'SELECT d.name, d.id ' \
                'FROM "%s" AS c JOIN %s AS d ON (c.child = d.id) '\
                'WHERE c.depth = ? AND c.parent = ?' % (
                    dim.closure_table, dim.table)
            args = (depth, parent_id)

        return self.cursor.execute(stm, args)

    def get_parents(self, dim):
        stm = 'SELECT id, name, parent FROM "%s"'\
            ' JOIN %s ON (child = id) WHERE depth = 1'\
            %(dim.table, dim.closure_table)
        self.cursor.execute(stm)
        return self.cursor.fetchall()

    def dice(self, space, msrs, cube):
        select = []
        joins = []
        where = []
        group_by = []
        params = {}

        for dim, coord, depth in cube:
            params[dim.name] = coord
            joins.append(self.child_join(space, dim))
            f = '%s.parent'% (dim.closure_table)
            select.append(f)
            group_by.append(f)
            params[dim.name + '_depth'] = depth

        select.extend('coalesce(sum(%s), 0)' % m.name for m in msrs)
        stm = 'SELECT %s FROM "%s"' % (', '.join(select), space._table)

        if joins:
            stm += ' ' + ' '.join(joins)
        if where:
            stm += ' WHERE ' +  ' AND '.join(where)
        if group_by:
            stm += ' GROUP BY ' + ', '.join(group_by)

        self.cursor.execute(stm, params)
        return self.cursor.fetchall()

    def child_join(self, spc, dim):
        closure = dim.closure_table
        join = 'JOIN %s ON (%s.child = "%s".%s'\
               ' AND %s.parent IN (SELECT child from "%s" WHERE parent = :%s'\
               ' AND depth = :%s))'\
               % (closure, closure, spc._table, dim.name, closure, closure,
                  dim.name, dim.name + '_depth')
        return join

    def close(self):
        self.connection.commit()
        self.connection.close()

    def get_columns_info(self, name):
        stm = 'PRAGMA foreign_key_list("%s")'
        fk = set(x[3] for x in self.cursor.execute(stm % name))

        stm = 'PRAGMA table_info("%s")'
        self.cursor.execute(stm % name)
        for space_info in list(self.cursor):
            col_name = space_info[1]
            col_type = space_info[2].lower()
            if col_name in fk:
                self.cursor.execute(stm % col_name + '_dim')
                for dim_info in self.cursor:
                    dim_col = dim_info[1]
                    dim_type = dim_info[2].lower()
                    if dim_col == 'name':
                        yield col_name, 'dimension', dim_type
                        break
            else:
                yield col_name, 'measure', col_type

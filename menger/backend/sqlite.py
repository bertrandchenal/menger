from itertools import chain, repeat
import sqlite3

from sql import SqlBackend


class SqliteBackend(SqlBackend):

    def __init__(self, path):
        self.connection = sqlite3.connect(path)
        self.cursor = self.connection.cursor()
        self.cursor.execute('PRAGMA journal_mode=WAL')
        super(SqlBackend, self).__init__()

    def close(self):
        self.connection.commit()
        self.connection.close()

    def register(self, space):
        self.space = space
        self.cursor.execute('PRAGMA foreign_keys=1')
        space_table = space._name
        for dim in space._dimensions:
            dim_table = '%s_%s' % (space_table, dim.name)

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
        cols = ', '.join(chain(
            ('%s INTEGER references %s_%s (id) NOT NULL' % (
                dim.name, space_table, dim.name
            ) for dim in space._dimensions),
            ('%s %s NOT NULL' % (msr.name, msr.type) for msr in space._measures)
        ))
        query = 'CREATE TABLE IF NOT EXISTS %s (%s)' % (
            space_table, cols)
        self.cursor.execute(query)

        for d in space._dimensions:
            self.cursor.execute(
                'CREATE INDEX IF NOT EXISTS %s_%s_index on %s (%s)' % (
                    space_table,
                    d.name,
                    space_table,
                    d.name
                    )
                )

        measures = [m.name for m in self.space._measures]
        dimensions = [d.name for d in self.space._dimensions]

        # get_stm
        select = ', '.join(measures)
        dim_where = 'WHERE ' + ' AND '.join("%s = ?" % d for d in dimensions)
        self.get_stm = 'SELECT %s FROM %s %s' % (
            select, space_table, dim_where)

        # update_stm
        set_stm = ', '.join('%s = ?' % m for m in measures)
        clause = ' and '.join('%s = ?' % d for d in dimensions)
        self.update_stm = 'UPDATE %s SET %s WHERE %s' % (
            space_table, set_stm, clause)

        #insert_stm
        fields = tuple(chain(dimensions, measures))

        val_stm = ', '.join('?' for f in fields)
        field_stm = ', '.join(fields)
        self.insert_stm = 'INSERT INTO %s (%s) VALUES (%s)' % (
            space_table, field_stm, val_stm)


    def create_coordinate(self, dim, name, parent_id):
        table = "%s_%s" % (self.space._name, dim.name)
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

    def get_childs(self, dim, parent_id, depth=1):
        table = "%s_%s" % (self.space._name, dim.name)
        closure = table + '_closure'

        if parent_id is None:
            stm = "SELECT name, id from %s where name is null" % table
            args = tuple()
        else:
            stm = 'SELECT d.name, d.id ' \
                'FROM %s AS c JOIN %s AS d ON (c.child = d.id) '\
                'WHERE c.depth = ? AND c.parent = ?' % (closure, table)
            args = (depth, parent_id)

        return self.cursor.execute(stm, args)

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

        select.extend('coalesce(sum(%s), 0)' % m.name for m in msrs)
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
            " AND %s.parent IN (SELECT child from %s WHERE parent = :%s" \
            " AND depth = :%s))" \
            % (cls, cls, spc, dim.name, cls, cls, dim.name,
               dim.name + '_depth')
        return join

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

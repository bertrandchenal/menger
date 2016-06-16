from collections import defaultdict
from itertools import chain, repeat
import sqlite3

from .sql import SqlBackend, LoadType


def format_query(stm, params):
    for k, v in params.items():
        stm = stm.replace(':'+k, str(v))
    return stm


class SqliteBackend(SqlBackend):

    def __init__(self, path, readonly=False):
        if readonly and path != ':memory:':
            path = 'file:%s?mode=ro' % path
            uri = True
        else:
            uri=False

        self.readonly = readonly
        self.connection = sqlite3.connect(path, uri=uri)
        self.cursor = self.connection.cursor()
        self.execute('PRAGMA journal_mode=WAL')
        self.execute('PRAGMA foreign_keys=1')
        self.nb_tmp = 0

        super(SqliteBackend, self).__init__()

    def execute(self, query, args=None):
        if args is not None:
            return self.cursor.execute(query, args)
        return self.cursor.execute(query)

    def init_tables(self, space):
        if space._name in self.init_done:
            return
        self.init_done.add(space._name)

        for dim in space._dimensions:
            # Dimension table
            self.execute(
                'CREATE TABLE IF NOT EXISTS "%s" ( '
                'id INTEGER PRIMARY KEY, '
                'name %s)' % (dim.table, dim.sql_type))

            # Closure table for the dimension
            self.execute(
                'CREATE TABLE IF NOT EXISTS "%s" ('
                'parent INTEGER REFERENCES "%s" (id) '
                  'ON DELETE CASCADE NOT NULL, '
                'child INTEGER REFERENCES "%s" (id) '
                  'ON DELETE CASCADE NOT NULL, '
                'depth INTEGER)' % (dim.closure_table, dim.table,
                                    dim.table))

            self.execute(
                'CREATE INDEX IF NOT EXISTS %s_idx '
                'ON %s (parent, depth)' % (dim.closure_table, dim.closure_table)
            )

        # Space (main) table
        cols = ', '.join(chain(
            ('"%s" INTEGER REFERENCES %s (id) ON DELETE CASCADE NOT NULL ' % (
                dim.name,  dim.table
            ) for dim in space._dimensions),
            ('"%s" %s NOT NULL' % (msr.name, msr.sql_type) \
             for msr in space._db_measures)
        ))
        query = 'CREATE TABLE IF NOT EXISTS "%s" (%s)' % (
            space._table, cols)
        self.execute(query)

        # Create index covering all dimensions
        self.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS %s_dim_index ON "%s" (%s)' % (
                space._table,
                space._table,
                ', '.join(d.name for d in space._dimensions)
                )
            )

        # Clean old (weak) indexes
        for d in space._dimensions:
            self.execute(
                'DROP INDEX IF EXISTS %s_%s_index' % (
                    space._table,
                    d.name,
                    )
                )

    def register(self, space):
        if not self.readonly:
            self.init_tables(space)

        # If the space is already known, nothing to do
        if space._name in self.stm:
            return

        stm_dict = self.stm[space._name]
        measures = [m.name for m in space._db_measures]
        dimensions = [d.name for d in space._dimensions]

        # get statement
        select = ', '.join(measures)
        dim_where = 'WHERE ' + ' AND '.join('"%s" = ?' % d for d in dimensions)
        stm_dict['get'] = 'SELECT %s FROM "%s" %s' % (
            select, space._table, dim_where)

        # update statement
        set_stm = ', '.join('"%s" = ?' % m for m in measures)
        clause = ' and '.join('"%s" = ?' % d for d in dimensions)
        stm_dict['update'] = 'UPDATE "%s" SET %s WHERE %s' % (
            space._table, set_stm, clause)

        #insert statement
        fields = tuple(chain(dimensions, measures))

        val_stm = ', '.join('?' for f in fields)
        field_stm = ', '.join(fields)
        stm_dict['insert'] = 'INSERT INTO "%s" (%s) VALUES (%s)' % (
            space._table, field_stm, val_stm)

        #delete statement
        cond_stm = ' AND '.join('%s = ?' % d for d in dimensions)
        stm_dict['delete'] = 'DELETE FROM "%s" WHERE %s' % (
            space._table, cond_stm)

    def load(self, space, keys_vals, load_type=None):
        # TODO check for equivalent in postgresql
        nb_edit = super(SqliteBackend, self).load(
            space, keys_vals, load_type=load_type)
        self.execute('VACUUM')
        self.execute('ANALYZE')
        return nb_edit

    def create_coordinate(self, dim, name, parent_id=None):
        # Fill dimension table
        self.execute(
            'INSERT into %s (name) VALUES (?)' % dim.table, (name,))
        last_id = self.cursor.lastrowid

        # New coordinate share same parents than parent_id but at one
        # more depth
        stm = 'INSERT INTO "%(cls)s" (parent, child, depth) '\
              'SELECT parent, ? as child, depth+1 FROM "%(cls)s" ' \
              'WHERE child = ?' % {'cls': dim.closure_table}
        self.execute(stm, (last_id, parent_id))

        # Add self reference
        stm = 'INSERT INTO "%(cls)s" (parent, child, depth) '\
              'VALUES (?, ?, ?)' % {'cls': dim.closure_table}
        self.execute(stm, (last_id, last_id, 0))
        return last_id

    def delete_coordinate(self, dim, coord_id):
        self.execute(
            'DELETE FROM %(dim)s WHERE id IN '
            '(SELECT CHILD FROM %(cls)s WHERE parent = ?)' % {
                'dim': dim.table,
                'cls': dim.closure_table
            }, (coord_id,))


    def reparent(self, dim, child, new_parent):
        """
        Move child from his current parent to the new one.
        """
        cls = dim.closure_table

        # Detach child
        self.execute(
            'DELETE FROM %s '
            'WHERE child IN (SELECT child FROM %s where parent = ?) '
            'AND parent NOT IN (SELECT child FROM %s WHERE parent = ?)' % (
                cls, cls, cls
            ),
            (child, child)
        )

        # Set new parent
        self.execute(
            'SELECT supertree.parent, subtree.child, '
            'supertree.depth + subtree.depth + 1 '
            'FROM %s AS supertree JOIN %s AS subtree '
            'WHERE subtree.parent = ? '
            'AND supertree.child = ?' % (cls, cls),
            (child, new_parent)
        )
        values = list(self.cursor)
        self.cursor.executemany(
            'INSERT INTO %s (parent, child, depth) values (?, ?, ?)' % cls,
            values
        )

    def merge(self, dim, parent_id, spaces):
        '''
        Merge two subtrees. If one name appear twice under the same parent
        one of them (with the biggest id) will be deleted and replaced
        by the other. If more than one dupicate is present this method
        must be re-executed (but it shouldn't be needed if called
        appropriately)
        '''

        # Find ids to merge
        self.execute(
            'SELECT name, min(child), max(child), count(*) as cnt '
            'FROM "%(cls)s" '
            'JOIN "%(dim)s" ON (child = id) '
            'WHERE depth = 1 AND parent = ? '
            'GROUP BY name HAVING cnt > 1' % {
                'cls': dim.closure_table,
                'dim': dim.table,
            }, (parent_id,))

        for name, id_min, id_max, cnt in self.cursor:
            self.execute(
                'UPDATE "%(cls)s" SET parent = ? WHERE parent = ?' % {
                'cls': dim.closure_table,
            }, (id_min, id_max))

            # Update space to use the new id
            for space in spaces:
                if not hasattr(space, dim.name):
                    continue

                # Fetch lines containing id_max (injecting id_min)
                select_cols = []
                for spc_dim in space._dimensions:
                    if spc_dim.name == dim.name:
                        select_cols.append("?")
                    else:
                        select_cols.append(spc_dim.name)
                select_cols.extend(m.name for m in  space._db_measures)
                self.execute(
                    'SELECT %(select_cols)s FROM "%(spc)s" '
                    'WHERE %(col)s = ?' % {
                        'spc': space._table,
                        'col': dim.name,
                        'select_cols': ','.join(select_cols),
                    }, (id_min, id_max,))

                # Re-import the data
                nd = len(space._dimensions)
                data = ((r[:nd], r[nd:]) for r in self.cursor)
                self.load(space, data, load_type=LoadType.increment)

                # Delete obsoleted lines
                self.execute(
                    'DELETE FROM "%(spc)s" WHERE %(col)s = ?' % {
                        'spc': space._table,
                        'col': dim.name,
                    }, (id_max,))

            # Clean old records
            self.execute(
                'DELETE FROM "%(dim)s" WHERE id = ?' % {
                'dim': dim.table,
            }, (id_max,))

            # Recurse on childs
            self.merge(dim, id_min, spaces)


    def prune(self, dim, parent_id):
        '''
        Will delete the given node if no children are found
        '''
        self.execute(
            'SELECT count(*) FROM %(cls)s '
            'WHERE parent = ? and depth = 1' % {'cls': dim.closure_table},
            (parent_id,)
        )
        cnt, = next(self.cursor)
        if cnt > 0:
            return

        self.execute('DELETE FROM %s WHERE id = ?' % dim.table,
                            (parent_id,))

    def rename(self, dim, record_id, new_name):
        self.execute('UPDATE %s SET name = ? WHERE id = ?' % dim.table,
                            (new_name, record_id)
        )

    def get_children(self, dim, parent_id, depth=1):
        if parent_id is None:
            stm = 'SELECT name, id from "%s" where name is null' % dim.table
            args = tuple()
        else:
            stm = 'SELECT d.name, d.id ' \
                'FROM "%s" AS c JOIN %s AS d ON (c.child = d.id) '\
                'WHERE c.depth = ? AND c.parent = ?' % (
                    dim.closure_table, dim.table)
            args = (depth, parent_id)
        res = list(self.execute(stm, args))
        return res

    def get_parents(self, dim):
        stm = 'SELECT id, name, parent FROM "%s"'\
            ' JOIN %s ON (child = id) WHERE depth = 1'\
            %(dim.table, dim.closure_table)
        self.execute(stm)
        return self.cursor.fetchall()

    def dice_query(self, space, fields, filters=None):
        from menger import Coordinate, Level, Measure

        filters = filters or []
        select = []
        joins = []
        tmp_tables = defaultdict(list)
        group_by = []
        params = {}
        nb_default = 0
        query_dims = set()

        # Add dimensions to select
        for field in fields:
            if isinstance(field, Measure):
                select.append('sum(%s)' % field.name)
            elif isinstance(field, Level):
                alias = 'tmp_%s' % self.nb_tmp
                self.nb_tmp += 1
                tmp_tables[field.dim.name].append(alias)
                joins.append(
                    self.tmp_join(space, alias, field.dim, field.depth)
                )
                col = '%s.parent' % alias
                select.append(col)
                group_by.append(col)
                query_dims.add(field.dim)
            else:
                if isinstance(field, Coordinate):
                    field = field.key()
                col = '%s_default' % nb_default
                nb_default += 1
                select.append(':' + col)
                params[col] = field

        # Enforce latest version if a version field is present on the
        # space but not in the query
        vdim = space._versioned
        for dim, *_ in filters:
            query_dims.add(dim)
        if vdim and vdim not in query_dims:
            last_version = vdim.last_coord()
            if last_version is not None:
                filters.append(vdim.match(last_version))

        tmp_tables, joins = self.build_filters(space, filters, tmp_tables,
                                               joins)

        # Base query
        stm = 'SELECT %s FROM "%s"' % (', '.join(select), space._table)
        if joins:
            stm += ' ' + ' '.join(joins)

        # Where clause
        where = []
        msrs = (f for f in fields if isinstance(f, Measure))
        zero_cond = ' OR '.join('%s != 0' % m.name for m in msrs)
        if zero_cond:
            where.append(zero_cond)
            stm += ' WHERE ' + zero_cond

        # Group clause
        if group_by:
            stm += ' GROUP BY ' + ', '.join(group_by)

        # print(format_query(stm, params))
        return stm, params

    def dice(self, space, fields, filters=[]):
        stm, params = self.dice_query(space, fields, filters)
        self.execute(stm, params)
        res = self.cursor.fetchall()
        return res

    def depth_cond(self, spc, dim, depth):
        pass

    def build_filters(self, space, filters, tmp_tables=None, joins=None):
        tmp_tables = tmp_tables or defaultdict(list)
        joins = joins or []

        # Create extra tmp tables for filters if not in selection
        for fdim, coords, *depths in filters:
            if fdim.name not in tmp_tables:
                alias = 'tmp_%s' % self.nb_tmp
                self.nb_tmp += 1
                tmp_tables[fdim.name].append(alias)
                joins.append(self.tmp_join(space, alias, fdim))

        # Filters dimensions
        for fdim, coords, *depths in filters:
            for tt in tmp_tables[fdim.name]:
                cond = 'SELECT child FROM %s WHERE parent in (%s)' %(
                    fdim.closure_table,
                    ','.join(str(c.key()) for c in coords)
                    )
                if depths:
                    cond += ' AND depth in (%s)' % ','.join(map(str, depths))
                qr = 'DELETE FROM %(tmp)s WHERE child NOT IN (%(cond)s)'
                params = {
                    'tmp': tt,
                    'cond': cond
                }
                self.execute(qr % params)

        return tmp_tables, joins

    def tmp_join(self, spc, alias, dim, depth=None):
        # Create tmp table
        qr = 'CREATE TEMPORARY table %s ('\
             'parent INTEGER, child INTEGER, depth INTEGER'\
             ')' % alias
        self.execute(qr)

        # Fill it
        if depth is None:
            qr = 'INSERT INTO %(tmp)s (parent, child, depth) '\
                 'SELECT parent, child, depth '\
                 'FROM %(cls)s WHERE parent = 1'
            params = {
                'cls': dim.closure_table,
                'tmp': alias,
                }
        else:
            subselect = 'SELECT child from "%(cls)s" ' \
                        'WHERE ('\
                          'parent = 1 ' \
                          'AND depth = %(depth)s'\
                        ')' % {
                            'cls': dim.closure_table,
                            'depth': depth + 1,
                        }

            qr = 'INSERT INTO %(tmp)s (parent, child, depth) '\
                 'SELECT parent, child, depth '\
                 'FROM %(cls)s WHERE parent IN (%(subselect)s)'
            params = {
                'cls': dim.closure_table,
                'tmp': alias,
                'subselect': subselect,
            }
        self.execute(qr % params)

        # Build join expression
        join = 'JOIN %(tmp)s ON (%(tmp)s.child = "%(spc)s"."%(dim)s")'
        params = {
            'dim': dim.name,
            'spc': spc._table,
            'tmp': alias,
        }

        return join % params

    def delete(self, space, filters):
        query = 'DELETE FROM %s' % space._table
        tmp_tables, joins = self.build_filters(space, filters)
        conditions = []
        for dim, tables in tmp_tables.items():
            for table in tables:
                cond = '%s in (SELECT child from %s)' % (dim, table)
                conditions.append(cond)
        if conditions:
            query +=  ' WHERE ' + ' AND '.join(conditions)
        self.execute(query)

    def snapshot(self, space, other_space, select, filters, to_delete):
        # Delete existing data
        self.delete(other_space, to_delete)

        # Copy into other_space
        dice_stm , dice_params = self.dice_query(space, select, filters)
        stm = 'INSERT INTO %s ' % other_space._table
        stm = stm + dice_stm
        self.execute(stm, dice_params)

    def glob(self, dim, parent_id, parent_depth, values, filters=[]):
        depth = len(values)
        query_args = {
            'parent_id': parent_id,
            'depth': depth,
        }
        format_args = {
            'cls': dim.closure_table,
            'dim': dim.table,
        }
        select = 'SELECT child from %(cls)s WHERE ' \
                 'parent = :parent_id AND depth = :depth'

        # Add condtions defined by value
        conditions = []
        for pos, name in enumerate(values):
            if name is None:
                continue
            query_args['name_%s' % pos] = name
            query_args['depth_%s' % pos] = depth - pos - 1
            cond = 'child IN ('\
                   'SELECT child FROM %(cls)s '\
                   'JOIN %(dim)s ON (parent=id) '\
                   'WHERE name = :name_%(pos)s AND depth=:depth_%(pos)s)' % {
                       'cls': dim.closure_table,
                       'dim': dim.table,
                       'pos': pos,
                   }
            conditions.append(cond)

        # Add conditions defined by filters
        total_depth = parent_depth + depth
        for key_depths in filters:
            sub_conds = []
            for key, filter_depth in key_depths:
                select_field, cond_field = 'child', 'parent'
                if filter_depth > total_depth:
                    select_field, cond_field = cond_field, select_field

                cond_field = 'parent' if filter_depth < total_depth else 'child'
                delta = abs(filter_depth - total_depth)
                sub_sel = 'SELECT %(select_field)s FROM %(cls)s WHERE '\
                          '(%(cond_field)s = %(key)s AND depth = %(delta)s)' % {
                              'select_field': select_field,
                              'cond_field': cond_field,
                              'key': key,
                              'delta': delta,
                              'cls': dim.closure_table,
                          }
                sub_conds.append(sub_sel)

            cond = 'child in (%s)' % ' UNION '.join(sub_conds)
            conditions.append(cond)

        query = select
        if conditions:
            query += ' AND ' + ' AND '.join(conditions)

        self.execute(query % format_args, query_args)
        return self.cursor.fetchall()

    def close(self, rollback=False):
        # Remove previous tmp tables if any
        for i in range(self.nb_tmp): # FIXME will fail with multi-threads
            table = 'tmp_%s' % i
            self.execute('DROP TABLE %s' % table)
        self.nb_tmp = 0

        if rollback:
            self.connection.rollback()
        else:
            self.connection.commit()

        self.connection.close()

    def get_columns_info(self, name):
        name = name + '_spc'
        stm = 'PRAGMA foreign_key_list("%s")'
        fk = set(x[3] for x in self.execute(stm % name))

        stm = 'PRAGMA table_info("%s")'
        self.execute(stm % name)
        for space_info in list(self.cursor):
            col_name = space_info[1]
            col_type = space_info[2].lower()
            if col_name in fk:
                self.execute('SELECT max(depth) from %s' % (
                    col_name + '_cls'))
                depth, = next(self.cursor)

                self.execute(stm % (col_name + '_dim'))
                for dim_info in self.cursor:
                    dim_col = dim_info[1]
                    dim_type = dim_info[2].lower()
                    if dim_col != 'name':
                        yield col_name, 'dimension', dim_type, depth
                        break
            else:
                yield col_name, 'measure', col_type, None

    def search(self, dim, substring, max_depth):
        if max_depth is None:
            query = 'SELECT name, null FROM %(dim)s ' \
                    'WHERE name like ? '\
                    'ORDER BY name'
            args = ('%' + substring + '%',)
        else:
            query = 'SELECT name, depth FROM %(dim)s ' \
                    'JOIN %(cls)s ON (parent = 1 and child = id) '\
                    'WHERE name like ? AND depth <= ?'\
                    'GROUP BY name, depth '\
                    'ORDER BY depth, name '\
                    % {
                        'dim': dim.table,
                        'cls': dim.closure_table,
                    }
            args = ('%' + substring + '%', max_depth)

        return self.execute(query, args)

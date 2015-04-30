from collections import defaultdict
from itertools import islice, takewhile

from .event import register, trigger

not_none = lambda x: x is not None
head = lambda x: tuple(takewhile(not_none, x))

class Dimension(object):

    def __init__(self, label, type=str, alias=None):
        self.label = label
        self.type = type
        self.db = None
        self.name = None
        self.alias = alias

        if self.type == str:
            self.sql_type = 'varchar'
        elif self.type == int:
            self.sql_type = 'integer'
        elif self.type == float:
            self.sql_type = 'float'
        else:
            raise Exception('Type %s not supported for dimension %s' % (
                type, label
            ))
        self.init_cache()
        register('clear_cache', self.init_cache)

    def set_db(self, db):
        self.db = db
        table = (self.alias or self.name).lower()
        self.table = table + '_dim'
        self.init_cache()

    def init_cache(self):
        self.key_cache = {}
        self.name_cache = {}

    def expand(self, values):
        return values

    def aliases(self, values):
        return []

    def unknow_coord(self, coord):
        from . import UserError
        raise UserError('"%s" on dimension "%s" is unknown' % (
            '/'.join(map(str, coord)), self.name))

    def coord(self, value=None):
        raise NotImplementedError()

    def key(self, coord, create=False):
        if coord in self.key_cache:
            return self.key_cache[coord]

        coord_id = self._get_key(coord)
        if coord_id is not None:
            return coord_id

        if not create:
            return None

        return self.create_id(coord)


class Tree(Dimension):

    '''A Tree dimension is defined by a list of level names, whose length
    is the dimension depth. In a Tree dimension, coordinates are
    tuples of strings like: ('grand parent', 'parent', 'child').

    '''

    def __init__(self, label, levels, type=str, alias=None):
        super(Tree, self).__init__(label, type=type, alias=alias)
        self.levels = levels
        self.depth = len(self.levels)

    def set_db(self, db):
        super(Tree, self).set_db(db)
        table = (self.alias or self.name).lower()
        self.closure_table = table + '_closure'

    def init_cache(self):
        super(Tree, self).init_cache()
        self.full_name_cache = {}

    def coord(self, value=None):
        if value is None:
            return tuple()
        if isinstance(value, tuple):
            return value
        if isinstance(value, list):
            return tuple(value)

        raise ValueError("Unexpected value %s" % value)

    def contains(self, coord):
        return self.key(coord) is not None

    def delete(self, coord):
        coord_id = self.key(coord)
        if not coord_id:
            return
        self.db.delete_coordinate(self, coord_id)
        # Reset cache
        trigger('clear_cache')

    def _get_key(self, coord):
        if len(coord) > self.depth:
            return None
        parent = coord[:-1]

        if coord:
            key = self.key(parent)
            for name, cid in self.db.get_children(self, key):
                name_tuple = parent + (name,)
                self.key_cache[name_tuple] = cid
        else:
            for name, cid in self.db.get_children(self, None):
                self.key_cache[parent] = cid

        return self.key_cache.get(coord)

    def get_name(self, coord_id):
        if coord_id in self.full_name_cache:
            return self.full_name_cache[coord_id]

        if coord_id not in self.name_cache:
            for id, name, parent in self.db.get_parents(self):
                self.name_cache[id] = (name, parent)

        name, parent = self.name_cache.get(coord_id, (None, None))
        if name is None:
            return tuple()

        parent_name = self.get_name(parent)
        if parent_name:
            res = parent_name + (name,)
        else:
            res = (name,)

        self.full_name_cache[coord_id] = res
        return res

    def create_id(self, coord):
        if not coord:
            parent = name = None
        else:
            parent = self.key(coord[:-1], create=True)
            name = coord[-1]

        new_id = self.db.create_coordinate(self, name, parent)
        self.key_cache[coord] = new_id
        self.name_cache[new_id] = (name, parent)
        return new_id

    def drill(self, values):
        key = self.key(values)
        if key is None:
            return
        children = self.db.get_children(self, key)
        for name, _ in sorted(children):
            yield name

    def glob(self, value, filters=[]):
        key_depths = []
        for vals in filters:
            key_depths.append([(self.key(v), len(v)) for v in vals])

        h = head(value)
        tail = value[len(h):]
        key_depths = []
        for values in filters:
            key_depths.append([(self.key(v), len(v)) for v in values])

        res = self.db.glob(self, self.key(h), len(h), tail, key_depths)
        return [self.get_name(child_id) for child_id, in res]

    def explode(self, coord):
        if coord is None:
            return None, None

        if None not in coord:
            key = self.key(coord)
            if key is None:
                self.unknow_coord(coord)
            return key, 0

        for pos, val in enumerate(coord):
            if val is not None:
                continue

            key = self.key(coord[:pos])
            if key is None:
                self.unknow_coord(coord)
            return key, len(coord) - pos

    def format(self, value, fmt_type=None, offset=None):
        return '/'.join(str(i) for i in islice(value, offset, None))

    def reparent(self, coord, new_parent_coord):
        # Late import to avoid loop
        from .space import iter_spaces

        curr_parent = coord[:-1]
        if curr_parent == new_parent_coord:
            return

        record_id = self.key(coord)
        new_parent_id = self.key(new_parent_coord)
        self.db.reparent(self, record_id, new_parent_id)

        # Merge any resulting duplicate
        self.db.merge(self, new_parent_id, iter_spaces())

        # Prune old parent
        self.db.prune(self, self.key(curr_parent))

        # Reset cache
        trigger('clear_cache')

    def rename(self, coord, new_name):
        # Late import to avoid loop
        from .space import iter_spaces

        record_id = self.key(coord)
        self.db.rename(self, record_id, new_name)

        # Merge any resulting duplicate
        parent_id = self.key(coord[:-1])
        self.db.merge(self, parent_id, iter_spaces())

        # Reset cache
        trigger('clear_cache')

    def search(self, prefix, max_depth):
        return self.db.search(self, prefix, max_depth)


# class Flat(Dimension):
#     '''In a flat dimension all coordinates are on the same level, hence a
#     coordinate is a simple string.

#     '''

#     def build_cache(self):
#         if not self.key_cache:
#             self.name_cache = dict(self.db.get_names(self))
#             self.key_cache = dict((v, k) for k, v in self.name_cache.items())

#     def _get_key(self, name, create=False):
#         self.build_cache()
#         return self.key_cache.get(name)

#     def get_name(self, key):
#         self.build_cache()
#         name = self.name_cache.get(key)
#         return name

#     def create_id(self, coord):
#         new_id = self.db.create_coordinate(self, coord)
#         self.key_cache[coord] = new_id
#         self.name_cache[new_id] = coord
#         return new_id

#     def rename(self, coord, new_name):
#         # Late import to avoid loop
#         record_id = self.key(coord)
#         self.db.rename(self, record_id, new_name)

#     def coord(self, value):
#         if not value or not isinstance(value, str):
#             raise ValueError('Unexpected value %s' % value)
#         return value

#     def drill(self, values):
#         key = self.key(values)
#         if key is None:
#             return
#         children = self.db.get_children(self, key)
#         for name, _ in sorted(children):
#             yield name

#     def search(self, prefix):
#         return self.db.search(self, prefix)

#     def explode(self, coord):
#         return coord, None

class Version(Tree):

    def __init__(self, label, type=str, alias=None):
        levels = [label]
        super(Version, self).__init__(label, levels=levels, type=type,
                                      alias=alias)
        if self.depth > 1:
            raise ValueError('Version dimension support only on level')

    def max(self):
        return (max(self.drill(tuple())),)

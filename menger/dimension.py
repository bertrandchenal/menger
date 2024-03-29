from collections import OrderedDict
from itertools import islice, takewhile

from .event import register, trigger
from . import ctx

not_none = lambda x: x is not None
head = lambda x: tuple(takewhile(not_none, x))

KEY_CACHE = {}
NAME_CACHE = {}
TUPLE_CACHE = {}

def clear_dimension_cache():
    global KEY_CACHE, NAME_CACHE, TUPLE_CACHE
    KEY_CACHE = {}
    NAME_CACHE = {}
    TUPLE_CACHE = {}
register('clear_cache', clear_dimension_cache)


def iindex(iterable, position):
    'Return item from iterable at given position'
    return next(islice(iterable, position, position+1))


class Dimension(object):

    def __init__(self, label, type=str, alias=None, fmt='leaf'):
        self.label = label
        self.type = type
        self.name = None
        self.alias = alias
        self.table = None
        self.fmt = fmt

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

    def set_name(self, name):
        self.name = name
        table = (self.alias or self.name).lower()
        self.table = table + '_dim'
        self.closure_table = table + '_cls'

    def expand(self, values):
        return values

    def aliases(self, values):
        return []

    @property
    def key_cache(self):
        return KEY_CACHE.setdefault(self.name, {})

    @property
    def name_cache(self):
        if self.name not in NAME_CACHE:
            res = ctx.db.get_parents(self)
            NAME_CACHE[self.name] = dict((i, (n, p)) for i, n ,p in res)
        return NAME_CACHE[self.name]

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

    def coord(self, value=None):
        if value is None:
            return tuple()
        if isinstance(value, (tuple, list)):
            return tuple(map(self.type, value))

        raise ValueError("Unexpected value %s" % value)

    def contains(self, coord):
        return self.key(coord) is not None

    def match(self, *coords, depth=None):
        'Return a filter tuple based on coordinates'
        coords = [self(c) for c in coords]
        if depth is not None:
            return (self, coords, depth)
        return (self, coords)

    def __call__(self, value):
        '''
        Instanciate a Coordinate object for the given value
        '''
        return Coordinate(self, value)

    def __repr__(self):
        return '<Dimension %s>' % self.name

class Level:

    def __init__(self, name, label, depth, dim):
        self.name = name
        self.label = label
        self.depth = depth
        self.dim = dim

    def __repr__(self):
        return '<Level %s on %s (depth: %s)>' % (
            self.name, self.dim.name, self.depth)

class Tree(Dimension):

    '''A Tree dimension is defined by a list of level names, whose length
    is the dimension depth. In a Tree dimension, coordinates are
    tuples of strings like: ('grand parent', 'parent', 'child').

    '''

    def __init__(self, label, levels=None, type=str, alias=None, fmt='leaf'):
        super(Tree, self).__init__(label, type=type, alias=alias, fmt=fmt)
        if not levels:
            levels = [(label, label)]
        elif not isinstance(levels[0], tuple):
            levels = [(l, l) for l in levels]

        self.levels = OrderedDict()
        for depth, (name, label) in enumerate(levels):
            self.levels[name] = Level(name, label, depth + 1, self)
        self.depth = len(self.levels)

    def __getitem__(self, level_id):
        if isinstance(level_id, int):
            if level_id < 0:
                level_id = len(self.levels) + level_id
            level_id = iindex(self.levels.keys(), level_id)
        return self.levels[level_id]

    @property
    def tuple_cache(self):
        return TUPLE_CACHE.setdefault(self.name, {})

    def delete(self, coord):
        coord_id = self.key(coord)
        if not coord_id:
            return
        ctx.db.delete_coordinate(self, coord_id)
        # Reset cache
        trigger('clear_cache')

    def _get_key(self, coord):
        if len(coord) > self.depth:
            raise Exception('Invalid key length')
        parent = coord[:-1]

        if coord:
            key = self.key(parent)
            for name, cid in ctx.db.get_children(self, key):
                name_tuple = parent + (name,)
                self.key_cache[name_tuple] = cid
        else:
            for name, cid in ctx.db.get_children(self, None):
                self.key_cache[parent] = cid

        return self.key_cache.get(coord)

    def get_name(self, coord_id):
        return self.name_cache[coord_id][0]

    def name_tuple(self, coord_id):
        res = self.tuple_cache.get(coord_id)
        if res is not None:
            return res
        vals = self.name_cache.get(coord_id)
        if not vals:
            return tuple()

        name, parent= vals
        leaf = (name,)
        if parent is not None:
            res = self.name_tuple(parent) + leaf
        else:
            res = leaf
        self.tuple_cache[coord_id] = res
        return res

    def create_id(self, coord):
        if not coord:
            parent = name = None
        else:
            parent = self.key(coord[:-1], create=True)
            name = coord[-1]

        new_id = ctx.db.create_coordinate(self, name, parent)
        self.key_cache[coord] = new_id
        self.name_cache[new_id] = (name, parent)
        return new_id

    def drill(self, values=tuple()):
        key = self.key(values)
        if key is None:
            return
        children = ctx.db.get_children(self, key)
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

        res = ctx.db.glob(self, self.key(h), len(h), tail, key_depths)
        return [self.name_tuple(child_id) for child_id, in res]

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
        new_parent_id = self.key(new_parent_coord, create=True)
        ctx.db.reparent(self, record_id, new_parent_id)

        # Merge any resulting duplicate
        ctx.db.merge(self, new_parent_id, iter_spaces())

        # Prune old parent
        ctx.db.prune(self, self.key(curr_parent))

        # Reset cache
        trigger('clear_cache')

    def rename(self, coord, new_name):
        # Late import to avoid loop
        from .space import iter_spaces

        record_id = self.key(coord)
        ctx.db.rename(self, record_id, new_name)

        # Merge any resulting duplicate
        parent_id = self.key(coord[:-1])
        ctx.db.merge(self, parent_id, iter_spaces())

        # Reset cache
        trigger('clear_cache')

    def search(self, prefix, max_depth=None):
        if max_depth is None:
            max_depth = self.depth
        return ctx.db.search(self, prefix, max_depth)

    def clone(self, depth):
        levels = [l.name for l in self.levels.values()][:depth]
        return Tree(self.name, levels, type=self.type, alias=self.alias)


class Date(Tree):

    def __init__(self, label):
        super(Date, self).__init__(label, levels=['Year', 'Month', 'Day'],
                                   type=int, fmt='full')

    def format(self, value, fmt_type=None, offset=None):
        fmt = lambda x: '%02d' % x
        return '/'.join(fmt(i) for i in islice(value, offset, None))


class Version(Tree):

    def __init__(self, label, type=str, alias=None, fmt='leaf'):
        levels = [label]
        super(Version, self).__init__(label, levels=levels, type=type,
                                      alias=alias, fmt=fmt)
        if self.depth > 1:
            raise ValueError('Version dimension support only on level')

    def last_coord(self):
        items = list(self.drill(tuple()))
        if not items:
            return None
        return (max(items),)

class Coordinate:

    def __init__(self, dim, value):
        self.dim = dim
        self.value = value

    def key(self):
        return self.dim.key(self.value)

    def __repr__(self):
        return '<Coordinate %s %s>' % (self.dim.name, self.value)


class Range(Dimension):

    '''
    A Range dimension contains float or int that will be partitioned
    into custom ranges.
    '''

    def __init__(self, label, range_def, type=float, alias=None, fmt='leaf'):
        super(Range, self).__init__(label, type=type, alias=alias, fmt=fmt)
        start, stop, step = range_def
        self.ranges = [(i , i + step) for i in range(start, stop, step)]
        self.levels = {label: Level(label, label, 0, self)}
        self.depth = 1

    def __getitem__(self, level_id):
        return self.levels[self.label]

    def _get_key(self, coord):
        return coord

    def get_name(self, coord_id):
        return '%s - %' % coord_id

    def name_tuple(self, coord_id):
        return '%s - %' % coord_id

    def drill(self, values=tuple()):
        return self.ranges

    def glob(self, value, filters=[]):
        return [self.name_tuple(r) for r in self.ranges]

    def format(self, value, fmt_type=None, offset=None):
        return '/'.join(str(i) for i in islice(value, offset, None))

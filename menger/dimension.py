from collections import defaultdict
from itertools import chain, islice


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

    def set_db(self, db):
        self.db = db
        table = (self.alias or self.name).lower()
        self.table = table + '_dim'
        self.closure_table = table + '_closure'
        self.init_cache()

    def init_cache(self):
        self.serialized = {}
        self.id_cache = {}
        self.name_cache = {}
        self.full_name_cache = {}


class Tree(Dimension):

    def __init__(self, label, levels, type=str, alias=None, ):
        super(Tree, self).__init__(label, type=type)
        self.levels = levels
        self.depth = len(self.levels)

    def key(self, coord, create=False):
        if len(coord) > self.depth:
            return None

        if coord in self.id_cache:
            return self.id_cache[coord]

        coord_id = self.get_id(coord)
        if coord_id is not None:
            return coord_id

        if not create:
            return None

        return self.create_id(coord)

    def contains(self, coord):
        return self.key(coord) is not None

    def delete(self, coord):
        coord_id = self.key(coord)
        if not coord_id:
            return
        self.db.delete_coordinate(self, coord_id)

    def get_id(self, coord):
        parent = coord[:-1]

        if coord:
            key = self.key(parent)
            for name, cid in self.db.get_children(self, key):
                name_tuple = parent + (name,)
                self.id_cache[name_tuple] = cid
        else:
            for name, cid in self.db.get_children(self, None):
                self.id_cache[parent] = cid

        return self.id_cache.get(coord)

    def get_name(self, coord_id):
        if coord_id in self.full_name_cache:
            return self.full_name_cache[coord_id]

        if coord_id not in self.name_cache:
            for id, name, parent in self.db.get_parents(self):
                self.name_cache[id] = (name, parent)

        name, parent = self.name_cache.get(coord_id, (None, None))
        if name is None:
            return ''

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
        self.id_cache[coord] = new_id
        self.name_cache[new_id] = (name, parent)
        return new_id

    def drill(self, values):
        key = self.key(values)
        if key is None:
            return
        children = self.db.get_children(self, key)
        for name, _ in sorted(children):
            yield name

    def glob(self, value):
        if not value:
            # empty tuple
            yield value
            return

        for res in self._glob([value]):
            yield res

    def _glob(self, values):
        for value in values:
            for pos, val in enumerate(value):
                if val is None:
                    for child in self.drill(value[:pos]):
                        child_glob = value[:pos] + (child,) + value[pos+1:]
                        for res in self._glob([child_glob]):
                            yield res
                    break
            else:
                # No None found
                yield value

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

    def unknow_coord(self, coord):
        from . import UserError
        raise UserError('"%s" on dimension "%s" is unknown' % (
            '/'.join(map(str, coord)), self.name))

    def reparent(self, coord, new_parent_coord):
        curr_parent = coord[:-1]
        if curr_parent == new_parent_coord:
            return

        record_id = self.key(coord)
        new_parent_id = self.key(new_parent_coord)
        self.db.reparent(self, record_id, new_parent_id)

        # Reset cache
        self.init_cache()

    def rename(self, coord, new_name):
        record_id = self.key(coord)
        self.db.rename(self, record_id, new_name)
        # Reset cache
        self.init_cache()

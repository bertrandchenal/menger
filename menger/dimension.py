from collections import defaultdict
from itertools import chain


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

    def set_db(self, db):
        self.db = db
        self.serialized = {}
        self.id_cache = {}
        self.name_cache = {}
        self.full_name_cache = {}
        table = (self.alias or self.name).lower()
        self.table = table + '_dim'
        self.closure_table = table + '_closure'


class Tree(Dimension):

    def __init__(self, label, levels, type=str, alias=None, ):
        super(Tree, self).__init__(label, type=type)
        self.levels = levels
        self.depth = len(self.levels)

    def key(self, coord, create=True):
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

    def get_id(self, coord):
        parent = coord[:-1]

        if coord:
            key = self.key(parent, False)
            for name, cid in self.db.get_childs(self, key):
                name_tuple = parent + (name,)
                self.id_cache[name_tuple] = cid
        else:
            for name, cid in self.db.get_childs(self, None):
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
            parent = self.key(coord[:-1])
            name = coord[-1]

        new_id = self.db.create_coordinate(self, name, parent)
        self.id_cache[coord] = new_id
        self.name_cache[new_id] = (name, parent)
        return new_id

    def drill(self, values):
        key = self.key(values, False)
        if key is None:
            return
        children = self.db.get_childs(self, key)
        for name, cid in sorted(children):
            yield name

    def glob(self, values):
        if not values or values[-1] is not None:
            yield values
            return
        values = values[:-1]
        for res in self.glob(values):
            for child in self.drill(res):
                yield values + (child,)

    def explode(self, coord):
        if coord is None:
            return None, None

        if None not in coord:
            key = self.key(coord, False)
            if key is None:
                self.unknow_coord(coord)
            return key, 0

        for pos, val in enumerate(coord):
            if val is not None:
                continue

            key = self.key(coord[:pos], False)
            if key is None:
                self.unknow_coord(coord)
            return key, len(coord) - pos

    def unknow_coord(self, coord):
        from . import UserError
        raise UserError('"%s" on dimension "%s" is unknown' % (
            '/'.join(map(str, coord)), self.name))

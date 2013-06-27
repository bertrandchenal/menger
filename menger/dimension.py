from collections import defaultdict
from itertools import chain


class Dimension(object):

    def __init__(self, label, type='varchar'):
        self.label = label
        self.type = type
        self.id_cache = {}
        self._db = None
        self._spc = None
        # TODO init dim.table and dim.closure_table

    def set_db(self, db):
        self._db = db
        self.serialized = {}


class Tree(Dimension):

    def key(self, coord, create=True):
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
            for name, cid in self._db.get_childs(self, key):
                self.id_cache[parent + (name,)] = cid
        else:
            for name, cid in self._db.get_childs(self, None):
                self.id_cache[parent] = cid

        return self.id_cache.get(coord)

    def create_id(self, coord):
        if not coord:
            parent = name = None
        else:
            parent = self.key(coord[:-1])
            name = coord[-1]

        new_id = self._db.create_coordinate(self, name, parent)
        self.id_cache[coord] = new_id
        return new_id

    def drill(self, coord):
        children = self._db.get_childs(self, self.key(coord, False))
        for name, cid in sorted(children):
            if name is not None:
                yield coord + (name,)

    def explode(self, coord):
        if '*' not in coord:
            yield coord
            return

        for pos, val in enumerate(coord):
            if val != '*':
                continue
            for new_val in self.drill(coord[:pos]):
                sub_coord = new_val + coord[pos+1:]
                for r in self.explode(sub_coord):
                    yield r

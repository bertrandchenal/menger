from collections import defaultdict

class Dimension(object):

    def __init__(self, label, type='varchar'):
        self.label = label
        self.type = type
        self.serialized = {} #s/serialized/ids/?
        self._db = None
        self._spc = None

    def set_db(self, db):
        self._db = db
        self.serialized = {}


class Tree(Dimension):

    default = tuple()

    def key(self, coord, create=True):
        coord = tuple(coord)
        coord_id = self.serialized.get(coord)
        if coord_id is not None:
            return coord_id

        if not self.serialized:
            self.fill_serialized()

        coord_id = self.serialized.get(coord)
        if coord_id is not None:
            return coord_id

        if not create:
            return None

        return self.add_coordinate(coord)

    def aggregates(self, coord): # XXX s/aggregates/build_key/
        if not coord:
            return (self.key(tuple()),)
        return self.aggregates(coord[:-1]) + (self.key(coord),)

    def add_coordinate(self, coord):
        if len(coord) == 0:
            new_id = self._db.create_coordinate(self, None, None)
            parent_coord_ids = tuple()
        else:
            parent_coord = coord[:-1]
            parent_coord_id = self.key(parent_coord)
            new_id = self._db.create_coordinate(
                self, coord[-1], parent_coord_id)

        self.serialized[coord] = new_id
        return new_id

    def fill_serialized(self):
        id2name = {}
        parent2children = defaultdict(set)
        for cid, pid, name in self._db.load_coordinates(self):
            id2name[cid] = name
            parent2children[pid].add(cid)

        level = [(tuple(), parent2children[None])]
        while level:
            new_level = []
            for parent, children in level:
                for child in children:
                    name = id2name[child]
                    key = parent + (name,)

                    self.serialized[key[1:]] = child
                    new_level.append((key, parent2children[child]))
            level = new_level

    def drill(self, *coord):
        #TODO fill cache
        children = self._db.get_child_coordinates(self, self.key(coord, False))
        for name in children:
            if name[0]:
                yield coord + (name[0],)

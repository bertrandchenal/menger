from collections import defaultdict

class Dimension(object):

    def __init__(self, label):
        self.label = label
        self.serialized = {} #s/serialized/ids/?
        self._db = None
        self._spc = None


class Tree(Dimension):

    default = []

    def key(self, coord):
        return self.aggregates(coord)[-1]

    def aggregates(self, coord):
        if coord is None:
            coord = tuple()
        else:
            coord = tuple(coord)

        coord_ids = self.serialized.get(coord)
        if coord_ids is not None:
            return coord_ids

        if not self.serialized:
            self.fill_serialized()

        coord_ids = self.serialized.get(coord)
        if coord_ids is not None:
            return coord_ids

        return self.add_coordinate(coord)

    def add_coordinate(self, coord):
        if len(coord) == 0:
            new_id = self._db.create_coordinate(self, None, None)
            parent_coord_ids = tuple()
        else:
            parent_coord = coord[:-1]
            parent_coord_ids = self.aggregates(parent_coord)
            new_id = self._db.create_coordinate(self, coord[-1],
                    parent_coord_ids[-1])

        coord_ids = parent_coord_ids + (new_id,)
        self.serialized[coord] = coord_ids
        return coord_ids

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
                    self.serialized[parent + (name,)] = child
                    new_level.append((child, parent2children[child]))
            level = new_level

    def drill(self, coord=[]):
        #TODO fill cache
        for name in self._db.get_child_coordinates(self, self.key(coord)):
            yield coord + [str(name[0])] #TODO ugly


# class Flat(Tree):

#     default = None

#     def key(cls, coord):
#         yield None
#         yield coord

#     def drill(self):
#         return self._db.meta.drill(self._name, None)

#     def store_coordinate(self, coord):
#         self._db.meta.store_coordinate(self._name, None, coord)

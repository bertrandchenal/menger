from itertools import islice
from json import dumps, loads
from collections import defaultdict
import calendar

class Dimension(object):

    def __init__(self, label):
        self.label = label
        self._db = None

    def serialize(self, coord):
        return dumps(coord)


class Tree(Dimension):

    default = []

    def aggregates(self, coord):
        for i in xrange(len(coord) + 1):
            yield coord[:i]

    def drill(self, coord=[]):
        for res in self._db.meta[self._name][self.serialize(coord)]:
            yield coord + [res]

    def store_coordinate(self, coord):
        for pos, item in enumerate(coord):
            prefix = self.serialize(coord[:pos])
            self._db.meta[self._name][prefix].add(item)

class Flat(Tree):

    default = None

    def aggregates(cls, coord):
        yield None
        yield coord

    def drill(self):
        return self._db.meta[self._name][self.serialize(None)]

    def store_coordinate(self, coord):
        self._db.meta[self._name][self.serialize(None)].add(coord)

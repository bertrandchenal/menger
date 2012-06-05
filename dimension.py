from itertools import islice
from json import dumps, loads
from collections import defaultdict
import calendar

import common

class Dimension(object):
    pass

class Tree(Dimension):

    def __init__(self, label):
        self.label = label

    def aggregates(self, coord):
        for i in xrange(len(coord) + 1):
            yield coord[:i]

    def drill(self, coord=[]):
        db = common.get_db(self._space_name)
        for res in db.meta[self._name][self.serialize(coord)]:
            yield coord + [res]

    def serialize(self, coord):
        return dumps(coord)

    def store_coordinate(self, coord):
        db = common.get_db(self._space_name)
        for pos, item in enumerate(coord):
            coord = self.serialize(coord[:pos])
            db.meta[self._name][coord].add(item)

class Flat(Tree):


    def aggregates(cls, coord):
        yield ''
        yield coord

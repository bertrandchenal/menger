from json import dumps
from collections import defaultdict
import calendar

import common


class Dimension(object):

    def __init__(self, label, **kwargs):
        self.label = label

    def store_coordinate(self, value):
        """
        Used to allow a dimension to historize coordinate. Sometimes
        necessary for drilling.
        """
        pass


class Date(Dimension):

    @staticmethod
    def aggregates(date_tuple):
        for i in xrange(4):
            yield date_tuple[:i]

    @staticmethod
    def drill(date_tuple):
        if len(date_tuple) == 1:
            year = date_tuple[0]
            for m in xrange(1, 13):
                yield (year, m)

        elif len(date_tuple) == 2:
            year = int(date_tuple[0])
            month = int(date_tuple[1])
            nb_days = calendar.monthrange(year, month)[1]
            for d in xrange(1, nb_days+1):
                yield (year, month, d)


class Flat(Dimension):

    @staticmethod
    def aggregates(value):
        yield value
        yield ''


class Tree(Dimension):

    def __init__(self, label, **kwargs):
        super(Tree, self).__init__(label, **kwargs)

    @staticmethod
    def aggregates(path):
        for i in xrange(len(path)):
            yield path[:i+1]
        yield []

    def drill(self, path=[]):
        db = common.get_db(self._space_name)
        for res in db.meta[self._name][str(path)]:
            yield path + [res]

    def store_coordinate(self, path):
        db = common.get_db(self._space_name)
        for pos, item in enumerate(path):
            db.meta[self._name][str(path[:pos])].add(item)

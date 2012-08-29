from copy import copy
from itertools import product, izip, imap
from collections import namedtuple
from json import dumps

import common
import dimension
import measure

SPACES = {}

class MetaSpace(type):

    def __new__(cls, name, bases, attrs):
        if not '_name' in attrs:
            attrs['_name'] = name

        for b in bases:
            if not type(b) == cls:
                continue
            if hasattr(b, '_dimensions'):
                for name, dim in b._dimensions.iteritems():
                    attrs[name] = copy(dim)

            if hasattr(b, '_measures'):
                for name, msr in b._measures.iteritems():
                    attrs[name] = copy(msr)

        dimensions = {}
        measures = {}
        for k, v in attrs.iteritems():
            # Collect dimensions
            if isinstance(v, dimension.Dimension):
                dimensions[k] = v
                v._name = k

            # Collect measures
            if isinstance(v, measure.Measure):
                measures[k] = v
                v._name = k

        attrs['_dimensions'] = dimensions
        attrs['_measures'] = measures
        spc = super(MetaSpace, cls).__new__(cls, name, bases, attrs)

        for msr in measures.itervalues():
            msr._spc = spc

        if bases:
            SPACES[attrs['_name']] = spc

        return spc


class Space:

    __metaclass__ = MetaSpace
    _db = None

    @classmethod
    def set_db(cls, db):
        cls._db = db
        for dim in cls._dimensions.itervalues():
            dim._db = db
        for msr in cls._measures.itervalues():
            msr._db = db

    @classmethod
    def aggregates(cls, point):
        for name, dim in cls._dimensions.iteritems():
            yield dim.aggregates(point[name])

    @classmethod
    def load(cls, points):
        for point in points:

            for name, dim in cls._dimensions.iteritems():
                dim.store_coordinate(point[name])

            for parent_coords in product(*(cls.aggregates(point))):
                cls.increment(parent_coords, point)

    @classmethod
    def increment(cls, coords, point):
        key = cls.serialize(coords)
        values = cls._db.get(key)
        for name, measure in cls._measures.iteritems():
            values[name] = measure.increment(values[name], point[name])

        cls._db.set(key, values)

    @classmethod
    def key(cls, point):
        return cls.serialize([
                point.get(name, dim.default) \
                    for name, dim in cls._dimensions.iteritems()
                ])

    @classmethod
    def serialize(cls, point, fill_default=True):
        return dumps(point)

    @classmethod
    def fetch(cls, *measures, **point):
        if len(measures) == 1:
            return cls._measures[measures[0]].fetch(**point)
        return tuple(cls._measures[m].fetch(**point) for m in measures)


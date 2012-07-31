from itertools import product, izip, imap
from collections import namedtuple
from json import dumps
import common
import dimension
import measure

SPACES = {}

class MetaSpace(type):

    def __new__(cls, name, bases, attrs):
        dimensions = {}
        measures = {}

        if not '_name' in attrs:
            attrs['_name'] = name

        for b in bases:
            if not hasattr(b, '_name'):
                continue
            if hasattr(b, '_dimensions'):
                dimensions.update(b._dimensions)
            if hasattr(b, '_measures'):
                measures.update(b._measures)

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

        if bases:
            SPACES[attrs['_name']] = spc

        for dim in dimensions.itervalues():
            dim._space = spc

        for msr in measures.itervalues():
            msr._space = spc

        return spc


class Space:

    __metaclass__ = MetaSpace
    _db = None

    @classmethod
    def source(cls):
        return None

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
    def serialize(cls, coords):
        return dumps(coords)

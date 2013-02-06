from copy import copy
from itertools import product, izip, imap
from collections import namedtuple
from json import dumps

import backend
import dimension
import measure

SPACES = {}

class MetaSpace(type):

    def __new__(cls, name, bases, attrs):
        if not '_name' in attrs:
            attrs['_name'] = name

        attrs['_name'] = attrs['_name'].lower()

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
        attrs['_read_cache'] = {}
        attrs['_insert_cache'] = {}
        attrs['_update_cache'] = {}

        spc = super(MetaSpace, cls).__new__(cls, name, bases, attrs)

        for dim in dimensions.itervalues():
            dim._spc = spc

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
            dim.set_db(db)

        for msr in cls._measures.itervalues():
            msr.set_db(db)

    @classmethod
    def aggregates(cls, point):
        for name, dim in cls._dimensions.iteritems():
            yield dim.aggregates(tuple(point[name]))

    @classmethod
    def key(cls, point):
        return tuple(
            dim.key(point.get(name, tuple())) \
                for name, dim in cls._dimensions.iteritems())

    @classmethod
    def load(cls, points):
        for point in points:
            for parent_coords in product(*tuple(cls.aggregates(point))):
                cls.increment(parent_coords, point)

    @classmethod
    def flush(cls):
        cls._db.update(cls, cls._update_cache)
        cls._db.insert(cls, cls._insert_cache)
        cls._db.commit()
        cls._update_cache.clear()
        cls._insert_cache.clear()
        cls._read_cache.clear()

    @classmethod
    def increment(cls, key, values):
        old_values = cls.get(key)
        iter_values = (values[msr] for msr in cls._measures)

        if old_values is None:
            new_values = tuple(iter_values)
        else:
            new_values = tuple(m.increment(x, y) for m, x, y in zip(
                    cls._measures.itervalues(), iter_values, old_values))

        if old_values is None or key in cls._insert_cache:
            if len(cls._insert_cache) > backend.MAX_CACHE:
                cls.flush()
                cls._update_cache[key] = new_values #TODO ugly
            else:
                cls._insert_cache[key] = new_values
        else:
            if len(cls._update_cache) > backend.MAX_CACHE:
                cls.flush()
            cls._update_cache[key] = new_values

    @classmethod
    def fetch(cls, **point):
        res = cls.get(cls.key(point))

        if res is None:
            res = tuple(0 for x in cls._measures)
        return dict(zip(cls._measures, res))

    @classmethod
    def get(cls, key):
        if key in cls._update_cache:
            return cls._update_cache[key]

        if key in cls._insert_cache:
            return cls._insert_cache[key]

        if key in cls._read_cache:
            return cls._read_cache[key]

        values = cls._db.get(cls, key)
        if len(cls._read_cache) > backend.MAX_CACHE: #TODO use lru
            cls._read_cache.clear()

        cls._read_cache[key] = values

        return values


def build_space(data_point, name='Space'):
    attributes = {}

    for k, v in data_point.iteritems():
        if isinstance(v, list):
            attributes[k] = dimension.Tree(k)
        elif isinstance(v, (int, float)):
            attributes[k] = measure.Sum(k)
        else:
            raise Exception('Unknow type %s (on key %s)' % (type(v), k))

    return type(name, (Space,), attributes)

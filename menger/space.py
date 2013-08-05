from contextlib import contextmanager
from copy import copy
from itertools import product, izip, imap, chain
from collections import namedtuple
from json import dumps

import backend
import dimension
import measure

SPACES = {}


class UserError(Exception):
    pass

class MetaSpace(type):

    def __new__(cls, name, bases, attrs):
        if not '_name' in attrs:
            attrs['_name'] = name

        attrs['_name'] = attrs['_name'].lower()

        for b in bases:
            if not type(b) == cls:
                continue
            if hasattr(b, '_dimensions'):
                for dim in b._dimensions:
                    attrs[dim.name] = copy(dim)

            if hasattr(b, '_measures'):
                for msr in b._measures:
                    attrs[msr.name] = copy(msr)

        dimensions = []
        measures = []
        for k, v in attrs.iteritems():
            # Collect dimensions
            if isinstance(v, dimension.Dimension):
                dimensions.append(v)
                v.name = k

            # Collect measures
            if isinstance(v, measure.Measure):
                measures.append(v)
                v.name = k

        attrs['_dimensions'] = dimensions
        attrs['_measures'] = measures

        spc = super(MetaSpace, cls).__new__(cls, name, bases, attrs)

        for dim in dimensions:
            dim._spc = spc

        if bases:
            SPACES[attrs['_name']] = spc

        return spc


class Space:

    __metaclass__ = MetaSpace
    _db = None
    MAX_CACHE = 100000

    @classmethod
    @contextmanager
    def connect(cls, uri):
        cls._db = backend.get_backend(uri)
        for dim in cls._dimensions:
            dim.set_db(cls._db)

        for msr in cls._measures:
            msr.set_db(cls._db)

        cls._db.register(cls)
        yield
        cls._db.close()

    @classmethod
    def key(cls, point, create=False):
        key = tuple(
            dim.key(point.get(name, tuple()), create=create)
            for dim in cls._dimensions)
        if not create:
            # When create is false one of the coord may be None
            if not all(key):
                return None
        return key

    @classmethod
    def load(cls, points):
        cls._db.increment(cls.convert(points))

    @classmethod
    def convert(cls, points):
        """
        Convert a list of points into a list of tuple (key, values)
        """
        for point in points:
            values = tuple(point[m.name] for m in cls._measures)
            coords = tuple(d.key(tuple(point[d.name])) for d in cls._dimensions)
            yield coords, values


    @classmethod
    def get(cls, point):
        key = cls.key(point, False)
        if key is None:
            return tuple(0 for m in cls._measures)
        return cls._db.get(key)

    @classmethod
    def dice(cls, point):
        key, depths = zip(*list(
                dim.explode(point.get(dim.name))
                for dim in cls._dimensions))

        idx = []
        for dim, depth in zip(cls._dimensions, depths):
            if depth is None or depth == 0:
                continue
            idx.append(dim)
        idx_len = len(idx)

        dim_name = lambda i,x: idx[i].get_name(x) if i < idx_len else x

        res = list(cls._db.get(key, depths))

        for pos, r in enumerate(res):
            res[pos] = tuple(dim_name(pos, x) for pos, x in enumerate(r))
        return res


def build_space(data_point, name):
    """
    Dynamically create a Space class based on a data point.
    """

    attributes = {}
    for k, v in data_point.iteritems():
        if isinstance(v, list):
            col_type = "integer"
            if isinstance(v[0], basestring):
                col_type = 'varchar'
            attributes[k] = dimension.Tree(k, type=col_type)
        elif isinstance(v, float):
            attributes[k] = measure.Sum(k, type='float')
        elif isinstance(v, int):
            attributes[k] = measure.Sum(k, type='integer')
        else:
            raise Exception('Unknow type %s (on key %s)' % (type(v), k))

    return type(name, (Space,), attributes)

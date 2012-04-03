from itertools import product, izip, imap
from collections import namedtuple
import common
import dimension


class MetaModel(type):

    def __new__(cls, name, bases, attrs):
        dimensions = {}

        if not '_name' in attrs:
            attrs['_name'] = name

        for b in bases:
            if not hasattr(b, '_name'):
                continue
            if hasattr(b, '_dimensions'):
                dimensions.update(b._dimensions)

        for k, v in attrs.iteritems():
            # Collect dimensions
            if isinstance(v, dimension.Dimension):
                dimensions[k] = v
                v._name = k
                v._space_name = name

        attrs['_dimensions_keys'] = sorted(dimensions.keys())
        attrs['_dimensions'] = dimensions

        model = super(MetaModel, cls).__new__(cls, name, bases, attrs)

        return model


class Space:

    __metaclass__ = MetaModel

    @classmethod
    def source(cls):
        return None

    @classmethod
    def aggregates(cls, point):
        for name in cls._dimensions_keys:
            dim = cls._dimensions[name]
            yield dim.aggregates(point[name])

    @classmethod
    def load(cls, points):
        db = common.get_db(cls._name)
        for point in points:
            for name, dim in cls._dimensions.iteritems():
                dim.store_coordinate(point[name])

            for parent_point in product(*tuple(cls.aggregates(point))):
                db.incr(parent_point, point['value'])

    @classmethod
    def fetch(cls, **kwargs):
        db = common.get_db(cls._name)
        dim = cls._dimensions
        key = [kwargs.get(k, '') for k in cls._dimensions_keys]
        return db.get(key)


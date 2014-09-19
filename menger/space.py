from copy import copy
from itertools import product, chain
from collections import namedtuple
from json import dumps

from . import backend
from . import dimension
from . import measure

SPACES = {}


class MetaSpace(type):

    def __new__(cls, name, bases, attrs):

        # Define meta-data
        if not '_name' in attrs:
            attrs['_name'] = name

        if not '_label' in attrs:
            attrs['_label'] = attrs['_name']

        attrs['_name'] = attrs['_name'].lower()

        if not '_table' in attrs:
            attrs['_table'] = attrs['_name'] + '_spc'

        # Inherits dimensions and measures
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
        for k, v in attrs.items():
            # Collect dimensions
            if isinstance(v, dimension.Dimension):
                dimensions.append(v)
                v.name = k

            # Collect measures
            elif isinstance(v, measure.Measure):
                measures.append(v)
                v.name = k
            else:
                continue

            # Plug custom format functions
            format_fn = attrs.get('format_' + k)
            if format_fn:
                v.format = format_fn

        key_fun = lambda x: x.name
        dimensions.sort(key=key_fun)
        measures.sort(key=key_fun)

        attrs['_dimensions'] = dimensions
        attrs['_measures'] = measures
        attrs['_db_measures'] = [
            m for m in measures if isinstance(m, measure.Sum)
        ]

        spc = super(MetaSpace, cls).__new__(cls, name, bases, attrs)

        if bases:
            SPACES[attrs['_name']] = spc
        return spc


class Space(metaclass=MetaSpace):

    _db = None
    _all = []

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
        return cls._db.load(cls, cls.convert(points))

    @classmethod
    def convert(cls, points):
        """
        Convert a list of points into a list of tuple (key, values)
        """
        for point in points:
            values = tuple(point[m.name] for m in cls._db_measures)
            coords = tuple(d.key(tuple(point[d.name])) for d in cls._dimensions)
            yield coords, values

    @classmethod
    def get(cls, point):
        key = cls.key(point, False)
        if key is None:
            return tuple(0 for m in cls._measures)
        return cls._db.dice(key) # FIXME signature looks wrong

    @classmethod
    def dice(cls, coordinates, measures, filters={}):
        cube = []
        cube_dims = []
        cube_filters = []
        cube_msrs = []
        for name, value in coordinates:
            dim = cls.get_dimension(name)
            cube_dims.append(dim)
            key, depth = dim.explode(value)
            cube.append((dim, key, depth))

        for name, values in filters.items():
            dim = cls.get_dimension(name)
            key_depths = []
            for value in values:
                key = dim.key(value, False)
                if key is None:
                    # filters value is not known (warning ?)
                    continue
                depth = dim.depth - len(value)
                key_depths.append((key, depth))
            if key_depths:
                cube_filters.append((dim, key_depths))

        for name in measures:
            if not hasattr(cls, name):
                raise Exception('%s is not a measure of %s' % (
                    name, cls._name))
            msr = getattr(cls, name)
            if not isinstance(msr, measure.Measure):
                raise Exception('%s is not a measure of %s' % (
                    name, cls._name))
            cube_msrs.append(msr)

        fn_msr = []
        msr_idx = {}
        xtr_msr = []
        if not cube_msrs:
            cube_msrs = cls._db_measures
        else:
            # Collect computed measure from the query
            for pos, m in enumerate(cube_msrs):
                if isinstance(m, measure.Computed):
                    cube_msrs[pos] = None
                    fn_msr.append((pos, m))
            # Collapse resulting list
            cube_msrs = list(filter(None, cube_msrs))

        if fn_msr:
            # Build msr_idx to acces future values by position
            for pos, m in enumerate(cube_msrs):
                msr_idx[m.name] = pos

            # Add extra measure if needed
            for _, m in fn_msr:
                for arg in m.args:
                    if arg not in msr_idx:
                        new_msr = getattr(cls, arg)
                        xtr_msr.append(new_msr)
                        pos = len(xtr_msr) + len(cube_msrs) - 1
                        msr_idx[arg] = pos
            cube_msrs = cube_msrs + xtr_msr

        rows = cls._db.dice(cls, cube, cube_msrs, cube_filters)
        nb_dim = len(cube_dims)
        nb_xtr = len(xtr_msr)

        # Yield key, value tuples (allows building a dict)
        for row in rows:
            # Key is the combination of coordinates
            key = tuple(d.get_name(i) for i, d in zip(row, cube_dims))
            values = row[nb_dim:]

            if not fn_msr:
                yield key, values
                continue

            fn_vals = []
            for pos, m in fn_msr:
                # Build arguments and launch computation
                args = [values[msr_idx[name]] for name in m.args]
                val = m.compute(args)
                fn_vals.append((pos, val))

            if nb_xtr:
                # Remove extra measure
                values = values[:-nb_xtr]

            values = tuple(cls.merge_computed_measures(values, fn_vals))
            yield key, values

    @staticmethod
    def merge_computed_measures(values, fn_vals):
        '''
        Equivalent to:
            for pos, v in fn_val:
                values.insert(pos, v)
            return values
        '''
        fn_vals = iter(fn_vals)
        fpos, fval = next(fn_vals, (None, None))
        for pos, val in enumerate(values):
            if fpos is not None and pos == fpos:
                yield fval
                fpos, fval = next(fn_vals, (None, None))
            yield val

        while fpos is not None:
            yield fval
            fpos, fval = next(fn_vals, (None, None))

    @classmethod
    def get_dimension(cls, name):
        msg = '%s is not a dimension of %s'
        if not hasattr(cls, name):
            raise Exception( msg % (name, cls._name))
        dim = getattr(cls, name)
        if not isinstance(dim, dimension.Dimension):
            raise Exception(msg % (name, cls._name))
        return dim


def get_space(name):
    return SPACES.get(name)

def iter_spaces():
    return SPACES.items()

def build_space(data_point, name):
    """
    Dynamically create a Space class based on a data point.
    """

    attributes = {}
    for k, v in data_point.items():
        if isinstance(v, list):
            col_type = int
            if isinstance(v[0], str):
                col_type = str
            levels = ['Level-%s' % i for i,_ in enumerate(v)]
            attributes[k] = dimension.Tree(k, levels, type=col_type)

        elif isinstance(v, float):
            attributes[k] = measure.Sum(k, type=float)

        elif isinstance(v, int):
            attributes[k] = measure.Sum(k, type=int)

        else:
            raise Exception('Unknow type %s (on key %s)' % (type(v), k))

    return type(name, (Space,), attributes)

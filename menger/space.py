from collections import OrderedDict, defaultdict
from copy import copy
from hashlib import md5
from itertools import chain
from json import dumps

from . import backend
from . import dimension
from . import measure
from .event import trigger
from . import ctx

SPACES = {}
SPACE_LIST = []

class MetaSpace(type):

    # The prepare function
    @classmethod
    def __prepare__(metacls, name, bases): # No keywords in this case
       return OrderedDict()

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

            for dim in getattr(b, '_dimensions', []):
                if dim.name in attrs:
                    # Keep current class dim, but at the righ position
                    attrs[dim.name] = attrs.pop(dim.name)
                attrs[dim.name] = copy(dim)

            for msr in getattr(b, '_measures', []):
                if msr.name in attrs:
                    # Keep current class msr, but at the righ position
                    attrs[msr.name] = attrs.pop(msr.name)
                attrs[msr.name] = copy(msr)

        dimensions = []
        measures = []
        versioned = None
        for k, v in attrs.items():
            # Collect dimensions
            if isinstance(v, dimension.Dimension):
                dimensions.append(v)
                v.set_name(k)
                if isinstance(v, dimension.Version):
                    if versioned is not None:
                        raise Exception('Maximum one version dimension is '
                                        'supported per space')
                    else:
                        versioned = v

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

        attrs['_dimensions'] = dimensions
        attrs['_versioned'] = versioned
        attrs['_measures'] = measures
        attrs['_db_measures'] = [
            m for m in measures if isinstance(m, measure.Sum)
        ]

        spc = super(MetaSpace, cls).__new__(cls, name, bases, attrs)

        if bases:
            SPACE_LIST.append(spc)
            SPACES[attrs['_name']] = spc
        return spc


class Space(metaclass=MetaSpace):

    @classmethod
    def key(cls, point, create=False):
        key = tuple(
            dim.key(dim.coord(point.get(name)), create=create)
            for dim in cls._dimensions)
        if not create:
            # When create is false one of the coord may be None
            if not all(key):
                return None
        return key

    @classmethod
    def load(cls, points, filters=None, load_type=None):
        nb_edit = ctx.db.load(cls, cls.convert(points, filters=filters),
                               load_type=load_type)
        trigger('clear_cache')
        return nb_edit

    @classmethod
    def convert(cls, points, filters=None):
        """
        Convert a list of points into a list of tuple (coord, values)
        """
        for point in points:
            if filters and not cls.match(point, filters):
                continue
            values = tuple(point[m.name] for m in cls._db_measures)
            coords = tuple(
                d.key(d.coord((point[d.name])), create=True) \
                for d in cls._dimensions
            )
            yield coords, values

    @classmethod
    def match(cls, point, filters):
        # AND loop
        for name, values in filters:
            coord = point[name]
            # OR loop
            for value in values:
                # Point shallower than filter -> mismatch
                if len(coord) < len(value):
                    continue
                # Check items
                if all(x == y for x, y in zip(coord, value)):
                    break
            else:
                # No value match coord
                return False
        return True


    @classmethod
    def build_filters(cls, filters):
        if not filters:
            return
        res = []
        for name, values, *depths in filters:
            dim = cls.get_dimension(name)
            keys = []
            for value in values:
                key = dim.key(value)
                if key is None:
                    # filters value is not known (warning ?)
                    continue
                keys.append(key)
            if keys:
                res.append((dim, keys) + tuple(depths))
        return res

    @classmethod
    def build_cube_dims(cls, coordinates=None):
        if not coordinates:
            return []
        dimensions = []
        for name, value in coordinates:
            dim = cls.get_dimension(name)
            value = dim.coord(value)
            key, depth = dim.explode(value)
            dimensions.append((dim, key, depth))
        return dimensions

    @classmethod
    def build_cube_msrs(cls, measures=None):
        for name in measures:
            if not hasattr(cls, name):
                raise Exception('%s is not a measure of %s' % (
                    name, cls._name))
            msr = getattr(cls, name)
            if not isinstance(msr, measure.Measure):
                raise Exception('%s is not a measure of %s' % (
                    name, cls._name))
            yield msr

    @classmethod
    def dice(cls, coordinates=[], measures=[], filters=[]):
        # XXX use args like this
        # select = ['country', 'date', ('as', 1, 'currency'), 'amout_eur']
        # filters = [
        #  ('date', [(2015, 7)]),
        #  ('country', [('EU', 'BE'), ('EU', 'FR')]), # User ACL
        #  ('country', [('EU',)]),                    # User drill
        # ]
        # group_by = {
        #   date: 3,
        #   country: 1,
        # }

        cube_dims = cls.build_cube_dims(coordinates)
        cube_filters = cls.build_filters(filters)
        if measures:
            cube_msrs = list(cls.build_cube_msrs(measures))
        else:
            cube_msrs = cls._db_measures

        fn_msr = defaultdict(list)
        msr_idx = {}
        xtr_msr = []
        # Collect computed measure from the query
        for pos, m in enumerate(cube_msrs):
            if isinstance(m, measure.Computed):
                cube_msrs[pos] = None
                fn_msr[m].append(pos)
        # Collapse resulting list
        cube_msrs = list(filter(None, cube_msrs))

        if fn_msr:
            # Fill msr_idx to acces future values by position
            for pos, m in enumerate(cube_msrs):
                msr_idx[m.name] = pos

            # Search for extra measures
            fn_args = list(chain(*(m.args for m in fn_msr)))
            depend_args = []
            dep_order = -1
            while fn_args:
                for arg in fn_args:
                    if arg in msr_idx:
                        continue
                    new_msr = getattr(cls, arg)
                    if new_msr in fn_msr:
                        continue
                    if isinstance(new_msr, measure.Computed):
                        for a in new_msr.args:
                            if a not in fn_msr:
                                depend_args.append(a)
                        fn_msr[new_msr].append(dep_order)
                        dep_order -= 1
                    else:
                        xtr_msr.append(new_msr)
                        pos = len(xtr_msr) + len(cube_msrs) - 1
                        msr_idx[arg] = pos
                fn_args = depend_args
                depend_args = []

            # Add extra measures to cube
            cube_msrs = cube_msrs + xtr_msr

            # Record how to loop on measures (to respect dependency
            # defined by declaration order)
            fn_idx = dict((m, pos) for pos, m in enumerate(cls._measures))
            fn_loop = sorted(
                ((pos, m) for m in fn_msr for pos in fn_msr[m]),
                key=lambda x: fn_idx[x[1]],
            )
        rows = ctx.db.dice(cls, cube_dims, cube_msrs, cube_filters)
        nb_dim = len(cube_dims)
        cube_dims = [x[0] for x in cube_dims]
        nb_xtr = len(xtr_msr)

        # Returns (key, values) tuples (allows building a dict)
        for row in rows:
            # Key is the combination of coordinates
            key = tuple(d.get_name(i) for i, d in zip(row, cube_dims))
            values = row[nb_dim:]

            if not fn_msr:
                yield key, values
                continue

            fn_vals = []
            fn_vals_by_name = {}
            for pos, m in fn_loop:
                # Build arguments and launch computation
                args = []
                for name in m.args:
                    if name in msr_idx:
                        val = values[msr_idx[name]]
                    else:
                        val = fn_vals_by_name[name]
                    args.append(val)
                val = m.compute(*args)
                fn_vals_by_name[m.name] = val
                # Add result to fn_vals only if it wasn't a dependency
                if pos >= 0:
                    fn_vals.append((pos, val))

            if nb_xtr:
                # Remove extra measures
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
    def delete(cls, filters=None):
        ctx.db.delete(cls, cls.build_filters(filters))

    @classmethod
    def snapshot(cls, other_space, coordinates=None, filters=None,
                 defaults=None):
        filters = filters or []
        defaults = defaults or {}

        # Build filters
        space_filters = cls.build_filters(filters)
        if defaults:
            for k, v in defaults.items():
                filters.append((k, [v]))
            other_filters = other_space.build_filters(filters)
        else:
            other_filters = space_filters

        # Compute default keys
        for d in other_space._dimensions:
            if d.name in defaults:
                defaults[d.name] = d.key(defaults[d.name])

        # Build cube dimensions
        if coordinates:
            cube_dims = other_space.build_cube_dims(coordinates)
        else:
            cube_dims = []
            for d in other_space._dimensions:
                cube_dims.append((d, d.key(d.coord()), len(d.levels)))

        ctx.db.snapshot(
            cls, other_space, cube_dims, other_space._db_measures,
            space_filters=space_filters,
            other_filters=other_filters,
            defaults=defaults,
        )

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
    return SPACE_LIST

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

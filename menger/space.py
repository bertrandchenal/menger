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
                    # If type changed, ignore attr
                    if not isinstance(attrs[dim.name], dimension.Dimension):
                        continue
                    # Keep current class dim, but at the righ position
                    attrs[dim.name] = attrs.pop(dim.name)
                else:
                    attrs[dim.name] = copy(dim)

            for msr in getattr(b, '_measures', []):
                if msr.name in attrs:
                    # If type changed, ignore attr
                    if not isinstance(attrs[msr.name], measure.Measure):
                        continue
                    # Keep current class msr, but at the righ position
                    attrs[msr.name] = attrs.pop(msr.name)
                else:
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
    def dice(cls, select=[], filters=[], format=None):
        fn_msr = defaultdict(list)
        msr_idx = {}
        xtr_msr = []

        if not select:
            select = cls.all_fields()
        else:
            select = select.copy()

        # Collect computed measure from the query
        for pos, field in enumerate(select):
            if isinstance(field, measure.Computed):
                select[pos] = None
                fn_msr[field].append(pos)
            elif isinstance(field, dimension.Dimension):
                # Take first level
                select[pos] = field[0]

        # Collapse resulting list
        select = list(filter(None, select))

        if fn_msr:
            # Fill msr_idx to acces future values by position
            for pos, m in enumerate(select):
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
                        pos = len(xtr_msr) + len(select) - 1
                        msr_idx[arg] = pos
                fn_args = depend_args
                depend_args = []

            # Add extra measures to select
            select = select + xtr_msr

            # Record how to loop on measures (to respect dependency
            # defined by declaration order)
            fn_idx = dict((m, pos) for pos, m in enumerate(cls._measures))
            fn_loop = sorted(
                ((pos, m) for m in fn_msr for pos in fn_msr[m]),
                key=lambda x: fn_idx[x[1]],
            )
        rows = ctx.db.dice(cls, select, filters)
        nb_xtr = len(xtr_msr)

        # Returns rows
        for row in rows:
            row = tuple(cls.get_names(row, select, format=format))
            if not fn_msr:
                yield row
                continue

            fn_vals = []
            fn_vals_by_name = {}
            for pos, m in fn_loop:
                # Build arguments and launch computation
                args = []
                for name in m.args:
                    if name in msr_idx:
                        val = row[msr_idx[name]]
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
                row = row[:-nb_xtr]

            row = tuple(cls.merge_computed_measures(row, fn_vals))
            yield row

    @classmethod
    def get_names(cls, row, select, format=format):
        for val, field in zip(row, select):
            if isinstance(field, (dimension.Level, dimension.Coordinate)):
                if format is None:
                    yield field.dim.name_tuple(val)
                elif format == 'full':
                    yield field.dim.format(field.dim.name_tuple(val))
                elif format == 'leaf':
                    yield field.dim.get_name(val)
            else:
                yield val

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
        ctx.db.delete(cls, filters)

    @classmethod
    def snapshot(cls, other_space, select=None, filters=None):
        filters = filters or []
        to_delete = filters[:]

        # Build select based on other_space if missing
        if not select:
            select = other_space.all_fields()

        # if static coord are provided we only delete the
        # corresponding rows
        for pos, field in enumerate(select):
            # Translate dimension into first level
            if isinstance(field, dimension.Dimension):
                select[pos] = field[0]
            elif isinstance(field, dimension.Coordinate):
                # Use static values as delete filter
                to_delete.append((field.dim, [field]))
                # Resolve coordinate
                select[pos] = field.key()
            elif not isinstance(field, (measure.Measure, dimension.Level)):
                raise ValueError('Unexpected field "%s" in snapshot' % field)

        ctx.db.snapshot(cls, other_space, select, filters=filters,
                        to_delete=to_delete)

    @classmethod
    def all_fields(cls):
        '''
        Return all fields (all dimensions and all non-computed measures)
        of the current space.
        '''
        fields = []
        for d in cls._dimensions:
            fields.append(d[-1])
        for m in cls._measures:
            if isinstance(m, measure.Computed):
                continue
            fields.append(m)
        return fields

    @classmethod
    def get_attr(cls, name):
        msg = '%s is not an attribute of %s'
        if not hasattr(cls, name):
            raise AttributeError( msg % (name, cls._name))
        attr = getattr(cls, name)
        if not isinstance(attr, (dimension.Dimension, measure.Measure)):
            raise AttributeError(msg % (name, cls._name))
        return attr

    @classmethod
    def get_dimension(cls, name):
        msg = '%s is not a dimension of %s'
        if not hasattr(cls, name):
            raise AttributeError( msg % (name, cls._name))
        dim = getattr(cls, name)
        if not isinstance(dim, dimension.Dimension):
            raise AttributeError(msg % (name, cls._name))
        return dim

    @classmethod
    def get_measure(cls, name):
        msg = '%s is not a measure of %s'
        if not hasattr(cls, name):
            raise AttributeError( msg % (name, cls._name))
        msr = getattr(cls, name)
        if not isinstance(msr, measure.Measure):
            raise AttributeError(msg % (name, cls._name))
        return msr

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

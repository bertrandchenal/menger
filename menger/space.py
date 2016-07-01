from collections import OrderedDict, defaultdict
from copy import copy
from hashlib import md5
from itertools import chain
from json import dumps
from time import time

from . import backend
from .dimension import Coordinate, Dimension, Level, Tree, Version
from .measure import Measure, Sum, Computed
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
            attrs['_pfl_table'] = attrs['_name'] + '_pfl'

        # Inherits dimensions and measures
        for b in bases:
            if not type(b) == cls:
                continue

            for dim in getattr(b, '_dimensions', []):
                if dim.name in attrs:
                    # If type changed, ignore attr
                    if not isinstance(attrs[dim.name], Dimension):
                        continue
                    # Keep current class dim, but at the righ position
                    attrs[dim.name] = attrs.pop(dim.name)
                else:
                    attrs[dim.name] = copy(dim)

            for msr in getattr(b, '_measures', []):
                if msr.name in attrs:
                    # If type changed, ignore attr
                    if not isinstance(attrs[msr.name], Measure):
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
            if isinstance(v, Dimension):
                dimensions.append(v)
                v.set_name(k)
                if isinstance(v, Version):
                    if versioned is not None:
                        raise Exception('Maximum one version dimension is '
                                        'supported per space')
                    else:
                        versioned = v

            # Collect measures
            elif isinstance(v, Measure):
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
            m for m in measures if isinstance(m, Sum)
        ]

        spc = super(MetaSpace, cls).__new__(cls, name, bases, attrs)

        if bases and not attrs.get('__ghost__'):
            SPACE_LIST.append(spc)
            SPACES[attrs['_name']] = spc
        return spc


class Space(metaclass=MetaSpace):

    _registered = False
    _cache_ratio = 0.1

    @classmethod
    def register(cls, init=False):
        if cls._registered and not init:
            return
        cls._registered = True
        ctx.db.register(cls, init=init)
        Profile.register(cls)

    @classmethod
    def refresh_cache(cls):
        Profile.register(cls, snapshot=True)

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
    def dice(cls, select=[], filters=[], dim_fmt=None, msr_fmt=None):
        fn_msr = defaultdict(list)
        msr_idx = {}
        xtr_msr = []

        if not select:
            select = cls.all_fields()
        else:
            select = select.copy()

        # Collect computed measure from the query
        for pos, field in enumerate(select):
            if isinstance(field, Computed):
                select[pos] = None
                fn_msr[field].append(pos)
            elif isinstance(field, Dimension):
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
                    if isinstance(new_msr, Computed):
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

        # Get best matching profile
        spc = cls
        profile = Profile.best(cls, select)
        if profile:
            spc = profile.ghost_spc

        rows = ctx.db.dice(spc, select, filters)
        nb_xtr = len(xtr_msr)

        # Returns rows
        for row in rows:
            row = tuple(cls.format(row, select, dim_fmt=dim_fmt))
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
    def format(cls, row, select, dim_fmt=None, msr_fmt=None):
        for val, field in zip(row, select):
            if isinstance(field, (Level, Coordinate)):
                if dim_fmt is None:
                    yield field.dim.name_tuple(val)
                elif dim_fmt == 'full':
                    yield field.dim.format(field.dim.name_tuple(val))
                elif dim_fmt == 'leaf':
                    yield field.dim.get_name(val)
            else:
                if msr_fmt is None:
                    yield val
                else:
                    yield field.format(val) # TODO pass msr_fmt as argument

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
            if isinstance(field, Dimension):
                select[pos] = field[0]
            elif isinstance(field, Coordinate):
                # Use static values as delete filter
                to_delete.append((field.dim, [field]))
                # Resolve coordinate
                select[pos] = field.key()
            elif not isinstance(field, (Measure, Level)):
                raise ValueError('Unexpected field "%s" in snapshot' % field)

        return ctx.db.snapshot(cls, other_space, select, filters=filters,
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
            if isinstance(m, Computed):
                continue
            fields.append(m)
        return fields

    @classmethod
    def get_attr(cls, name):
        msg = '%s is not an attribute of %s'
        if not hasattr(cls, name):
            raise AttributeError( msg % (name, cls._name))
        attr = getattr(cls, name)
        if not isinstance(attr, (Dimension, Measure)):
            raise AttributeError(msg % (name, cls._name))
        return attr

    @classmethod
    def get_dimension(cls, name):
        msg = '%s is not a dimension of %s'
        if not hasattr(cls, name):
            raise AttributeError( msg % (name, cls._name))
        dim = getattr(cls, name)
        if not isinstance(dim, Dimension):
            raise AttributeError(msg % (name, cls._name))
        return dim

    @classmethod
    def get_measure(cls, name):
        msg = '%s is not a measure of %s'
        if not hasattr(cls, name):
            raise AttributeError( msg % (name, cls._name))
        msr = getattr(cls, name)
        if not isinstance(msr, Measure):
            raise AttributeError(msg % (name, cls._name))
        return msr

    @classmethod
    def clone(cls, _id, values, ghost=False):
        attributes = OrderedDict()
        for d in cls._dimensions:
            if values[d.name] == 0:
                continue
            attributes[d.name] = d.clone(values[d.name])
        for m in cls._measures:
            attributes[m.name] = m.clone()

        # Allows metaclass mechanism to threat ghost spaces as such
        attributes['__ghost__'] = ghost
        name = cls._name + '_cache_%s' % _id
        return type(name, (Space,), attributes)

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
            attributes[k] = Tree(k, levels, type=col_type)

        elif isinstance(v, float):
            attributes[k] = Sum(k, type=float)

        elif isinstance(v, int):
            attributes[k] = Sum(k, type=int)

        else:
            raise Exception('Unknow type %s (on key %s)' % (type(v), k))

    return type(name, (Space,), attributes)


class Profile:

    _all_profiles = defaultdict(dict)
    _hits = defaultdict(lambda: defaultdict(int))
    _last_sync = 0

    def __init__(self, spc, id_, sgn_dict, size=None):
        self.spc = spc
        self.id_ = id_
        self.sgn_dict = sgn_dict
        self.size = size

        self._all_profiles[spc][id_] = self
        self.ghost_spc = spc.clone(id_, self.sgn_dict, ghost=True)

    @classmethod
    def best(cls, spc, select):
        # Build signature
        sgn = cls.signature(spc, select)
        # Increment signature counter
        cls._hits[spc._name][sgn] += 1
        now = time()
        if cls._last_sync < now - 1:
            cls.sync()
            cls._last_sync = now

        # Find the best matching profile
        key = lambda p: p.size
        for pfl in sorted(cls._all_profiles[spc].values(), key=key):
            if pfl.match(sgn):
                return pfl

    @classmethod
    def sync(cls):
        # Save hits numbers to the db
        for name, sgn_vals in cls._hits.items():
            space = get_space(name)
            for sgn, nb_hits in sgn_vals.items():
                if not any(v for _, v in sgn):
                    continue
                ctx.db.inc_profile(space, sgn, nb_hits)
        cls._hits.clear()

    @classmethod
    def signature(cls, spc, select):
        # Creates a tuple containing the name and depth of each
        # dimension in the select list
        values = defaultdict(int)
        for field in select:
            if isinstance(field, Level):
                dim = field.dim
                depth = field.depth
            elif isinstance(field, Dimension):
                dim = field
                depth = 1
            else:
                continue
            values[dim.name] = field.depth
        sgn = tuple((d.name, values[d.name]) for d in spc._dimensions)
        return sgn

    @classmethod
    def register(cls, space, snapshot=False):
        # Reset profile list
        cls._all_profiles[space] = {}
        res = list(ctx.db.get_profiles(space, sort_on=('hits', 'DESC')))
        max_cache = ctx.db.size(space) * space._cache_ratio
        # Loop on db profiles
        for id_ , size, sgn in res:
            do_snap = snapshot and max_cache > 0
            if size is None and not do_snap:
                continue
            pfl = Profile(space, id_, sgn, size=size)
            if do_snap and max_cache > 0:
                # Create snapshot
                pfl.snapshot()
                max_cache -= pfl.size
            if max_cache <= 0:
                # Remove old data
                pfl.reset()

    def reset(self): # XXX trigger this method for event clear_cache
        ctx.db.reset_profile(self.spc, self.ghost_spc, self.id_)

    def snapshot(self):
        self.reset()
        ctx.db.register(self.ghost_spc, init=True, ghost=True)
        self.size = self.spc.snapshot(self.ghost_spc)
        # Save new size in db
        ctx.db.set_profile(self.spc, self.id_, self.size)

    def match(self, sgn):
        # Returns True if all profile signature is large enough on
        # each dimension
        ok = all(self.sgn_dict[dim] >= depth for dim, depth in sgn)
        return ok

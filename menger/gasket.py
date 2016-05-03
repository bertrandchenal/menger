from collections import defaultdict
import re

from pandas import DataFrame

from . import get_space


LEVEL_RE = re.compile('^(.+)\[(.+)\]$')


def dice_by_spc(space, select, filters=None, format='leaf'):
    filters = filters or []
    columns = [s.label for s in select]
    res = space.dice(select, filters, format=format)
    df = DataFrame.from_records(res, columns=columns)
    return df


def dice(query):
    fltrs = query.get('filters', [])
    msr_group = defaultdict(list)
    main_spc = None
    idx = []
    select = []

    for name in query['select']:
        if '.' in name:
            spc, _ = name.split('.')
            main_spc = get_space(spc)
            break

    for name in query['select']:
        if '.' in name:
            # Handle measures
            spc, name = name.split('.')
            if main_spc is None:
                main_spc = get_space(spc)
            msr = get_space(spc).get_measure(name)
            msr_group[spc].append(msr)
            continue

        # Handle dimensions
        m = LEVEL_RE.match(name)
        if m:
            groups = m.groups()
            if len(groups) != 2:
                msg = '"%s" not understood' % name
                raise AttributeError(msg)
            name, level = groups
            dim = main_spc.get_dimension(name)
            attr = dim[level]
        else:
            attr = main_spc.get_dimension(name)
        select.append(attr)
        idx.append(attr.label)

    filters = []
    for name, vals in fltrs:
        dim = main_spc.get_dimension(name)
        cond = [dim.match(tuple(v)) for v in vals]
        filters.extend(cond)

    data = None
    format = query.get('format')
    for spc, msrs in msr_group.items():
        space = get_space(spc)
        spc_data = dice_by_spc(space, select + msrs,
                               filters=filters, format=format)
        if data is None:
            data = spc_data
        else:
            data = data.merge(spc_data, on=idx)

    pivot = query.get('pivot_on')
    if pivot:
        data = data.set_index(idx).unstack(pivot)
        data.columns = data.columns.swaplevel(0, -1)
        data = data.sortlevel(0, axis=1)
    return data.iloc[:query.get('limit')]

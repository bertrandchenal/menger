from collections import defaultdict
from itertools import product, chain
import locale
import re

from pandas import DataFrame

from . import get_space
from . import dimension

LEVEL_RE = re.compile('^(.+)\[(.+)\]$')


def get_label(item):
    if isinstance(item, dimension.Level):
        return '%s: %s' % (item.dim.label, item.label)
    return item.label

def dice_by_spc(space, select, filters=None, dim_fmt='leaf'):
    filters = filters or []
    columns = []
    for s in select:
        columns.append(get_label(s))

    res = space.dice(select, filters, dim_fmt=dim_fmt)
    # TODO add an iterator on res that will raise LimitException if
    # the result gets to large
    df = DataFrame.from_records(res, columns=columns)
    return df


def dice(query):
    fltrs = query.get('filters', [])
    msr_group = defaultdict(list)
    main_spc = None
    idx = []
    dims = []
    to_label = {}

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
            if msr in msr_group[spc]:
                continue
            msr_group[spc].append(msr)
            continue

        # Handle dimensions
        m = LEVEL_RE.match(name)
        if m:
            groups = m.groups()
            if len(groups) != 2:
                msg = '"%s" not understood' % name
                raise AttributeError(msg)
            dim_name, level = groups
            dim = main_spc.get_dimension(dim_name)

            # getitem on dimension also support integers
            try:
                level = int(level)
            except ValueError:
                pass

            attr = dim[level]
        else:
            attr = main_spc.get_dimension(name)
        if attr in dims:
            continue
        dims.append(attr)
        label = get_label(attr)
        idx.append(label)
        to_label[name] = label

    filters = []
    for name, vals in fltrs:
        dim = main_spc.get_dimension(name)
        cond = [dim.match(tuple(v)) for v in vals]
        filters.extend(cond)

    data = None
    dim_fmt = query.get('dim_fmt', 'auto')
    for spc, msrs in msr_group.items():
        space = get_space(spc)
        select = dims + msrs
        spc_data = dice_by_spc(space, select, filters=filters, dim_fmt=dim_fmt)
        if data is None:
            data = spc_data
        else:
            suffixes = [' - %s' % s._label for s in (space, prev_space)]
            data = data.merge(spc_data, on=idx, suffixes=suffixes)
        prev_space = space

    # Generate all combination of selected dimensions
    if not query.get('skip_zero') and idx:
        full_idx = DataFrame(
            list(product(*(data[i].drop_duplicates() for i in idx))),
            columns=idx
        )
        data = full_idx.merge(data, on=idx, how='left')

    # Pivot dataframe
    pivot = query.get('pivot_on')
    if pivot is not None and not isinstance(pivot, (list, tuple)):
        pivot = [pivot]
    if pivot is not None and len(dims) > len(pivot):
        cols = data.columns.values
        for pos, name in enumerate(pivot):
            if name not in cols:
                # Interpret pivot as select item
                pivot[pos] = to_label.get(name, name)
        data = data.set_index(idx).unstack(level=pivot)
        data.reset_index(inplace=True)
        headers = list(zip(*list(data.columns.values)))

    else:
        headers = [list(data.columns.values)]
    # Hide empty lines
    if query.get('skip_zero'):
        data.dropna(how='all', inplace=True)

    # Replace NaN's with zero
    data.fillna(0, inplace=True)

    # Apply limit & sort
    sort_by = list(data.columns.values)
    ascending = True
    if query.get('sort_by'):
        sort_pos, direction = query['sort_by']
        sort_pos = min(sort_pos, len(sort_by) - 1)
        sort_by.insert(0, sort_by.pop(sort_pos))
        ascending = direction == 'asc'
    data = data.sort_values(sort_by, ascending=ascending)
    limit = query.get('limit')
    if limit is not None:
        data = data.iloc[:limit]

    # Compute totals
    totals = [''] * len(data.columns)
    all_msrs = list(chain(*msr_group.values()))
    by_labels = {get_label(f): f for f in all_msrs}
    if pivot is not None:
        for pos, column in enumerate(data.columns.values):
            field = by_labels.get(column[0])
            if field is None:
                continue
            totals[pos] = field.format(data.iloc[:, pos].sum())

    else:
        for mpos, m in enumerate(all_msrs):
            pos = mpos + len(dims)
            totals[pos] = m.format(data.iloc[:, pos].sum())

    # We did pass measure formating to space.dice to make above sort
    # works, so we do it now
    msr_fmt = query.get('msr_fmt')
    if msr_fmt:
        if pivot is not None:
            for column in data.columns.values:
                field = by_labels.get(column)
                if field is None:
                    continue
                data[column] = data[column].apply(field.format)
        else:
            for mpos, m in enumerate(all_msrs):
                pos = mpos + len(dims)
                data.iloc[:, pos] = data.iloc[:, pos].apply(m.format)

    return {
        'data': data,
        'headers': headers,
        'totals': totals,
    }

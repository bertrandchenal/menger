from collections import defaultdict
import locale
import re

from pandas import DataFrame

from . import get_space


LEVEL_RE = re.compile('^(.+)\[(.+)\]$')


def dice_by_spc(space, select, filters=None, dim_fmt='leaf'):
    filters = filters or []
    columns = [s.label for s in select]
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
            name, level = groups
            dim = main_spc.get_dimension(name)

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
        idx.append(attr.label)

    filters = []
    for name, vals in fltrs:
        dim = main_spc.get_dimension(name)
        cond = [dim.match(tuple(v)) for v in vals]
        filters.extend(cond)

    data = None
    dim_fmt = query.get('dim_fmt')
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

    # Pivot dataframe
    pivot = query.get('pivot_on')
    if pivot is not None and len(dims) > 1:
        data = data.set_index(idx).unstack(pivot)
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
    data = data.iloc[:query.get('limit')]

    # We did pass measure formating to space.dice to make above sort
    # works, so we do it now
    msr_fmt = query.get('msr_fmt')
    if msr_fmt:
        by_labels = {f.label: f for f in msrs}
        if pivot is not None:
            for column in data.columns.values:
                field = by_labels.get(column[0])
                if field is None:
                    continue
                data[column] = data[column].apply(field.format)
        else:
            for mpos, m in enumerate(msrs):
                pos = mpos + len(dims)
                data.iloc[:, pos] = data.iloc[:, pos].apply(m.format)

    return {
        'data': data,
        'headers': headers,
    }

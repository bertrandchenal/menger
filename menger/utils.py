from itertools import chain
import json

from .measure import Measure


class Cli(object):

    def __init__(self, space, query_args, fmt, prog=None, fd=None):
        self.space = space
        self.prog = prog or ''
        self.fmt = fmt or 'col'
        self.fd = fd
        self.args = query_args
        getattr(self, 'do_' + query_args[0])()

    @classmethod
    def actions(cls):
        return tuple(m[3:] for m in dir(cls) if m.startswith('do_'))

    @classmethod
    def formats(cls):
        return tuple(m[4:] for m in dir(cls) if m.startswith('fmt'))

    def do_dice(self):
        '''
        Usage:
          %(prog)s dice [dim_name=drill_path ...] [msr...]
        examples:
          %(prog)s dice
          %(prog)s dice date
          %(prog)s drill date=2022/*
          %(prog)s drill date=*/* geography amount average
        '''
        from . import UserError

        measures = []
        dimensions = []

        # Pre-fill args without values
        for name, values in self.splitted_args():
            if isinstance(getattr(self.space, name), Measure):
                measures.append(name)
                continue

            if not values:
                values = (None,)
            dimensions.append((name, values))

        # Force at least one dimesion
        if not dimensions:
            first = self.space._dimensions[0].name
            value = (None,)
            dimensions.append((first, value))

        # Query DB
        try:
            results = self.space.dice(dimensions, measures)
        except UserError as e:
            print('Error:', e , file=self.fd)
            return

        # build headers
        headers = list(getattr(self.space, n).label for n, _ in dimensions)
        if measures:
            headers.extend(getattr(self.space, m).label for m in measures)
        else:
            headers.extend(m.label for m in self.space._measures)

        # Output Results
        content = list(self.format_rows(sorted(results)))

        fmt = getattr(self, 'fmt_' + self.fmt)
        fmt(content, headers)

    def format_rows(self, rows):
        for key, vals in rows:
            res = ['/'.join(str(i) for i in col) for col in key]
            res += [ '%.2f' % v for v in vals]
            yield res

    def fmt_json(self, rows, headers):
        data = [dict(zip(headers, row)) for row in rows]
        print(json.dumps(data, indent=4))

    def fmt_col(self, rows, headers):
        sep = ' '
        lengths = (len(h) for h in headers)
        for row in rows:
            lengths = map(max, (len(i) for i in row), lengths)

        lengths = list(lengths)
        self.print_line(headers, lengths, sep=sep)
        for row in rows:
            self.print_line(row, lengths, sep=sep)

    def print_line(self, items, lengths, sep=' '):
        line = ' '.join(i.ljust(l) for i,l in zip(items, lengths))
        print(line, file=self.fd)

    def do_drill(self):
        '''
        Usage:
          %(prog)s drill dim_name
          %(prog)s drill dim_name=drill_path
        examples:
          %(prog)s drill date
          %(prog)s drill date=*/*
        '''
        for name, values in self.splitted_args():
            self.drill(name, values)

    def do_help(self):
        '''
        Usage:
          %(prog)s help action
        '''
        if len(self.args) == 1:
            name = 'help'
        else:
            name = self.args[1]
            if name not in self.actions():
                name = 'help'

        method = getattr(self, 'do_' + name)
        print(method.__doc__ % {'prog': self.prog})

    def do_info(self):
        '''
        Usage:
          %(prog)s info
        '''
        print('Dimensions', file=self.fd)
        for dim in self.space._dimensions:
            print(' ' + dim.name, file=self.fd)
        print('Measures', file=self.fd)
        for msr in self.space._measures:
            print(' ' + msr.name, file=self.fd)

    def drill(self, name, values):
        values = values or (None,)

        if not hasattr(self.space, name):
            exit('"%s" has no dimension "%s"' % (
                    self.space._name, name))

        dim = getattr(self.space, name)
        for res in sorted(dim.glob(values)):
            print('/'.join(map(str, res)), file=self.fd)

    def do_load(self):
        '''
        Usage:
          %(prog)s load [path ...]
        '''
        for path in self.args:
            fh = open(path)
            first = next(fh, "").strip()
            if not first:
                print('File %s ignored' % path, file=self.fd)
                continue
            first = json.loads(first)

            with self.connect(first):
                self.space.load([first])
                self.space.load((json.loads(l.strip()) for l in fh))
                fh.close()

    def splitted_args(self):
        for arg in self.args[1:]:
            if '=' in arg:
                name, values = arg.split('=')
                values = tuple(values.split('/'))
                if not hasattr(self.space, name):
                    exit('"%s" has no dimension "%s"' % (
                            self.space._name, name))

                dim = getattr(self.space, name)
                values = tuple(None if v == '*' else dim.type(v) \
                               for v in values)

            else:
                name = arg
                values = None
            yield name, values

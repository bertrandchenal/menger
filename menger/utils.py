from itertools import takewhile
import argparse
import json
import re

from .dimension import Dimension
from .measure import Measure
from .space import iter_spaces

LEVEL_RE = re.compile('^(.+)\[(.+)\]$')

class Cli(object):

    def __init__(self, space, query_args, fmt, prog=None, fd=None):
        self.space = space
        self.prog = prog or ''
        self.fmt = fmt or 'col'
        self.fd = fd
        self.args = query_args
        getattr(self, 'do_' + query_args[0])()

    def do_dice(self):
        '''
        Usage:
          %(prog)s dice [dim_name=drill_path ...] [msr...]
        examples:
          %(prog)s dice date
          %(prog)s dice date=2022/*
          %(prog)s dice date=*/* geography amount average
        '''
        from . import UserError

        select = []
        filters = []
        args = list(self.splitted_args())

        # Pre-fill args without values
        for attr, values in args:
            if isinstance(attr, Measure):
                select.append(attr)
                continue
            if values:
                depth = len(values) - 1
                values = tuple(takewhile(lambda x: x is not None, values))
                filters.append(attr.match(values))
                level = attr[depth]
                select.append(level)
            else:
                select.append(attr)

        # Force at least one dimension
        if not select:
            first = self.space._dimensions[0][0]
            select.append(first)

        # Query DB
        try:
            results = self.space.dice(select, filters=filters)
        except UserError as e:
            print('Error:', e , file=self.fd)
            return

        # build headers
        headers = list(n.label for n, _ in args)

        # Output Results
        content = list(self.format_rows(sorted(results)))

        fmt = getattr(self, 'fmt_' + self.fmt)
        fmt(content, headers)

    def format_rows(self, rows):
        for vals in rows:
            yield list(map(self.format_cell, vals))

    def format_cell(self, value):
        if isinstance(value, tuple):
            return '/'.join(str(v) for v in value)
        return str(value)

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
        line = ' '.join(i.ljust(l) for i, l in zip(items, lengths))
        try:
            print(line.rstrip(), file=self.fd)
        except BrokenPipeError:
            pass

    def do_drill(self):
        '''
        Usage:
          %(prog)s drill dim_name
          %(prog)s drill dim_name=drill_path
        examples:
          %(prog)s drill date
          %(prog)s drill date=*/*
        '''
        for attr, values in self.splitted_args():
            self.drill(attr, values)

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
            levels = ', '.join(dim.levels)
            print(' %s  [%s]' % (dim.name, levels), file=self.fd)
        print('Measures', file=self.fd)
        for msr in self.space._measures:
            print(' ' + msr.name, file=self.fd)

    def drill(self, dim, values):
        values = values or (None,)

        for res in sorted(dim.glob(values)):
            print('/'.join(map(str, res)), file=self.fd)

    def do_load(self):
        '''
        Usage:
          %(prog)s load [path ...]
        '''
        for path in self.args[1:]:
            fh = open(path)
            self.space.load((json.loads(l.strip()) for l in fh))
            fh.close()

    def splitted_args(self):
        for arg in self.args[1:]:
            if '=' in arg:
                name, values = arg.split('=')
                values = tuple(values.split('/'))
                attr = self.get_attr(name)
                values = tuple(None if v == '*' else attr.type(v) \
                               for v in values)

            else:
                values = None
                # Try to detect dim[level] pattern
                m = LEVEL_RE.match(arg)
                if m:
                    groups = m.groups()
                    if len(groups) != 2:
                        exit('Argument "%s" not understood' % arg)
                    name, level = groups
                    dim = self.get_dim(name)
                    attr = self.get_level(dim, level)
                else:
                    attr = self.get_attr(arg)
            yield attr, values

    def get_level(self, dim, name):
        try:
            return dim[name]
        except KeyError:
            exit("Dimension %s as no level %s" % (dim.name, name))

    def get_dim(self, name):
        try:
            return self.space.get_dimension(name)
        except AttributeError as e:
            exit(e)

    def get_attr(self, name):
        if not hasattr(self.space, name):
            exit('"%s" has no attribute "%s"' % (
                self.space._name, name))
        return getattr(self.space, name)

    @classmethod
    def actions(cls):
        return tuple(m[3:] for m in dir(cls) if m.startswith('do_'))

    @classmethod
    def formats(cls):
        return tuple(m[4:] for m in dir(cls) if m.startswith('fmt'))

    @classmethod
    def run(cls, default_space=None):
        parser = argparse.ArgumentParser(description='Cli reports.')

        actions = ' | '.join(Cli.actions())
        parser.add_argument('query', nargs='+', help=actions)
        spaces = ' | '.join(s._name for s in iter_spaces())
        parser.add_argument('--space', '-s', default=default_space, help=spaces)
        formats =' | '.join(Cli.formats())
        parser.add_argument('--format', '-f', default='col', help=formats)
        args = parser.parse_args()

        if args.query[0] not in Cli.actions():
            parser.print_help()
            exit()

        spc = None
        if not args.space:
            spc = next(iter(iter_spaces()))
        else:
            for space in iter_spaces():
                if args.space.lower() == space._name.lower():
                    spc = space
                    break

        if spc is None:
            print('Space "%s" not found' % args.space)
            exit()

        cli = Cli(spc, args.query, args.format, prog=parser.prog)

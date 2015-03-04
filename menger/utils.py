from itertools import chain
from .measure import Measure

class Cli(object):

    actions = ('info', 'drill', 'dice', 'help')

    def __init__(self, space, query_args, fd=None):
        self.space = space
        self.fd = fd

        self.args = self.split_args(query_args[1:])
        getattr(self, 'do_' + query_args[0])()

    def do_dice(self):
        from . import UserError

        measures = []
        dimensions = []

        # Pre-fill args without values
        for name, values in self.args:
            if isinstance(getattr(self.space, name), Measure):
                measures.append(name)
                continue

            if not values:
                values = (None,)
            dimensions.append((name, values))

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

        self.print_table(content, headers)

    def format_rows(self, rows):
        for key, vals in rows:
            res = ['/'.join(str(i) for i in col) for col in key]
            res += [ '%.2f' % v for v in vals]
            yield res

    def print_table(self, rows, headers, sep=' ', page_len=10):
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
        for name, values in self.args:
            self.drill(name, values)

    def do_info(self):
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

    def split_args(self, args):
        for arg in args:
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

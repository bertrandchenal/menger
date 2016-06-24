import locale

class Measure(object):

    def __init__(self, label, type=float):
        self.label = label
        self.name = None
        self.type = type

    def format(self, value, fmt_type=None):
        if self.type == float:
            return locale.format('%.2f', value)
        return self.type(value)

    def aggregator(self):
        total = 0
        while True:
            new_value = yield
            if new_value is None:
                yield total
                return
            total += new_value

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name

    def __repr__(self):
        return '<Measure %s>' % self.name


class Sum(Measure):

    def __init__(self, label, type=float):
        super(Sum, self).__init__(label, type=type)
        if self.type == int:
            self.sql_type = 'integer'
        elif self.type == float:
            self.sql_type = 'float'
        else:
            raise Exception('Type %s not supported for dimension %s' % (
                type, label
            ))

    def increment(self, old_value, new_value):
        return old_value + new_value

    def clone(self):
        return Sum(self.label, self.type)


class Computed(Measure):

    def __init__(self, label,  *args):
        self.args = args
        super(Computed, self).__init__(label)

    def compute(self, *args):
        raise NotImplementedError


class Average(Computed):

    def compute(self, total, count):
        if count == 0:
            return 0
        return total / count

    def aggregator(self):
        cnt = 0
        total = 0
        while True:
            new_value = yield
            if new_value is None:
                yield cnt if cnt == 0 else total / cnt
                return
            total += new_value
            cnt += 1

    def clone(self):
        return Average(self.label, *self.args)


class Difference(Computed):

    def compute(self, first_msr, second_msr):
        return first_msr - second_msr

    def clone(self):
        return Difference(self.label, *self.args)

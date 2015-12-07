import locale

class Measure(object):

    def __init__(self, label):
        self.label = label
        self.name = None

    def format(self, value, fmt_type=None):
        return value

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


class Sum(Measure):

    def __init__(self, label, type=float):
        self.type = type
        if self.type == int:
            self.sql_type = 'integer'
        elif self.type == float:
            self.sql_type = 'float'
        else:
            raise Exception('Type %s not supported for dimension %s' % (
                type, label
            ))
        super(Sum, self).__init__(label)

    def format(self, value, fmt_type=None):
        if self.type == float:
            return locale.format('%.2f', value)
        return value

    def increment(self, old_value, new_value):
        return old_value + new_value


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


class Difference(Computed):

    def compute(self, first_msr, second_msr):
        return first_msr - second_msr

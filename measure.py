
class Measure(object):
    pass

class Sum(Measure):

    def __init__(self, label):
        self.label = label

    def increment(self, old_value, new_value):
        return old_value + new_value

    def fetch(self, **point):
        spc = self._space
        key = spc.serialize(
            [point.get(name, dim.default) \
                      for name, dim in spc._dimensions.iteritems()
            ])
        return spc._db.get(key)[self._name]


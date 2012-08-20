
class Measure(object):

    def __init__(self, label):
        self.label = label
        self._db = None
        self._spc = None

class Sum(Measure):

    def increment(self, old_value, new_value):
        return old_value + new_value

    def fetch(self, **point):
        key = self._spc.key(point)
        return self._db.get(key)[self._name]

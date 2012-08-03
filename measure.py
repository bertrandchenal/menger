
class Measure(object):

    def __init__(self, label):
        self.label = label
        self._space = None

class Sum(Measure):

    def increment(self, old_value, new_value):
        return old_value + new_value


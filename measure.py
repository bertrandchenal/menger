
class Measure(object):
    pass

class Sum(Measure):

    def __init__(self, label):
        self.label = label

    def increment(self, old_value, new_value):
        return old_value + new_value



class Measure(object):

    def __init__(self, label, type='float'):
        self.type = type
        self.label = label
        self._db = None

    def set_db(self, db):
        self._db = db


class Sum(Measure):

    def increment(self, old_value, new_value):
        return old_value + new_value

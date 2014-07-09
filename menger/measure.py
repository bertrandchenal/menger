
class Measure(object):

    def __init__(self, label, type=float):
        self.type = type
        self.label = label
        self._db = None

        if self.type == str:
            self.sql_type = 'varchar'
        elif self.type == int:
            self.sql_type = 'integer'
        elif self.type == float:
            self.sql_type = 'float'
        else:
            raise Exception('Type %s not supported for dimension %s' % (
                type, label
            ))

    def set_db(self, db):
        self._db = db

    def format(self, value, type=None):
        return value

class Sum(Measure):

    def increment(self, old_value, new_value):
        return old_value + new_value

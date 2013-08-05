from itertools import tee, izip
from operator import add
from base import BaseBackend


class SqlBackend(BaseBackend):

    def load(self, keys_vals):
        nb_edit = 0
        for key, vals in keys_vals:
            db_vals = self.get(key)
            if not db_vals:
                self.insert(key, vals)
            elif db_vals != vals:
                self.update(key, vals)
            else:
                continue
            nb_edit += 1
        return nb_edit

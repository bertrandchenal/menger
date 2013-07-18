from itertools import tee, izip
from operator import add
from base import BaseBackend


class SqlBackend(BaseBackend):

    def increment(self, keys_vals):
        for key, vals in keys_vals:
            if self.exist(key):
                self.update(key, vals)
            else:
                self.insert(key, vals)
            self.update_cache(key, vals)

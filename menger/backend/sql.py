from collections import defaultdict
from enum import Enum
from itertools import tee
from operator import add

from .base import BaseBackend

class LoadType(Enum):
    default = 0
    increment = 1
    create_only = 2

class SqlBackend(BaseBackend):

    def __init__(self):
        self.init_done = set()
        self.stm = defaultdict(dict)

    def load(self, space, keys_vals, load_type=None):
        nb_insert = nb_update = 0
        for key, vals in keys_vals:
            db_vals = self.get(space, key)
            if not db_vals:
                self.insert(space, key, vals)
                nb_insert += 1
            elif load_type == LoadType.create_only:
                continue
            elif load_type == LoadType.increment:
                map(add, db_vals, vals)
                self.update(space, key, vals)
                nb_update += 1
            elif db_vals != vals:
                self.update(space, key, vals)
                nb_update += 1
            else:
                continue

        return nb_insert, nb_update

    def get(self, space, key):
        self.cursor.execute(self.stm[space._name]['get'], key)
        return self.cursor.fetchone()

    def update(self, space, key, vals):
        if any(vals):
            self.cursor.execute(self.stm[space._name]['update'], vals + key)
        else:
            # Delete row of all values are zero
            self.cursor.execute(self.stm[space._name]['delete'], key)

    def insert(self, space, key, vals):
        if not any(vals):
            # Skip if all values are zero
            return
        self.cursor.execute(self.stm[space._name]['insert'], key + vals)


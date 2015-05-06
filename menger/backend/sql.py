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
        self.get_stm = {}
        self.insert_stm = {}
        self.update_stm = {}
        self.delete_stm = {}

    def load(self, space, keys_vals, load_type=None):
        nb_edit = 0
        for key, vals in keys_vals:
            db_vals = self.get(space, key)
            if not db_vals:
                self.insert(space, key, vals)
            elif load_type == LoadType.create_only:
                continue
            elif load_type == LoadType.increment:
                map(add, db_vals, vals)
                self.update(space, key, vals)
            elif db_vals != vals:
                self.update(space, key, vals)
            else:
                continue
            nb_edit += 1
        return nb_edit

    def get(self, space, key):
        self.cursor.execute(self.get_stm[space._name], key)
        return self.cursor.fetchone()

    def update(self, space, key, vals):
        if any(vals):
            self.cursor.execute(self.update_stm[space._name], vals + key)
        else:
            # Delete row of all values are zero
            self.cursor.execute(self.delete_stm[space._name], key)

    def insert(self, space, key, vals):
        if not any(vals):
            # Skip if all values are zero
            return
        self.cursor.execute(self.insert_stm[space._name], key + vals)


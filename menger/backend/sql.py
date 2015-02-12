from itertools import tee
from operator import add
from .base import BaseBackend


class SqlBackend(BaseBackend):

    def __init__(self):
        self.get_stm = {}
        self.insert_stm = {}
        self.update_stm = {}

    def load(self, space, keys_vals, increment=False):
        nb_edit = 0
        for key, vals in keys_vals:
            db_vals = self.get(space, key)
            if not db_vals:
                self.insert(space, key, vals)
            elif increment:
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

    def update(self, space, k, v):
        self.cursor.execute(self.update_stm[space._name], v + k)

    def insert(self, space, k, v):
        self.cursor.execute(self.insert_stm[space._name], k + v)


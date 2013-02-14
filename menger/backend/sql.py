from itertools import imap
from operator import add
from base import BaseBackend

class SqlBackend(BaseBackend):

    def increment(self, key, values):
        if key in self.write_buffer:
            old_values = self.write_buffer[key]
            self.write_buffer[key] = tuple(imap(add, old_values, values))
        else:
            self.write_buffer[key] = values

        if len(self.write_buffer) > self.space.MAX_CACHE:
            self.flush()

    def flush(self):
        to_update = {}
        to_insert = {}
        for key, old_values in self.get(self.write_buffer, True):
            values = self.write_buffer[key]
            if old_values:
                to_update[key] = tuple(imap(add, old_values, values))
            else:
                to_insert[key] = values

        if to_update:
            self.update(to_update)
        if to_insert:
            self.insert(to_insert)

        self.old_read_cache = self.read_cache
        self.read_cache = {}
        self.write_buffer.clear()
        self.connection.commit()

    def close(self):
        if self.space:
            self.flush()

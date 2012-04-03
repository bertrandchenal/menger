from os import path, mkdir
import json
from collections import defaultdict
from contextlib import contextmanager
from leveldb import LevelDB, WriteBatch

base_path = None
all_db = {}

class LevelDBBackend(object):

    def __init__(self, ldb, meta):
        self.ldb = ldb
        self._read_cache = {}
        self._write_cache = {}
        self.meta = defaultdict(lambda: defaultdict(set))
        for dim, subdict in meta.iteritems():
            for key, value in subdict.iteritems():
                self.meta[dim][key].update(value)

    def get(self, key):
        key = json.dumps(key)
        if key in self._write_cache:
            return self._write_cache[key]

        if key in self._read_cache:
            return self._read_cache[key]

        try:
            value = float(self.ldb.Get(key))
        except KeyError:
            value = 0

        self._read_cache[key] = value
        return value

    def incr(self, key, increment):
        key = json.dumps(key)

        if key in self._read_cache:
            value = self._read_cache.pop(key)

        elif key in self._write_cache:
            value = self._write_cache[key]
        else:
            try:
                value = float(self.ldb.Get(key))
            except KeyError:
                value = 0.0

        self._write_cache[key] = value + increment

    def close(self, namespace):
        meta = {}
        for dim, subdict in self.meta.iteritems():
            meta[dim] = {}
            for key, value in self.meta[dim].iteritems():
                meta[dim][key] = list(value)

        db_path = path.join(base_path, namespace, 'meta')
        json.dump(meta, open(db_path, 'w'))

        batch = WriteBatch()
        for key, value in self._write_cache.iteritems():
            batch.Put(key, str(value))
        self.ldb.Write(batch)


def close_all_db():
    for name, db in all_db.iteritems():
        db.close(name)

def get_db(namespace):
    if namespace in all_db:
        return all_db[namespace]

    db_path = path.join(base_path, namespace)

    if not path.exists(db_path):
        mkdir(db_path)

    ldb = LevelDB(path.join(db_path, 'data'))

    meta_path = path.join(db_path, 'meta')
    if path.exists(meta_path):
        meta = json.load(open(meta_path))
    else:
        meta = {}

    db = LevelDBBackend(ldb, meta)
    all_db[namespace] = db
    return db


@contextmanager
def connect(data_path):
    global base_path
    base_path = data_path

    yield

    close_all_db()

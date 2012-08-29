import errno
from os import path, makedirs
from json import loads, dumps, dump, load
from collections import defaultdict
from contextlib import contextmanager
from leveldb import LevelDB, WriteBatch

import space


MAX_CACHE = 10000


class LevelDBBackend(object):

    def __init__(self, ldb, meta):
        self.ldb = ldb
        self._read_cache = {}
        self._write_cache = {}
        # Init self.meta with meta values
        self.meta = defaultdict(lambda: defaultdict(set))
        for dim, subdict in meta.iteritems():
            for key, value in subdict.iteritems():
                self.meta[dim][key].update(value)

    def get(self, key):
        if key in self._write_cache:
            return self._write_cache[key]

        if key in self._read_cache:
            return self._read_cache[key]

        try:
            value = loads(self.ldb.Get(key))
        except KeyError:
            value = defaultdict(float)

        if len(self._read_cache) > MAX_CACHE:
            self._read_cache.clear()
        self._read_cache[key] = value
        return value

    def set(self, key, value):
        if len(self._write_cache) > MAX_CACHE:
            self.flush()
        self._write_cache[key] = value

    def flush(self):
        batch = WriteBatch()
        for key, value in self._write_cache.iteritems():
            batch.Put(key, dumps(value))
        self.ldb.Write(batch)
        self._write_cache.clear()
        #XXX instead of clearing everything, store keys in different
        #dict wrt key length, and then clear dicts with bigger lenght

    def close(self, uri, namespace):
        meta = {}
        for dim, subdict in self.meta.iteritems():
            meta[dim] = {}
            for key, value in self.meta[dim].iteritems():
                meta[dim][key] = list(value)

        db_path = path.join(uri, namespace, 'meta')
        with open(db_path, 'w') as fh:
            dump(meta, fh)

        self.flush()


def get_db(uri, name, backend=LevelDBBackend):

    db_path = path.join(uri, name)
    try:
        makedirs(db_path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    ldb = LevelDB(path.join(db_path, 'data'))
    meta_path = path.join(db_path, 'meta')
    if path.exists(meta_path):
        with open(meta_path) as fh:
            meta = load(fh)
    else:
        meta = {}

    db = LevelDBBackend(ldb, meta)
    return db


@contextmanager
def connect(uri, backend=None):
    for name, spc in space.SPACES.iteritems():
        db = get_db(uri, name, backend=backend)
        spc.set_db(db)
    yield
    for name, spc in space.SPACES.iteritems():
        spc._db.close(uri, name)


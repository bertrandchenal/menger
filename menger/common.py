import errno
import os
from json import loads, dumps, dump, load
from collections import defaultdict
from contextlib import contextmanager
import sqlite3
try:
    from leveldb import LevelDB, WriteBatch
except:
    pass


import space


MAX_CACHE = 10**4


class Backend(object):

    def __init__(self, meta):
        self._read_cache = {}
        self._write_cache = {}
        self.meta = meta

    def get(self, key):
        if key in self._write_cache:
            return self._write_cache[key]

        if key in self._read_cache:
            return self._read_cache[key]

        value = self.db_get(key)
        if value is None:
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
        self.flush_write_cache()
        self._write_cache.clear()

    def flush_write_cache(self):
        raise NotImplementedError()

    def db_get(self, key):
        raise NotImplementedError()

    def close(self):
        self.meta.close()
        self.flush()


class LevelDBBackend(Backend):

    def __init__(self, db_path, meta):
        self.ldb = LevelDB(db_path)
        super(LevelDBBackend, self).__init__(meta)

    def db_get(self, key):
        try:
            return loads(self.ldb.Get(key))
        except KeyError:
            return None

    def flush_write_cache(self):
        batch = WriteBatch()
        for key, value in self._write_cache.iteritems():
            batch.Put(key, dumps(value))
        self.ldb.Write(batch)
        #XXX instead of clearing everything, store keys in different
        #dict wrt key length, and then clear dicts with bigger lenght

class SqliteBackend(Backend):

    def __init__(self, db_path, meta):
        db_file = db_path + 'sqlite'
        self.connection = sqlite3.connect(db_file)
        self.cursor = self.connection.cursor()
        self.cursor.execute('CREATE TABLE IF NOT EXISTS data (' \
            'key TEXT,' \
            'value TEXT,' \
            'PRIMARY KEY(key))')
        super(SqliteBackend, self).__init__(meta)

    def db_get(self, key):
        self.cursor.execute('SELECT value FROM data WHERE key = ?', (key,))
        res = self.cursor.fetchone()
        if res is None:
            return None
        return loads(res[0])

    def flush_write_cache(self):
        all_values = (
            (k, dumps(v)) for k, v in self._write_cache.iteritems()
            )
        self.cursor.executemany('INSERT OR REPLACE into data (key, value)'\
                                    ' VALUES (?, ?)', all_values);
        # self.connection.commit()

    def close(self):
        super(SqliteBackend, self).close()
        self.connection.commit()


class MetaData(object):

    def __init__(self, path):
        self.path = path
        if os.path.exists(path):
            with open(path) as fh:
                disk_data = load(fh)
        else:
            disk_data = {}

        self.data = defaultdict(lambda: defaultdict(set))
        for dim, subdict in disk_data.iteritems():
            for key, value in subdict.iteritems():
                self.data[dim][key].update(value)

    def close(self):
        disk_data = {}
        for dim, subdict in self.data.iteritems():
            disk_data[dim] = {}
            for key, value in self.data[dim].iteritems():
                disk_data[dim][key] = list(value)

        with open(self.path, 'w') as fh:
            dump(disk_data, fh)

    def __getitem__(self, name):
        return self.data[name]


def get_db(uri, name, backend):

    data_path = os.path.join(uri, name)
    try:
        os.makedirs(data_path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    meta_path = os.path.join(data_path, 'meta')
    db_path = os.path.join(data_path, 'db')

    meta = MetaData(meta_path)
    if backend == 'sqlite':
        db = SqliteBackend(db_path, meta)
    elif backend ==  'leveldb':
        db = LevelDBBackend(db_path, meta)
    else:
        raise Exception('Backend %s not known' % backend)
    return db


@contextmanager
def connect(uri, backend='leveldb'):
    for name, spc in space.SPACES.iteritems():
        db = get_db(uri, name, backend=backend)
        spc.set_db(db)
    yield
    for name, spc in space.SPACES.iteritems():
        spc._db.close()


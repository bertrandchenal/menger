from contextlib import contextmanager
from sqlite import SqliteBackend

try:
    import psycopg2
except:
    psycopg2 = None

if psycopg2 is None:
    PGBackend = None
else:
    from postgresql import PGBackend

MAX_CACHE = 1000

@contextmanager
def connect(uri='sqlite:///:memory:'):
    """
    uri string examples:

    sqlite:// / foo.db
    sqlite:// / /absolute/path/to/foo.db
    postgresql:// scott:tiger@localhost / mydatabase
    postgresql:// user:password@ / dbname
    """
    from .. import space

    engine, other = uri.split('://', 1)
    host, db = other.split('/', 1)

    if engine == 'postgresql':
        if PGBackend is None:
            exit('Postgresql backend unavailable, please install psycopg')
        cn_str = "dbname='%s' " % db

        auth, host = host.split('@', 1)
        if auth:
            login, password = auth.split(':', 1)
            cn_str += "user='%s' " % login
            cn_str += "password='%s' " % password

        if host:
            cn_str += "host='%s' " % host


        backend = PGBackend(cn_str)

    elif engine == 'sqlite':
        backend = SqliteBackend(db)

    else:
        raise Exception('Backend %s not known' % backend)

    for spc in space.SPACES.itervalues():
        backend.register(spc)
    yield
    for spc in space.SPACES.itervalues():
        spc.flush()

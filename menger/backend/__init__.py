from .sql import SqlBackend, LoadType
from .sqlite import SqliteBackend

try:
    import psycopg2
except:
    psycopg2 = None

if psycopg2 is None:
    PGBackend = None
else:
    from .postgresql import PGBackend


def get_backend(uri):
    """
    uri string examples:

    sqlite:///foo.db
    sqlite:////absolute/path/to/foo.db
    postgresql://scott:tiger@localhost / mydatabase
    postgresql://user:password@ / dbname
    """

    if not '://' in uri:
        uri = 'sqlite:///' + uri

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

    return backend

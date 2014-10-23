from contextlib import contextmanager

from .space import Space, build_space, SPACES, get_space, iter_spaces, register
from . import backend
from .dimension import Dimension
from .measure import Measure


class UserError(Exception):
    pass


@contextmanager
def connect(uri, rollback_on_close=False):
    db = backend.get_backend(uri)
    for name, cls in SPACES.items():
        cls._db = db
        for dim in cls._dimensions:
            dim.set_db(db)

        for msr in cls._db_measures:
            msr.set_db(db)

        db.register(cls)
    yield
    db.close(rollback=rollback_on_close)


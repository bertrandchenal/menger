from contextlib import contextmanager

from .space import Space, build_space, SPACES
from . import backend
from .dimension import Dimension
from .measure import Measure


class UserError(Exception):
    pass


@contextmanager
def connect(uri):
    db = backend.get_backend(uri)
    for cls in SPACES:
        cls._db = db
        for dim in cls._dimensions:
            dim.set_db(db)

        for msr in cls._measures:
            msr.set_db(db)

        db.register(cls)
    yield
    db.close()


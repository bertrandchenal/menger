from contextlib import contextmanager
import threading

ctx = threading.local()

from .backend import LoadType, get_backend
from .utils import Cli
from .dimension import Coordinate, Dimension, Level
from .event import register, trigger
from .measure import Measure
from .space import Space, build_space, get_space, iter_spaces

try:
    import pandas
except ImportError:
    pass
else:
    from . import gasket


class UserError(Exception):
    pass


@contextmanager
def connect(uri, rollback_on_close=False, init=False):
    db = get_backend(uri)
    ctx.db = db
    ctx.uri = uri
    for cls in iter_spaces():
        cls.register(init=init)
    try:
        yield db
    except:
        db.close(rollback=True)
        raise
    else:
        db.close(rollback=rollback_on_close)
        if init:
            trigger('clear_cache')

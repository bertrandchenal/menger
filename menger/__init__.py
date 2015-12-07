from contextlib import contextmanager
import threading

ctx = threading.local()

from .backend import LoadType, get_backend
from .utils import Cli
from .dimension import Coordinate, Dimension, Level
from .event import register, trigger
from .measure import Measure
from .space import Space, build_space, get_space, iter_spaces


class UserError(Exception):
    pass


@contextmanager
def connect(uri, rollback_on_close=False, readonly=False):
    print("%s should clear cache only when not readonly", __file__)
    trigger('clear_cache')
    db = get_backend(uri, readonly=readonly)
    for cls in iter_spaces():
        db.register(cls)
    ctx.db = db
    ctx.uri = uri
    try:
        yield db
    except:
        db.close(rollback=True)
        raise
    else:
        db.close(rollback=rollback_on_close)


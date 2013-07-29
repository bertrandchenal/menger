from itertools import imap, izip, repeat
from operator import add


class BaseBackend(object):

    def __init__(self):
        self.space = None

    def build_space(self, name):
        from .. import space, dimension, measure

        columns = list(self.get_columns_info(name.lower()))

        if len(columns) == 0:
            return None #FIXME better behaviour needed

        attributes = {}
        # FIXME "dim_type" is not a good name
        for col_name, col_type, dim_type in columns:
            if col_type == 'dimension':
                attributes[col_name] = dimension.Tree(col_name, type=dim_type)
            elif col_type == 'measure':
                attributes[col_name] = measure.Sum(col_name, type=dim_type)
            else:
                raise Exception('Unknow type %s (on column %s)' % (
                    col_type, col_name))

        return type(name, (space.Space,), attributes)

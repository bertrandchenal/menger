from itertools import imap, izip, repeat
from operator import add

class BaseBackend(object):

    def build_space(self, name):
        from .. import space, dimension, measure

        columns = list(self.get_columns_info(name))

        if len(columns) == 0:
            return None

        attributes = {}
        for col_name, col_type, dim_type in columns:
            if col_type == 'integer':
                attributes[col_name] = dimension.Tree(col_name, type=dim_type)
            elif col_type == 'real':
                attributes[col_name] = measure.Sum(col_name)
            else:
                raise Exception('Unknow type %s (on column %s)' % (
                        col_type, col_name))

        return type(name, (space.Space,), attributes)

    def fetch(self, keys):
        for key, res in self.get(keys):
            res = res or repeat(0)
            if key in self.write_buffer:
                inc = self.write_buffer.get(key)
                res = tuple(imap(add, res, inc))
            yield dict(izip(
                    (m for m, _ in self.space._measures),
                    res))


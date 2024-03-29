from itertools import repeat
from operator import add


class BaseBackend(object):

    def build_space(self, name):
        from .. import space, dimension, measure

        columns = list(self.get_columns_info(name.lower()))

        if len(columns) == 0:
            raise Exception('Unable to build space, nothing found.')

        attributes = {}
        # FIXME "dim_type" is not a good name
        for col_name, col_type, dim_type, depth in columns:
            if dim_type == 'integer':
                dim_type = int
            elif dim_type == 'float':
                dim_type = float
            elif dim_type == 'varchar':
                dim_type = str

            if col_type == 'dimension':
                levels = ['Level-%s' % i for i in range(depth)]
                attributes[col_name] = dimension.Tree(
                    col_name, levels, type=dim_type)

            elif col_type == 'measure':
                attributes[col_name] = measure.Sum(col_name, type=dim_type)

            else:
                raise Exception('Unknow type %s (on column %s)' % (
                    col_type, col_name))

        return type(name, (space.Space,), attributes)

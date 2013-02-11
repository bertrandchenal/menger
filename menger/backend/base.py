from contextlib import contextmanager


class BaseBackend(object):

    @contextmanager
    def connect(self, uri='sqlite:///:memory:'):
        """
        Return a context manager that takes care of registering space
        and flushong data.
        """
        from .. import space

        for spc in space.SPACES.itervalues():
            self.register(spc)
        yield
        for spc in space.SPACES.itervalues():
            spc.flush()


    def build_space(self, name):
        from .. import space, dimension, measure

        columns = self.get_columns_info(name)

        if len(columns) == 0:
            return None

        attributes = {}
        for col_name, col_type in columns:
            if col_type == 'integer':
                attributes[col_name] = dimension.Tree(col_name)
            elif col_type == 'real':
                attributes[col_name] = measure.Sum(col_name)
            else:
                raise Exception('Unknow type %s (on column %s)' % (
                        col_type, col_name))

        return type(name, (space.Space,), attributes)

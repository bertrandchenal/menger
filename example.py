import os
from random import sample
from string import letters


from menger import measure, dimension, Space, common

db_path = '/dev/shm/test.db'

class Item(Space):

    category = dimension.Tree('Category')
    total = measure.Sum('Total')

class NonSpace(object):

    ignore_me = dimension.Tree('Ignore Me')


class Test(Item, NonSpace):

    name = dimension.Tree('Name')
    count = measure.Sum('Count')


item_data = [
    {'category': ['A', 'B', 'C'],
     'total': 15,
     },

    {'category': ['A', 'B', 'D'],
     'total': 9,
     },
    ]

def test_data(nb):
    for i in xrange(nb):
        name = ''.join(sample(letters, 5))
        yield {'category': ['A', 'B', 'C'],
               'name': [name],
               'count': 1,
               'total': 7,
               }

def main(backend):
    # Decrement max_cache to trigger the limit quicker
    common.MAX_CACHE = 100

    if os.path.exists(db_path):
        os.remove(db_path)

    with common.connect():
        Item.load(item_data*50)
        assert list(Item.category.drill(['A'])) == [['A', 'B']]
        assert list(Item.category.drill(['A', 'B'])) == [
            ['A', 'B', 'C'],
            ['A', 'B', 'D']
            ]
        assert list(Item.category.drill(['A', 'B', 'C'])) == []
        assert Item.fetch(category=['A'])['total'] == (15 + 9) * 50

    with common.connect('sqlite', db_path):
        Test.load([{'category': ['A', 'B', 'C'],
               'name': ['test'],
               'count': 1,
               'total': 7,
               }])

    with common.connect('sqlite', db_path):
        # Force cache invalidation
        nb_items = common.MAX_CACHE * 10
        Test.load(test_data(nb_items))
        res = Test.fetch(category=['A'])
        assert res['total'] == 7 * (nb_items + 1)
        assert res['count'] == nb_items + 1


if __name__ == '__main__':
    import time
    print 'test with sqlite'
    t = time.clock()
    main('sqlite')
    print ' done in %s sec' % (time.clock() - t)

import os
from random import sample
from string import letters

from menger import measure, dimension, Space, backend

class Item(Space):
    category = dimension.Tree('Category')
    total = measure.Sum('Total')

class NonSpace(object):
    ignore_me = dimension.Tree('Ignore Me')


class Test(NonSpace, Item):
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

def main(uri):
    # Decrement max_cache to trigger the limit quicker
    backend.MAX_CACHE = 100

    # Load and Fetch
    with backend.connect(uri):
        Item.load([{'category': [], 'name': [], 'total': 1, 'amount': 1}])
        assert Item.fetch()['total'] == 1

    if uri != 'sqlite:///:memory:':
        with backend.connect(uri):
            assert Item.fetch()['total'] == 1

    # Drill
    with backend.connect(uri):
        Item.load(item_data*50)
        assert list(Item.category.drill(['A'])) == [['A', 'B']]
        assert list(Item.category.drill(['A', 'B'])) == [
            ['A', 'B', 'C'],
            ['A', 'B', 'D']
            ]
        assert list(Item.category.drill(['A', 'B', 'C'])) == []
        assert Item.fetch(category=['A'])['total'] == (15 + 9) * 50

    # Force cache invalidation
    with backend.connect(uri):
        nb_items = backend.MAX_CACHE * 10
        Test.load(test_data(nb_items))
        res = Test.fetch(category=['A'])

        assert res['total'] == 7 * nb_items
        assert res['count'] == 1 * nb_items

if __name__ == '__main__':
    import time

    db_path = 'test.db'
    if os.path.exists(db_path):
        os.remove(db_path)

    uris = (
        'sqlite:///' + db_path,
        'sqlite:///:memory:',
        'postgresql://@/menger',
        )

    for uri in uris:
        print 'Test with %s' % uri
        t = time.clock()
        main(uri)
        print ' done in %s sec' % (time.clock() - t)

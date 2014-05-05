import os
from random import sample
from string import letters

from menger import measure, dimension, Space

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
    # Load and Fetch
    with Item.connect(uri):
        Item.load([{'category': [], 'name': [], 'total': 1, 'amount': 1}])
        assert next(Item.dice('total'))['total'] == 1

    if uri != 'sqlite:///:memory:':
        with Item.connect(uri):
            assert next(Item.dice('total'))['total'] == 1

    # Drill
    with Item.connect(uri):
        Item.load(item_data*50)

        assert tuple(Item.category.drill(('A',))) == (('B'),)
        assert tuple(Item.category.drill(('A', 'B'))) == ('C', 'D')
        assert tuple(Item.category.drill(('A', 'B', 'C'))) == tuple()
        assert next(Item.dice('total', category=('A',)))['total'] == 24


    # Force cache invalidation
    Test.MAX_CACHE = 100
    nb_items = 1000
    items = list(test_data(nb_items))

    with Test.connect(uri):
        Test.load(items)
        res = next(Test.dice('total', 'count'))

        assert res['total'] == 7 * nb_items
        assert res['count'] == 1 * nb_items

    with Test.connect(uri):
        for item in items:
            Test.dice('total', **item)

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
        t = time.time()
        c = time.clock()
        main(uri)
        print ' done in %.3s sec (%.3f CPU sec)' % (
            time.time() - t, (time.clock() - c))

import os
from random import sample
from string import ascii_letters

from menger import measure, dimension, Space, connect

class Item(Space):
    category = dimension.Tree('Category', levels=['One', 'Two', 'Three'])
    total = measure.Sum('Total')

class NonSpace(object):
    ignore_me = dimension.Tree('Ignore Me', levels=[])


class Test(NonSpace, Item):
    name = dimension.Tree('Name', levels=['Name'])
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
    for i in range(nb):
        name = ''.join(sample(ascii_letters, 5))
        yield {'category': ['A', 'B', 'C'],
               'name': [name],
               'count': 1,
               'total': 7,
               }

def main(uri):
    # Load and Fetch
    with connect(uri):
        Item.load([{'category': [], 'name': [], 'total': 1, 'amount': 1}])
        assert next(Item.dice([], ['total'])) == ((), (1.0,))

    if uri != 'sqlite:///:memory:':
        with connect(uri):
            assert next(Item.dice([], ['total'])) == ((), (1.0,))

    # Drill
    with connect(uri):
        Item.load(item_data*50)

        assert tuple(Item.category.drill(('A',))) == (('B'),)
        assert tuple(Item.category.drill(('A', 'B'))) == ('C', 'D')
        assert tuple(Item.category.drill(('A', 'B', 'C'))) == tuple()
        cube = [('category', ('A',))]
        assert next(Item.dice(cube, ['total'])) == ((('A',),), (24.0,))


    # Force cache invalidation
    nb_items = 1000
    items = list(test_data(nb_items))

    with connect(uri):
        Test.load(items)
        res = next(Test.dice([], ['total', 'count']))
        _, (total, count) = res

        assert total == 7 * nb_items
        assert count == 1 * nb_items

    with connect(uri):
        for item in items:
            Test.dice(item.items(), ['total'])

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
        print('Test with %s' % uri)
        t = time.time()
        c = time.clock()
        main(uri)
        print(' done in %.3s sec (%.3f CPU sec)' % (
            time.time() - t, (time.clock() - c)))

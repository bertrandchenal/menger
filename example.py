import os
from random import sample
from shutil import rmtree
from string import letters


from menger import measure, dimension, Space, common

db_path = 'test-db'

class Item(Space):

    _name='my_item'

    category = dimension.Tree('Category')
    total = measure.Sum('Total')

class NonSpace(object):

    ignore_me = dimension.Flat('Ignore Me')


class Test(Item, NonSpace):

    name = dimension.Flat('Name')
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
               'name': name,
               'count': 1,
               'total': 7,
               }

def main():
    # Decrement max_cache to trigger the limit quicker
    common.MAX_CACHE = 10

    if os.path.exists(db_path):
        rmtree(db_path)

    with common.connect(db_path):
        Item.load(item_data*50)
        assert list(Item.category.drill(['A'])) == [['A', 'B']]
        assert list(Item.category.drill(['A', 'B'])) == [
            ['A', 'B', 'C'],
            ['A', 'B', 'D']
            ]
        assert list(Item.category.drill(['A', 'B', 'C'])) == []
        assert Item.total.fetch(category=['A']) == (15 + 9) * 50

    with common.connect(db_path):
        Test.load([{'category': ['A', 'B', 'C'],
               'name': 'test',
               'count': 1,
               'total': 7,
               }])
    with common.connect(db_path):
        # Force usage of read_cache and Use Space.fetch instead of
        # Measure.fetch
        assert Test.total.fetch(category=['A', 'B', 'C'], name='test') \
            == Test.fetch('total', category=['A', 'B', 'C'], name='test')

        # Force cache invalidation
        Test.load(test_data(common.MAX_CACHE + 5))
        Test.name.drill()

        assert Test.fetch('total', 'count', category=['A']) == (
            7 * (common.MAX_CACHE + 5 + 1.0),
            common.MAX_CACHE + 5 + 1.0
            )

if __name__ == '__main__':
    main()

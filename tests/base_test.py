import os

import pytest
from menger import dimension, Space, measure, connect

URI = '/tmp/test.db'

DATA = [
    {'date': [2014, 1, 1],
     'place': ['EU', 'BE', 'BRU'],
     'total': 2,
     'count': 1},
    {'date': [2014, 1, 2],
     'place': ['EU', 'BE', 'CRL'],
     'total': 4,
     'count': 1},
    {'date': [2014, 1, 1],
     'place': ['EU', 'FR', 'ORY'],
     'total': 8,
     'count': 1},
    {'date': [2014, 1, 2],
     'place': ['USA', 'NYC', 'JFK'],
     'total': 16,
     'count': 1},
]

class Cube(Space):
    date = dimension.Tree('Date', ['Year', 'Month', 'Day'], int)
    place = dimension.Tree('Place', ['Region', 'Country', 'City'], str)

    total = measure.Sum('Total')
    count = measure.Sum('Count')
    average = measure.Average('Average', 'total', 'count')

@pytest.yield_fixture(scope='function')
def session():
    # Remove previous db
    if URI != ':memory:' and os.path.exists(URI):
        os.unlink(URI)

    with connect(URI, init=True) as db:
        try:
            Cube.load(DATA)
            yield 'session'
        except:
            raise

def drill_check(to_check):
    for check in to_check:
        coordinate = check['coordinate']
        dimension = check['dimension']
        dim = getattr(Cube, dimension)
        res = list(dim.drill(coordinate))
        assert res == check['result']

def dice_check(to_check, cube=None):
    cube = cube or Cube
    for check in to_check:
        select = check['select']
        filters = check.get('filters')
        res = sorted(cube.dice(select, filters=filters))
        assert res == check['values']

def test_dice(session, cube=None):
    checks = [
        {'select': [Cube.total, Cube.count, Cube.average],
         'values' : [((30.0, 4.0, 7.5))]
     },
        {'select': [Cube.date['Day'], Cube.total],
         'filters': [Cube.date.match((2014, 1, 1))],
         'values' : [((2014, 1, 1), 10.0)]
     },
        {'select': [Cube.date['Day'], Cube.total],
         'filters': [Cube.date.match((2014, 1))],
         'values' : [((2014, 1, 1), 10.0,),
                     ((2014, 1, 2), 20.0,)]
     },
        {'select': [Cube.date['Day'], Cube.place['Country'], Cube.total],
         'filters': [Cube.date.match((2014, 1))],
         'values' : [
             ((2014, 1, 1), ('EU', 'BE'), 2.0,),
             ((2014, 1, 1), ('EU', 'FR'), 8.0,),
             ((2014, 1, 2), ('EU', 'BE'), 4.0,),
             ((2014, 1, 2), ('USA', 'NYC'), 16.0)
         ]
     },
        {'select': [Cube.date['Day'], Cube.place['Country'], Cube.total],
         'filters': [Cube.date.match((2014, 1), depth=1)],
         'values' : [
             ((2014, 1, 1), ('EU', 'BE'), 2.0,),
             ((2014, 1, 1), ('EU', 'FR'), 8.0,),
             ((2014, 1, 2), ('EU', 'BE'), 4.0,),
             ((2014, 1, 2), ('USA', 'NYC'), 16.0)
         ]
     },
        {'select': [Cube.date['Day'], Cube.place['Country'], Cube.total],
         'filters': [Cube.date.match((2014, 1), depth=0)],
         'values' : []
     },
        {'select': [Cube.date['Day'], Cube.place['Country'], Cube.total],
         'filters': [Cube.date.match((2014, 1), depth=2)],
         'values' : []
     },
    ]
    dice_check(checks, cube=cube)


def test_drill(session):

    checks = [
        {
            'coordinate' : tuple(),
            'result': ['EU', 'USA'],
            'dimension': 'place',
        },
        {
            'coordinate' : ('EU',),
            'result': ['BE', 'FR'],
            'dimension': 'place',
        },
        {
            'coordinate' : tuple(),
            'result': [2014],
            'dimension': 'date',
        },
        {
            'coordinate' : (2014,),
            'result': [1],
            'dimension': 'date',
        },
        {
            'coordinate' : (2015,),
            'result': [],
            'dimension': 'date',
        },
    ]

    drill_check(checks)

def test_glob(session):
    res = Cube.date.glob((None, 1, None))
    assert res == [(2014, 1, 1), (2014, 1, 2)]

    res = Cube.date.glob((None, None, 1))
    assert res == [(2014, 1, 1)]

    res = Cube.date.glob((None, None))
    assert res == [(2014, 1)]

    res = Cube.date.glob((2014, None))
    assert res == [(2014, 1)]

    res = Cube.date.glob((2014,))
    assert res == [(2014,)]

    res = Cube.date.glob(tuple())
    assert res == [tuple()]


def test_dice_filter(session):
    filters = [Cube.date.match((2014, 1, 1))]
    checks = [
        {'select': [Cube.date['Day'], Cube.total, Cube.count],
         'filters': filters,
         'values' : [((2014, 1, 1), 10.0, 2.0)]
     },
    ]
    dice_check(checks)

    checks = [
        {'select': [Cube.total, Cube.count],
         'filters': filters,
         'values' : [(10.0, 2.0)]
     },
    ]
    dice_check(checks)

    checks = [
        {'select': [Cube.date, Cube.total, Cube.count],
         'filters': filters,
         'values' : [((2014,), 10.0, 2.0)]
     },
    ]
    dice_check(checks)


def test_glob_filter(session):
    filters = [[(2014, 1, 1)]]
    res = Cube.date.glob((None, 1, None), filters=filters)
    assert res == [(2014, 1, 1)]

    # (2014, 1, 1) OR (2014, 1, 2)
    filters = [[(2014, 1, 1), (2014, 1, 2)]]
    res = Cube.date.glob((None, 1, None), filters=filters)
    assert res == [(2014, 1, 1), (2014, 1, 2)]

    # (2014, 1, 1) AND (2014, 1, 2)
    filters = [[(2014, 1, 1)], [(2014, 1, 2)]]
    res = Cube.date.glob((None, 1, None), filters=filters)
    assert res == []

    # (2014, 1, 1) AND (2014, 1)
    filters = [[(2014, 1, 1)], [(2014, 1)]]
    res = Cube.date.glob((None, 1, None), filters=filters)
    assert res == [(2014, 1, 1)]


def test_load_filter(session):
    # Filter match
    data = [
        {'date': [2014, 1, 3],
         'place': ['USA', 'BE', 'BRU'],
         'total': 32,
         'count': 1},
    ]
    filters = [
        ('date', [(2014,)]),
    ]
    Cube.load(data, filters=filters)
    dice_check([
        {'select': [Cube.date['Day'], Cube.total, Cube.count],
         'filters': [Cube.date.match((2014, 1, 3))],
         'values' : [((2014, 1, 3), 32.0, 1.0)]
     }])

    # Filter doesn't
    data = [
        {'date': [2014, 1, 3],
         'place': ['USA', 'BE', 'BRU'],
         'total': 64,
         'count': 1},
    ]
    filters = [
        ('date', [(2015,)]),
    ]
    Cube.load(data, filters=filters)
    dice_check([
        {'select': [Cube.date['Day'], Cube.total, Cube.count],
         'filters': [Cube.date.match((2014, 1, 3))],
         'values' : [((2014, 1, 3), 32.0, 1.0)]
     }])

    # Filter does and doesn't (OR clause)
    data = [
        {'date': [2014, 1, 3],
         'place': ['USA', 'BE', 'BRU'],
         'total': 128,
         'count': 1},
    ]
    filters = [
        ('date', [(2015,), (2014,)]),
    ]
    Cube.load(data, filters=filters)
    dice_check([
        {'select': [Cube.date['Day'], Cube.total, Cube.count],
         'filters': [Cube.date.match((2014, 1, 3))],
         'values' : [((2014, 1, 3), 128.0, 1.0)]
     }])

    # Filter does and doesn't (AND clause)
    data = [
        {'date': [2014, 1, 3],
         'place': ['USA', 'BE', 'BRU'],
         'total': 256,
         'count': 1},
    ]
    filters = [
        ('date', [(2015,)]),
        ('date', [(2014,)]),
    ]
    Cube.load(data, filters=filters)

    dice_check([
        {'select': [Cube.date['Day'], Cube.total, Cube.count],
         'filters': [Cube.date.match((2014, 1, 3))],
         'values' : [((2014, 1, 3), 128.0, 1.0)]
     }])

def test_default(session):
    fixed_coord = Cube.date((2014, 1,))
    checks = [
        {'select': [fixed_coord, Cube.place, Cube.total],
         'values' : [
             (fixed_coord.value, ('EU',), 14),
             (fixed_coord.value, ('USA',), 16),
         ]},
    ]
    dice_check(checks)

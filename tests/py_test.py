import pytest
from menger import dimension, Space, measure, connect

URI = ':memory:'


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


def reparent():
    Cube.place.reparent(('EU', 'BE', 'CRL'), ('EU', 'FR'))
    Cube.place.reparent(('EU', 'BE'), ('USA',))

def rename():
    Cube.place.rename(('EU', 'FR', 'ORY'), 'CDG')

@pytest.yield_fixture(scope='function')
def session():
    with connect(URI):
        Cube.load(DATA)
        yield 'session'


def test_dice(session):
    checks = [
        {'coordinates': [],
         'measures': ['total', 'count'],
         'values' : [((), (30.0, 4.0))]
     },
        {'coordinates': [('date', (2014, 1, 1))],
         'measures': ['total'],
         'values' : [(((2014, 1, 1),), (10.0,))]
     },
        {'coordinates': [('date', (2014, 1, None))],
         'measures': ['total'],
         'values' : [(((2014, 1, 1),), (10.0,)),
                     (((2014, 1, 2),), (20.0,))]
     },
        {'coordinates': [('date', (2014, 1, None)), ('place', (None, None))],
         'measures': ['total'],
         'values' : [
             (((2014, 1, 1), ('EU', 'BE')), (2.0,)),
             (((2014, 1, 1), ('EU', 'FR')), (8.0,)),
             (((2014, 1, 2), ('EU', 'BE')), (4.0,)),
             (((2014, 1, 2), ('USA', 'NYC')), (16.0,))
         ]
     },
    ]

    reparent_checks = [
        {'coordinates': [],
         'measures': ['total', 'count'],
         'values' : [((), (30.0, 4.0))]
     },
        {'coordinates': [('date', (2014, 1, 1))],
         'measures': ['total'],
         'values' : [(((2014, 1, 1),), (10.0,))]
     },
        {'coordinates': [('date', (2014, 1, None))],
         'measures': ['total'],
         'values' : [(((2014, 1, 1),), (10.0,)),
                     (((2014, 1, 2),), (20.0,))]
     },
        {'coordinates': [('date', (2014, 1, None)), ('place', (None, None))],
         'measures': ['total'],
         'values' :[
             (((2014, 1, 1), ('USA', 'BE')), (2.0,)),
             (((2014, 1, 1), ('EU', 'FR')), (8.0,)),
             (((2014, 1, 2), ('EU', 'FR')), (4.0,)),
             (((2014, 1, 2), ('USA', 'NYC')), (16.0,))]
     },
    ]

    rename_checks = [
        {'coordinates': [('place', ('EU', 'FR', None))],
         'measures': ['total'],
         'values' :[
             ((('EU', 'FR', 'CRL'),), (4.0,)),
             ((('EU', 'FR', 'CDG'),), (8.0,)),
         ]
     },
    ]

    dice_check(checks)
    reparent()
    dice_check(reparent_checks)
    rename()
    dice_check(rename_checks)

def dice_check(to_check):
    for check in to_check:
        coordinates = check['coordinates']
        measures = check['measures']
        res = list(Cube.dice(coordinates=coordinates, measures=measures))
        assert res == check['values']


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

    reparent_checks = [
        {
            'coordinate' : tuple(),
            'result': ['EU', 'USA'],
            'dimension': 'place',
        },
        {
            'coordinate' : ('EU',),
            'result': ['FR'],
            'dimension': 'place',
        },
        {
            'coordinate' : ('EU', 'FR'),
            'result': ['CRL', 'ORY'],
            'dimension': 'place',
        },
        {
            'coordinate' : ('USA',),
            'result': ['BE', 'NYC'],
            'dimension': 'place',
        },
    ]

    rename_checks = [
        {
            'coordinate' : ('EU', 'FR'),
            'result': ['CDG', 'CRL'],
            'dimension': 'place',
        },
    ]

    drill_check(checks)
    reparent()
    drill_check(reparent_checks)
    rename()
    drill_check(rename_checks)

def drill_check(to_check):
    for check in to_check:
        coordinate = check['coordinate']
        dimension = check['dimension']
        dim = getattr(Cube, dimension)
        res = list(dim.drill(coordinate))
        assert res == check['result']

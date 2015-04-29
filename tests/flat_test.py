import os

import pytest

from menger import dimension, Space, measure, connect
from base_test import URI


DATA = [
    {'name': 'ham',
     'place': ['EU', 'BE', 'BRU'],
     'total': 2},
    {'name': 'spam',
     'place': ['EU', 'BE', 'CRL'],
     'total': 4},
    {'name': 'ham',
     'place': ['EU', 'FR', 'ORY'],
     'total': 8},
    {'name': 'spam',
     'place': ['USA', 'NYC', 'JFK'],
     'total': 16},
]

class FlatCube(Space):
    name = dimension.Flat('Name', int)
    place = dimension.Tree('Place', ['Region', 'Country', 'City'], str)

    total = measure.Sum('Total')


@pytest.yield_fixture(scope='function')
def session():
    # Remove previous db
    if URI != ':memory:' and os.path.exists(URI):
        os.unlink(URI)

    with connect(URI):
        FlatCube.load(DATA)
        yield 'session'


def test_flat(session):
    res = list(FlatCube.dice())
    assert res == [((), (30.0,))]

    res = sorted(FlatCube.dice(coordinates=[('name', None)]))
    assert res == [(('ham',), (10.0,)), (('spam',), (20.0,))]

    res = sorted(FlatCube.dice(coordinates=[('place', (None,))]))
    assert res == [((('EU',),), (14.0,)), ((('USA',),), (16.0,))]

    coordinates = [
        ('place', (None,)),
        ('name', None),
    ]
    expected = [
        ((('EU',), 'ham'), (10.0,)),
        ((('EU',), 'spam'), (4.0,)),
        ((('USA',), 'spam'), (16.0,)),
    ]
    res = sorted(FlatCube.dice(coordinates=coordinates))
    assert res == expected

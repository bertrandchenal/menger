import os

import pytest

from menger import dimension, Space, measure, connect
from .base_test import URI

DATA = [
    {'version': ['2015-01'],
     'place': ['EU', 'BE', 'BRU'],
     'total': 2},
    {'version': ['2015-02'],
     'place': ['EU', 'BE', 'BRU'],
     'total': 20},
    {'version': ['2015-01'],
     'place': ['EU', 'BE', 'CRL'],
     'total': 4},
    {'version': ['2015-02'],
     'place': ['EU', 'BE', 'CRL'],
     'total': 40},
    {'version': ['2015-01'],
     'place': ['EU', 'FR', 'ORY'],
     'total': 8},
    {'version': ['2015-02'],
     'place': ['EU', 'FR', 'ORY'],
     'total': 80},
    {'version': ['2015-01'],
     'place': ['USA', 'NYC', 'JFK'],
     'total': 16},
    {'version': ['2015-02'],
     'place': ['USA', 'NYC', 'JFK'],
     'total': 160},
]

class VersionCube(Space):
    version = dimension.Version('Version', str)
    place = dimension.Tree('Place', ['Region', 'Country', 'City'], str)
    total = measure.Sum('Total')
    average = measure.Average('Average', 'total', 'count')


@pytest.yield_fixture(scope='function')
def session():
    # Remove previous db
    if URI != ':memory:' and os.path.exists(URI):
        os.unlink(URI)

    with connect(URI):
        VersionCube.load(DATA)
        yield 'session'


def test_version(session):
    res = list(VersionCube.dice([VersionCube.total]))
    assert res == [(300.0,)]

    res = sorted(VersionCube.dice([VersionCube.place, VersionCube.total]))
    assert res == [
        (('EU',), 140.0),
        (('USA',), 160.0),
    ]

    res = sorted(VersionCube.dice([VersionCube.version, VersionCube.total]))
    assert res == [
        (('2015-01',), 30.0),
        (('2015-02',), 300.0),
    ]

    res = sorted(VersionCube.dice(
        [VersionCube.version, VersionCube.total],
        [VersionCube.version.match(('2015-01',))],
    ))
    assert res == [(('2015-01',), 30.0)]

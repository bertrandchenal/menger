import pytest

from menger import Space, dimension, measure, gasket
from .base_test import Cube, session


DATA = [
    {'date': [2014, 1],
     'place': ['EU'],
     'other_total': 10,
     'other_count': 2},
    {'date': [2014, 2],
     'place': ['EU'],
     'other_total': 4,
     'other_count': 1},
    {'date': [2014, 1],
     'place': ['USA'],
     'other_total': 16,
     'other_count': 1},
]


class AnotherCube(Space):
    date = dimension.Tree('Date', ['Year', 'Month'], int)
    place = dimension.Tree('Place', ['Region'], str)

    other_total = measure.Sum('Other Total')
    other_count = measure.Sum('Other Count')
    other_average = measure.Average('Average', 'other_total', 'other_count')


def test_alone(session):
    # Test only measures
    ref_data = list(Cube.dice([Cube.total, Cube.count, Cube.average]))
    query = {
        'select': ['cube.total', 'cube.count', 'cube.average'],
    }
    check_data = gasket.dice(query)['data']
    check_data = [tuple(row) for row in check_data.values]
    assert ref_data == check_data

    # Test measure format
    query['msr_fmt'] = 'auto'
    check_data = gasket.dice(query)['data']
    check_data = [tuple(row) for row in check_data.values]
    assert check_data == [('30.00', '4.00', '7.50')]

    # Test dimension format
    for fmt in [None, 'leaf', 'full']:
        ref_data = list(Cube.dice([
            Cube.place['Country'],
            Cube.count
        ], dim_fmt=fmt))
        query = {
            'select': ['place[Country]', 'cube.count'],
            'dim_fmt': fmt,
        }
        check_data = gasket.dice(query)['data']
        check_data = [tuple(row) for row in check_data.values]
        assert ref_data == check_data


def test_multi(session):
    AnotherCube.load(DATA)

    query = {
        'select': ['place', 'cube.count', 'anothercube.other_count',
                   'cube.total', 'anothercube.other_total'],
    }
    check_data = gasket.dice(query)['data']
    count_check = check_data['Count'] == check_data['Other Count']
    assert count_check.all()

    total_check = check_data['Total'] == check_data['Other Total']
    assert total_check.all()


def test_pivot(session):
    query = {
        'select': ['date[Day]', 'place', 'cube.count'],
        'pivot_on': ['date[Day]'],
        'dim_fmt': 'leaf',
    }
    res = gasket.dice(query)
    check_data = res['data'].reset_index()
    assert all(check_data['Place'].values == ['EU', 'USA'])
    assert res['headers'] == [('Place', 'Count', 'Count'), ('', 1, 2)]

    # Use user-friendly  name to pivot
    query['pivot_on'] = ['Date: Day']
    check_data = gasket.dice(query)['data'].reset_index()
    assert all(check_data['Place'].values == ['EU', 'USA'])

    # Pivot on both dim
    query['pivot_on'] = ['Date: Day', 'place']
    res = gasket.dice(query)
    assert list(res['data']['Place'].values) == ['EU', 'USA'] * 2

    # Pivot by id
    query['pivot_on'] = 1
    res = gasket.dice(query)
    assert list(res['data']['Count']['EU']) == [2.0, 1.0]

def test_limit(session):
    # Test only measures
    query = {
        'select': ['place[City]', 'cube.total'],
        'dim_fmt': 'full',
    }

    query['limit'] = 3
    check_data = gasket.dice(query)['data']
    assert len(check_data) == 3

    query['limit'] = 10
    check_data = gasket.dice(query)['data']
    assert len(check_data) == 4


def test_filter(session):
    # Test only measures
    query = {
        'select': ['place[City]', 'cube.total'],
        'dim_fmt': 'leaf',
        'filters': [('place', [('EU', 'BE')])],
    }

    check_data = gasket.dice(query)['data']
    assert all(check_data['Place: City'].values == ['BRU', 'CRL'])

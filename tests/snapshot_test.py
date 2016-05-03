from .base_test import Cube, test_dice, dice_check, session
from menger import dimension, Space, measure


class OtherCube(Space):
    date = dimension.Tree('Date', ['Year', 'Month', 'Day'], int)
    place = dimension.Tree('Place', ['Region', 'Country', 'City'], str)

    total = measure.Sum('Total')
    count = measure.Sum('Count')
    average = measure.Average('Average', 'total', 'count')


def test_snapshot(session):
    Cube.snapshot(OtherCube)
    test_dice(session, OtherCube)


def test_snapshot_filter(session):
    filters = [Cube.date.match((2014, 1, 1))]
    Cube.snapshot(OtherCube, filters=filters)
    checks = [
        {'select': [Cube.total, Cube.count],
         'values' : [(10.0, 2.0)]
     },
    ]
    dice_check(checks, OtherCube)

def test_snapshot_default(session):
    select = [
        OtherCube.date['Day'],
        OtherCube.place(('EU', 'BE', 'BRU')),
        OtherCube.total,
        OtherCube.count,
    ]
    Cube.snapshot(OtherCube, select)
    checks = [
        {'select': [Cube.total, Cube.count],
         'values' : [(30.0, 4.0)]
     },
    ]
    dice_check(checks, OtherCube)


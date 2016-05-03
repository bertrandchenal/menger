from .base_test import Cube, test_dice, dice_check, session, drill_check

def test_delete_leaf(session):
    Cube.delete([
        Cube.place.match(('EU', 'BE', 'CRL'))
    ])
    checks = [
        {'select': [Cube.total, Cube.count],
         'values' : [(26.0, 3.0)]
     },
        {'select': [Cube.date['Day'], Cube.total],
         'filters': [Cube.date.match((2014, 1, 1))],
         'values' : [((2014, 1, 1), 10.0)]
     },
        {'select': [Cube.date['Day'], Cube.total],
         'filters': [Cube.date.match((2014, 1))],
         'values' : [((2014, 1, 1), 10.0),
                     ((2014, 1, 2), 16.0)]
     },
        {'select': [Cube.date['Day'], Cube.place['Country'], Cube.total],
         'filter': [Cube.date.match((2014, 1))],
         'values' : [
             ((2014, 1, 1), ('EU', 'BE'), 2.0),
             ((2014, 1, 1), ('EU', 'FR'), 8.0),
             ((2014, 1, 2), ('USA', 'NYC'), 16.0)
         ]
     },
    ]
    dice_check(checks)


def test_delete_node(session):

    Cube.delete([
        Cube.place.match(('EU', 'BE'))
    ])
    checks = [
        {'select': [Cube.total, Cube.count],
         'values' : [(24.0, 2.0)]
     },
        {'select': [Cube.date['Day'], Cube.total],
         'filters': [Cube.date.match((2014, 1, 1))],
         'values' : [((2014, 1, 1), 8.0)]
     },
        {'select': [Cube.date['Day'], Cube.total],
         'filters': [Cube.date.match((2014, 1))],
         'values' : [((2014, 1, 1), 8.0),
                     ((2014, 1, 2), 16.0)]
     },
        {'select': [Cube.date['Day'], Cube.place['Country'], Cube.total],
         'filters': [Cube.date.match((2014, 1))],
         'values': [
             ((2014, 1, 1), ('EU', 'FR'), 8.0),
             ((2014, 1, 2), ('USA', 'NYC'), 16.0)
         ]
     },
    ]
    dice_check(checks)

def test_mixed_levels(session):

    # Load data one level higher
    Cube.load([
        {'date': [2014, 1, 1],
         'place': ['EU', 'BE'],
         'total': 1.1,
         'count': 100},
    ])

    Cube.delete([
        Cube.place.match(('EU', 'BE'), depth=0)
    ])

    checks = [
        {'select': [Cube.total, Cube.count, Cube.average],
         'values' : [(30.0, 4.0, 7.5)]
     },
        {'select': [Cube.date['Day'], Cube.total],
         'filters': [Cube.date.match((2014, 1, 1))],
         'values' : [((2014, 1, 1), 10.0)]
     },
        {'select': [Cube.date['Day'], Cube.total],
         'filters': [Cube.date.match((2014, 1))],
         'values' : [((2014, 1, 1), 10.0),
                     ((2014, 1, 2), 20.0)]
     },
        {'select': [Cube.date['Day'], Cube.place['Country'], Cube.total],
         'filters': [Cube.date.match((2014, 1))],
         'values' : [
             ((2014, 1, 1), ('EU', 'BE'), 2.0),
             ((2014, 1, 1), ('EU', 'FR'), 8.0),
             ((2014, 1, 2), ('EU', 'BE'), 4.0),
             ((2014, 1, 2), ('USA', 'NYC'), 16.0)
         ]
     },
    ]
    dice_check(checks)

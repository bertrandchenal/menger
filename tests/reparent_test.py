from base_test import Cube, test_dice, dice_check, session, drill_check

def test_reparent_leaf(session):
    Cube.place.reparent(('EU', 'BE', 'CRL'), ('EU', 'FR'))

    reparent_dice_checks = [
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
             (((2014, 1, 1), ('EU', 'BE')), (2.0,)),
             (((2014, 1, 1), ('EU', 'FR')), (8.0,)),
             (((2014, 1, 2), ('EU', 'FR')), (4.0,)),
             (((2014, 1, 2), ('USA', 'NYC')), (16.0,))]
     }, # FIXME TEST DRILL LAST LEVEL
    ]
    dice_check(reparent_dice_checks)

    reparent_drill_checks = [
        {
            'coordinate' : tuple(),
            'result': ['EU', 'USA'],
            'dimension': 'place',
        },
        {
            'coordinate' : ('EU',),
            'result': ['BE', 'FR'], # TODO BE should disappear
            'dimension': 'place',
        },
        {
            'coordinate' : ('EU', 'FR'),
            'result': ['CRL', 'ORY'],
            'dimension': 'place',
        },
    ]
    drill_check(reparent_drill_checks)


def test_reparent_subtree(session):
    Cube.place.reparent(('EU', 'BE'), ('USA',))
    reparent_dice_checks = [
        {'coordinates': [],
         'measures': ['total', 'count'],
         'values' : [((), (30.0, 4.0))]
        },
        {'coordinates': [('date', (2014, 1, None)), ('place', (None, None))],
         'measures': ['total'],
         'values' : [
             (((2014, 1, 1), ('EU', 'FR')), (8.0,)),
             (((2014, 1, 1), ('USA', 'BE')), (2.0,)),
             (((2014, 1, 2), ('USA', 'BE')), (4.0,)),
             (((2014, 1, 2), ('USA', 'NYC')), (16.0,))]
     },
    ]
    dice_check(reparent_dice_checks)

    reparent_drill_checks = [
        {
            'coordinate' : ('USA',),
            'result': ['BE', 'NYC'],
            'dimension': 'place',
        },
    ]
    drill_check(reparent_drill_checks)


def test_merge_reparent(session):
    # Force clone subtree
    Cube.load([
        {'date': [2014, 1, 1],
         'place': ['USA', 'BE', 'BRU'],
         'total': 2,
         'count': 1},
    ])

    Cube.place.reparent(('EU', 'BE'), ('USA',))

    reparent_dice_checks = [
        {'coordinates': [],
         'measures': ['total', 'count'],
         'values' : [((), (30.0, 4.0))]
        },
        {'coordinates': [('date', (2014, 1, None)), ('place', (None, None))],
         'measures': ['total'],
         'values' : [
             (((2014, 1, 1), ('EU', 'FR')), (8.0,)),
             (((2014, 1, 1), ('USA', 'BE')), (2.0,)),
             (((2014, 1, 2), ('USA', 'BE')), (4.0,)),
             (((2014, 1, 2), ('USA', 'NYC')), (16.0,))]
     },
    ]
    dice_check(reparent_dice_checks)

    reparent_drill_checks = [
        {
            'coordinate' : ('USA',),
            'result': ['BE', 'NYC'],
            'dimension': 'place',
        },
    ]
    drill_check(reparent_drill_checks)


def test_rename(session):
    Cube.place.rename(('EU', 'FR', 'ORY'), 'CDG')

    rename_dice_checks = [
        {'coordinates': [('place', ('EU', 'FR', None))],
         'measures': ['total'],
         'values' :[
             ((('EU', 'FR', 'CDG'),), (8.0,)),
         ]
     },
    ]
    dice_check(rename_dice_checks)

    rename_drill_checks = [
        {
            'coordinate' : ('EU', 'FR'),
            'result': ['CDG'],
            'dimension': 'place',
        },
    ]
    drill_check(rename_drill_checks)


def test_wrong_reparent(session):
    Cube.place.reparent(('EU', 'JA'), ('USA',))

def test_prune_reparent(session):
    Cube.place.reparent(('EU', 'FR', 'ORY'), ('EU', 'BE'))
    drill_check([
        {
            'coordinate' : ('EU',),
            'result': ['BE'],
            'dimension': 'place',
        },
    ])


def test_merge_rename(session):
    Cube.place.rename(('EU', 'BE', 'BRU'), 'CRL')

    rename_dice_checks = [
        {'coordinates': [('place', ('EU', 'BE', None))],
         'measures': ['total'],
         'values' :[
             ((('EU', 'BE', 'CRL'),), (6.0,)),
         ]
     },
    ]

    dice_check(rename_dice_checks)

    rename_drill_checks = [
        {
            'coordinate' : ('EU', 'BE'),
            'result': ['CRL'],
            'dimension': 'place',
        },
    ]
    drill_check(rename_drill_checks)


def test_delete(session):

    Cube.place.delete(('USA',))

    dice_check([
        {'coordinates': [],
         'measures': ['total', 'count'],
         'values' :[
             ((), (14.0, 3.0)),
         ]
     },
    ])

    drill_check([
        {
            'coordinate' : tuple(),
            'result': ['EU'],
            'dimension': 'place',
        },
    ])

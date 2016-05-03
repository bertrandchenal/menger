
# Menger

Menger is an OLAP library written in Python and released under the ISC licence.
Menger uses Sqlite as backend, the support for Postgresql is currently disabled


## Example

Let's say we want to collect statistics about the length of blog posts. We
start by creating a `Post` class that inherits from Menger's `Space` class:

    :::python
    from menger import Space, dimension, measure

    class Post(Space):
        date = dimension.Tree('Date', ['Year', 'Month', 'Day'], int)
        author = dimension.Tree('Author')
        words = measure.Sum('Number of Words')
        signs = measure.Sum('Number of Signs')
        average = measure.Average('Average', 'signs', 'words')


The `load` method allows to store data points (records):

    :::python
    from menger import connect
    with connect('example.db'):
        Post.load([
            {'date': [2012, 7, 26],
             'author': ['John'],
             'words': 148,
             'signs': 743},
            {'date': [2012, 8, 7],
             'author': ['John'],
             'words': 34,
             'signs': 145},
            {'date': [2012, 8, 9],
             'author': ['Bill'],
             'words': 523,
             'signs': 2622},
        ])

We can now retrieve aggregated measures with `dice`:

    :::python
    with connect('example.db'):
        res = Post.dice()
        print(list(res))
    # Gives:
    # [((2012, 7, 26), ('John',), 148.0, 743.0),
    # ((2012, 8, 7), ('John',), 34.0, 145.0),
    # ((2012, 8, 9), ('Bill',), 523.0, 2622.0)]


The select argument allows to select on which measure and dimension
(and level) to dice:

    :::python
    with connect('example.db'):
        res = Post.dice([Post.date['Month'], Post.average])
        print(list(res))
    # Gives:
    # [((2012, 7), 5.02), ((2012, 8), 4.96)]


The drill method allows to explore dimensions

    :::python
    print(list(Post.date.drill((2012, 8))))
    # Gives: [7, 9]

## Commnand line helper

The command line helper allows to manipulate menger objects from the
shell. After the spaces definition, simply the following to your
script:

    :::python
    with connect('belgium.db'):
        from menger import Cli
        Cli.run()

The following session is based on a the population for belgium between
2010 and 2015:

    :::bash
    $ ./belgium.py info
    Dimensions
     geography  [region, province, arrondissement, commune]
     civil_status  [status]
     nationality  [nationality]
     sex  [sex]
     age  [age]
     year  [year]
    Measures
     population

    $ ./belgium.py drill year
    2010
    2011
    2012
    2013
    2014
    2015

    $ ./belgium.py dice year population
    Year Population
    2010 10839905
    2011 10951266
    2012 11035948
    2013 11099554
    2014 11150516
    2015 11209044


    $ ./belgium.py dice year=2010 geography population
    Year Geography                    Population
    2010 Région de Bruxelles-Capitale 1089538
    2010 Région flamande              6251983
    2010 Région wallonne              3498384

    $ ./belgium.py dice year=2015 geography[province] population
    Year Province                                        Population
    2015 Région de Bruxelles-Capitale/                   1175173
    2015 Région flamande/Province de Brabant flamand     1114299
    2015 Région flamande/Province de Flandre occidentale 1178996
    2015 Région flamande/Province de Flandre orientale   1477346
    2015 Région flamande/Province de Limbourg            860204
    2015 Région flamande/Province d’Anvers               1813282
    2015 Région wallonne/Province de Brabant wallon      393700
    2015 Région wallonne/Province de Hainaut             1335360
    2015 Région wallonne/Province de Liège               1094791
    2015 Région wallonne/Province de Luxembourg          278748
    2015 Région wallonne/Province de Namur               487145

    $ ./belgium.py dice year=2015 geography="Région wallonne/*/*" population
    Year Geography                                                                  Population
    2015 Région wallonne/Province de Brabant wallon/Arrondissement de Nivelles      393700
    2015 Région wallonne/Province de Hainaut/Arrondissement de Charleroi            429854
    2015 Région wallonne/Province de Hainaut/Arrondissement de Mons                 257804
    2015 Région wallonne/Province de Hainaut/Arrondissement de Mouscron             75200
    2015 Région wallonne/Province de Hainaut/Arrondissement de Soignies             188389
    2015 Région wallonne/Province de Hainaut/Arrondissement de Thuin                151115
    2015 Région wallonne/Province de Hainaut/Arrondissement de Tournai              146831
    2015 Région wallonne/Province de Hainaut/Arrondissement d’Ath                   86167
    2015 Région wallonne/Province de Liège/Arrondissement de Huy                    111839
    2015 Région wallonne/Province de Liège/Arrondissement de Liège                  618887
    2015 Région wallonne/Province de Liège/Arrondissement de Verviers               285214
    2015 Région wallonne/Province de Liège/Arrondissement de Waremme                78851
    2015 Région wallonne/Province de Luxembourg/Arrondissement de Bastogne          46857
    2015 Région wallonne/Province de Luxembourg/Arrondissement de Marche-en-Famenne 55857
    2015 Région wallonne/Province de Luxembourg/Arrondissement de Neufchâteau       62099
    2015 Région wallonne/Province de Luxembourg/Arrondissement de Virton            53279
    2015 Région wallonne/Province de Luxembourg/Arrondissement d’Arlon              60656
    2015 Région wallonne/Province de Namur/Arrondissement de Dinant                 108941
    2015 Région wallonne/Province de Namur/Arrondissement de Namur                  311684
    2015 Région wallonne/Province de Namur/Arrondissement de Philippeville          66520


## Performance

The population dataset contains `2,705,776` lines, a common query take
around 5 seconds on a modest Intel Core M:

    $ time ./belgium.py dice year population > /dev/null

    real0m5.841s
    user0m5.752s
    sys0m0.612s

    $ time ./belgium.py dice year=2015 geography="Région wallonne/*/*" population > /dev/null

    real 0m5.133s
    sys 0m4.904s
    user 0m0.752s


A more costly query, that involve all dimensions takes around 14 seconds:

    :::bash
    $ time ./belgium.py dice year geography age sex civil_status nationality | wc
    24224  177591 1447930

    real0m14.098s
    user0m13.664s
    sys0m0.980s


Most of the time we don't need so mush depth on all dimension by
reducing the geography dimension to the province level and by removing
the age dimension, the same query is several order of magnitude faster:

    $ time ./belgium.py dice year geography sex civil_status nationality --space PopulationShallow | wc
    289    1829   16118

    real0m0.682s
    user0m0.732s
    sys0m0.472s


## Documentation TODO

See the tests folder for examples on the following features:

  - Snapshots
  - Versioning
  - Filter & Limit

Roadmap:

  - Revive Postgresql support
  - Support for ranges on scalar dimensions

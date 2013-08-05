
# Menger

Menger is an ORM-like, ISC-licensed statistics storage.

Menger is designed to receive a flow of data as input and provide
live statistics. It works by pre-computing statistics for each
combination of possible query. So when a record like the following is
added:

    :::python
    {'date': ['2012', '8', '17'], 'author': ["Bill"], 'nb_words': 523}

the `nb_words` column is incremented by 523 in 8 lines in the
database. They correspond to the following combination:

    :::python
    ([], [])
    ([], ['Bill'])
    (['2012'], [])
    (['2012'], ['Bill'])
    (['2012', '8'], [])
    (['2012', '8'], ['Bill'])
    (['2012', '8', '17'], [])
    (['2012', '8', '17'], ['Bill'])

Currently Postgresql and Sqlite are supported.


## Example

Let's say we want to collect statistics about the length of blog posts. We
start by creating a `Post` class that inherits from Menger's `Space` class:

    :::python
    class Post(Space):

        date = dimension.Tree('Category')
        author = dimension.Tree('Author)
        nb_words = measure.Sum('Number of Words')
        nb_typos = measure.Sum('Number of Typos')

A `Space` class comprises one or several dimensions and one or several
measures.

Measures aggregates values. Dimensions allows to characterise those
aggregates.


The `load` method allows to store data points (records):

    :::python
    Post.load([
        {'date': ['2012', '7', '26'], 'author': ['John'], 'nb_words': 148, 'nb_typos': 1},
        {'date': ['2012', '8', '7'], 'author': ['John'], 'nb_words': 34, 'nb_typos': 0},
        {'date': ['2012', '8', '9'], 'author': ['Bill'], 'nb_words': 523, 'nb_typos': 2},
        ])

We can now retrieve aggregated measures with `fetch`:

    :::python
    Post.fetch() # gives {'nb_words': 705, 'nb_typos': 3}
    Post.fetch(author=['John'], date=['2012', '7']) # gives {'nb_words': 148, 'nb_typos': 1}

Or `drill` the dimensions (i.e. get subcategories of a dimension):

    :::python
    Post.date.drill(['2012']) # gives [('2012', '7'), ('2012', '8')]

Full code listing:

    :::python
    from menger import Space, dimension, measure
    from menger.common import connect

    class Post(Space):

        date = dimension.Tree('Category')
        author = dimension.Flat('Category')
        nb_words = measure.Sum('Number of Words')
        nb_typos = measure.Sum('Number of Typos')

    db_uri = 'mng.db'
    with Post.connect(db_uri): # See menger/backend/__init__.py for uri examples
        Post.load([
            {'date': ['2012', '7', '26'], 'author': ['John'], 'nb_words': 148, 'nb_typos': 1},
            {'date': ['2012', '8', '7'], 'author': ['John'], 'nb_words': 34, 'nb_typos': 0},
            {'date': ['2012', '8', '9'], 'author': ['Bill'], 'nb_words': 523, 'nb_typos': 2},
            ])

        print Post.dice({}) # gives {'nb_words': 705, 'nb_typos': 3}
        print Post.dice(author=['John'], date=['2012', '7']) # gives {'nb_words': 148, 'nb_typos': 1}

        print list(Post.date.drill('2012'))

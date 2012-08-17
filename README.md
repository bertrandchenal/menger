
# Lattice

Lattice is an ISC licensed statistics storage.

Lattice is designed to receive a flow of data as input and provide
live statistics. It works by pre-computing statistics for each
combination of possible query. So when a new data point like this is
added:

    :::python
    {'date': ['2012', '8', '17'], 'author': Bill", 'nb_words': 523}

Eight counters are incremented:

    :::python
    ([], None)
    ([], 'Bill')
    (['2012'], None)
    (['2012'], 'Bill')
    (['2012', '8'], None)
    (['2012', '8'], 'Bill')
    (['2012', '8', '17'], None)
    (['2012', '8', '17'], 'Bill')


Each counter is stored in a LevelDB database.


## Example

Let's say we want to collect statistics about length of blog posts. We
start by creating a `Post` class that inherit from `Space`:

    :::python
    class Post(Space):

        date = dimension.Tree('Category')
        author = dimension.Flat('Category')
        nb_words = measure.Sum('Number of Words')
        nb_typos = measure.Sum('Number of Typos')

A space class is made of one or several dimensions and one or several
measures.

The `load` method allows to store data points:

    :::python
    Post.load([
        {'date': ['2012', '7', '26'], 'author': 'John', 'nb_words': 148, 'nb_typos': 1},
        {'date': ['2012', '8', '7'], 'author': 'John', 'nb_words': 34, 'nb_typos': 0},
        {'date': ['2012', '8', '9'], 'author': 'Bill', 'nb_words': 523, 'nb_typos': 2},
        ])

We can now retrieve data with `fetch`:

    :::python
    Post.fetch('nb_words') # prints 705 (148+34+523)
    Post.fetch('nb_words', author='Bill') # prints 523
    Post.fetch('nb_words', 'nb_typos', author='John', date=['2012', '7']) # prints 523

Or `drill` the dimensions:

    :::python
    Post.date.drill(['2012']) # yields ['2012', '7'] and ['2012', '8']

Full code listing:

    :::python
    from lattice import Space, dimension, measure
    from lattice.common import connect

    class Post(Space):

        date = dimension.Tree('Category')
        author = dimension.Flat('Category')
        nb_words = measure.Sum('Number of Words')
        nb_typos = measure.Sum('Number of Typos')

    with connect('db/Post'):
        Post.load([
            {'date': ['2012', '7', '26'], 'author': 'John', 'nb_words': 148, 'nb_typos': 1},
            {'date': ['2012', '8', '7'], 'author': 'John', 'nb_words': 34, 'nb_typos': 0},
            {'date': ['2012', '8', '9'], 'author': 'Bill', 'nb_words': 523, 'nb_typos': 2},
            ])

        print Post.fetch('nb_words') # prints 705 (148+34+523)
        print Post.fetch('nb_words', author='Bill') # prints 523
        print Post.fetch('nb_words', 'nb_typos', author='John', date=['2012', '7']) # prints (148, 1)

        print list(Post.date.drill(['2012']))


# Menger

Menger is an ORM-like, ISC-licensed statistics storage.

Menger is designed to receive a flow of data as input and provide
live statistics. It works by pre-computing statistics for each
combination of possible query. So when a record like the following is
added:

    :::python
    {'date': ['2012', '8', '17'], 'author': "Bill", 'nb_words': 523}

eight counters (or indices) are updated:

    :::python
    ([], None)
    ([], 'Bill')
    (['2012'], None)
    (['2012'], 'Bill')
    (['2012', '8'], None)
    (['2012', '8'], 'Bill')
    (['2012', '8', '17'], None)
    (['2012', '8', '17'], 'Bill')


Each counter is stored in the database (LevelDB or Sqlite).


## Example

Let's say we want to collect statistics about the length of blog posts. We
start by creating a `Post` class that inherits from Menger's `Space` class:

    :::python
    class Post(Space):

        date = dimension.Tree('Category')
        author = dimension.Flat('Category')
        nb_words = measure.Sum('Number of Words')
        nb_typos = measure.Sum('Number of Typos')

A `Space` class comprises one or several dimensions and one or several
measures.

Measures are caracteristics of the class that can be averaged, or compared
through all objects. Dimensions are caracteristics of the class that can 
act as categories when computing a measure's aggregated value.

The `load` method allows to store data points (records):

    :::python
    Post.load([
        {'date': ['2012', '7', '26'], 'author': 'John', 'nb_words': 148, 'nb_typos': 1},
        {'date': ['2012', '8', '7'], 'author': 'John', 'nb_words': 34, 'nb_typos': 0},
        {'date': ['2012', '8', '9'], 'author': 'Bill', 'nb_words': 523, 'nb_typos': 2},
        ])

We can now retrieve aggregated measures with `fetch`:

    :::python
    Post.nb_words.fetch() # prints 705 (148+34+523)
    Post.nb_words.fetch(author='Bill') # prints 523
    Post.fetch('nb_words', 'nb_typos', author='John', date=['2012', '7']) # prints (148, 1)

Or `drill` the dimensions (i.e. get subcategories of a dimension):

    :::python
    Post.date.drill(['2012']) # yields ['2012', '7'] and ['2012', '8']

Full code listing:

    :::python
    from menger import Space, dimension, measure
    from menger.common import connect

    class Post(Space):

        date = dimension.Tree('Category')
        author = dimension.Flat('Category')
        nb_words = measure.Sum('Number of Words')
        nb_typos = measure.Sum('Number of Typos')

    with connect('db/Post', backend='sqlite'): # available backends: 'sqlite' or 'leveldb'
        Post.load([
            {'date': ['2012', '7', '26'], 'author': 'John', 'nb_words': 148, 'nb_typos': 1},
            {'date': ['2012', '8', '7'], 'author': 'John', 'nb_words': 34, 'nb_typos': 0},
            {'date': ['2012', '8', '9'], 'author': 'Bill', 'nb_words': 523, 'nb_typos': 2},
            ])

        print Post.nb_words.fetch() # prints 705 (148+34+523)
        print Post.nb_words.fetch(author='Bill') # prints 523
        print Post.fetch('nb_words', 'nb_typos', author='John', date=['2012', '7']) # prints (148, 1)

        print list(Post.date.drill(['2012']))

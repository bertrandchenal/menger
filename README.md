
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

Let's say we want to collect statistics about length of blog posts. we
start by creating a Space class:


    :::python
    class Post(Space):

        date = dimension.Tree('Category')
        author = dimension.Flat('Category')
        nb_words = measure.Sum('Number')

The `load` method allows to store data points:

    :::python
    Post.load([
        {'date': ['2012', '8', '17'], 'author': Bill", 'nb_words': 523},
        {'date': ['2012', '7', '29'], 'author': John", 'nb_words': 148},
        ])

We can now retrieve data with fetch:

    :::python
    Post.fetch('nb_words') # returns 671
    Post.fetch('nb_words', author='Bill') # returns 523
    Post.fetch('nb_words', author='Bill', date=['2012', '8']) # returns 523

Or drill the dimensions:

    :::python
    Post.date.drill(['2012']) # yields ['2012', '7'] and ['2012', '8']


Full listing:

    :::python
    from lattice import Space, dimension, measure
    from lattice.common import connect
    
    class Post(Space):
    
        date = dimension.Tree('Category')
        author = dimension.Flat('Category')
        nb_words = measure.Sum('Number')
    
    
    with connect('db/Post'):
        Post.load([
                {
                    'date': ['2012', '8', '17'],
                    'author': "Bill",
                    'nb_words': 523
                    },
                {
                    'date': ['2012', '7', '29'],
                    'author': "John",
                    'nb_words': 148
                    },
                ])
    
        print Post.fetch('nb_words')
        print Post.fetch('nb_words', author='Bill')
        print Post.fetch('nb_words', author='Bill', date=['2012', '8'])
    
        print list(Post.date.drill(['2012']))

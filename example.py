from pprint import pprint
from menger import measure, dimension, Space, connect

class Post(Space):
    date = dimension.Tree('Date', ['Year', 'Month', 'Day'], int)
    author = dimension.Tree('Author')
    words = measure.Sum('Number of Words')
    signs = measure.Sum('Number of Signs')
    average = measure.Average('Average', 'signs', 'words')


def run():
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
    print('Top-level dice')
    pprint(list(Post.dice()))

    print('Per Month dice')
    pprint(list(Post.dice([Post.date['Month'], Post.average])))

    print('Date drill')
    pprint(list(Post.date.drill((2012, 8))))

with connect(':memory:'):
    run()

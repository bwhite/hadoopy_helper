import hadoopy
try:
    import numpy as np
except ImportError:
    pass


def mapper(k, v):
    yield k, ''


def reducer(k, vs):
    yield k, ''

if __name__ == '__main__':
    hadoopy.run(mapper, reducer)

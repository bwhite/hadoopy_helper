import hadoopy
try:
    import numpy as np
except ImportError:
    pass


class Mapper(object):

    def __init__(self):
        self.count = 0

    def map(self, k, v):
        self.count += 1

    def close(self):
        yield 0, self.count


def reducer(k, vs):
    yield k, sum(vs)


if __name__ == '__main__':
    hadoopy.run(Mapper, reducer)

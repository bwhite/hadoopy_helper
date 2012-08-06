import hadoopy
import random
import os
try:
    import numpy as np
except ImportError:
    pass


class Mapper(object):

    def __init__(self):
        self.alpha = float(os.environ['ALPHA'])

    def map(self, k, v):
        out = random.random()
        if out < self.alpha:
            yield out, (k, v)


def reducer(out, kvs):
    # NOTE(brandyn): The reducer is so that readtb only has to read 1 file
    # and so that they are uniformly distributed
    for kv in kvs:
        yield kv


if __name__ == '__main__':
    hadoopy.run(Mapper, reducer, required_cmdenvs=['ALPHA'])

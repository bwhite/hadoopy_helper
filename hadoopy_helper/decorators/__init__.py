#- Limit first K-inputs
#- Limit first K-outputs
#- Profile init, map, reduce, close function calls and provide statistics (min, max, mean, total, num)

#- Convert exception to counter
#- Skip bad input decorator
#- Secondary sort w/ partitioner
#- Matrix block computation (e.g., kernels)
#- Random filter inputs (e.g., skip 10% of inputs, only allow 100 total inputs in)
#- Image load (given an (id, image_binary), produce a numpy array of the image)
#- Video load (given an (id, video_binary), produce a viderator)
#- Delay init to first call of map function (useful if init takes a while and a prior filter is applied directly to the Key/Value without using the class)
#- Ignore first K errors (print them, skip inputs, optionally print inputs, optionally writetb them to hdfs)
#- Validate input type (i.e., give a function that executes before all inputs)
#- Validate output type (i.e., give a function that executes on all outputs)
#- Load pickles from cmdenvs
#- Load readtb from cmdenvs
#- Chain mapper (apply multiple map-only tasks with an optional final reduce).  This can be used automatically by hadoopy_flow
#- Multi-file join
#- Secure job execution (pass data encryption key to the workers directly)


def _output_iter(iter_or_none):
    if iter_or_none is None:
        return ()
    return iter_or_none


def max_inputs(k):

    def decorator(func):
        state = {'k': k}

        def wrap(*args, **kw):
            if state['k'] <= 0:
                return ()
            state['k'] -= 1
            return _output_iter(func(*args, **kw))
        return wrap
    return decorator


def max_outputs(k):

    def decorator(func):
        state = {'k': k}

        def wrap(*args, **kw):
            if state['k'] <= 0:
                return
            for x in _output_iter(func(*args, **kw)):
                yield x
                state['k'] -= 1
                if state['k'] <= 0:
                    return
        return wrap
    return decorator


def profile():

    def decorator(func):
        state = {'num_calls': 0,
                 'total_time': 0.,
                 'min_time': float('inf'),
                 'max_time': float('-inf'),
                 'name': repr(func)}

        def report():
            import json
            print(json.dumps(state))
        import atexit
        atexit.register(report)

        def wrap(*args, **kw):
            import time
            st = time.time()
            for x in _output_iter(func(*args, **kw)):
                yield x
            t = time.time() - st
            state['num_calls'] += 1
            state['total_time'] += t
            state['max_time'] = max(state['max_time'], t)
            state['min_time'] = min(state['min_time'], t)
        return wrap
    return decorator


#class TestMapper(object):
#
#    def __init__(self):
#        pass

#    @max_inputs(5)
#    @profile()
#    def map(self, k, v):
#        print((k, v))
#        yield k, v

#if __name__ == '__main__':
#    i = TestMapper()
#    for x in range(10):
#        print list(i.map(1, 1))

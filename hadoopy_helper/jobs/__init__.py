import hadoopy
import hadoopy_helper
import os
import logging


def _lf(fn):
    from . import __path__
    return os.path.join(__path__[0], fn)


def _random_sample_alpha(n, m, p=.01):
    """Find an alpha such that X = np.sum(np.random(n) < alpha) where X >= m with probability p

    Args:
        n: Number of total values
        m: Desired number of samples
        p: Failure probability

    Returns:
        Probability threshold
    """
    if n < m:
        return 1.
    elif m == 0:
        return 0.
    else:
        import scipy as sp
        import scipy.optimize
        import scipy.stats
        # Need to search somewhere between m / n <= x <= 1.
        return sp.optimize.bisect(lambda alpha: (p - sp.stats.distributions.binom.cdf(m - 1, n, alpha)),
                                  m / float(n), 1., xtol=1e-10, maxiter=1000)


def unique_keys(hdfs_input, hdfs_temp_dir=None):
    with hadoopy_helper.hdfs_temp(hdfs_temp_dir=hdfs_temp_dir) as hdfs_output:
        hadoopy.launch_frozen(hdfs_input, hdfs_output, _lf('unique_keys.py'))
        for x in hadoopy.readtb(hdfs_output):
            yield x[0]


def count_kvs(hdfs_input, hdfs_temp_dir=None):
    with hadoopy_helper.hdfs_temp(hdfs_temp_dir=hdfs_temp_dir) as hdfs_output:
        hadoopy.launch_frozen(hdfs_input, hdfs_output, _lf('count.py'), num_reducers=1)
        return sum(x for _, x in hadoopy.readtb(hdfs_output))


def random_sample(hdfs_input, m, n=None, p=.01, hdfs_temp_dir=None):
    """Return an iterator of m kv pairs selected uniformly from the input

    Finds an alpha such that X = np.sum(np.random(n) < alpha) where X >= m with probability p.
    If more kv pairs are returned from Hadoop, then they are ignored.  The resulting kv pairs
    are uniformly random from the input.

    Args:
        m: Desired number of samples (you will get this many as long as n >= m with probability (1-p))
        n: Number of total values (default None uses count_kvs to compute this)
        p: Failure probability (default .01 means there is 1 failure out of 100 runs)

    Yields:
        Sample k/v pairs
    """
    if n is None:
        n = count_kvs(hdfs_input)
    alpha = _random_sample_alpha(n, m, p=p)
    num_outputs = 0
    with hadoopy_helper.hdfs_temp(hdfs_temp_dir=hdfs_temp_dir) as hdfs_output:
        hadoopy.launch_frozen(hdfs_input, hdfs_output, _lf('random_sample.py'),
                              cmdenvs={'ALPHA': alpha})
        for kv in hadoopy.readtb(hdfs_output):
            if num_outputs >= m:
                return
            yield kv
            num_outputs += 1
    if num_outputs < m:
        logging.warn('random_sampler: num_outputs[%d] when m[%d].  To prevent this, call with a smaller value of p (currently [%f]).' % (num_outputs, m, p))

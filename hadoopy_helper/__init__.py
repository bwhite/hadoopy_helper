import hadoopy
import contextlib
import cPickle as pickle
import math
import os
import random
import time
import contextlib
import hadoopy_helper.jobs
import glob
import logging
import multiprocessing
import random
HDFS_TEMP_DIR = '_hadoopy_temp'


@contextlib.contextmanager
def hdfs_temp(hdfs_temp_dir=None):
    if hdfs_temp_dir is None:
        hdfs_temp_dir = HDFS_TEMP_DIR
    temp_path = hadoopy.abspath('%s/%f-%f' % (hdfs_temp_dir, time.time(), random.random()))
    yield temp_path
    if hadoopy.exists(temp_path):
        hadoopy.rmr(temp_path)


def _local_iter(in_name, max_input=None):
    if isinstance(in_name, str):
        in_name = [in_name]
    if isinstance(in_name[0], str):
        if max_input is not None:
            max_input = int(math.ceil(max_input / float(len(in_name))))
        for cur_in_name in in_name:
            for x, y in enumerate(hadoopy.readtb(cur_in_name)):
                if max_input is not None and x > max_input:
                    break
                yield y
    else:
        for x, y in enumerate(in_name):
            if max_input is not None and x > max_input:
                break
            yield y


@contextlib.contextmanager
def local_cache(cache_path, max_input=None, *args, **kw):
    dir_name = os.path.dirname(os.path.abspath(cache_path))
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    with local(*args, **kw) as local_launch:
        def launch(in_name, out_name, script_path, **kw2):
            # If local kv cache doesn't exist, then copy the correct number of values there
            if not os.path.exists(cache_path):
                kvs = list(_local_iter(in_name, max_input))
                with open(cache_path, 'w') as fp:
                    pickle.dump(kvs, fp, -1)
            else:
                with open(cache_path) as fp:
                    kvs = pickle.load(fp)
            return local_launch(kvs, out_name, script_path, **kw2)
        yield launch


@contextlib.contextmanager
def local(files=(), max_input=None):

    def launch(in_name, out_name, script_path, **kw):
        # If local kv cache doesn't exist, then copy the correct number of values there
        try:
            kw['files'] = list(kw['files']) + list(files)
        except KeyError:
            kw['files'] = files
        return hadoopy.launch_local(_local_iter(in_name, max_input), None, script_path, **kw)['output']
    yield launch

_multiprocessing_manager = None
def _instrument_hadoopy_cache():
    global _multiprocessing_manager
    if _multiprocessing_manager is None:
        _multiprocessing_manager = multiprocessing.Manager()
        hadoopy._freeze.FREEZE_CACHE = _multiprocessing_manager.dict(hadoopy._freeze.FREEZE_CACHE)


def _freeze_script(fns, freeze_dict, python_cmd):
    for fn in fns:
        try:
            hadoopy._runner._check_script(fn, files=glob.glob(os.path.dirname(fn) + '/*.py'), python_cmd=python_cmd)
        except ValueError:
            logging.warn('prefreeze: Skipping script[%s] as it could not be executed' % fn)
            continue
        logging.info('prefreeze: Freezing script[%s]' % fn)
        if fn in freeze_dict:
            continue
        hadoopy.freeze_script(fn)
        freeze_dict.update(hadoopy._freeze.FREEZE_CACHE)


def prefreeze(scripts, python_cmd='python', max_procs=1):
    # TODO: Allow prioritizing
    _instrument_hadoopy_cache()
    if isinstance(scripts, (str, unicode)):
        scripts = [scripts]
    fns = []
    for script in scripts:
        for fn in glob.glob(script):
            fns.append(os.path.abspath(fn))
    random.shuffle(fns)  # Mix them up to prevent hot spots
    fns_per_proc = max(len(fns) / max_procs, 1)
    chunks = [fns[i:i + fns_per_proc] for i in xrange(0, len(fns), fns_per_proc)]
    for chunk in chunks:
        multiprocessing.Process(target=_freeze_script, args=(chunk, hadoopy._freeze.FREEZE_CACHE, python_cmd)).start()

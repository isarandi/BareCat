import functools

import multiprocessing_utils

from barecat.core import barecat as barecat


def threadlocal_decorate(decorator):
    def my_decorator(fun):
        local = multiprocessing_utils.local()

        @functools.wraps(fun)
        def wrapper(*args, **kwargs):
            if not hasattr(local, 'fn'):
                local.fn = decorator(fun)
            return local.fn(*args, **kwargs)

        return wrapper

    return my_decorator


@threadlocal_decorate(functools.lru_cache())
def get_cached_reader(path, auto_codec=True):
    return barecat.Barecat(path, readonly=True, auto_codec=auto_codec)


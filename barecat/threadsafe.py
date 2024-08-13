import functools
import multiprocessing_utils
import barecat.barecat_unif as barecat_unif


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
def get_cached_reader(path):
    return barecat_unif.BareCat(path, readonly=True, auto_codec=True)

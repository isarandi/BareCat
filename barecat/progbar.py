import sys


def is_running_in_jupyter_notebook():
    try:
        # noinspection PyUnresolvedReferences
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True  # Jupyter notebook or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


def progressbar(iterable=None, *args, **kwargs):
    import tqdm
    if is_running_in_jupyter_notebook():
        return tqdm.notebook.tqdm(iterable, *args, **kwargs)
    elif sys.stdout.isatty():
        return tqdm.tqdm(iterable, *args, dynamic_ncols=True, **kwargs)
    elif iterable is None:
        class X:
            def update(self, *a, **kw):
                pass

        return X()
    else:
        return iterable


def progressbar_items(dictionary, *args, **kwargs):
    return progressbar(dictionary.items(), total=len(dictionary), *args, **kwargs)

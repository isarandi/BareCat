import os
import os.path as osp
import shutil
import sys

import barecat.barecat as barecat_


def create_from_stdin_paths(target_path, shard_size, zero_terminated=False, overwrite=False):
    with barecat_.Writer(target_path, shard_size=shard_size, overwrite=overwrite) as writer:
        if zero_terminated:
            input_paths = iterate_zero_terminated(sys.stdin.buffer)
        else:
            input_paths = (l.rstrip('\n') for l in sys.stdin)

        for input_path in progressbar(input_paths, desc='Writing files', unit='files'):
            writer.add_by_path(input_path)


def extract(barecat_path, target_directory):
    with barecat_.Reader(barecat_path) as reader:
        for path_in_archive in progressbar(reader, desc='Extracting files', unit='files'):
            with reader.open(path_in_archive) as file_in_archive:
                target_path = osp.join(target_directory, path_in_archive)
                os.makedirs(osp.dirname(target_path), exist_ok=True)
                with open(target_path, 'wb') as output_file:
                    shutil.copyfileobj(file_in_archive, output_file)


def write_index(dictionary, target_path):
    with barecat_.IndexWriter(target_path) as writer:
        for path, address in dictionary.items():
            writer[path] = address


def read_index(path):
    with barecat_.IndexReader(path) as reader:
        return dict(reader.items())


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
    import sys
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


def iterate_zero_terminated(fileobj):
    partial_path = b''
    while chunk := fileobj.read(4096):
        parts = chunk.split(b'\x00')
        parts[0] = partial_path + parts[0]
        partial_path = parts.pop()

        for input_path in parts:
            input_path = input_path.decode()
            yield input_path

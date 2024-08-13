import functools
import glob
import itertools
import os
import os.path as osp
import queue
import shutil
import sys
import threading

import barecat.barecat_unif as barecat_unif


def create_from_stdin_paths(target_path, shard_size, zero_terminated=False, overwrite=False):
    with barecat_unif.BareCat(
            target_path, shard_size=shard_size, readonly=False, overwrite=overwrite) as writer:
        if zero_terminated:
            input_paths = iterate_zero_terminated(sys.stdin.buffer)
        else:
            input_paths = (l.rstrip('\n') for l in sys.stdin)

        for input_path in progressbar(input_paths, desc='Packing files', unit=' files'):
            writer.add_by_path(input_path)


def create_from_stdin_paths_workers(
        target_path, shard_size, zero_terminated=False, overwrite=False, workers=8):
    import simplepyutils as spu
    q = queue.Queue(maxsize=workers * 2)
    writer_thread = threading.Thread(
        target=barecat_writer_main, args=(target_path, shard_size, overwrite, q))
    writer_thread.start()

    if zero_terminated:
        input_paths = iterate_zero_terminated(sys.stdin.buffer)
    else:
        input_paths = (l.rstrip('\n') for l in sys.stdin)

    def putter(input_path):
        return lambda x: q.put((input_path, x))

    with spu.ThrottledPool(workers) as pool:
        for input_path in progressbar(input_paths, desc='Packing files', unit=' files'):
            pool.apply_async(
                read_content, (input_path,), callback=putter(input_path))

    q.put(None)
    writer_thread.join()


def read_content(input_path):
    with open(input_path, 'rb') as in_file:
        return in_file.read()


def barecat_writer_main(target_path, shard_size, overwrite, q):
    with barecat_unif.BareCat(
            target_path, shard_size=shard_size, readonly=False, overwrite=overwrite) as writer:
        while (data := q.get()) is not None:
            input_path, content = data
            writer[input_path] = content


def extract(barecat_path, target_directory):
    with barecat_unif.BareCat(barecat_path) as reader:
        for path_in_archive in progressbar(reader, desc='Extracting files', unit=' files'):
            with reader.open(path_in_archive) as file_in_archive:
                target_path = osp.join(target_directory, path_in_archive)
                os.makedirs(osp.dirname(target_path), exist_ok=True)
                with open(target_path, 'wb') as output_file:
                    shutil.copyfileobj(file_in_archive, output_file)


def merge(source_paths, target_path, shard_size, overwrite=False):
    with barecat_unif.BareCat(
            target_path, shard_size=shard_size, readonly=False, overwrite=overwrite) as writer:
        for source_path in source_paths:
            with barecat_unif.BareCat(source_path, readonly=True) as reader:
                for path_in_archive in progressbar(
                        reader, desc=f'Merging files from {source_path}', unit=' files'):
                    size = reader.index.get_file_size(path_in_archive)
                    with reader.open(path_in_archive) as file_in_archive:
                        try:
                            writer.add(path_in_archive, fileobj=file_in_archive, size=size)
                        except ValueError:
                            print(f'Skipping duplicate file {path_in_archive}.')


def merge_symlink(source_paths, target_path, overwrite=False):
    buffer_size = 256
    with barecat_unif.Index(f'{target_path}-sqlite-index', readonly=False) as index_writer:

        total_shards = sum(len(glob.glob(f'{p}-*-of-*')) for p in source_paths)
        i_out_shard = 0

        for source_path in source_paths:
            with barecat_unif.Index(
                    f'{source_path}-sqlite-index', buffer_size=buffer_size) as reader:
                for items in chunked(progressbar_items(reader), buffer_size):
                    # try:
                    index_writer.add_items(
                        [(path, (shard + i_out_shard, offset, size, crc32))
                         for path, (shard, offset, size, crc32) in items])
                    # except ValueError:
                    #    print(f'Skipping duplicate file {path}.')

            for shard_path in sorted(glob.glob(f'{source_path}-*-of-*')):
                os.symlink(
                    os.path.relpath(shard_path, start=os.path.dirname(target_path)),
                    f'{target_path}-{i_out_shard:05d}-of-{total_shards:05d}')
                i_out_shard += 1


def write_index(dictionary, target_path):
    with barecat_unif.Index(target_path, readonly=False) as writer:
        for path, address in dictionary.items():
            writer[path] = address


def read_index(path):
    with barecat_unif.Index(path) as reader:
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


def remove(path):
    index_path = f'{path}-sqlite-index'
    shard_paths = glob.glob(f'{path}-shard-*')
    for path in [index_path] + shard_paths:
        os.remove(path)


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


# From `more-itertools` package.
def chunked(iterable, n, strict=False):
    """Break *iterable* into lists of length *n*:

        >>> list(chunked([1, 2, 3, 4, 5, 6], 3))
        [[1, 2, 3], [4, 5, 6]]

    By the default, the last yielded list will have fewer than *n* elements
    if the length of *iterable* is not divisible by *n*:

        >>> list(chunked([1, 2, 3, 4, 5, 6, 7, 8], 3))
        [[1, 2, 3], [4, 5, 6], [7, 8]]

    To use a fill-in value instead, see the :func:`grouper` recipe.

    If the length of *iterable* is not divisible by *n* and *strict* is
    ``True``, then ``ValueError`` will be raised before the last
    list is yielded.

    """
    iterator = iter(functools.partial(take, n, iter(iterable)), [])
    if strict:
        if n is None:
            raise ValueError('n must not be None when using strict mode.')

        def ret():
            for chunk in iterator:
                if len(chunk) != n:
                    raise ValueError('iterable is not divisible by n.')
                yield chunk

        return iter(ret())
    else:
        return iterator


def take(n, iterable):
    """Return first *n* items of the iterable as a list.

        >>> take(3, range(10))
        [0, 1, 2]

    If there are fewer than *n* items in the iterable, all of them are
    returned.

        >>> take(10, range(3))
        [0, 1, 2]

    """
    return list(itertools.islice(iterable, n))

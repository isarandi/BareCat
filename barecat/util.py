
import functools
import glob
import itertools
import os
import os.path as osp
import shutil
from datetime import datetime

import crc32c as crc32c_lib


def read_file(input_path, mode='r'):
    with open(input_path, mode) as f:
        return f.read()


def remove(path):
    index_path = f'{path}-sqlite-index'
    shard_paths = glob.glob(f'{path}-shard-?????')
    for path in [index_path] + shard_paths:
        os.remove(path)


def exists(path):
    index_path = f'{path}-sqlite-index'
    shard_paths = glob.glob(f'{path}-shard-?????')
    return osp.exists(index_path) or len(shard_paths) > 0


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


def copy_n_bytes(src_file, dest_file, n=None, bufsize=64 * 1024):
    if n is None:
        return shutil.copyfileobj(src_file, dest_file, bufsize)

    bytes_to_copy = n
    while bytes_to_copy > 0:
        data = src_file.read(min(bufsize, bytes_to_copy))
        if not data:
            raise ValueError('Unexpected EOF')

        dest_file.write(data)
        bytes_to_copy -= len(data)


def normalize_path(path):
    x = osp.normpath(path).removeprefix('/')
    return '' if x == '.' else x


def get_parent(path):
    if path == '':
        # root already, has no parent
        return b'\x00'

    partition = path.rpartition('/')
    return partition[0]


def partition_path(path):
    if path == '':
        # root already, has no parent
        return b'\x00', path

    parts = path.rpartition('/')
    return parts[0], parts[2]


def get_ancestors(path):
    yield ''
    for i in range(len(path)):
        if path[i] == '/':
            yield path[:i]


def reopen(file, mode):
    if file.mode == mode:
        return file
    file.close()
    return open_(file.name, mode)


def fileobj_crc32c_until_end(fileobj, bufsize=64 * 1024):
    crc32c = 0
    while chunk := fileobj.read(bufsize):
        crc32c = crc32c_lib.crc32c(chunk, crc32c)
    return crc32c


def fileobj_crc32c(fileobj, size=-1, bufsize=64 * 1024):
    if size == -1 or size is None:
        return fileobj_crc32c_until_end(fileobj, bufsize)

    crc32c = 0
    n_full_bufs, remainder = divmod(size, bufsize)

    for _ in range(n_full_bufs):
        data = fileobj.read(bufsize)
        if len(data) != bufsize:
            raise ValueError('Unexpected EOF')
        crc32c = crc32c_lib.crc32c(data, crc32c)

    if remainder:
        data = fileobj.read(remainder)
        if len(data) != remainder:
            raise ValueError('Unexpected EOF')
        crc32c = crc32c_lib.crc32c(data, crc32c)

    return crc32c


def copyfileobj_crc32c_until_end(src_file, dst_file, bufsize=64 * 1024):
    crc32c = 0
    size = 0
    while chunk := src_file.read(bufsize):
        dst_file.write(chunk)
        crc32c = crc32c_lib.crc32c(chunk, crc32c)
        size += len(chunk)
    return size, crc32c


def copyfileobj_crc32c(src_file, dst_file, size=None, bufsize=64 * 1024):
    if size is None:
        return copyfileobj_crc32c_until_end(src_file, dst_file, bufsize)

    crc32c = 0
    n_bytes_transferred = 0
    n_full_bufs, remainder = divmod(size, bufsize)

    for _ in range(n_full_bufs):
        data = src_file.read(bufsize)
        if len(data) != bufsize:
            raise ValueError('Unexpected EOF')

        crc32c = crc32c_lib.crc32c(data, crc32c)
        n_written = dst_file.write(data)
        if n_written != len(data):
            raise ValueError('Unexpected write problem')

        n_bytes_transferred += n_written

    if remainder:
        data = src_file.read(remainder)
        if len(data) != remainder:
            raise ValueError('Unexpected EOF')

        crc32c = crc32c_lib.crc32c(data, crc32c)
        n_written = dst_file.write(data)
        if n_written != len(data):
            raise ValueError('Unexpected write problem')

        n_bytes_transferred += n_written

    return n_bytes_transferred, crc32c


def copyfileobj(src_file, dst_file, size=None, bufsize=64 * 1024):
    if size is None:
        return shutil.copyfileobj(src_file, dst_file, bufsize)

    n_bytes_transferred = 0
    nreads, remainder = divmod(size, bufsize)

    for _ in range(nreads):
        data = src_file.read(bufsize)
        dst_file.write(data)
        n_bytes_transferred += len(data)

    if remainder:
        data = src_file.read(remainder)
        dst_file.write(data)
        n_bytes_transferred += len(data)

    return n_bytes_transferred


def write_zeroes(file, n, bufsize=64 * 1024):
    n_written = 0
    if n >= bufsize:
        zeroes = bytearray(bufsize)
        while n >= bufsize:
            n_written += file.write(zeroes)
            n -= bufsize
    n_written += file.write(bytearray(n))
    return n_written


def raise_if_readonly(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if self.readonly:
            raise PermissionError('This function is not allowed in readonly mode')
        return method(self, *args, **kwargs)

    return wrapper


def raise_if_append_only(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if self.append_only:
            raise PermissionError('This function is not allowed in append-only mode')
        return method(self, *args, **kwargs)

    return wrapper


def raise_if_readonly_or_append_only(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if self.readonly or self.append_only:
            raise PermissionError('This function is not allowed in append-only mode')
        return method(self, *args, **kwargs)

    return wrapper


def parse_size(size):
    if size is None:
        return None
    units = dict(K=1024, M=1024 ** 2, G=1024 ** 3, T=1024 ** 4)
    size = size.upper()

    for unit, factor in units.items():
        if unit in size:
            return int(float(size.replace(unit, "")) * factor)

    return int(size)


def open_(path, mode, *args, **kwargs):
    # This is like open() but supports an additional mode 'ax+b' which is like
    # 'x+b' in that it fails if the file already exists, and creates it if it doesn't,
    # but it also opens the file in append mode, like 'a+b'

    if sorted(mode) == sorted('ax+b'):
        fd = os.open(path, os.O_APPEND)
        return os.fdopen(fd, 'a+b', *args, **kwargs)
    return open(path, mode, *args, **kwargs)


def datetime_to_ns(dt):
    return int(dt.timestamp() * 1e9)


def ns_to_datetime(ns):
    return datetime.fromtimestamp(ns / 1e9)


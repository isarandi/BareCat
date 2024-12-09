from barecat.cli_impl import (
    archive2barecat, barecat2archive, extract, merge, merge_symlink, read_index, write_index)
from barecat.common import BarecatDirInfo, BarecatFileInfo, FileSection, Order, SHARD_SIZE_UNLIMITED
from barecat.core.barecat import Barecat
from barecat.core.index import Index
from barecat.threadsafe import get_cached_reader


def open(path, mode='r', auto_codec=False, threadsafe_reader=True):
    if mode == 'r':
        return Barecat(path, readonly=True, threadsafe=threadsafe_reader, auto_codec=auto_codec)
    elif mode == 'w+':
        return Barecat(path, readonly=False, overwrite=True, exist_ok=True, append_only=False,
                       auto_codec=auto_codec)
    elif mode == 'r+':
        return Barecat(path, readonly=False, overwrite=False, exist_ok=True, append_only=False,
                       auto_codec=auto_codec)
    elif mode == 'a+':
        return Barecat(path, readonly=False, overwrite=False, exist_ok=True, append_only=True,
                       auto_codec=auto_codec)
    elif mode == 'ax+':
        return Barecat(path, readonly=False, overwrite=False, exist_ok=False, append_only=True,
                       auto_codec=auto_codec)
    elif mode == 'x+':
        return Barecat(path, readonly=False, overwrite=False, exist_ok=False, append_only=False,
                       auto_codec=auto_codec)
    else:
        raise ValueError(f"Invalid mode: {mode}")

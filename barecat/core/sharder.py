import glob
import os
import os.path as osp
import shutil
import crc32c as crc32c_lib

from barecat.common import FileSection
from barecat.util import (
    copyfileobj, copyfileobj_crc32c, open_, raise_if_readonly, reopen, write_zeroes)


class Sharder:
    def __init__(
            self, path, shard_size_limit=None, readonly=True, append_only=False, threadsafe=False,
            allow_writing_symlinked_shard=False):

        self.path = path
        self.readonly = readonly
        self.append_only = append_only
        self.threadsafe = threadsafe
        self.allow_writing_symlinked_shard = allow_writing_symlinked_shard

        self.shard_size_limit = shard_size_limit

        if readonly:
            self.shard_mode_nonlast = 'rb'
            self.shard_mode_last_existing = 'rb'
            self.shard_mode_new = 'rb'
        elif append_only:
            self.shard_mode_nonlast = 'rb'
            self.shard_mode_last_existing = 'a+b'
            self.shard_mode_new = 'ax+b'
        else:
            self.shard_mode_nonlast = 'r+b'
            self.shard_mode_last_existing = 'r+b'
            self.shard_mode_new = 'x+b'

        self._shard_files = None
        if threadsafe:
            import multiprocessing_utils
            self.local = multiprocessing_utils.local()
        else:
            self.local = None

    # READING
    def readinto_from_address(self, shard, offset, buffer, expected_crc32c=None):
        shard_file = self.shard_files[shard]
        shard_file.seek(offset)
        num_read = shard_file.readinto(buffer)
        if expected_crc32c is not None and crc32c_lib.crc32c(buffer[:num_read]) != expected_crc32c:
            raise ValueError('CRC32C mismatch')
        return num_read

    def read_from_address(self, shard, offset, size, expected_crc32c=None):
        shard_file = self.shard_files[shard]
        shard_file.seek(offset)
        data = shard_file.read(size)
        if expected_crc32c is not None and crc32c_lib.crc32c(data) != expected_crc32c:
            raise ValueError('CRC32C mismatch')
        return data

    def open_from_address(self, shard, offset, size, mode='r'):
        return FileSection(self.shard_files[shard], offset, size, readonly=mode in ('r', 'rb'))

    # WRITING
    @raise_if_readonly
    def add_by_path(self, filesys_path, shard, offset, size, raise_if_cannot_fit=False):
        with open(filesys_path, 'rb') as in_file:
            return self.add(
                shard, offset, size, fileobj=in_file, raise_if_cannot_fit=raise_if_cannot_fit)

    @raise_if_readonly
    def reopen_current_shard(self, mode):
        return self.reopen_shard(len(self.shard_files) - 1, mode)

    @raise_if_readonly
    def reopen_shard(self, shard_number, mode):
        if mode != 'rb' and shard_number != len(self.shard_files) - 1:
            self.raise_if_append_only(
                'Cannot change mode of non-last shard in an append-only Barecat')
        self.shard_files[shard_number] = reopen(self.shard_files[shard_number], mode)
        return self.shard_files[shard_number]

    @raise_if_readonly
    def reopen_shards(self):
        for i in range(len(self.shard_files)):
            if i == len(self.shard_files) - 1:
                mode = self.shard_mode_last_existing
            else:
                mode = self.shard_mode_nonlast
            self.reopen_shard(i, mode)

    @raise_if_readonly
    def start_new_shard(self):
        self.reopen_current_shard(self.shard_mode_nonlast)
        new_shard_file = open_(
            f'{self.path}-shard-{len(self.shard_files):05d}', self.shard_mode_new)
        self.shard_files.append(new_shard_file)
        return new_shard_file

    @raise_if_readonly
    def start_new_shard_and_transfer_last_file(self, offset, size):
        self.raise_if_readonly('Cannot add to a read-only Barecat')

        old_shard_file = self.reopen_current_shard('r+b')
        new_shard_file = open_(
            f'{self.path}-shard-{len(self.shard_files):05d}', self.shard_mode_new)
        old_shard_file.seek(offset)
        copyfileobj(old_shard_file, new_shard_file, size)
        old_shard_file.truncate(offset)
        self.reopen_current_shard(self.shard_mode_nonlast)

        self.shard_files.append(new_shard_file)
        return new_shard_file

    @raise_if_readonly
    def add(
            self, shard=None, offset=None, size=None, data=None, fileobj=None,
            bufsize=shutil.COPY_BUFSIZE,
            raise_if_cannot_fit=False):
        if data is None and fileobj is None:
            raise ValueError('Either data or fileobj must be provided')
        if data is not None and fileobj is not None:
            raise ValueError('Both data and fileobj cannot be provided')
        if data is not None and size is not None and size != len(data):
            raise ValueError('Specified size does not match the length of the data')
        if shard is None and offset is not None:
            raise ValueError('Offset cannot be specified without a shard')
        if shard is not None and offset is None:
            raise ValueError('Shard cannot be specified without an offset')

        if size is None and data is not None:
            size = len(data)

        if shard is None:
            shard_file = self.shard_files[-1]
            shard = len(self.shard_files) - 1
            offset = shard_file.seek(0, os.SEEK_END)
        else:
            self.ensure_open_shards(shard)
            shard_file = self.shard_files[shard]
            shard_file.seek(offset)

        offset_real = offset
        shard_real = shard
        if size is not None:
            if size > self.shard_size_limit:
                raise ValueError(f'File is too large to fit into a shard')
            if offset + size > self.shard_size_limit:
                if raise_if_cannot_fit:
                    raise ValueError(f'File does not fit in the shard')
                shard_file = self.start_new_shard()
                offset_real = 0
                shard_real = len(self.shard_files) - 1

        if data is not None:
            shard_file.write(data)
            crc32c = crc32c_lib.crc32c(data)
            size_real = len(data)
        else:
            size_real, crc32c = copyfileobj_crc32c(fileobj, shard_file, size, bufsize)
            if size is not None and size != size_real:
                raise ValueError(f'Size mismatch! Expected {size}, got only {size_real}')

        if offset_real + size_real > self.shard_size_limit:
            if raise_if_cannot_fit:
                raise ValueError('File does not fit in the shard')
            self.start_new_shard_and_transfer_last_file(offset_real, size_real)
            offset_real = 0
            shard_real = len(self.shard_files) - 1

        return shard_real, offset_real, size_real, crc32c

    def reserve(self, size):
        if size > self.shard_size_limit:
            raise ValueError(f'File is too large to fit into a shard')

        shard_file = self.shard_files[-1]
        offset = shard_file.seek(0, os.SEEK_END)
        if offset + size > self.shard_size_limit:
            shard_file = self.start_new_shard()
            offset = 0

        shard_file.seek(offset)
        write_zeroes(shard_file, size)
        shard_file.flush()
        return len(self.shard_files) - 1, offset

    @property
    def total_physical_size_seek(self):
        return sum(self.physical_shard_end(i) for i in range(len(self.shard_files)))

    @property
    def total_physical_size_stat(self):
        return sum(osp.getsize(f.name) for f in self.shard_files)

    def physical_shard_end(self, shard_number):
        return self.shard_files[shard_number].seek(0, os.SEEK_END)

    # THREADSAFE
    @property
    def shard_files(self):
        if self.local is None:
            if self._shard_files is None:
                self._shard_files = self.open_shard_files()
            return self._shard_files
        try:
            return self.local.shard_files
        except AttributeError:
            self.local.shard_files = self.open_shard_files()
            return self.local.shard_files

    def ensure_open_shards(self, shard_id):
        num_current_shards = len(self.shard_files)
        if num_current_shards < shard_id + 1:
            for i in range(num_current_shards, shard_id + 1):
                self.shard_files.append(open_(
                    f'{self.path}-shard-{i:05d}', mode=self.shard_mode_nonlast))

    def open_shard_files(self):
        shard_paths = sorted(glob.glob(f'{self.path}-shard-?????'))
        if not self.readonly and not self.allow_writing_symlinked_shard and any(
                osp.islink(p) for p in shard_paths):
            raise ValueError(
                'Writing symlinked shards was disabled in this Barecat '
                '(allow_writing_symlinked_shard on the constructor)')

        shard_files_nonlast = [open_(p, mode=self.shard_mode_nonlast) for p in shard_paths[:-1]]
        last_shard_name = f'{self.path}-shard-{len(shard_files_nonlast):05d}'
        try:
            last_shard_file = open_(last_shard_name, mode=self.shard_mode_last_existing)
        except FileNotFoundError as e:
            if self.readonly:
                raise
            last_shard_file = open_(last_shard_name, mode=self.shard_mode_new)

        return shard_files_nonlast + [last_shard_file]

    def truncate_all_to_logical_size(self, logical_shard_ends):
        shard_files = self.shard_files
        for i in range(len(shard_files) - 1, 0, -1):
            if logical_shard_ends[i] == 0:
                shard_files[i].truncate(0)
                shard_files[i].close()
                os.remove(shard_files[i].name)
                del shard_files[i]
            else:
                break
        for i, f in enumerate(self.shard_files):
            f.truncate(logical_shard_ends[i])
        self.reopen_current_shard(self.shard_mode_last_existing)

    def close(self):
        for f in self.shard_files:
            f.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def raise_if_readonly(self, message):
        if self.readonly:
            raise ValueError(message)

    def raise_if_append_only(self, message):
        if self.append_only:
            raise ValueError(message)

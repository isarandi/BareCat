import bz2
import stat
import os
import os.path as osp
import shutil
from collections.abc import MutableMapping

import crc32c as crc32c_lib

import barecat.progbar
import barecat.util
from barecat.common import BarecatDirInfo, BarecatFileInfo
from barecat.core.index import Index, normalize_path
from barecat.core.sharder import Sharder
from barecat.defrag import BarecatDefragger
from barecat.exceptions import (
    FileExistsBarecatError, FileNotFoundBarecatError, IsADirectoryBarecatError)
from barecat.util import copyfileobj, raise_if_readonly, raise_if_readonly_or_append_only


class Barecat(MutableMapping):
    def __init__(
            self, path, shard_size_limit=None, readonly=True, overwrite=False, auto_codec=False,
            exist_ok=True, append_only=False, threadsafe=False,
            allow_writing_symlinked_shard=False):
        if threadsafe and not readonly:
            raise ValueError('Threadsafe mode is only supported for readonly Barecat.')

        if not readonly and barecat.util.exists(path):
            if not exist_ok:
                raise FileExistsError(path)
            if overwrite:
                print(f'Overwriting existing Barecat at {path}')
                barecat.util.remove(path)

        self.path = path
        self.readonly = readonly
        self.append_only = append_only
        self.auto_codec = auto_codec
        self.threadsafe = threadsafe
        self.allow_writing_symlinked_shard = allow_writing_symlinked_shard

        # Index
        self._index = None
        if threadsafe:
            import multiprocessing_utils
            self.local = multiprocessing_utils.local()
        else:
            self.local = None

        if not readonly and shard_size_limit is not None:
            self.shard_size_limit = shard_size_limit

        # Shards
        self.sharder = Sharder(
            path, shard_size_limit=self.shard_size_limit, append_only=append_only,
            readonly=readonly, threadsafe=threadsafe,
            allow_writing_symlinked_shard=allow_writing_symlinked_shard)

        self.codecs = {}
        if auto_codec:
            import barecat.codecs as bcc
            self.register_codec(['.jpg', '.jpeg'], bcc.encode_jpeg, bcc.decode_jpeg)
            self.register_codec(['.msgpack'], bcc.encode_msgpack_np, bcc.decode_msgpack_np)
            self.bz_compressor = bz2.BZ2Compressor(9)
            self.register_codec(
                ['.bz2'], self.bz_compressor.compress, bz2.decompress, nonfinal=True)

    ## Dict-like API: keys are filepaths, values are the file contents (bytes or decoded objects)
    def __getitem__(self, path):
        # Typically used in training loop
        path = normalize_path(path)
        row = self.index.fetch_one(
            "SELECT shard, offset, size, crc32c FROM files WHERE path=?", (path,))
        if row is None:
            raise KeyError(path)
        raw_data = self.sharder.read_from_address(
            row['shard'], row['offset'], row['size'], row['crc32c'])
        return self.decode(path, raw_data)


    def get(self, path, default=None):
        try:
            return self[path]
        except KeyError:
            return default

    def items(self):
        for finfo in self.index.iter_all_fileinfos():
            data = self.read(finfo)
            yield finfo.path, self.decode(finfo.path, data)

    def keys(self):
        return self.files()

    def values(self):
        for key, value in self.items():
            yield value

    def __contains__(self, path):
        return self.index.isfile(path)

    def __len__(self):
        return self.index.num_files

    def __iter__(self):
        return self.index.iter_all_filepaths()

    def __setitem__(self, path, content):
        self.add(path, data=self.encode(path, content))

    def setdefault(self, key, default = None, /):
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    def __delitem__(self, path):
        try:
            self.remove(path)
        except FileNotFoundBarecatError as e:
            raise KeyError(path)

    # Filesystem-like API
    # READING
    def open(self, item: BarecatFileInfo | str, mode='r'):
        finfo = self.index._as_fileinfo(item)
        return self.sharder.open_from_address(finfo.shard, finfo.offset, finfo.size, mode)

    def exists(self, path):
        return self.index.exists(path)

    def isfile(self, path):
        return self.index.isfile(path)

    def isdir(self, path):
        return self.index.isdir(path)

    def listdir(self, path):
        return self.index.listdir_names(path)

    def walk(self, path):
        return self.index.walk_names(path)

    def scandir(self, path):
        return self.index.iterdir_infos(path)

    def glob(self, pattern, recursive=False, include_hidden=False):
        # Equivalent to Python's glob.glob
        return self.index.glob_paths(pattern, recursive, include_hidden)

    def globfiles(self, pattern, recursive=False, include_hidden=False):
        return self.index.glob_paths(pattern, recursive, include_hidden, only_files=True)

    def iglob(self, pattern, recursive=False, include_hidden=False):
        return self.index.iterglob_paths(pattern, recursive, include_hidden)

    def iglobfiles(self, pattern, recursive=False, include_hidden=False):
        return self.index.iterglob_paths(pattern, recursive, include_hidden, only_files=True)

    def files(self):
        return self.index.iter_all_filepaths()

    def dirs(self):
        return self.index.iter_all_dirpaths()

    @property
    def num_files(self):
        return self.index.num_files

    @property
    def num_dirs(self):
        return self.index.num_dirs

    @property
    def total_size(self):
        return self.index.total_size

    def readinto(self, item: BarecatFileInfo | str, buffer, offset=0):
        # Used in fuse mount
        if isinstance(item, BarecatFileInfo):
            shard, offset_in_shard, size_in_shard, exp_crc32c = (
                item.shard, item.offset, item.size, item.crc32c)
        else:
            path = normalize_path(item)
            row = self.index.fetch_one(
                "SELECT shard, offset, size, crc32c FROM files WHERE path=?", (path,))
            if row is None:
                raise FileNotFoundBarecatError(path)
            shard, offset_in_shard, size_in_shard, exp_crc32c = row

        offset = max(0, min(offset, size_in_shard))
        size_to_read = min(len(buffer), size_in_shard - offset)

        if size_to_read != size_in_shard:
            exp_crc32c = None

        return self.sharder.readinto_from_address(
            shard, offset_in_shard + offset, buffer[:size_to_read], exp_crc32c)

    def read(self, item: BarecatFileInfo | str, offset=0, size=-1):
        finfo = self.index._as_fileinfo(item)
        with self.open(finfo, 'rb') as f:
            f.seek(offset)
            data = f.read(size)
        if offset == 0 and (size == -1 or size == finfo.size) and finfo.crc32c is not None:
            crc32c = crc32c_lib.crc32c(data)
            if crc32c != finfo.crc32c:
                raise ValueError(
                    f"CRC32C mismatch for {finfo.path}. Expected {finfo.crc32c}, got {crc32c}")
        return data

    # WRITING
    @raise_if_readonly
    def add_by_path(self, filesys_path, store_path=None, dir_exist_ok=False):
        if store_path is None:
            store_path = filesys_path

        statresult = os.stat(filesys_path)
        if stat.S_ISDIR(statresult.st_mode):
            finfo = BarecatDirInfo(path=store_path)
            finfo.fill_from_statresult(statresult)
            self.index.add_dir(finfo, exist_ok=dir_exist_ok)
            return

        finfo = BarecatFileInfo(path=store_path)
        finfo.fill_from_statresult(statresult)
        with open(filesys_path, 'rb') as in_file:
            self.add(finfo, fileobj=in_file)

    @raise_if_readonly
    def add(
            self, finfo: BarecatFileInfo | BarecatDirInfo, *, data=None, fileobj=None,
            bufsize=shutil.COPY_BUFSIZE, dir_exist_ok=False):
        if isinstance(finfo, BarecatDirInfo):
            self.index.add_dir(finfo, exist_ok=dir_exist_ok)
            return

        finfo.shard, finfo.offset, finfo.size, finfo.crc32c = self.sharder.add(
            size=finfo.size, data=data, fileobj=fileobj, bufsize=bufsize)

        try:
            self.index.add_file(finfo)
        except FileExistsBarecatError:
            # If the file already exists, we need to truncate the shard file back
            shard_file = self.sharder.shard_files[finfo.shard]
            with open(shard_file.name, 'r+b') as f:
                f.truncate(finfo.offset)
            raise

    # DELETION
    @raise_if_readonly_or_append_only
    def remove(self, item: BarecatFileInfo | str):
        try:
            finfo = self.index._as_fileinfo(item)
        except FileNotFoundBarecatError:
            if self.isdir(item):
                raise IsADirectoryBarecatError(item)
            raise

        # If this is the last file in the shard, we can just truncate the shard file
        end = finfo.offset + finfo.size
        if (end >= self.sharder.shard_files[finfo.shard].tell() and
                end >= osp.getsize(self.sharder.shard_files[finfo.shard].name) and
                end == self.index.logical_shard_end(finfo.shard)):
            with open(self.sharder.shard_files[finfo.shard].name, 'r+b') as f:
                f.truncate(finfo.offset)
        self.index.remove_file(finfo)

    @raise_if_readonly_or_append_only
    def rmdir(self, item: BarecatDirInfo | str):
        self.index.remove_empty_dir(item)

    @raise_if_readonly_or_append_only
    def remove_recursively(self, dirpath):
        self.index.remove_recursively(dirpath)

    # RENAMING
    @raise_if_readonly_or_append_only
    def rename(self, old_path, new_path):
        self.index.rename(old_path, new_path)

    @property
    def total_physical_size_seek(self):
        return self.sharder.total_physical_size_seek

    @property
    def total_physical_size_stat(self):
        return self.sharder.total_physical_size_stat

    @property
    def total_logical_size(self):
        return self.index.total_size

    # MERGING
    @raise_if_readonly
    def merge_from_other_barecat(self, source_path, ignore_duplicates=False):
        out_shard_number = len(self.sharder.shard_files) - 1
        out_shard = self.sharder.shard_files[-1]
        out_shard_offset = out_shard.tell()

        source_index_path = f'{source_path}-sqlite-index'
        self.index.cursor.execute(f"ATTACH DATABASE 'file:{source_index_path}?mode=ro' AS sourcedb")

        if self.shard_size_limit is not None:
            in_max_size = self.index.fetch_one(
                "SELECT MAX(size) FROM sourcedb.files")[0]
            if in_max_size > self.shard_size_limit:
                self.index.cursor.execute("DETACH DATABASE sourcedb")
                raise ValueError('Files in the source archive are larger than the shard size')

        # Upsert all directories
        self.index.cursor.execute("""
            INSERT INTO dirs (
                path, num_subdirs, num_files, size_tree, num_files_tree,
                mode, uid, gid, mtime_ns)
            SELECT path, num_subdirs, num_files, size_tree, num_files_tree,
                mode, uid, gid, mtime_ns
            FROM sourcedb.dirs WHERE true
            ON CONFLICT (dirs.path) DO UPDATE SET
                num_subdirs = num_subdirs + excluded.num_subdirs,
                num_files = num_files + excluded.num_files,
                size_tree = size_tree + excluded.size_tree,
                num_files_tree = num_files_tree + excluded.num_files_tree,
                mode = COALESCE(
                    dirs.mode | excluded.mode,
                    COALESCE(dirs.mode, 0) | excluded.mode,
                    dirs.mode | COALESCE(excluded.mode, 0)),
                uid = COALESCE(excluded.uid, dirs.uid),
                gid = COALESCE(excluded.gid, dirs.gid),
                mtime_ns = COALESCE(
                    MAX(dirs.mtime_ns, excluded.mtime_ns),
                    MAX(COALESCE(dirs.mtime_ns, 0), excluded.mtime_ns),
                    MAX(dirs.mtime_ns, COALESCE(excluded.mtime_ns, 0)))
            """)

        in_shard_number = 0
        in_shard_path = f'{source_path}-shard-{in_shard_number:05d}'
        in_shard = open(in_shard_path, 'rb')
        in_shard_offset = 0
        in_shard_end = self.index.fetch_one("""
            SELECT MAX(offset + size) FROM sourcedb.files WHERE shard=?
            """, (in_shard_number,))[0]

        while True:
            if self.shard_size_limit is not None:
                out_shard_space_left = self.shard_size_limit - out_shard_offset
                # check how much of the in_shard we can put in the current out_shard
                fetched = self.index.fetch_one("""
                    SELECT MAX(offset + size) - :in_shard_offset AS max_offset_size_adjusted
                    FROM sourcedb.files
                    WHERE offset + size <= :in_shard_offset + :out_shard_space_left
                    AND shard = :in_shard_number""", dict(
                    in_shard_offset=in_shard_offset,
                    out_shard_space_left=out_shard_space_left,
                    in_shard_number=in_shard_number
                ))
                if fetched is None:
                    # No file of the current in_shard fits in the current out_shard, must start a
                    # new one
                    self.sharder.start_new_shard()
                    out_shard_number += 1
                    out_shard_offset = 0
                    continue

                max_copiable_amount = fetched[0]
            else:
                max_copiable_amount = None

            # now we need to update the index, but we need to update the offset and shard
            # of the files that we copied
            maybe_ignore = 'OR IGNORE' if ignore_duplicates else ''
            self.index.cursor.execute(f"""
                INSERT {maybe_ignore} INTO files (
                    path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns)
                SELECT path, :out_shard_number, offset - :in_shard_offset + :out_shard_offset,
                    size, crc32c, mode, uid, gid, mtime_ns 
                FROM sourcedb.files
                WHERE offset >= :in_shard_offset AND shard = :in_shard_number""" + ("""
                AND offset + size <= :in_shard_offset + :max_copiable_amount
                """ if max_copiable_amount is not None else ""), dict(
                out_shard_number=out_shard_number,
                in_shard_offset=in_shard_offset,
                out_shard_offset=out_shard_offset,
                in_shard_number=in_shard_number,
                max_copiable_amount=max_copiable_amount
            ))
            copyfileobj(in_shard, out_shard, max_copiable_amount)
            out_shard_offset = out_shard.tell()
            in_shard_offset = in_shard.tell()
            if in_shard_offset == in_shard_end:
                # we finished this in_shard, move to the next one
                in_shard.close()
                in_shard_number += 1
                in_shard_path = f'{source_path}-shard-{in_shard_number:05d}'
                try:
                    in_shard = open(in_shard_path, 'rb')
                except FileNotFoundError:
                    # done with all in_shards of this source
                    break
                in_shard_offset = 0
                in_shard_end = self.index.fetch_one("""
                    SELECT MAX(offset + size) FROM sourcedb.files WHERE shard=?
                    """, (in_shard_number,))[0]

        in_shard.close()
        self.index.conn.commit()
        self.index.cursor.execute("DETACH DATABASE sourcedb")

    @property
    def shard_size_limit(self):
        return self.index.shard_size_limit

    @shard_size_limit.setter
    def shard_size_limit(self, value):
        self.index.shard_size_limit = value

    def logical_shard_end(self, shard_number):
        return self.index.logical_shard_end(shard_number)

    def physical_shard_end(self, shard_number):
        return self.sharder.physical_shard_end(shard_number)

    def raise_if_readonly(self, message):
        if self.readonly:
            raise ValueError(message)

    def raise_if_append_only(self, message):
        if self.append_only:
            raise ValueError(message)

    # THREADSAFE
    @property
    def index(self):
        if not self.local:
            if self._index is None:
                self._index = Index(f'{self.path}-sqlite-index', readonly=self.readonly)
            return self._index
        try:
            return self.local.index
        except AttributeError:
            self.local.index = Index(f'{self.path}-sqlite-index', readonly=self.readonly)
            return self.local.index

    # CONSISTENCY CHECKS
    def check_crc32c(self, item: BarecatFileInfo | str):
        finfo = self.index._as_fileinfo(item)
        with self.open(finfo, 'rb') as f:
            crc32c = barecat.util.fileobj_crc32c_until_end(f)
        if finfo.crc32c is not None and crc32c != finfo.crc32c:
            print(f"CRC32C mismatch for {finfo.path}. Expected {finfo.crc32c}, got {crc32c}")
            return False
        return True

    def verify_integrity(self, quick=False):
        is_good = True
        if quick:
            try:
                if not self.check_crc32c(self.index.get_last_file()):
                    is_good = False
            except LookupError:
                pass  # no files
        else:
            n_printed = 0
            for fi in barecat.progbar.progressbar(
                    self.index.iter_all_fileinfos(), total=self.num_files):
                if not self.check_crc32c(fi):
                    is_good = False
                    if n_printed >= 10:
                        print('...')
                        break
                    n_printed += 1

        if not self.index.verify_integrity():
            is_good = False
        return is_good

    # CODECS
    def register_codec(self, exts, encoder, decoder, nonfinal=False):
        for ext in exts:
            self.codecs[ext] = (encoder, decoder, nonfinal)

    def encode(self, path, data):
        noext, ext = osp.splitext(path)
        try:
            encoder, decoder, nonfinal = self.codecs[ext.lower()]
        except KeyError:
            return data
        else:
            if nonfinal:
                data = self.encode(noext, data)
            return encoder(data)

    def decode(self, path, data):
        noext, ext = osp.splitext(path)
        try:
            encoder, decoder, nonfinal = self.codecs[ext.lower()]
            data = decoder(data)
            if nonfinal:
                data = self.decode(noext, data)
            return data
        except KeyError:
            return data

    # PICKLING
    def __reduce__(self):
        if not self.readonly:
            raise ValueError('Cannot pickle a non-readonly Barecat')
        return self.__class__, (
            self.path, None, True, False, self.auto_codec, True, False, self.threadsafe)

    def truncate_all_to_logical_size(self):
        logical_shard_ends = [
            self.index.logical_shard_end(i) for i in range(len(self.sharder.shard_files))]
        self.sharder.truncate_all_to_logical_size(logical_shard_ends)

    # DEFRAG
    def defrag(self, quick=False):
        defragger = BarecatDefragger(self)
        if quick:
            return defragger.defrag_quick()
        else:
            return defragger.defrag()

    def close(self):
        self.index.close()
        self.sharder.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

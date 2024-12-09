import bz2
import glob
import io
import os
import os.path as osp
import shutil
import crc32c as crc32c_lib
from typing import List

import barecat.util
from barecat.common import BarecatDirInfo, BarecatFileInfo, FileSection
from barecat.defrag import BarecatDefragger
from barecat.exceptions import (
    FileExistsBarecatError, FileNotFoundBarecatError, IsADirectoryBarecatError)
from barecat.core.index import Index, normalize_path
from barecat.util import (
    open_, raise_if_readonly, raise_if_readonly_or_append_only, reopen,
    copyfileobj_crc32c, copyfileobj)


class Barecat:
    def __init__(
            self, path, shard_size_limit=None, readonly=True, overwrite=False, auto_codec=False,
            exist_ok=True, append_only=False, threadsafe=False,
            allow_writing_symlinked_shard=False):
        if threadsafe and not readonly:
            raise ValueError('Threadsafe mode is only supported for readonly Barecat.')

        path = path.removesuffix('-sqlite-index')

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
        self.shard_paths = sorted(glob.glob(f'{self.path}-shard-?????'))
        if not self.readonly and not allow_writing_symlinked_shard and any(
                osp.islink(p) for p in self.shard_paths):
            raise ValueError(
                'Writing symlinked shards was disabled in this Barecat '
                '(allow_writing_symlinked_shard on the constructor)')

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

        if threadsafe:
            self.shard_files = self._shard_files_local
        else:
            self.shard_files: List[io.BufferedRandom] = self.open_shard_files()

        self.codecs = {}
        if auto_codec:
            import barecat.codecs as bcc
            self.register_codec(['.jpg', '.jpeg'], bcc.encode_jpeg, bcc.decode_jpeg)
            self.register_codec(['.msgpack'], bcc.encode_msgpack_np, bcc.decode_msgpack_np)
            self.bz_compressor = bz2.BZ2Compressor(9)
            self.register_codec(
                ['.bz2'], self.bz_compressor.compress, bz2.decompress, nonfinal=True)

    # READING
    ## Dict-like API: keys are filepaths, values are the file contents (bytes)
    def __getitem__(self, path):
        # Typically used in training loop
        path = normalize_path(path)
        cursor = self.index.cursor
        cursor.execute("SELECT shard, offset, size FROM files WHERE path=?", (path,))
        row = cursor.fetchone()
        if row is None:
            raise KeyError(path)
        shard_file = self.shard_files[row['shard']]
        shard_file.seek(row['offset'])
        return shard_file.read(row['size'])

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

    # Filesystem-like interface
    def open(self, item: BarecatFileInfo | str, mode='r'):
        # if mode not in ('r', 'rb'):
        #    raise NotImplementedError('Only read mode is supported in open() for now')
        finfo = self.index._as_fileinfo(item)
        return FileSection(
            self.shard_files[finfo.shard], finfo.offset, finfo.size, readonly=mode in ('r', 'rb'))

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

    def glob(self, pattern):
        return self.index.glob_paths(pattern)

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
            shard, offset_in_shard, size_in_shard = item.shard, item.offset, item.size
        else:
            path = normalize_path(item)
            cursor = self.index.cursor
            cursor.execute("SELECT shard, offset, size FROM files WHERE path=?", (path,))
            row = cursor.fetchone()
            if row is None:
                raise FileNotFoundBarecatError(path)
            shard, offset_in_shard, size_in_shard = row

        shard_file = self.shard_files[shard]
        offset = max(0, min(offset, size_in_shard))
        size_to_read = min(len(buffer), size_in_shard - offset)
        shard_file.seek(offset_in_shard + offset)
        return shard_file.readinto(buffer[:size_to_read])

    def read(self, item: BarecatFileInfo | str, offset=0, size=-1, check_crc32c=True):
        finfo = self.index._as_fileinfo(item)
        with self.open(finfo, 'rb') as f:
            f.seek(offset)
            data = f.read(size)
        if check_crc32c and offset == 0 and size == -1 and finfo.crc32c is not None:
            crc32c = crc32c_lib.crc32c(data)
            if crc32c != finfo.crc32c:
                raise ValueError(
                    f"CRC32C mismatch for {finfo.path}. Expected {finfo.crc32c}, got {crc32c}")
        return data

    # WRITING
    @raise_if_readonly
    def __setitem__(self, path, content):
        self.add(path, data=self.encode(path, content))

    @raise_if_readonly
    def add_by_path(self, filesys_path, store_path=None):
        if store_path is None:
            store_path = filesys_path
        finfo = BarecatFileInfo(path=store_path)
        finfo.fill_from_statresult(os.stat(filesys_path))
        with open(filesys_path, 'rb') as in_file:
            self.add(finfo, fileobj=in_file)

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
            self, finfo: BarecatFileInfo | BarecatDirInfo, *, data=None, fileobj=None,
            bufsize=shutil.COPY_BUFSIZE, dir_exist_ok=False):
        if isinstance(finfo, BarecatDirInfo):
            self.index.add_dir(finfo, exist_ok=dir_exist_ok)
            return

        if data is None and fileobj is None:
            raise ValueError('Either data or fileobj must be provided')
        if data is not None and fileobj is not None:
            raise ValueError('Both data and fileobj cannot be provided')
        if data is not None and finfo.size is not None and finfo.size != len(data):
            raise ValueError('Size does not match the length of the data')

        if finfo.size is None and data is not None:
            finfo.size = len(data)

        shard_file = self.shard_files[-1]
        shard_file.seek(0, os.SEEK_END)

        if finfo.size is not None:
            if finfo.size > self.shard_size_limit:
                raise ValueError(f'File "{finfo.path}" is too large to fit into a shard')
            if shard_file.tell() + finfo.size > self.shard_size_limit:
                shard_file = self.start_new_shard()

        offset = shard_file.tell()

        if data is not None:
            shard_file.write(data)
            if finfo.crc32c is None:
                finfo.crc32c = crc32c_lib.crc32c(data)
        else:
            if finfo.crc32c is None:
                size_real, finfo.crc32c = copyfileobj_crc32c(fileobj, shard_file, finfo.size, bufsize)
            else:
                size_real = copyfileobj(fileobj, shard_file, finfo.size, bufsize)

            if finfo.size is not None and finfo.size != size_real:
                raise ValueError('Size mismatch')
            finfo.size = size_real

        if offset + finfo.size > self.shard_size_limit:
            shard_file = self.start_new_shard_and_transfer_last_file(offset, finfo.size)
            offset = 0

        finfo.shard = len(self.shard_files) - 1
        finfo.offset = offset

        try:
            self.index.add_file(finfo)
        except FileExistsBarecatError:
            with open(shard_file.name, 'r+b') as f:
                f.truncate(offset)
            raise
        finally:
            # There was an exception while writing the shard, so the shard may contain data that is
            # not accounted for in the index. So we truncate the shard file back.
            # if path not in self.index:
            #    shard_file.truncate(offset)
            pass

    # def truncate(self, item: BarecatFileInfo | str, new_size):
    #     self.raise_if_readonly('Cannot write to a read-only Barecat')
    #     self.raise_if_append_only('Cannot write to an append-only Barecat')
    #
    #     finfo = self.index._as_fileinfo(item)
    #     try:
    #         self.index.truncate(finfo, new_size)
    #         new_offset = finfo.offset  # old offset is kept
    #     except NotEnoughSpaceBarecatError:

    # DELETION
    def __delitem__(self, path):
        try:
            self.remove(path)
        except FileNotFoundBarecatError as e:
            raise KeyError(path)

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
        if (end >= self.shard_files[finfo.shard].tell() and
                end >= osp.getsize(self.shard_files[finfo.shard].name) and
                end == self.index.logical_shard_end(finfo.shard)):
            with open(self.shard_files[finfo.shard].name, 'r+b') as f:
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
        return sum(self.physical_shard_end(i) for i in range(len(self.shard_files)))

    @property
    def total_physical_size_stat(self):
        return sum(osp.getsize(f.name) for f in self.shard_files)

    @property
    def total_logical_size(self):
        return self.index.total_size

    # MERGING
    @raise_if_readonly
    def merge_from_other_barecat(self, source_path, ignore_duplicates=False):
        out_shard_number = len(self.shard_files) - 1
        out_shard = self.shard_files[-1]
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
                    self.start_new_shard()
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
        return self.shard_files[shard_number].seek(0, os.SEEK_END)

    def raise_if_readonly(self, message):
        if self.readonly:
            raise ValueError(message)

    def raise_if_append_only(self, message):
        if self.append_only:
            raise ValueError(message)

    # THREADSAFE
    @property
    def index(self):
        if self.local is None:
            if self._index is None:
                self._index = Index(f'{self.path}-sqlite-index', readonly=self.readonly)
            return self._index
        try:
            return self.local.index
        except AttributeError:
            self.local.index = Index(f'{self.path}-sqlite-index', readonly=self.readonly)
            return self.local.index

    @property
    def _shard_files_local(self):
        if not self.local:
            raise ValueError('Threadsafe mode is not enabled')
        try:
            return self.local.shard_files
        except AttributeError:
            self.local.shard_files = self.open_shard_files()
            return self.local.shard_files

    def open_shard_files(self):
        shard_files_nonlast = [open_(p, mode=self.shard_mode_nonlast) for p in
                               self.shard_paths[:-1]]
        last_shard_name = f'{self.path}-shard-{len(shard_files_nonlast):05d}'
        try:
            last_shard_file = open_(last_shard_name, mode=self.shard_mode_last_existing)
        except FileNotFoundError as e:
            if self.readonly:
                raise
            last_shard_file = open_(last_shard_name, mode=self.shard_mode_new)

        if not self.readonly:
            # repair last shard if it was not closed properly
            # this is logically consistent with append-only as well, since no existing file
            # is removed, just the invalid space is truncated
            # readonly mode is strict though, so we don't repair in that case
            last_shard_size_on_disk = osp.getsize(last_shard_name)
            last_shard_size_in_index = self.logical_shard_end(len(shard_files_nonlast))
            if last_shard_size_on_disk != last_shard_size_in_index:
                last_shard_file = reopen(last_shard_file, 'r+b')
                last_shard_file.truncate(last_shard_size_in_index)
                last_shard_file = reopen(last_shard_file, self.shard_mode_last_existing)

        return shard_files_nonlast + [last_shard_file]

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
            for fi in self.index.iter_all_fileinfos():
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
        for i in range(len(self.shard_files) - 1, 0, -1):
            if self.index.logical_shard_end(i) == 0:
                self.shard_files[i].truncate(0)
                self.shard_files[i].close()
                os.remove(self.shard_files[i].name)
                del self.shard_files[i]
            else:
                break

        for i, f in enumerate(self.shard_files):
            f.truncate(self.index.logical_shard_end(i))

        self.reopen_current_shard(self.shard_mode_last_existing)

    # DEFRAG
    def defrag(self, quick=False):
        defragger = BarecatDefragger(self)
        if quick:
            return defragger.defrag_quick()
        else:
            return defragger.defrag()

    def close(self):
        self.index.close()
        for f in self.shard_files:
            f.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()



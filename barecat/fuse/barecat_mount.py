import errno
import itertools
import os
import os.path as osp
import crc32c as crc32c_lib
from collections import deque
from datetime import datetime
from stat import S_IFDIR, S_IFREG
from typing import NamedTuple

from barecat.core import barecat as barecat_
from barecat.exceptions import (
    DirectoryNotEmptyBarecatError, FileExistsBarecatError, FileNotFoundBarecatError)
from barecat.fuse import (
    FuseDirEntry, FuseError, FuseFileInfo, FuseReadDirBufferFull, FuseReadDirFlags, PyFuse, Stat,
    StatPtr)
from barecat.common import BarecatDirInfo, BarecatFileInfo
from barecat.util import fileobj_crc32c


class BarecatFuse(PyFuse):
    def __init__(self, bc: barecat_.Barecat, enable_defrag: bool = False):
        super().__init__()
        self.bc = bc
        self.enable_defrag = enable_defrag and not self.bc.readonly
        self.st_blksize = 4096
        self.pending_finfo: BarecatFileInfo = None
        self.pending_fh = None
        self.readdir_generators = {}
        self.readdir_raw_cursors = {}  # str->CursorState
        self.last_fh = 0

        # For defrag heuristics
        self.del_size_since_last_defrag = 0
        self.total_size = self.bc.total_size
        self.total_size_max = self.total_size

    def mount(self, mountpoint, readonly=False, single_threaded=True, foreground=True):
        fuse_args = ['barecat-mount', mountpoint]
        if foreground:
            fuse_args += ['-f']
        if single_threaded:
            fuse_args += ['-s']
        if readonly:
            fuse_args += ['-o', 'ro']
        self.main(fuse_args)

    def getattr(self, path: str, fi: FuseFileInfo):
        try:
            return file_info_to_stat(self.bc.index.lookup_file(path))
        except FileNotFoundBarecatError:
            pass
        try:
            return dir_info_to_stat(self.bc.index.lookup_dir(path))
        except FileNotFoundBarecatError:
            pass
        if self.is_pending(path, fi):
            return file_info_to_stat(self.pending_finfo)
        raise FuseError(errno.ENOENT)

    def getattr_raw(self, path: str, stat_out: StatPtr, fi: FuseFileInfo):
        cursor = self.bc.index.cursor

        # Try as file
        cursor.execute(
            """SELECT size, mode, uid, gid, mtime_ns FROM files WHERE path=?""",
            (path[1:],))
        row = cursor.fetchone()
        if row is not None:
            size, mode, uid, gid, mtime_ns = row
            stat_out.st_mode = (0o644 if mode is None else mode) | S_IFREG
            stat_out.st_size = size
            stat_out.st_blocks = (size + 511) // 512
            stat_out.st_blksize = self.st_blksize
            if uid:
                stat_out.st_uid = uid
            if gid:
                stat_out.st_gid = gid
            if mtime_ns:
                stat_out.mtime_ns = mtime_ns
                stat_out.ctime_ns = mtime_ns
                stat_out.atime_ns = mtime_ns
            stat_out.st_nlink = 1
            return 0

        # Try as directory
        cursor.execute(
            """SELECT num_subdirs, mode, uid, gid, mtime_ns FROM dirs WHERE path=?""",
            (path[1:],))
        row = cursor.fetchone()
        if row is not None:
            num_subdirs, mode, uid, gid, mtime_ns = row
            stat_out.st_mode = (0o755 if mode is None else mode) | S_IFDIR
            stat_out.st_size = 4096
            stat_out.st_blocks = 0
            stat_out.st_blksize = self.st_blksize
            if uid:
                stat_out.st_uid = uid
            if gid:
                stat_out.st_gid = gid
            if mtime_ns:
                stat_out.mtime_ns = mtime_ns
                stat_out.ctime_ns = mtime_ns
                stat_out.atime_ns = mtime_ns
            stat_out.st_nlink = 2 + num_subdirs
            return 0

        # Check if it's the pending one
        if self.is_pending(path, fi):
            pf = self.pending_finfo
            stat_out.st_mode = pf.mode | S_IFREG
            stat_out.st_size = pf.size
            stat_out.st_blocks = (pf.size + 511) // 512
            stat_out.st_blksize = self.st_blksize
            stat_out.st_uid = pf.uid
            stat_out.st_gid = pf.gid
            stat_out.mtime_ns = pf.mtime_ns
            stat_out.ctime_ns = pf.mtime_ns
            stat_out.atime_ns = pf.mtime_ns
            stat_out.st_nlink = 1
            return 0

        # Not found
        return -errno.ENOENT

    def _next_fh(self):
        self.last_fh += 1
        return self.last_fh

    def opendir(self, path: str, fi: FuseFileInfo):
        if self.bc.isdir(path):
            fi.fh = self._next_fh()
            fi.cache_readdir = True
            return
        if self.bc.isfile(path):
            raise FuseError(errno.ENOTDIR)
        raise FuseError(errno.ENOENT)

    def releasedir(self, path: str, fi: FuseFileInfo):
        try:
            del self.readdir_generators[fi.fh]
        except KeyError:
            pass

        try:
            cursorstate = self.readdir_raw_cursors.pop(fi.fh)
            cursorstate.close()
        except KeyError:
            pass

    def readdir(self, path: str, offset, fi: FuseFileInfo, flags: FuseReadDirFlags):
        generator = self.readdir_generators.get(fi.fh)
        if generator is not None:
            yield from self.resumable_generate(generator, offset, fi)
        elif offset == 0:
            dinfo = self.bc.index.lookup_dir(path)
            has_many_entries = dinfo.num_files + dinfo.num_subdirs > 256
            if has_many_entries:
                generator = self.generate_direntries(
                    path, in_one_go=False, fill_attr=flags & FuseReadDirFlags.PLUS)
                self.readdir_generators[fi.fh] = generator
                yield from self.resumable_generate(generator, offset, fi)
            else:
                yield from self.generate_direntries(path, in_one_go=True, fill_attr=False)
        else:
            raise FuseError(errno.EINVAL)

    def resumable_generate(self, generator, offset, fi):
        was_empty = True
        for entry in generator:
            was_empty = False
            offset += 1
            entry.offset = offset
            try:
                yield entry
            except FuseReadDirBufferFull:
                self.readdir_generators[fi.fh] = itertools.chain([entry], generator)
                raise
        if was_empty:
            del self.readdir_generators[fi.fh]

    def generate_direntries(self, path, in_one_go=False, fill_attr=False):
        yield FuseDirEntry('.')
        yield FuseDirEntry('..')
        if in_one_go:
            infos = self.bc.index.listdir_infos(path)
        else:
            infos = self.bc.index.iterdir_infos(path)
        for info in infos:
            stat = file_or_dir_info_to_stat(info) if fill_attr else None
            yield FuseDirEntry(name=osp.basename(info.path), stat=stat)

        if self.pending_finfo is not None and osp.dirname(self.pending_finfo.path) == path:
            stat = file_info_to_stat(self.pending_finfo) if fill_attr else None
            yield FuseDirEntry(name=osp.basename(self.pending_finfo.path), stat=stat)

    def readdir_raw(
            self, path: str, filler, statbuf: StatPtr, offset: int, fi: FuseFileInfo,
            flags: FuseReadDirFlags):
        if cursorstate := self.readdir_raw_cursors.get(fi.fh):
            # This is a running readdir that we are resuming now
            return self.resume_readdir_raw(cursorstate, filler, statbuf, offset)

        cursor = self.bc.index.cursor
        cursor.execute("SELECT num_subdirs, num_files from dirs WHERE path=?1", (path[1:],))
        row = cursor.fetchone()
        if row is None:
            return -errno.ENOENT

        has_many_entries = row[0] + row[1] > 256
        if has_many_entries:
            cursor = self.bc.index.conn.cursor()
            cursor.row_factory = None
            cursorstate = BufferedCursor(cursor, fetchsize=512)
            self.readdir_raw_cursors[fi.fh] = cursorstate
            cursor.execute("""
                SELECT path, mode, num_subdirs as size, mtime_ns, uid, gid, 0 as isfile 
                FROM dirs WHERE parent=?1
                UNION ALL
                SELECT path, mode, size, mtime_ns, uid, gid, 1 as isfile
                FROM files WHERE parent=?1
                """, (path[1:],))
            cursorstate.append_left(ReaddirEntry('.', None, None, None, None, None, 0))
            cursorstate.append_left(ReaddirEntry('..', None, None, None, None, None, 0))
            return self.resume_readdir_raw(cursorstate, filler, statbuf, offset)

        # This is a small directory, we can read it all at once
        filler(".", False, 0)
        filler("..", False, 0)
        cursor.execute("""
            SELECT path FROM dirs WHERE parent=?1
            UNION ALL
            SELECT path FROM files WHERE parent=?1""", (path[1:],))
        for row in cursor:
            filler(osp.basename(row[0]), False, 0)
        return 0

    def resume_readdir_raw(self, cursorstate, filler, statbuf: StatPtr, offset):
        while e := cursorstate.pop_right():
            e = ReaddirEntry(*e)
            if e.isfile:
                statbuf.st_mode = (0o644 if e.mode is None else e.mode) | S_IFREG
                statbuf.st_size = e.size
                statbuf.st_blocks = (e.size + 511) // 512
                statbuf.st_blksize = self.st_blksize
                if e.uid:
                    statbuf.st_uid = e.uid
                if e.gid:
                    statbuf.st_gid = e.gid
                if e.mtime_ns:
                    statbuf.mtime_ns = e.mtime_ns
                    statbuf.ctime_ns = e.mtime_ns
                    statbuf.atime_ns = e.mtime_ns
                statbuf.st_nlink = 1
                ret = filler(osp.basename(e.name), True, offset + 1)
            else:
                statbuf.st_mode = (0o755 if e.mode is None else e.mode) | S_IFDIR
                statbuf.st_size = 4096
                statbuf.st_blocks = 0
                statbuf.st_blksize = self.st_blksize
                if e.uid:
                    statbuf.st_uid = e.uid
                if e.gid:
                    statbuf.st_gid = e.gid
                if e.mtime_ns:
                    statbuf.mtime_ns = e.mtime_ns
                    statbuf.ctime_ns = e.mtime_ns
                    statbuf.atime_ns = e.mtime_ns
                if e.size is not None:
                    statbuf.st_nlink = 2 + e.size
                else:
                    statbuf.st_nlink = 2
                ret = filler(osp.basename(e.name), True, offset + 1)
            if ret != 0:
                cursorstate.append_right(e)
                return 0
            offset += 1
        return 0

    def is_pending(self, path, fi=None):
        return ((fi is not None and fi.fh == self.pending_fh) or
                (self.pending_finfo is not None and path[1:] == self.pending_finfo.path))

    def read(self, path: str, buf: memoryview, offset: int, fi: FuseFileInfo):
        item = self.pending_finfo if self.is_pending(path, fi) else path
        try:
            return self.bc.readinto(item, buf, offset)
        except FileNotFoundBarecatError:
            raise FuseError(errno.ENOENT)

    def create_raw(self, path: str, mode, fi: FuseFileInfo):
        if self.pending_finfo is not None:
            return -errno.EBUSY
        if self.bc.exists(path):
            return -errno.EEXIST
        if not self.bc.isdir(osp.dirname(path)):
            return -errno.ENOENT

        shard_file = self.bc.sharder.shard_files[-1]
        offset = shard_file.seek(0, os.SEEK_END)
        context = PyFuse.get_context()
        self.pending_finfo = BarecatFileInfo(
            path=path[1:], mode=mode, shard=len(self.bc.sharder.shard_files) - 1, offset=offset, size=0,
            crc32c=0, mtime_ns=nanosec_now(), uid=context.uid, gid=context.gid)
        self.pending_fh = self._next_fh()
        fi.fh = self.pending_fh
        return 0

    def mkdir(self, path, mode):
        context = PyFuse.get_context()
        dinfo = BarecatDirInfo(
            path=path[1:], mode=mode, mtime_ns=nanosec_now(),
            num_files=0, num_subdirs=0, size_tree=0, num_files_tree=0,
            uid=context.uid, gid=context.gid)
        try:
            self.bc.index.add_dir(dinfo)
        except FileExistsBarecatError:
            raise FuseError(errno.EEXIST)

    def unlink(self, path):
        finfo = self.bc.index.lookup_file(path)
        self.bc.remove(finfo)

        # Heuristics for defragging
        if self.enable_defrag:
            self.del_size_since_last_defrag += finfo.size
            self.total_size -= finfo.size

            if self.del_size_since_last_defrag < 100 * 1024 * 1024:
                # Quick return if only little has been deleted since the last defrag
                return

            del_frac_current = (
                    self.del_size_since_last_defrag /
                    (self.total_size + self.del_size_since_last_defrag))
            del_frac_max = (
                    self.del_size_since_last_defrag /
                    (self.total_size_max + self.del_size_since_last_defrag))
            growth_factor = self.total_size / self.total_size_max

            # The idea is that we don't want to defrag too often if someone is deleting huge
            # portions
            # of the data. What we really want to avoid is growing beyond the initial size of the
            # logical data or beyond the max size over the course of this session.
            # So if we deleted 10% and we are close to the max we've been in this session, we should
            # defrag. But even if we are not close to session max, if we drastically deleted 80%
            # of the
            # data, we should defrag.
            huge_drop = del_frac_current >= 0.3 or del_frac_max >= 1.0 / 3.0
            near_max = del_frac_current >= 0.1 and growth_factor > 0.8
            deleted_some = self.del_size_since_last_defrag > 100 * 1024 * 1024

            if (huge_drop or near_max) and deleted_some:
                freed_space = self.bc.defrag(quick=True)
                print(f'Defragged, freed {freed_space} bytes')
                self.del_size_since_last_defrag = 0

    def rmdir(self, path):
        try:
            self.bc.index.remove_empty_dir(path)
        except DirectoryNotEmptyBarecatError:
            raise FuseError(errno.ENOTEMPTY)

    def write(self, path: str, buf: memoryview, offset: int, fi: FuseFileInfo):

        if self.is_pending(path, fi):
            shard_file = self.bc.sharder.shard_files[-1]
            shard_file.seek(self.pending_finfo.offset + offset)
            n_written = shard_file.write(buf)

            # Update pending metadata
            # Check if we are still writing sequentially so CRC32C can be calculated
            if self.pending_finfo.crc32c is not None and offset == self.pending_finfo.size:
                self.pending_finfo.crc32c = crc32c_lib.crc32c(buf, self.pending_finfo.crc32c)
            else:
                # We are not writing sequentially anymore, so we can't calculate CRC32C
                self.pending_finfo.crc32c = None
            self.pending_finfo.size = max(self.pending_finfo.size, offset + n_written)

            if self.pending_finfo.end > self.bc.shard_size_limit:
                if self.pending_finfo.size > self.bc.shard_size_limit:
                    raise FuseError(errno.ENOSPC)
                self.bc.sharder.start_new_shard_and_transfer_last_file(
                    self.pending_finfo.offset, self.pending_finfo.size)
                self.pending_finfo.offset = 0
                self.pending_finfo.shard = len(self.bc.sharder.shard_files) - 1

            self.pending_finfo.update_mtime()
            return n_written

        if self.bc.exists(path):
            raise FuseError(errno.EEXIST)
        else:
            raise FuseError(errno.ENOENT)

    def release(self, path, fi):
        if self.is_pending(path, fi):
            if self.pending_finfo.crc32c is None:
                shard_file = self.bc.sharder.shard_files[-1]
                self.bc.sharder.shard_files[-1].seek(self.pending_finfo.offset)
                self.pending_finfo.crc32c = fileobj_crc32c(shard_file, self.pending_finfo.size)

            self.bc.index.add_file(self.pending_finfo)
            if self.enable_defrag:
                self.total_size += self.pending_finfo.size
                self.total_size_max = max(self.total_size_max, self.total_size)
            self.pending_finfo = None

    def rename(self, old, new, flags):
        if self.is_pending(old):
            if self.bc.exists(new):
                raise FuseError(errno.EEXIST)
            else:
                self.pending_finfo.path = new
                return

        if self.is_pending(new):
            raise FuseError(errno.EEXIST)
        try:
            self.bc.index.rename(old, new)
        except FileNotFoundBarecatError:
            raise FuseError(errno.ENOENT)
        except FileExistsBarecatError:
            raise FuseError(errno.EEXIST)

    def truncate(self, path, length, fi):
        if self.is_pending(path, fi):
            if length > self.pending_finfo.size:
                shard_file = self.bc.sharder.shard_files[-1]
                shard_file.seek(self.pending_finfo.offset + self.pending_finfo.size)
                shard_file.write(bytearray(length - self.pending_finfo.size))
            self.pending_finfo.size = length
            self.pending_finfo.update_mtime()
            return
        raise FuseError(errno.EACCES)

    def chmod(self, path: str, mode: int, fi: FuseFileInfo):
        if self.is_pending(path, fi):
            self.pending_finfo.mode = mode
            return
        try:
            self.bc.index.chmod(path, mode)
        except FileNotFoundBarecatError:
            raise FuseError(errno.ENOENT)

    def chown(self, path: str, uid: int, gid: int, fi: FuseFileInfo):
        if self.is_pending(path, fi):
            self.pending_finfo.uid = uid
            self.pending_finfo.gid = gid
            return
        try:
            self.bc.index.chown(path, uid, gid)
        except FileNotFoundBarecatError:
            raise FuseError(errno.ENOENT)

    def fallocate(self, path: str, mode: int, offset: int, length: int, fi: FuseFileInfo):
        if not offset == 0 and length == 0:
            raise FuseError(errno.EINVAL)

    def utimens(self, path: str, atime_ns: int, mtime_ns: int, fi: FuseFileInfo):
        if self.is_pending(path, fi):
            self.pending_finfo.mtime_ns = mtime_ns
            return
        try:
            self.bc.index.update_mtime(path, mtime_ns)
        except FileNotFoundBarecatError:
            raise FuseError(errno.ENOENT)

    def close(self):
        self.bc.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class BufferedCursor:
    def __init__(self, cursor, fetchsize=512):
        self.cursor = cursor
        self.buffer = deque()
        self.fetchsize = fetchsize

    def pop_right(self):
        try:
            return self.buffer.pop()
        except IndexError:
            fetches = self.cursor.fetchmany(self.fetchsize)
            if not fetches:
                return None
            result = fetches.pop(0)
            self.buffer.extendleft(fetches)
            return result

    def append_right(self, item):
        self.buffer.append(item)

    def append_left(self, item):
        self.buffer.appendleft(item)

    def close(self):
        self.cursor.close()


class ReaddirEntry(NamedTuple):
    name: str
    mode: int
    size: int
    mtime_ns: int
    uid: int
    gid: int
    isfile: int


def nanosec_now():
    return int(datetime.now().timestamp() * 1e9)


def file_info_to_stat(finfo: BarecatFileInfo):
    return Stat(
        mode=S_IFREG | (finfo.mode or 0o666), nlink=1, size=finfo.size,
        blocks=(finfo.size + 511) // 512, blksize=0,
        mtime_ns=finfo.mtime_ns or 0, atime_ns=finfo.mtime_ns, ctime_ns=finfo.mtime_ns,
        uid=finfo.uid or 0, gid=finfo.gid or 0)


def dir_info_to_stat(dinfo: BarecatDirInfo):
    return Stat(
        mode=S_IFDIR | (dinfo.mode or 0o777), nlink=2 + dinfo.num_subdirs, size=4096,
        blocks=0, blksize=0,
        mtime_ns=dinfo.mtime_ns or 0, atime_ns=dinfo.mtime_ns, ctime_ns=dinfo.mtime_ns,
        uid=dinfo.uid or 0, gid=dinfo.gid or 0)


def file_or_dir_info_to_stat(info: BarecatFileInfo | BarecatDirInfo):
    if isinstance(info, BarecatFileInfo):
        return file_info_to_stat(info)
    return dir_info_to_stat(info)


def mount(barecat_path, mountpoint, readonly=False, single_threaded=True, foreground=True):
    with barecat_.Barecat(barecat_path, readonly=readonly) as bc:
        bc_fuse = BarecatFuse(bc)
        bc_fuse.mount(mountpoint, readonly=readonly, single_threaded=single_threaded,
                      foreground=foreground)

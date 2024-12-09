# distutils: define_macros=FUSE_USE_VERSION=39
# cython: language_level = 3
import typing
# Import Cython and Python libraries
from typing import List
from dataclasses import dataclass
from libc.string cimport memset
from libc.stdlib cimport free, malloc
from posix.types cimport mode_t, off_t, uid_t, gid_t, dev_t, ino_t, blksize_t, blkcnt_t, pid_t
from libc.stdint cimport int64_t, uint64_t
from libc.time cimport time_t
from enum import IntFlag


cdef extern from "sys/stat.h":
    struct timespec:
        time_t tv_sec
        long tv_nsec

    struct stat:
        int st_mode
        int st_nlink
        off_t st_size
        blkcnt_t st_blocks
        dev_t st_dev
        ino_t st_ino
        uid_t st_uid
        gid_t st_gid
        dev_t st_rdev
        blksize_t st_blksize
        time_t st_atime
        time_t st_mtime
        time_t st_ctime
        timespec st_atim
        timespec st_mtim
        timespec st_ctim



cdef extern from "<sys/time.h>":
    cdef struct timespec:
        time_t tv_sec
        long tv_nsec

cdef extern from "fcntl.h":
    int S_IFMT
    int S_IFDIR
    int S_IFCHR
    int S_IFBLK
    int S_IFREG
    int S_IFIFO
    int S_IFLNK
    int S_IFSOCK

cdef extern from "fuse3/fuse.h":

    unsigned int RENAME_NOREPLACE
    unsigned int RENAME_EXCHANGE

    struct fuse_file_info:
        int flags
        int writepage
        int direct_io
        int keep_cache
        int flush
        int nonseekable
        int flock_release
        int cache_readdir
        int padding
        int padding2
        int fh
        int lock_owner
        int poll_events

    struct fuse_config:
        int set_gid
        unsigned int gid
        int set_uid
        unsigned int uid
        int set_mode
        unsigned int umask
        double entry_timeout
        double negative_timeout
        double attr_timeout
        int intr
        int intr_signal
        int remember
        int hard_remove
        int use_ino
        int readdir_ino
        int direct_io
        int kernel_cache
        int auto_cache
        int ac_attr_timeout_set
        double ac_attr_timeout
        int nullpath_ok
        int show_help
        char *modules
        int debug

    struct fuse_conn_info:
        unsigned proto_major
        unsigned proto_minor
        unsigned max_write
        unsigned max_read
        unsigned max_readahead
        unsigned capable
        unsigned want
        unsigned max_background
        unsigned congestion_threshold
        unsigned time_gran
        unsigned reserved[22]


    struct statvfs:
        unsigned long f_bsize
        unsigned long f_frsize
        unsigned long f_blocks
        unsigned long f_bfree
        unsigned long f_bavail
        unsigned long f_files
        unsigned long f_ffree
        unsigned long f_favail
        unsigned long f_fsid
        unsigned long f_flag
        unsigned long f_namemax
        int __f_spare[6]

    enum fuse_readdir_flags:
        FUSE_READDIR_PLUS = (1 << 0)

    enum fuse_fill_dir_flags:
        FUSE_FILL_DIR_PLUS = (1 << 1)

    struct flock:
        short l_type
        short l_whence
        off_t l_start
        off_t l_len
        pid_t l_pid


    struct fuse_pollhandle:
        pass

    ctypedef int(*fuse_fill_dir_t)(
            void *buf, const char *name, const stat *stbuf, off_t off, fuse_fill_dir_flags flags)


    struct fuse:
        pass

    struct fuse_context:
        fuse* fuse
        uid_t uid
        gid_t gid
        pid_t pid
        void *private_data
        mode_t umask

    fuse_context* fuse_get_context();

    struct fuse_operations:
        int (*getattr)(const char *, stat *, fuse_file_info *)
        int (*readdir)(const char *, void *, fuse_fill_dir_t, off_t, fuse_file_info *, fuse_readdir_flags)
        int (*mkdir)(const char *, mode_t)
        int (*create)(const char *, mode_t, fuse_file_info *)
        int (*read)(const char *, char *, size_t, off_t, fuse_file_info *)
        int (*write)(const char *, const char *, size_t, off_t, fuse_file_info *)
        int (*release)(const char *, fuse_file_info *)
        int (*truncate)(const char *, off_t, fuse_file_info *)
        int (*unlink)(const char *)
        int (*rmdir)(const char *)
        int (*rename)(const char *, const char *, unsigned int)
        int (*readlink)(const char *, char *, size_t);
        int (*mknod)(const char *, mode_t, dev_t);
        int (*symlink)(const char *, const char *);
        int (*link)(const char *, const char *);
        int (*chmod)(const char *, mode_t, fuse_file_info *);
        int (*chown)(const char *, uid_t, gid_t, fuse_file_info *);
        int (*open)(const char *, fuse_file_info *);
        int (*statfs)(const char *, statvfs *);
        int (*flush)(const char *, fuse_file_info *);
        int (*fsync)(const char *, int, fuse_file_info *);
        int (*setxattr)(const char *, const char*, const char*, size_t, int);
        int (*getxattr)(const char *, const char*, char*, size_t);
        int (*listxattr)(const char *, fuse_file_info *);
        int (*removexattr)(const char *, const char*);
        int (*opendir)(const char *, fuse_file_info *);
        int (*releasedir)(const char *, fuse_file_info *);
        int (*fsyncdir)(const char *, int, fuse_file_info *);
        int (*access)(const char *, int);
        int (*lock)(const char *, fuse_file_info *, int cmd, flock *);
        int (*utimens)(const char *, const timespec *, fuse_file_info *);
        int (*bmap)(const char *, size_t blocksize, uint64_t *idx);
        int (*ioctl)(const char *, unsigned int cmd, void *arg, fuse_file_info *, unsigned int flags, void *data);
        int (*poll)(const char *, fuse_file_info *, fuse_pollhandle *ph, unsigned *reventsp);
        int (*flock)(const char *, fuse_file_info *, int op);
        int (*fallocate)(const char *, int, off_t, off_t, fuse_file_info *);
        int (*copy_file_range)(const char *, fuse_file_info *, off_t, const char *, fuse_file_info *, off_t, size_t, int);
        int (*lseek)(const char *, off_t, int, fuse_file_info *);
        void (*destroy)(void *);
        void* (*init)(fuse_conn_info *, fuse_config *);


    int fuse_main(int argc, char ** argv, const fuse_operations *op, void *user_data)

class FuseReadDirFlags(IntFlag):
    PLUS = 1 << 0

cdef class FuseFileInfo:
    cdef fuse_file_info* _ffi  # Store the fuse_file_info* pointer

    @staticmethod
    cdef FuseFileInfo from_ptr(fuse_file_info* ffi_ptr):
        if ffi_ptr is NULL:
            return None
        cdef FuseFileInfo wrapper = FuseFileInfo.__new__(FuseFileInfo)
        wrapper._ffi = ffi_ptr
        return wrapper

    @property
    def flags(self):
        return self._ffi.flags

    @flags.setter
    def flags(self, value):
        self._ffi.flags = value

    @property
    def writepage(self):
        return self._ffi.writepage

    @writepage.setter
    def writepage(self, value):
        self._ffi.writepage = value

    @property
    def direct_io(self):
        return self._ffi.direct_io

    @direct_io.setter
    def direct_io(self, value):
        self._ffi.direct_io = value

    @property
    def keep_cache(self):
        return self._ffi.keep_cache

    @keep_cache.setter
    def keep_cache(self, value):
        self._ffi.keep_cache = value

    @property
    def flush(self):
        return self._ffi.flush

    @flush.setter
    def flush(self, value):
        self._ffi.flush = value

    @property
    def nonseekable(self):
        return self._ffi.nonseekable

    @nonseekable.setter
    def nonseekable(self, value):
        self._ffi.nonseekable = value

    @property
    def flock_release(self):
        return self._ffi.flock_release

    @flock_release.setter
    def flock_release(self, value):
        self._ffi.flock_release = value

    @property
    def cache_readdir(self):
        return self._ffi.cache_readdir

    @cache_readdir.setter
    def cache_readdir(self, value):
        self._ffi.cache_readdir = value

    @property
    def fh(self):
        return self._ffi.fh

    @fh.setter
    def fh(self, value):
        self._ffi.fh = value

    @property
    def lock_owner(self):
        return self._ffi.lock_owner

    @lock_owner.setter
    def lock_owner(self, value):
        self._ffi.lock_owner = value

    @property
    def poll_events(self):
        return self._ffi.poll_events

    @poll_events.setter
    def poll_events(self, value):
        self._ffi.poll_events = value


cdef class FuseContext:
    cdef fuse_context* _fc

    @staticmethod
    cdef FuseContext from_ptr(fuse_context* fc_ptr):
        if fc_ptr is NULL:
            return None
        cdef FuseContext wrapper = FuseContext.__new__(FuseContext)
        wrapper._fc = fc_ptr
        return wrapper

    @property
    def uid(self):
        return self._fc.uid

    @property
    def gid(self):
        return self._fc.gid

    @property
    def pid(self):
        return self._fc.pid

    @property
    def umask(self):
        return self._fc.umask



import os
import sys

class FuseError(Exception):
    def __init__(self, int error_number):
        super().__init__(os.strerror(error_number))
        self.error_number = error_number


@dataclass
class FuseDirEntry:
    name: str
    stat: dict = None
    offset: int = 0



cdef class StatPtr:
    cdef stat* _stat  # Store the stat* pointer

    @staticmethod
    cdef StatPtr from_ptr(stat* stat_ptr):
        if stat_ptr is NULL:
            return None
        cdef StatPtr wrapper = StatPtr.__new__(StatPtr)
        wrapper._stat = stat_ptr
        return wrapper

    @property
    def st_mode(self):
        return self._stat.st_mode

    @st_mode.setter
    def st_mode(self, value):
        self._stat.st_mode = value

    @property
    def st_nlink(self):
        return self._stat.st_nlink

    @st_nlink.setter
    def st_nlink(self, value):
        self._stat.st_nlink = value

    @property
    def st_size(self):
        return self._stat.st_size

    @st_size.setter
    def st_size(self, value):
        self._stat.st_size = value

    @property
    def st_blocks(self):
        return self._stat.st_blocks

    @st_blocks.setter
    def st_blocks(self, value):
        self._stat.st_blocks = value

    @property
    def st_dev(self):
        return self._stat.st_dev

    @st_dev.setter
    def st_dev(self, value):
        self._stat.st_dev = value

    @property
    def st_ino(self):
        return self._stat.st_ino

    @st_ino.setter
    def st_ino(self, value):
        self._stat.st_ino = value

    @property
    def st_uid(self):
        return self._stat.st_uid

    @st_uid.setter
    def st_uid(self, value):
        self._stat.st_uid = value

    @property
    def st_gid(self):
        return self._stat.st_gid

    @st_gid.setter
    def st_gid(self, value):
        self._stat.st_gid = value

    @property
    def st_rdev(self):
        return self._stat.st_rdev

    @st_rdev.setter
    def st_rdev(self, value):
        self._stat.st_rdev = value

    @property
    def st_blksize(self):
        return self._stat.st_blksize

    @st_blksize.setter
    def st_blksize(self, value):
        self._stat.st_blksize = value

    @property
    def atime_ns(self):
        return self._stat.st_atim.tv_sec * 1_000_000_000 + self._stat.st_atim.tv_nsec

    @atime_ns.setter
    def atime_ns(self, value):
        self._stat.st_atime = value // 1_000_000_000
        self._stat.st_atim.tv_sec = value // 1_000_000_000
        self._stat.st_atim.tv_nsec = value % 1_000_000_000

    @property
    def mtime_ns(self):
        return self._stat.st_mtim.tv_sec * 1_000_000_000 + self._stat.st_mtim.tv_nsec

    @mtime_ns.setter
    def mtime_ns(self, value):
        self._stat.st_mtime = value // 1_000_000_000
        self._stat.st_mtim.tv_sec = value // 1_000_000_000
        self._stat.st_mtim.tv_nsec = value % 1_000_000_000

    @property
    def ctime_ns(self):
        return self._stat.st_ctim.tv_sec * 1_000_000_000 + self._stat.st_ctim.tv_nsec

    @ctime_ns.setter
    def ctime_ns(self, value):
        self._stat.st_ctime = value // 1_000_000_000
        self._stat.st_ctim.tv_sec = value // 1_000_000_000
        self._stat.st_ctim.tv_nsec = value % 1_000_000_000


cdef class StatvfsPtr:
    cdef statvfs* _statvfs  # Store the statvfs* pointer

    @staticmethod
    cdef StatvfsPtr from_ptr(statvfs* statvfs_ptr):
        if statvfs_ptr is NULL:
            return None
        cdef StatvfsPtr wrapper = StatvfsPtr.__new__(StatvfsPtr)
        wrapper._statvfs = statvfs_ptr
        return wrapper

    @property
    def f_bsize(self):
        return self._statvfs.f_bsize

    @f_bsize.setter
    def f_bsize(self, value):
        self._statvfs.f_bsize = value

    @property
    def f_frsize(self):
        return self._statvfs.f_frsize

    @f_frsize.setter
    def f_frsize(self, value):
        self._statvfs.f_frsize = value

    @property
    def f_blocks(self):
        return self._statvfs.f_blocks

    @f_blocks.setter
    def f_blocks(self, value):
        self._statvfs.f_blocks = value

    @property
    def f_bfree(self):
        return self._statvfs.f_bfree

    @f_bfree.setter
    def f_bfree(self, value):
        self._statvfs.f_bfree = value

    @property
    def f_bavail(self):
        return self._statvfs.f_bavail

    @f_bavail.setter
    def f_bavail(self, value):
        self._statvfs.f_bavail = value

    @property
    def f_files(self):
        return self._statvfs.f_files

    @f_files.setter
    def f_files(self, value):
        self._statvfs.f_files = value

    @property
    def f_ffree(self):
        return self._statvfs.f_ffree

    @f_ffree.setter
    def f_ffree(self, value):
        self._statvfs.f_ffree = value

    @property
    def f_favail(self):
        return self._statvfs.f_favail

    @f_favail.setter
    def f_favail(self, value):
        self._statvfs.f_favail = value

    @property
    def f_fsid(self):
        return self._statvfs.f_fsid

    @f_fsid.setter
    def f_fsid(self, value):
        self._statvfs.f_fsid = value

    @property
    def f_flag(self):
        return self._statvfs.f_flag

    @f_flag.setter
    def f_flag(self, value):
        self._statvfs.f_flag = value

    @property
    def f_namemax(self):
        return self._statvfs.f_namemax

    @f_namemax.setter
    def f_namemax(self, value):
        self._statvfs.f_namemax = value


@dataclass
class Stat:
    mode: int = 0
    nlink: int = 0
    size: int = 0
    blocks: int = 0
    dev: int = 0
    ino: int = 0
    uid: int = 0
    gid: int = 0
    rdev: int = 0
    blksize: int = 0
    atime_ns: int = 0
    mtime_ns: int = 0
    ctime_ns: int = 0

@dataclass
class StatVFS:
    bsize: int = 0
    frsize: int = 0
    blocks: int = 0
    bfree: int = 0
    bavail: int = 0
    files: int = 0
    ffree: int = 0
    favail: int = 0
    fsid: int = 0
    flag: int = 0
    namemax: int = 0


cdef class PyFuse:
    def getattr(self, path: str, fi: FuseFileInfo):
        pass

    def readdir(self, path: str, offset: off_t, fi: FuseFileInfo, flags: FuseReadDirFlags):
        pass

    def readdir_no_offset(self, path: str, fi: FuseFileInfo, flags: FuseReadDirFlags):
        pass

    def readdir_offset(self, path: str, offset: off_t, fi: FuseFileInfo, flags: FuseReadDirFlags):
        pass

    def mkdir(self, path: str, mode: mode_t):
        pass

    def create(self, path: str, mode: mode_t, fi: FuseFileInfo):
        pass

    def read(self, path: str, buf: memoryview, offset: off_t, fi: FuseFileInfo):
        pass

    def write(self, path: str, buf: memoryview, offset: off_t, fi: FuseFileInfo):
        pass

    def open(self, path: str, fi: FuseFileInfo):
        pass

    def release(self, path: str, fi):
        pass

    def truncate(self, path: str, size: off_t, fi: FuseFileInfo):
        pass

    def unlink(self, path: str):
        pass

    def rmdir(self, path: str):
        pass

    def rename(self, old: str, new: str, flags: int):
        pass

    def opendir(self, path: str, fi: FuseFileInfo):
        pass

    def releasedir(self, path: str, fi: FuseFileInfo):
        pass

    def statfs(self, path: str):
        pass

    def flush(self, path: str, fi: FuseFileInfo):
        pass

    def fsync(self, path: str, datasync: bool, fi: FuseFileInfo):
        pass

    def fsyncdir(self, path: str, datasync: bool, fi: FuseFileInfo):
        pass

    def chmod(self, path: str, mode: mode_t, fi: FuseFileInfo):
        pass

    def chown(self, path: str, uid: uid_t, gid: gid_t, fi: FuseFileInfo):
        pass

    def fallocate(self, path: str, mode: int, offset: off_t, length: off_t, fi: FuseFileInfo):
        pass

    def utimens(self, path: str, atime_ns: int, mtime_ns: int, fi: FuseFileInfo):
        pass

    # Raw versions

    def getattr_raw(self, path: str, stat_out: StatPtr, fi: FuseFileInfo):
        pass

    def readdir_raw(self, path: str, filler, statbuf: StatPtr, offset: off_t, fi: FuseFileInfo, flags: FuseReadDirFlags):
        pass

    def mkdir_raw(self, path: str, mode: mode_t):
        pass

    def create_raw(self, path: str, mode: mode_t, fi: FuseFileInfo):
        pass

    def read_raw(self, path: str, buf: memoryview, offset: off_t, fi: FuseFileInfo):
        pass

    def write_raw(self, path: str, buf: memoryview, offset: off_t, fi: FuseFileInfo):
        pass

    def open_raw(self, path: str, fi: FuseFileInfo):
        pass

    def release_raw(self, path: str, fi):
        pass

    def truncate_raw(self, path: str, size: off_t, fi: FuseFileInfo):
        pass

    def unlink_raw(self, path: str):
        pass

    def rmdir_raw(self, path: str):
        pass

    def rename_raw(self, old: str, new: str, flags: int):
        pass

    def opendir_raw(self, path: str, fi: FuseFileInfo):
        pass

    def releasedir_raw(self, path: str, fi: FuseFileInfo):
        pass

    def statfs_raw(self, path: str):
        pass

    def flush_raw(self, path: str, fi: FuseFileInfo):
        pass

    def fsync_raw(self, path: str, datasync: bool, fi: FuseFileInfo):
        pass

    def fsyncdir_raw(self, path: str, datasync: bool, fi: FuseFileInfo):
        pass

    def chmod_raw(self, path: str, mode: mode_t, fi: FuseFileInfo):
        pass

    def chown_raw(self, path: str, uid: uid_t, gid: gid_t, fi: FuseFileInfo):
        pass

    def fallocate_raw(self, path: str, mode: int, offset: off_t, length: off_t, fi: FuseFileInfo):
        pass

    def utimens_raw(self, path: str, atime_ns: int, mtime_ns: int, fi: FuseFileInfo):
        pass

    @staticmethod
    def get_context():
        return FuseContext.from_ptr(fuse_get_context())

    def main(self, args: List[str]):
        wrapped_fuse_main(self, args)

    @classmethod
    def is_method_overridden(cls, instance, method_name: str) -> bool:
        method = getattr(instance, method_name, None).__func__
        class_method = getattr(cls, method_name, None)
        return method is not class_method

cdef PyFuse get_py_fuse():
    return <PyFuse> fuse_get_context().private_data

cdef void fill_stat(stat *stbuf, object s):
    stbuf.st_mode = s.mode
    stbuf.st_nlink = s.nlink
    stbuf.st_size = s.size
    stbuf.st_blocks = s.blocks
    stbuf.st_dev = s.dev
    stbuf.st_ino = s.ino
    stbuf.st_uid = s.uid
    stbuf.st_gid = s.gid
    stbuf.st_rdev = s.rdev
    stbuf.st_blksize = s.blksize
    stbuf.st_atime = s.atime_ns // 1_000_000_000
    stbuf.st_mtime = s.mtime_ns // 1_000_000_000
    stbuf.st_ctime = s.ctime_ns // 1_000_000_000
    stbuf.st_atim.tv_sec = s.atime_ns // 1_000_000_000
    stbuf.st_mtim.tv_sec = s.mtime_ns // 1_000_000_000
    stbuf.st_ctim.tv_sec = s.ctime_ns // 1_000_000_000
    stbuf.st_atim.tv_nsec = s.atime_ns % 1_000_000_000
    stbuf.st_mtim.tv_nsec = s.mtime_ns % 1_000_000_000
    stbuf.st_ctim.tv_nsec = s.ctime_ns % 1_000_000_000

cdef void fill_statvfs(statvfs *stbuf, object s):
    stbuf.f_bsize = s.bsize
    stbuf.f_frsize = s.frsize
    stbuf.f_blocks = s.blocks
    stbuf.f_bfree = s.bfree
    stbuf.f_bavail = s.bavail
    stbuf.f_files = s.files
    stbuf.f_ffree = s.ffree
    stbuf.f_favail = s.favail
    stbuf.f_fsid = s.fsid
    stbuf.f_flag = s.flag
    stbuf.f_namemax = s.namemax


# Define callback methods for FUSE that will call corresponding Python methods
cdef int c_getattr(const char *path, stat *stbuf, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        st = py_fuse.getattr(py_path, FuseFileInfo.from_ptr(fi))
        fill_stat(stbuf, st)
    except FuseError as e:
        return -e.error_number
    return 0

cdef int c_getattr_raw(const char *path, stat *stbuf, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    memset(stbuf, 0, sizeof(stat))
    return py_fuse.getattr_raw(py_path, StatPtr.from_ptr(stbuf), FuseFileInfo.from_ptr(fi))

class FuseReadDirBufferFull(Exception):
    pass

cdef int c_readdir(
        const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, fuse_file_info *fi,
        fuse_readdir_flags flags) noexcept:

    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')

    cdef stat stbuf
    try:
        generator = py_fuse.readdir(py_path, offset, FuseFileInfo.from_ptr(fi), FuseReadDirFlags(flags))
        for entry in generator:
            if entry.stat is not None:
                memset(&stbuf, 0, sizeof(stat))
                fill_stat(&stbuf, entry.stat)
                res = filler(buf, entry.name.encode('utf-8'), &stbuf, entry.offset, FUSE_FILL_DIR_PLUS)
            else:
                res = filler(buf, entry.name.encode('utf-8'), NULL, entry.offset, <fuse_fill_dir_flags> 0)

            if res != 0:
                try:
                    generator.throw(FuseReadDirBufferFull())
                except (StopIteration, FuseReadDirBufferFull):
                    pass
                return 0

    except FuseError as e:
        return -e.error_number
    return 0


cdef int c_readdir_raw(
        const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, fuse_file_info *fi,
        fuse_readdir_flags flags) noexcept:

    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    cdef stat stbuf
    memset(&stbuf, 0, sizeof(stat))

    def pyfiller(name_, filled_stat, off_):
        if filled_stat:
            return filler(buf, name_.encode('utf-8'), &stbuf, off_, FUSE_FILL_DIR_PLUS)
        else:
            return filler(buf, name_.encode('utf-8'), NULL, off_, <fuse_fill_dir_flags> 0)

    return py_fuse.readdir_raw(
        py_path, pyfiller, StatPtr.from_ptr(&stbuf), offset, FuseFileInfo.from_ptr(fi),
        FuseReadDirFlags(flags))


cdef int c_mkdir(const char *path, mode_t mode) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        py_fuse.mkdir(py_path, mode)
    except FuseError as e:
        return -e.error_number
    return 0

cdef int c_mkdir_raw(const char *path, mode_t mode) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    return py_fuse.mkdir_raw(py_path, mode)


cdef int c_create(const char *path, mode_t mode, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        py_fuse.create(py_path, mode, FuseFileInfo.from_ptr(fi))
    except FuseError as e:
        return -e.error_number
    return 0


cdef int c_create_raw(const char *path, mode_t mode, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    return py_fuse.create_raw(py_path, mode, FuseFileInfo.from_ptr(fi))


cdef int c_read(const char *path, char *buf, size_t size, off_t offset, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        pybuf = memoryview(<char[:size]>buf).cast('B')
        return py_fuse.read(py_path, pybuf, offset, FuseFileInfo.from_ptr(fi))
    except FuseError as e:
        return -e.error_number


cdef int c_read_raw(const char *path, char *buf, size_t size, off_t offset, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    pybuf = memoryview(<char[:size]>buf).cast('B')
    return py_fuse.read_raw(py_path, pybuf, offset, FuseFileInfo.from_ptr(fi))


cdef int c_write(const char *path, const char *buf, size_t size, off_t offset, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    pybuf = memoryview(<const char[:size]>buf).cast('B')
    try:
        py_fuse.write(py_path, pybuf, offset, FuseFileInfo.from_ptr(fi))
    except FuseError as e:
        return -e.error_number
    return size


cdef int c_write_raw(const char *path, const char *buf, size_t size, off_t offset, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    pybuf = memoryview(<const char[:size]>buf).cast('B')
    return py_fuse.write_raw(py_path, pybuf, offset, FuseFileInfo.from_ptr(fi))


cdef int c_release(const char *path, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    py_fuse.release(py_path, FuseFileInfo.from_ptr(fi))
    return 0


cdef int c_release_raw(const char *path, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    return py_fuse.release_raw(py_path, FuseFileInfo.from_ptr(fi))


cdef int c_truncate(const char *path, off_t size, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        py_fuse.truncate(py_path, size, FuseFileInfo.from_ptr(fi))
    except FuseError as e:
        return -e.error_number
    return 0


cdef int c_truncate_raw(const char *path, off_t size, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    return py_fuse.truncate_raw(py_path, size, FuseFileInfo.from_ptr(fi))


cdef int c_unlink(const char *path) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        py_fuse.unlink(py_path)
    except FuseError as e:
        return -e.error_number
    return 0


cdef int c_unlink_raw(const char *path) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    return py_fuse.unlink_raw(py_path)


cdef int c_rmdir(const char *path) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        py_fuse.rmdir(py_path)
    except FuseError as e:
        return -e.error_number
    return 0


cdef int c_rmdir_raw(const char *path) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    return py_fuse.rmdir_raw(py_path)


cdef int c_rename(const char *old, const char *new, unsigned int flags) noexcept:
    py_fuse = get_py_fuse()
    py_old = old.decode('utf-8')
    py_new = new.decode('utf-8')
    try:
        py_fuse.rename(py_old, py_new, flags)
    except FuseError as e:
        return -e.error_number
    return 0


cdef int c_rename_raw(const char *old, const char *new, unsigned int flags) noexcept:
    py_fuse = get_py_fuse()
    py_old = old.decode('utf-8')
    py_new = new.decode('utf-8')
    return py_fuse.rename_raw(py_old, py_new, flags)


cdef int c_opendir(const char *path, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        py_fuse.opendir(py_path, FuseFileInfo.from_ptr(fi))
    except FuseError as e:
        return -e.error_number
    return 0


cdef int c_opendir_raw(const char *path, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    return py_fuse.opendir_raw(py_path, FuseFileInfo.from_ptr(fi))


cdef int c_releasedir(const char *path, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        py_fuse.releasedir(py_path, FuseFileInfo.from_ptr(fi))
    except FuseError as e:
        return -e.error_number
    return 0


cdef int c_releasedir_raw(const char *path, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    return py_fuse.releasedir_raw(py_path, FuseFileInfo.from_ptr(fi))


cdef int c_open(const char *path, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        py_fuse.open(py_path, FuseFileInfo.from_ptr(fi))
    except FuseError as e:
        return -e.error_number
    return 0


cdef int c_open_raw(const char *path, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    return py_fuse.open_raw(py_path, FuseFileInfo.from_ptr(fi))


cdef int c_statfs(const char *path, statvfs *stbuf) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        st = py_fuse.statfs(py_path)
        fill_statvfs(stbuf, st)
    except FuseError as e:
        return -e.error_number
    return 0


cdef int c_statfs_raw(const char *path, statvfs *stbuf) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    return py_fuse.statfs_raw(py_path, StatvfsPtr.from_ptr(stbuf))


cdef int c_flush(const char *path, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        py_fuse.flush(py_path, FuseFileInfo.from_ptr(fi))
    except FuseError as e:
        return -e.error_number
    return 0


cdef int c_flush_raw(const char *path, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    return py_fuse.flush_raw(py_path, FuseFileInfo.from_ptr(fi))


cdef int c_fsync(const char *path, int datasync, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        py_fuse.fsync(py_path, datasync != 0, FuseFileInfo.from_ptr(fi))
    except FuseError as e:
        return -e.error_number
    return 0


cdef int c_fsync_raw(const char *path, int datasync, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    return py_fuse.fsync_raw(py_path, datasync, FuseFileInfo.from_ptr(fi))


cdef int c_fsyncdir(const char *path, int datasync, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        py_fuse.fsyncdir(py_path, datasync != 0, FuseFileInfo.from_ptr(fi))
    except FuseError as e:
        return -e.error_number
    return 0


cdef int c_fsyncdir_raw(const char *path, int datasync, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    return py_fuse.fsyncdir_raw(py_path, datasync, FuseFileInfo.from_ptr(fi))


cdef int c_chmod(const char *path, mode_t mode, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        py_fuse.chmod(py_path, mode, FuseFileInfo.from_ptr(fi))
    except FuseError as e:
        return -e.error_number
    return 0


cdef int c_chmod_raw(const char *path, mode_t mode, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    return py_fuse.chmod_raw(py_path, mode, FuseFileInfo.from_ptr(fi))


cdef int c_chown(const char *path, uid_t uid, gid_t gid, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        py_fuse.chown(py_path, uid, gid, FuseFileInfo.from_ptr(fi))
    except FuseError as e:
        return -e.error_number
    return 0


cdef int c_chown_raw(const char *path, uid_t uid, gid_t gid, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    return py_fuse.chown_raw(py_path, uid, gid, FuseFileInfo.from_ptr(fi))


cdef int c_fallocate(const char *path, int mode, off_t offset, off_t length, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        py_fuse.fallocate(py_path, mode, offset, length, FuseFileInfo.from_ptr(fi))
    except FuseError as e:
        return -e.error_number
    return 0


cdef int c_fallocate_raw(const char *path, int mode, off_t offset, off_t length, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    return py_fuse.fallocate_raw(py_path, mode, offset, length, FuseFileInfo.from_ptr(fi))


cdef int c_utimens(const char *path, const timespec* ts, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    try:
        atime_ns = int(ts[0].tv_sec) * 1_000_000_000 + int(ts[0].tv_nsec)
        mtime_ns = int(ts[1].tv_sec) * 1_000_000_000 + int(ts[1].tv_nsec)
        py_fuse.utimens(py_path, atime_ns, mtime_ns, FuseFileInfo.from_ptr(fi))
    except FuseError as e:
        return -e.error_number
    return 0


cdef int c_utimens_raw(const char *path, const timespec* ts, fuse_file_info *fi) noexcept:
    py_fuse = get_py_fuse()
    py_path = path.decode('utf-8')
    atime_ns = int(ts[0].tv_sec) * 1_000_000_000 + int(ts[0].tv_nsec)
    mtime_ns = int(ts[1].tv_sec) * 1_000_000_000 + int(ts[1].tv_nsec)
    return py_fuse.utimens_raw(py_path, atime_ns, mtime_ns, FuseFileInfo.from_ptr(fi))


def wrapped_fuse_main(PyFuse py_fuse, list args):
    argv = [str.encode(arg) for arg in args]
    argc = len(argv)
    if argc > 128:
        raise ValueError("Too many arguments")

    cdef char* argv_c[128]
    for i in range(argc):
        argv_c[i] = argv[i]

    cdef void * user_data = <void *> py_fuse
    cdef fuse_operations fuse_ops = fuse_operations()
    memset(&fuse_ops, 0, sizeof(fuse_operations))

    if PyFuse.is_method_overridden(py_fuse, "getattr_raw"):
        fuse_ops.getattr = c_getattr_raw
    elif PyFuse.is_method_overridden(py_fuse, "getattr"):
        fuse_ops.getattr = c_getattr

    if PyFuse.is_method_overridden(py_fuse, "readdir_raw"):
        fuse_ops.readdir = c_readdir_raw
    elif PyFuse.is_method_overridden(py_fuse, "readdir"):
        fuse_ops.readdir = c_readdir

    if PyFuse.is_method_overridden(py_fuse, "mkdir_raw"):
        fuse_ops.mkdir = c_mkdir_raw
    elif PyFuse.is_method_overridden(py_fuse, "mkdir"):
        fuse_ops.mkdir = c_mkdir

    if PyFuse.is_method_overridden(py_fuse, "create_raw"):
        fuse_ops.create = c_create_raw
    elif PyFuse.is_method_overridden(py_fuse, "create"):
        fuse_ops.create = c_create

    if PyFuse.is_method_overridden(py_fuse, "write_raw"):
        fuse_ops.write = c_write_raw
    elif PyFuse.is_method_overridden(py_fuse, "write"):
        fuse_ops.write = c_write

    if PyFuse.is_method_overridden(py_fuse, "release_raw"):
        fuse_ops.release = c_release_raw
    elif PyFuse.is_method_overridden(py_fuse, "release"):
        fuse_ops.release = c_release

    if PyFuse.is_method_overridden(py_fuse, "truncate_raw"):
        fuse_ops.truncate = c_truncate_raw
    elif PyFuse.is_method_overridden(py_fuse, "truncate"):
        fuse_ops.truncate = c_truncate

    if PyFuse.is_method_overridden(py_fuse, "unlink_raw"):
        fuse_ops.unlink = c_unlink_raw
    elif PyFuse.is_method_overridden(py_fuse, "unlink"):
        fuse_ops.unlink = c_unlink

    if PyFuse.is_method_overridden(py_fuse, "rmdir_raw"):
        fuse_ops.rmdir = c_rmdir_raw
    elif PyFuse.is_method_overridden(py_fuse, "rmdir"):
        fuse_ops.rmdir = c_rmdir

    if PyFuse.is_method_overridden(py_fuse, "rename_raw"):
        fuse_ops.rename = c_rename_raw
    elif PyFuse.is_method_overridden(py_fuse, "rename"):
        fuse_ops.rename = c_rename

    if PyFuse.is_method_overridden(py_fuse, "read_raw"):
        fuse_ops.read = c_read_raw
    elif PyFuse.is_method_overridden(py_fuse, "read"):
        fuse_ops.read = c_read

    if PyFuse.is_method_overridden(py_fuse, "opendir_raw"):
        fuse_ops.opendir = c_opendir_raw
    elif PyFuse.is_method_overridden(py_fuse, "opendir"):
        fuse_ops.opendir = c_opendir

    if PyFuse.is_method_overridden(py_fuse, "releasedir_raw"):
        fuse_ops.releasedir = c_releasedir_raw
    elif PyFuse.is_method_overridden(py_fuse, "releasedir"):
        fuse_ops.releasedir = c_releasedir

    if PyFuse.is_method_overridden(py_fuse, "open_raw"):
        fuse_ops.open = c_open_raw
    elif PyFuse.is_method_overridden(py_fuse, "open"):
        fuse_ops.open = c_open

    if PyFuse.is_method_overridden(py_fuse, "statfs_raw"):
        fuse_ops.statfs = c_statfs_raw
    elif PyFuse.is_method_overridden(py_fuse, "statfs"):
        fuse_ops.statfs = c_statfs

    if PyFuse.is_method_overridden(py_fuse, "flush_raw"):
        fuse_ops.flush = c_flush_raw
    elif PyFuse.is_method_overridden(py_fuse, "flush"):
        fuse_ops.flush = c_flush

    if PyFuse.is_method_overridden(py_fuse, "fsync_raw"):
        fuse_ops.fsync = c_fsync_raw
    elif PyFuse.is_method_overridden(py_fuse, "fsync"):
        fuse_ops.fsync = c_fsync

    if PyFuse.is_method_overridden(py_fuse, "fsyncdir_raw"):
        fuse_ops.fsyncdir = c_fsyncdir_raw
    elif PyFuse.is_method_overridden(py_fuse, "fsyncdir"):
        fuse_ops.fsyncdir = c_fsyncdir

    if PyFuse.is_method_overridden(py_fuse, "chmod_raw"):
        fuse_ops.chmod = c_chmod_raw
    elif PyFuse.is_method_overridden(py_fuse, "chmod"):
        fuse_ops.chmod = c_chmod

    if PyFuse.is_method_overridden(py_fuse, "chown_raw"):
        fuse_ops.chown = c_chown_raw
    elif PyFuse.is_method_overridden(py_fuse, "chown"):
        fuse_ops.chown = c_chown

    if PyFuse.is_method_overridden(py_fuse, "fallocate_raw"):
        fuse_ops.fallocate = c_fallocate_raw
    elif PyFuse.is_method_overridden(py_fuse, "fallocate"):
        fuse_ops.fallocate = c_fallocate

    if PyFuse.is_method_overridden(py_fuse, "utimens_raw"):
        fuse_ops.utimens = c_utimens_raw
    elif PyFuse.is_method_overridden(py_fuse, "utimens"):
        fuse_ops.utimens = c_utimens

    fuse_main(argc, argv_c, &fuse_ops, user_data)

import io
import os
from datetime import datetime
from enum import Flag, auto

from barecat.util import datetime_to_ns, normalize_path, ns_to_datetime

SHARD_SIZE_UNLIMITED = (1 << 63) - 1


class BaseInfo:
    def __init__(
            self,
            path: str | None = None,
            mode: int | None = None,
            uid: int | None = None,
            gid: int | None = None,
            mtime_ns: int | datetime | None = None):
        self._path = normalize_path(path)
        self.mode = mode
        self.uid = uid
        self.gid = gid
        self.mtime_ns = mtime_ns
        if isinstance(self.mtime_ns, datetime):
            self.mtime_ns = datetime_to_ns(self.mtime_ns)

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        self._path = normalize_path(value)

    @property
    def mtime_dt(self):
        return ns_to_datetime(self.mtime_ns) if self.mtime_ns else None

    @mtime_dt.setter
    def mtime_dt(self, dt: datetime):
        self.mtime_ns = datetime_to_ns(dt)

    def update_mtime(self):
        self.mtime_dt = datetime.now()

    def fill_from_statresult(self, s: os.stat_result):
        self.mode = s.st_mode
        self.uid = s.st_uid
        self.gid = s.st_gid
        self.mtime_ns = s.st_mtime_ns

    @classmethod
    def row_factory(cls, cursor, row):
        # Raw construction without any of that property business or validation, just for speed
        instance = cls.__new__(cls)
        for field, value in zip(cursor.description, row):
            if field[0] == 'path':
                instance._path = value
            else:
                object.__setattr__(instance, field[0], value)
        return instance


class BarecatFileInfo(BaseInfo):
    def __init__(
            self,
            path: str | None = None,
            mode: int | None = None,
            uid: int | None = None,
            gid: int | None = None,
            mtime_ns: int | datetime | None = None,
            shard: int | None = None,
            offset: int | None = None,
            size: int | None = None,
            crc32c: int | None = None):
        super().__init__(path, mode, uid, gid, mtime_ns)
        self.shard = shard
        self.offset = offset
        self.size = size
        self.crc32c = crc32c

    def asdict(self):
        return dict(
            path=self.path, shard=self.shard, offset=self.offset, size=self.size,
            crc32c=self.crc32c, mode=self.mode, uid=self.uid, gid=self.gid,
            mtime_ns=self.mtime_ns)

    def fill_from_statresult(self, s: os.stat_result):
        super().fill_from_statresult(s)
        self.size = s.st_size

    @property
    def end(self):
        return self.offset + self.size


class BarecatDirInfo(BaseInfo):
    def __init__(
            self,
            path: str | None = None,
            mode: int | None = None,
            uid: int | None = None,
            gid: int | None = None,
            mtime_ns: int | datetime | None = None,
            num_subdirs: bool | None = None,
            num_files: int | None = None,
            size_tree: int | None = None,
            num_files_tree: int | None = None):
        super().__init__(path, mode, uid, gid, mtime_ns)
        self.num_subdirs = num_subdirs
        self.num_files = num_files
        self.size_tree = size_tree
        self.num_files_tree = num_files_tree

    def asdict(self):
        return dict(
            path=self.path, num_subdirs=self.num_subdirs, num_files=self.num_files,
            size_tree=self.size_tree, num_files_tree=self.num_files_tree,
            mode=self.mode, uid=self.uid, gid=self.gid,
            mtime_ns=self.mtime_ns)

    @property
    def num_entries(self):
        return self.num_subdirs + self.num_files

    def fill_from_statresult(self, s: os.stat_result):
        super().fill_from_statresult(s)
        self.num_subdirs = s.st_nlink - 2


class Order(Flag):
    ANY = auto()
    RANDOM = auto()
    ADDRESS = auto()
    PATH = auto()
    DESC = auto()

    def as_query_text(self):
        if self & Order.ADDRESS and self & Order.DESC:
            return ' ORDER BY shard DESC, offset DESC'
        elif self & Order.ADDRESS:
            return ' ORDER BY shard, offset'
        elif self & Order.PATH and self & Order.DESC:
            return ' ORDER BY path DESC'
        elif self & Order.PATH:
            return ' ORDER BY path'
        elif self & Order.RANDOM:
            return ' ORDER BY RANDOM()'
        return ''


class FileSection(io.IOBase):
    def __init__(self, file, start, size, readonly=True):
        self.file = file
        self.start = start
        self.end = start + size
        self.position = start
        self.readonly = readonly

    def read(self, size=-1):
        if size == -1:
            size = self.end - self.position

        size = min(size, self.end - self.position)
        self.file.seek(self.position)
        data = self.file.read(size)
        self.position += len(data)
        return data

    def readinto(self, buffer, /):
        size = min(len(buffer), self.end - self.position)
        if size == 0:
            return 0

        self.file.seek(self.position)
        num_read = self.file.readinto(buffer[:size])
        self.position += num_read
        return num_read

    def readall(self):
        return self.read()

    def readable(self):
        return True

    def writable(self):
        return not self.readonly

    def write(self, data):
        if self.readonly:
            raise PermissionError('Cannot write to a read-only file section')

        if self.position + len(data) > self.end:
            raise EOFError('Cannot write past the end of the section')

        self.file.seek(self.position)
        n_written = self.file.write(data)
        self.position += n_written
        return n_written

    def readline(self, size=-1):
        size = min(size, self.end - self.position)
        if size == -1:
            size = self.end - self.position

        self.file.seek(self.position)
        data = self.file.readline(size)

        self.position += len(data)
        return data

    def tell(self):
        return self.position - self.start

    def seek(self, offset, whence=0):
        if whence == io.SEEK_SET:
            new_position = self.start + offset
        elif whence == io.SEEK_CUR:
            new_position = self.position + offset
        elif whence == io.SEEK_END:
            new_position = self.end + offset
        else:
            raise ValueError(f"Invalid value for whence: {whence}")

        if new_position < self.start or new_position > self.end:
            raise EOFError("Seek position out of bounds")

        self.position = new_position
        return self.position - self.start

    def close(self):
        pass

    @property
    def size(self):
        return self.end - self.start

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

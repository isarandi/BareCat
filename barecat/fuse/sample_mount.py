import errno
import io
import itertools
import os.path as osp
import stat

from barecat.fuse import FuseDirEntry, FuseError, FuseFileInfo, FuseReadDirFlags, PyFuse, Stat


class MemoryFuse(PyFuse):
    def __init__(self):
        super().__init__()
        self.files = {}
        self.dirs = {'/'}

    def getattr(self, path: str, fi: FuseFileInfo):
        if path in self.dirs:
            return Stat(mode=stat.S_IFDIR | 0o755, nlink=2, size=4096, blocks=8)

        if path in self.files:
            return Stat(
                mode=stat.S_IFREG | 0o644, nlink=1, size=self.files[path].getbuffer().nbytes,
                blocks=8)

        raise FuseError(errno.ENOENT)

    def readdir(self, path: str, offset, fi: FuseFileInfo, flags: FuseReadDirFlags):
        yield FuseDirEntry('.')
        yield FuseDirEntry('..')
        for iterpath in itertools.chain(self.dirs, self.files):
            if osp.dirname(iterpath) == path and iterpath != '/':
                yield FuseDirEntry(osp.basename(iterpath))

    def mkdir(self, path: str, mode: int):
        if path in self.dirs or path in self.files:
            raise FuseError(errno.EEXIST)
        self.dirs.add(path)

    def read(self, path: str, buf: memoryview, offset: int, fi: FuseFileInfo):
        if path not in self.files:
            raise FuseError(errno.ENOENT)

        self.files[path].seek(offset)
        n_read = self.files[path].readinto(buf)
        return n_read

    def write(self, path: str, buf: memoryview, offset: int, fi: FuseFileInfo):
        try:
            self.files[path].seek(offset)
            self.files[path].write(buf)
        except KeyError:
            raise FuseError(errno.ENOENT)

    def create(self, path: str, mode: int, fi: FuseFileInfo):
        if path in self.dirs or path in self.files:
            raise FuseError(errno.EEXIST)

        self.files[path] = io.BytesIO()

    def unlink(self, path: str):
        if path not in self.files:
            raise FuseError(errno.ENOENT)

        del self.files[path]

    def rmdir(self, path: str):
        if path not in self.dirs:
            raise FuseError(errno.ENOENT)

        for d in itertools.chain(self.dirs, self.files):
            if osp.dirname(d) == path:
                raise FuseError(errno.ENOTEMPTY)

        self.dirs.remove(path)

    def truncate(self, path: str, length: int, fi: FuseFileInfo):
        if path not in self.files:
            raise FuseError(errno.ENOENT)

        self.files[path].truncate(length)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def main():
    with MemoryFuse() as mf:
        mf.main(['mountit', '/tmp/mnt', '-f', '-s'])


if __name__ == "__main__":
    main()

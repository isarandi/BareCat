import binascii
import glob
import io
import os.path as osp
import shutil

from barecat.indexing import IndexReader, IndexWriter


class Reader:
    def __init__(self, path, decoder=None):
        self.index = IndexReader(f'{path}-sqlite-index')
        shard_names = sorted(glob.glob(f'{path}-shard-*'))
        self.shard_files = [open(p, mode='rb') for p in shard_names]
        self.decoder = decoder if decoder is not None else lambda x: x

    def __getitem__(self, path):
        shard, offset, size, crc32 = self.index[path]
        return self.read_from_address(shard, offset, size, crc32)

    def open(self, path):
        shard, offset, size, crc32 = self.index[path]
        return FileSection(self.shard_files[shard], offset, size)

    def read_nth(self, n):
        path, (shard, offset, size, crc32) = self.index.get_nth(n)
        return path, self.read_from_address(shard, offset, size, crc32)

    def read_from_address(self, shard, offset, size, expected_crc32=None):
        try:
            shard_file = self.shard_files[shard]
        except IndexError:
            raise IndexError(f"Shard {shard} not found") from None
        shard_file.seek(offset)
        data = shard_file.read(size)

        if expected_crc32 is not None:
            crc32 = binascii.crc32(data)
            if crc32 != expected_crc32:
                path = self.index.reverse_lookup(shard, offset)
                raise ValueError(
                    f"CRC32 mismatch for {path}. Expected {expected_crc32}, got {crc32}")

        return self.decoder(data)

    def items(self):
        for path, (shard, offset, size, crc32) in self.index.items():
            yield path, self.read_from_address(shard, offset, size, crc32)

    def items_random(self):
        for path, (shard, offset, size, crc32) in self.index.items_random():
            yield path, self.read_from_address(shard, offset, size, crc32)

    def close(self):
        self.index.close()
        for f in self.shard_files:
            f.close()

    def __contains__(self, path):
        return path in self.index

    def __len__(self):
        return len(self.index)

    def __iter__(self):
        yield from iter(self.index)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class Writer:
    def __init__(self, path, shard_size=None, overwrite=False, encoder=None, append=False):
        self.path = path
        self.shard_size = shard_size
        self.index = IndexWriter(
            f'{self.path}-sqlite-index', overwrite=overwrite, append=append)

        if append:
            self.i_shard = max(0, len(glob.glob(f'{self.path}-shard-*')) - 1)
            self.shard_file = open(
                f'{self.path}-shard-{self.i_shard:05d}', mode='ab')
        else:
            self.i_shard = 0
            self.shard_file = open(
                f'{self.path}-shard-{self.i_shard:05d}', mode='wb' if overwrite else 'xb')

        self.encoder = encoder if encoder is not None else lambda x: x

    def __setitem__(self, path, content):
        packed_content = self.encoder(content)
        size = len(packed_content)
        self.add_by_fileobj(path, io.BytesIO(packed_content), size, bufsize=size)

    def add_by_path(self, path, path_transform=None):
        store_path = path_transform(path) if path_transform else path
        size = osp.getsize(path)
        with open(path, 'rb') as in_file:
            self.add_by_fileobj(store_path, in_file, size)

    def add_by_fileobj(self, path, fileobj, size, bufsize=shutil.COPY_BUFSIZE):
        if self.shard_size is not None:
            if size > self.shard_size:
                raise ValueError(f'File "{path}" is too large to fit into a shard')
            if self.shard_file.tell() + size > self.shard_size:
                self.shard_file.close()
                self.i_shard += 1
                self.shard_file = open(f'{self.path}-shard-{self.i_shard:05d}', 'wb')

        offset = self.shard_file.tell()
        crc32 = 0
        while chunk := fileobj.read(bufsize):
            self.shard_file.write(chunk)
            crc32 = binascii.crc32(chunk, crc32)
        self.index.add_item(path, self.i_shard, offset, size, crc32)

    def close(self):
        self.index.close()
        self.shard_file.close()

    def __contains__(self, path):
        return path in self.index

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class FileSection:
    def __init__(self, file, start, size):
        self.file = file
        self.start = start
        self.end = start + size
        self.position = start

    def read(self, size=-1):
        size = min(size, self.end - self.position)
        if size == -1:
            size = self.end - self.position

        self.file.seek(self.position)
        data = self.file.read(size)

        self.position += len(data)
        return data

    def readline(self, size=-1):
        size = min(size, self.end - self.position)
        if size == -1:
            size = self.end - self.position

        self.file.seek(self.position)
        data = self.file.readline(size)

        self.position += len(data)
        return data

    def tell(self):
        return self.position

    def seek(self, offset, whence=0):
        if whence == 0:
            self.position = self.start + offset
        elif whence == 1:
            self.position += offset
        elif whence == 2:
            self.position = self.end + offset

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

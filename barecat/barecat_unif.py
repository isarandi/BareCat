import binascii
import glob
import os
import os.path as osp
import shutil

import barecat.util
from barecat.indexing_unif import Index, PathAlreadyInBareCatError
import signal
import bz2
import contextlib


class BareCat:
    def __init__(
            self, path, shard_size=None, readonly=True, overwrite=False, auto_codec=False):
        if path.endswith('-sqlite-index'):
            path = path.removesuffix('-sqlite-index')

        if not readonly and overwrite:
            try:
                barecat.util.remove(path)
            except FileNotFoundError:
                pass

        self.path = path
        self.shard_size = shard_size
        self.readonly = readonly
        self.index = Index(f'{self.path}-sqlite-index', readonly=self.readonly)
        self.rename_shards()

        shard_names = sorted(glob.glob(f'{self.path}-shard-*'))
        shard_files_nonlast = [open(p, mode='rb') for p in shard_names[:-1]]
        last_shard_file = open(
            f'{self.path}-shard-{len(shard_files_nonlast):05d}',
            mode='rb' if self.readonly else 'a+b')
        self.shard_files = shard_files_nonlast + [last_shard_file]
        if not self.readonly:
            last_shard_file.truncate(self.index.get_shard_size(len(self.shard_files) - 1))

        self.codecs = {}
        if auto_codec:
            import barecat.codecs as bcc
            self.register_codec(['.jpg', '.jpeg'], bcc.encode_jpeg, bcc.decode_jpeg)
            self.register_codec(['.msgpack'], bcc.encode_msgpack_np, bcc.decode_msgpack_np)
            self.bz_compressor = bz2.BZ2Compressor(9)
            self.register_codec(
                ['.bz2'], self.bz_compressor.compress, bz2.decompress, nonfinal=True)

    # READING
    def __getitem__(self, path):
        try:
            shard, offset, size, crc32 = self.index[path]
        except KeyError as e:
            raise FileNotFoundError(path) from e

        data = self.read_from_address(shard, offset, size, crc32)
        return self.decode(path, data)

    def read_from_address(self, shard, offset, size, expected_crc32=None):
        shard_file = self.shard_files[shard]
        shard_file.seek(offset)
        data = shard_file.read(size)

        if expected_crc32 is not None:
            crc32 = binascii.crc32(data)
            if crc32 != expected_crc32:
                path = self.index.reverse_lookup(shard, offset, size)
                raise ValueError(
                    f"CRC32 mismatch for {path}. Expected {expected_crc32}, got {crc32}")
        return data

    def items(self, sorted=True):
        for path, (shard, offset, size, crc32) in self.index.items(sorted=sorted):
            data = self.read_from_address(shard, offset, size, crc32)
            yield path, self.decode(path, data)

    def items_random(self):
        for path, (shard, offset, size, crc32) in self.index.items_random():
            data = self.read_from_address(shard, offset, size, crc32)
            yield path, self.decode(path, data)

    def open(self, path, mode='r'):
        if mode != 'r':
            raise NotImplementedError('Only read mode is supported for opening files')
        shard, offset, size, crc32 = self.index[path]
        return FileSection(self.shard_files[shard], offset, size)

    # WRITING
    def __setitem__(self, path, content):
        if self.readonly:
            raise ValueError('Cannot add to a read-only BareCat')
        self.add(path, data=self.encode(path, content))

    def add_by_path(self, path, path_transform=None):
        if self.readonly:
            raise ValueError('Cannot add to a read-only BareCat')
        store_path = path_transform(path) if path_transform else path
        size = osp.getsize(path)
        with open(path, 'rb') as in_file:
            self.add(store_path, fileobj=in_file, size=size)

    def add(self, path, /, data=None, fileobj=None, size=None, bufsize=shutil.COPY_BUFSIZE):
        if self.readonly:
            raise ValueError('Cannot add to a read-only BareCat')

        # if path in self.index:
        #     raise PathAlreadyInBareCatError(path)

        if data is None and (fileobj is None or size is None):
            raise ValueError('Either data or fileobj+size must be provided')
        if data is not None and (fileobj is not None or size is not None):
            raise ValueError('Both data and fileobj cannot be provided')

        if size is None:
            size = len(data)

        shard_file = self.shard_files[-1]
        shard_file.seek(0, os.SEEK_END)

        if self.shard_size is not None:
            if size > self.shard_size:
                # TODO we may implement spreading one member file over multiple shards
                # but this is not very relevant because the shard size is usually much
                # larger than the size of a single file
                raise ValueError(f'File "{path}" is too large to fit into a shard')
            if shard_file.tell() + size > self.shard_size:
                # Reopen in read mode, as this shard is now done for writing
                shard_file.close()
                self.shard_files[-1] = open(self.shard_files[-1].name, mode='rb')
                shard_file = open(f'{self.path}-shard-{len(self.shard_files):05d}', 'a+b')
                self.shard_files.append(shard_file)

        offset = shard_file.tell()

        try:
            if data is not None:
                shard_file.write(data)
                crc32 = binascii.crc32(data)
            else:
                assert fileobj is not None
                crc32 = 0
                while chunk := fileobj.read(bufsize):
                    shard_file.write(chunk)
                    crc32 = binascii.crc32(chunk, crc32)

            self.index.add_item(path, len(self.shard_files) - 1, offset, size, crc32)
        except PathAlreadyInBareCatError:
            shard_file.truncate(offset)
            raise
        finally:
            # There was an exception while writing the shard, so the shard may contain data that is
            # not accounted for in the index. So we truncate the shard file back.
            # if path not in self.index:
            #    shard_file.truncate(offset)
            pass

    # DELETION
    def __delitem__(self, path):
        if self.readonly:
            raise ValueError('Cannot delete from a read-only BareCat')
        del self.index[path]

    def delete_recursive(self, dirpath):
        if self.readonly:
            raise ValueError('Cannot delete from a read-only BareCat')
        self.index.delete_recursive(dirpath)

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

    # DEFRAGMENTATION, CONSISTENCY CHECKS
    def defragment(self):
        if self.readonly:
            raise ValueError('Cannot defragment a read-only BareCat')

        new_shard = 0
        new_offset = 0
        for path, (shard, offset, size, crc32) in self.index.items(sorted=True):
            if new_offset + size > self.shard_size:
                self.shard_files[new_shard].truncate(new_offset)
                new_shard += 1
                new_offset = 0

            if not (new_shard == shard and new_offset == offset):
                has_overlap = shard == new_shard and new_offset + size > offset
                if has_overlap:
                    data = self.read_from_address(shard, offset, size)

                try:
                    if has_overlap:
                        self.shard_files[new_shard].seek(new_offset)
                        self.shard_files[new_shard].write(data)
                    else:
                        src_section = FileSection(self.shard_files[shard], offset, size)
                        dst_section = FileSection(self.shard_files[new_shard], new_offset, size)
                        shutil.copyfileobj(src_section, dst_section, length=size)

                    self.index.move_item(path, new_shard, new_offset)
                except BaseException:
                    if has_overlap:
                        # try to write the data back to the original location
                        self.shard_files[shard].seek(offset)
                        self.shard_files[shard].write(data)

            new_offset += size

        self.shard_files[new_shard].truncate(new_offset)
        for i in range(new_shard + 1, len(self.shard_files)):
            self.shard_files[i].close()
            os.remove(self.shard_files[i].name)

    def needs_defragmentation(self):
        # check if total size of shards is larger than the sum of the sizes of the files in index
        total_size_of_shards = sum(osp.getsize(f.name) for f in self.shard_files)
        total_size_in_index = self.index.get_dir_info('')[0]
        return total_size_of_shards > total_size_in_index

    def get_corrupt_paths(self):
        corrupt_paths = []

        for path, (shard, offset, size, expected_crc32) in self.index.items():
            shard_file = self.shard_files[shard]
            shard_file.seek(offset)
            crc32 = 0
            while chunk := shard_file.read(min(size, shutil.COPY_BUFSIZE)):
                crc32 = binascii.crc32(chunk, crc32)
                size -= len(chunk)

            if crc32 != expected_crc32:
                corrupt_paths.append(path)

        return corrupt_paths

    def verify_integrity(self, fast=False):
        if not fast:
            corrupt_paths = self.get_corrupt_paths()
            if corrupt_paths:
                raise ValueError(f'Corrupt files: {corrupt_paths}')
        else:
            path, (shard, offset, size, crc32) = self.index.get_last_file()
            self.read_from_address(shard, offset, size, crc32)

        self.index.verify_integrity()

        if self.needs_defragmentation():
            raise ValueError('Shards need defragmentation')

    def rename_shards(self):
        shard_names = sorted(glob.glob(f'{self.path}-*-of-*'))
        for i, shard_name in enumerate(shard_names):
            new_name = f'{self.path}-shard-{i:05d}'
            os.rename(shard_name, new_name)

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

    def write(self, data):
        if self.position + len(data) > self.end:
            raise ValueError('Cannot write past the end of the section')

        self.file.seek(self.position)
        self.file.write(data)
        self.position += len(data)

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

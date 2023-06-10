import glob
import hashlib
import io
import json
import os
import os.path as osp
import shutil
import sqlite3


class Reader:
    def __init__(self, path):
        self.index = IndexReader(f'{path}-sqlite-index')
        shard_names = sorted(glob.glob(f'{path}-*-of-*'))
        self.shard_files = [open(p, mode='rb') for p in shard_names]

    def __getitem__(self, path):
        address = self.index[path]
        return self.read_from_address(address)

    def open(self, path):
        shard, offset, size = self.index[path]
        shard_file = self.shard_files[shard]
        return FileSection(shard_file, offset, size)

    def read_from_address(self, address):
        shard, offset, size = address
        shard_file = self.shard_files[shard]
        shard_file.seek(offset)
        return shard_file.read(size)

    def items(self):
        for path, address in self.index.items():
            yield path, self.read_from_address(address)

    def items_random(self):
        for path, address in self.index.items_random():
            yield path, self.read_from_address(address)

    def listdir(self, path):
        return self.index.listdir(path)

    def walk(self, dirpath=''):
        return self.index.walk(dirpath)

    def get_subtree_size(self, dirpath):
        return self.index.get_subtree_size(dirpath)

    def get_subtree_file_count(self, dirpath):
        return self.index.get_subtree_file_count(dirpath)

    def get_file_size(self, path):
        return self.index.get_file_size(path)

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
    def __init__(self, path, shard_size=None, write_checksums=True, overwrite=False):
        self.path = path
        self.shard_size = shard_size
        self.index = IndexWriter(f'{self.path}-sqlite-index', overwrite=overwrite)
        self.i_shard = 0
        self.shard_file = open(
            f'{self.path}-{self.i_shard:05d}', mode='wb' if overwrite else 'xb')
        self.write_checksums = write_checksums

    def add_by_content(self, path, file_content):
        self.add_by_fileobj(path, io.BytesIO(file_content), len(file_content))

    def add_by_path(self, path, path_transform=None):
        store_path = path_transform(path) if path_transform else path
        size = osp.getsize(path)
        with open(path, 'rb') as in_file:
            self.add_by_fileobj(store_path, in_file, size)

    def add_by_fileobj(self, path, fileobj, size):
        if self.shard_size is not None:
            if size > self.shard_size:
                raise ValueError(f'File "{path}" is too large to fit into a shard')
            if self.shard_file.tell() + size > self.shard_size:
                self.shard_file.close()
                self.i_shard += 1
                self.shard_file = open(f'{self.path}-{self.i_shard:05d}', 'wb')

        offset = self.shard_file.tell()
        shutil.copyfileobj(fileobj, self.shard_file)
        self.index[path] = (self.i_shard, offset, size)

    def close(self):
        self.index.close()
        self.shard_file.close()
        self.rename_shards()
        if self.write_checksums:
            self.write_checksum_file()

    def rename_shards(self):
        n_shards = self.i_shard + 1
        for i in range(n_shards):
            shard_file = f'{self.path}-{i:05d}'
            renamed_file = f'{self.path}-{i:05d}-of-{n_shards:05d}'
            os.rename(shard_file, renamed_file)
        return n_shards

    def write_checksum_file(self):
        n_shards = self.i_shard + 1
        checksums = {
            f'{i:05d}-of-{n_shards:05d}':
                get_sha1(f'{self.path}-{i:05d}-of-{n_shards:05d}')
            for i in range(n_shards)}
        checksums['sqlite-index'] = get_sha1(f'{self.path}-sqlite-index')
        with open(f'{self.path}-sha1-checksums', 'w') as f:
            json.dump(checksums, f, indent=2)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class IndexReader:
    def __init__(self, path):
        self.conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
        self.cursor = self.conn.cursor()

    def __getitem__(self, path):
        path = normalize_path(path)
        return self.fetch_one('SELECT shard, offset, size FROM files WHERE path=?', (path,))

    def items(self):
        for path, shard, offset, size in self.fetch_iter(
                'SELECT path, shard, offset, size FROM files ORDER BY shard, offset'):
            yield path, (shard, offset, size)

    def items_random(self):
        for path, shard, offset, size in self.fetch_iter(
                'SELECT path, shard, offset, size FROM files ORDER BY RANDOM()'):
            yield path, (shard, offset, size)

    def __len__(self):
        return self.fetch_one('SELECT COUNT(*) FROM files')[0]

    def __iter__(self):
        for path, in self.fetch_iter('SELECT path FROM files ORDER BY shard, offset'):
            yield path

    def iter_random(self):
        for path, in self.fetch_iter('SELECT path FROM files ORDER BY RANDOM()'):
            yield path

    def listdir(self, dirpath):
        subdirs = [
            osp.basename(path)
            for path, in self.fetch_all('SELECT path FROM directories WHERE parent=?', (dirpath,))]
        files = [
            osp.basename(path)
            for path, in self.fetch_all('SELECT path FROM files WHERE parent=?', (dirpath,))]
        return subdirs, files

    def walk(self, root):
        root = normalize_path(root)
        dirs_to_walk = [root]

        while dirs_to_walk:
            dirpath = dirs_to_walk.pop()
            subdirs, files = self.listdir(dirpath)

            yield dirpath, subdirs, files

            for subdir in subdirs:
                subdir_path = osp.join(dirpath, subdir)
                dirs_to_walk.append(subdir_path)

    def get_subtree_size(self, dirpath):
        dirpath = normalize_path(dirpath)
        return self.fetch_one('SELECT total_size FROM directories WHERE path = ?', (dirpath,))[0]

    def get_subtree_file_count(self, dirpath):
        dirpath = normalize_path(dirpath)
        return self.fetch_one(
            'SELECT total_file_count FROM directories WHERE path = ?', (dirpath,))[0]

    def get_last_inserted_item(self):
        return self.fetch_one('SELECT path FROM files ORDER BY shard DESC, offset DESC LIMIT 1')[0]

    def get_file_size(self, path):
        path = normalize_path(path)
        return self.fetch_one('SELECT size FROM files WHERE path=?', (path,))[0]

    def __contains__(self, path):
        return self.fetch_one('SELECT 1 FROM files WHERE path=?', (path,)) is not None

    def fetch_iter(self, query, params=(), buffer_size=32):
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        while rows := cursor.fetchmany(buffer_size):
            yield from rows
        cursor.close()

    def fetch_one(self, query, params=()):
        self.cursor.execute(query, params)
        return self.cursor.fetchone()

    def fetch_all(self, query, params=()):
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class IndexWriter:
    def __init__(self, path, overwrite=False):
        if osp.exists(path):
            if overwrite:
                os.remove(path)
            else:
                raise FileExistsError(path)

        self.conn = sqlite3.connect(path)
        self.cursor = self.conn.cursor()
        self.cursor.execute(f"""
            CREATE TABLE files (
                path TEXT PRIMARY KEY, 
                parent TEXT, 
                shard INTEGER, 
                offset INTEGER, 
                size INTEGER
            )
        """)
        self.cursor.execute(f"""
            CREATE TABLE directories (
                path TEXT PRIMARY KEY, 
                parent TEXT,
                total_size INTEGER DEFAULT 0, 
                total_file_count INTEGER DEFAULT 0
            )
        """)
        self.cursor.execute(
            f'CREATE INDEX idx_directories_parent ON directories (parent)')
        self.cursor.execute(f'CREATE INDEX idx_files_parent ON files (parent)')

    def __setitem__(self, path, address):
        path = normalize_path(path)
        ancestors = get_ancestors(path)
        shard, offset, size = address

        self.cursor.execute('BEGIN TRANSACTION')
        try:
            self.cursor.execute(
                'INSERT INTO files VALUES (?, ?, ?, ?, ?)',
                (path, get_parent(path), *address))

            self.cursor.executemany("""
                INSERT INTO directories (path, parent, total_size, total_file_count) 
                VALUES (?, ?, ?, 1)
                ON CONFLICT(path) DO UPDATE 
                SET total_size = total_size + excluded.total_size, 
                    total_file_count = total_file_count + 1
                """, ((ancestor, get_parent(ancestor), size) for ancestor in ancestors))

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def close(self):
        self.cursor.close()
        self.conn.commit()
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def normalize_path(path):
    return osp.normpath(path).removeprefix('/').removeprefix('.')


def get_parent(path):
    if path == '':
        # root already, has no parent
        return b'\x00'

    partition = path.rpartition('/')
    return partition[0]


def get_ancestors(path):
    components = path.split('/')
    return ('/'.join(components[:i]) for i in range(len(components)))


def get_sha1(path):
    checksum = hashlib.sha1()
    with open(path, 'rb') as f:
        while chunk := f.read(8192):
            checksum.update(chunk)
    return checksum.hexdigest()


class FileSection:
    # Not thread-safe!
    def __init__(self, file, start, size):
        self.file = file
        self.start = start
        self.end = start + size
        self.position = 0

    def read(self, size=-1):
        self.file.seek(self.start + self.position)

        if self.position + size > self.end or size == -1:
            size = self.end - self.position
        result = self.file.read(size)
        self.position += len(result)
        return result

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def tell(self):
        return self.position

    def seek(self, offset, whence=0):
        if whence == 0:
            self.position = offset
        elif whence == 1:
            self.position += offset
        elif whence == 2:
            self.position = self.end + offset

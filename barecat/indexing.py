import itertools
import os
import os.path as osp
import sqlite3


class IndexReader:
    def __init__(self, path):
        self.conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
        self.fetcher = Fetcher(self.conn)

    def __getitem__(self, path):
        path = normalize_path(path)
        return self.fetcher.fetch_one(
            'SELECT shard, offset, size, crc32 FROM files WHERE path=?', (path,))

    def get_nth(self, n):
        path, shard, offset, size, crc32 = self.fetcher.fetch_one(
            'SELECT path, shard, offset, size, crc32 FROM files WHERE rowid=?', (n + 1,))
        return path, (shard, offset, size, crc32)

    def items(self, sorted=False):
        for path, shard, offset, size, crc32 in self.fetcher.fetch_iter(
                'SELECT path, shard, offset, size, crc32 FROM files' +
                (' ORDER BY shard, offset' if sorted else '')):
            yield path, (shard, offset, size, crc32)

    def items_random(self):
        for path, shard, offset, size, crc32 in self.fetcher.fetch_iter(
                'SELECT path, shard, offset, size, crc32 FROM files ORDER BY RANDOM()'):
            yield path, (shard, offset, size, crc32)

    def __len__(self):
        return self.fetcher.fetch_one('SELECT MAX(ROWID)+1 FROM files')[0]

    def __iter__(self):
        for path, in self.fetcher.fetch_iter('SELECT path FROM files ORDER BY shard, offset'):
            yield path

    def iter_random(self):
        for path, in self.fetcher.fetch_iter('SELECT path FROM files ORDER BY RANDOM()'):
            yield path

    def _listdir(self, dirpath):
        subdirs = (
            path for path, in self.fetcher.fetch_iter(
            'SELECT path FROM directories WHERE parent=?', (dirpath,)))
        files = (
            path for path, in self.fetcher.fetch_all(
            'SELECT path FROM files WHERE parent=?', (dirpath,)))
        return subdirs, files

    def get_files_with_size(self, dirpath):
        dirpath = normalize_path(dirpath)
        return self.fetcher.fetch_all('SELECT path, size FROM files WHERE parent=?', (dirpath,))

    def get_subdir_infos(self, dirpath):
        dirpath = normalize_path(dirpath)
        return self.fetcher.fetch_all(
            'SELECT path, total_size, total_file_count, has_subdirs, has_files '
            'FROM directories WHERE parent=?', (dirpath,))

    def get_dir_info(self, dirpath):
        dirpath = normalize_path(dirpath)
        return self.fetcher.fetch_one(
            'SELECT total_size, total_file_count, has_subdirs, has_files '
            'FROM directories WHERE path=?', (dirpath,))

    def listdir_with_sizes_and_counts(self, path):
        path = normalize_path(path)
        cursor = self.conn.cursor()
        subdirs = [
            (osp.basename(path), total_size, total_file_count)
            for path, total_size, total_file_count in self.fetcher.fetch_all(
                'SELECT path, total_size, total_file_count FROM directories WHERE parent=?',
                (path,), cursor)]
        files = [
            (osp.basename(path), size)
            for path, size in self.fetcher.fetch_all(
                'SELECT path, size FROM files WHERE parent=?', (path,), cursor)]
        return subdirs, files

    def listdir(self, dirpath):
        dirpath = normalize_path(dirpath)
        return self._listdir(dirpath)

    def walk(self, root):
        root = normalize_path(root)
        dirs_to_walk = iter([root])

        while (dirpath := next(dirs_to_walk, None)) is not None:
            subdirs, files = self._listdir(dirpath)
            yield dirpath, subdirs, files
            dirs_to_walk = iter(itertools.chain(subdirs, dirs_to_walk))

    def get_subtree_size(self, dirpath):
        dirpath = normalize_path(dirpath)
        return self.fetcher.fetch_one(
            'SELECT total_size FROM directories WHERE path = ?', (dirpath,))[0]

    def get_subtree_file_count(self, dirpath):
        dirpath = normalize_path(dirpath)
        return self.fetcher.fetch_one(
            'SELECT total_file_count FROM directories WHERE path = ?', (dirpath,))[0]

    def get_file_size(self, path):
        path = normalize_path(path)
        return self.fetcher.fetch_one('SELECT size FROM files WHERE path=?', (path,))[0]

    def __contains__(self, path):
        return self.fetcher.fetch_one('SELECT 1 FROM files WHERE path=?', (path,)) is not None

    def reverse_lookup(self, shard, offset):
        return self.fetcher.fetch_one(
            'SELECT path FROM files WHERE shard=? AND offset=?', (shard, offset))[0]

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class Fetcher:
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor()

    def fetch_iter(self, query, params=(), buffer_size=32, cursor=None):
        cursor = self.conn.cursor() if cursor is None else cursor
        cursor.execute(query, params)
        while rows := cursor.fetchmany(buffer_size):
            yield from rows

    def fetch_one(self, query, params=(), cursor=None):
        cursor = self.cursor if cursor is None else cursor
        cursor.execute(query, params)
        return cursor.fetchone()

    def fetch_all(self, query, params=(), cursor=None):
        cursor = self.cursor if cursor is None else cursor
        cursor.execute(query, params)
        return cursor.fetchall()


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
                size INTEGER,
                crc32 INTEGER NULL
            )
        """)
        self.cursor.execute(f"""
            CREATE TABLE directories (
                path TEXT PRIMARY KEY, 
                parent TEXT,
                has_subdirs BOOLEAN DEFAULT FALSE,
                has_files BOOLEAN DEFAULT FALSE,
                total_size INTEGER DEFAULT 0, 
                total_file_count INTEGER DEFAULT 0
            )
        """)
        self.cursor.execute('CREATE INDEX idx_directories_parent ON directories (parent)')
        self.cursor.execute('CREATE INDEX idx_files_parent ON files (parent)')

    def add_item(self, path, shard, offset, size, crc32=None):
        path = normalize_path(path)
        ancestors = get_ancestors(path)
        parent = get_parent(path)
        self.cursor.execute(
            'INSERT INTO files VALUES (?, ?, ?, ?, ?, ?)',
            (path, parent, shard, offset, size, crc32))

        self.cursor.executemany("""
            INSERT INTO directories (
                path, parent, has_subdirs, has_files, total_size, total_file_count)
            VALUES (?, ?, ?, ?, ?, 1)
            ON CONFLICT (path) DO UPDATE SET
                has_subdirs = has_subdirs OR excluded.has_subdirs,
                has_files = has_files OR excluded.has_files,
                total_size = total_size + excluded.total_size,
                total_file_count = total_file_count + 1
            """, ((ancestor, get_parent(ancestor), ancestor != parent, ancestor == parent, size)
                  for ancestor in ancestors))

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
    yield ''
    for i in range(len(path)):
        if path[i] == '/':
            yield path[:i]

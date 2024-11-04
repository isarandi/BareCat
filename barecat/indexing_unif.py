import collections
import contextlib
import itertools
import os.path as osp
import sqlite3
import signal


class Index:
    def __init__(self, path, buffer_size=32, readonly=True):
        is_new = not osp.exists(path)
        try:
            if readonly:
                self.conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
            else:
                self.conn = sqlite3.connect(path)
        except sqlite3.OperationalError as e:
            if not osp.exists(path):
                raise FileNotFoundError(f'Index file {path} does not exist.') from e
            else:
                raise RuntimeError(f'Could not open index {path}') from e

        self.cursor = self.conn.cursor()
        self.fetcher = Fetcher(self.conn, self.cursor, buffer_size=buffer_size)

        if is_new:
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

    # READING
    def __getitem__(self, path):
        path = normalize_path(path)
        res = self.fetcher.fetch_one(
            'SELECT shard, offset, size, crc32 FROM files WHERE path=?', (path,))
        if res is None:
            raise KeyError(path)
        return res

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
        # number of files
        return self.fetcher.fetch_one('SELECT COUNT(*) FROM files')[0]

    def __iter__(self):
        for path, in self.fetcher.fetch_iter('SELECT path FROM files ORDER BY shard, offset'):
            yield path

    def iter_random(self):
        for path, in self.fetcher.fetch_iter('SELECT path FROM files ORDER BY RANDOM()'):
            yield path

    def iter_shard(self, shard):
        for path, in self.fetcher.fetch_iter(
                'SELECT path FROM files WHERE shard=?', (shard,)):
            yield path

    def get_last_file(self):
        last = self.fetcher.fetch_one(
            'SELECT path, shard, offset, size, crc32 FROM files '
            'ORDER BY shard DESC, offset DESC LIMIT 1')
        if last is None:
            raise IndexError('Index is empty')
        path, shard, offset, size, crc32 = last
        return path, (shard, offset, size, crc32)

    def _listdir(self, dirpath):
        subdirs = (
            path for path, in self.fetcher.fetch_iter(
            'SELECT path FROM directories WHERE parent=?', (dirpath,)))
        files = (
            path for path, in self.fetcher.fetch_iter(
            'SELECT path FROM files WHERE parent=?', (dirpath,)))
        return subdirs, files

    def get_files_with_size(self, dirpath):
        dirpath = normalize_path(dirpath)
        return self.fetcher.fetch_all(
            'SELECT path, size FROM files WHERE parent=?', (dirpath,))

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
        return self.fetcher.fetch_one(
            'SELECT size FROM files WHERE path=?', (path,))[0]

    def __contains__(self, path):
        path = normalize_path(path)
        return self.fetcher.fetch_one(
            'SELECT 1 FROM files WHERE path=?', (path,)) is not None

    def reverse_lookup(self, shard, offset, size):
        return self.fetcher.fetch_one(
            'SELECT path FROM files WHERE shard=? AND offset=? AND size=?',
            (shard, offset, size))[0]

    def get_shard_size(self, shard):
        result = self.fetcher.fetch_one(
            'SELECT offset + size FROM files WHERE shard=? ORDER BY offset DESC LIMIT 1',
            (shard,))
        if result is None:
            return 0
        return result[0]

    # WRITING
    def add_item(self, path, shard, offset, size, crc32=None):
        path = normalize_path(path)
        ancestors = get_ancestors(path)
        parent = get_parent(path)

        with transaction(self.cursor):
            try:
                self.cursor.execute(
                    'INSERT INTO files VALUES (?, ?, ?, ?, ?, ?)',
                    (path, parent, shard, offset, size, crc32))
            except sqlite3.IntegrityError as e:
                raise PathAlreadyInBareCatError(path) from e

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

    def add_items(self, items):
        paths = [normalize_path(item[0]) for item in items]
        ancestorss = [get_ancestors(p) for p in paths]
        parents = [get_parent(path) for path in paths]
        sizes = [item[1][2] for item in items]

        with transaction(self.cursor):
            try:
                self.cursor.executemany(
                    'INSERT INTO files VALUES (?, ?, ?, ?, ?, ?)',
                    ((path, parent, shard, offset, size, crc32)
                     for (path, (shard, offset, size, crc32)), parent in zip(items, parents)))
            except sqlite3.IntegrityError as e:
                raise PathAlreadyInBareCatError(paths) from e

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
                      for size, parent, ancestors in zip(sizes, parents, ancestorss)
                      for ancestor in ancestors))

    def move_item(self, path, new_shard, new_offset):
        path = normalize_path(path)
        self.cursor.execute(
            'UPDATE files SET shard=?, offset=? WHERE path=?',
            (new_shard, new_offset, path))

    # DELETING
    def __delitem__(self, path):
        path = normalize_path(path)
        ancestors = get_ancestors(path)
        parent = get_parent(path)
        size = self.get_file_size(path)

        with transaction(self.cursor):
            self.cursor.execute('DELETE FROM files WHERE path=?', (path,))
            self.cursor.executemany("""
                UPDATE directories SET 
                    total_size = total_size - :size,
                    total_file_count = total_file_count - 1
                WHERE path=:dirname
                """, (dict(size=size, dirname=ancestor) for ancestor in ancestors))

            parent_direct_file_count = self.fetcher.fetch_one(
                'SELECT COUNT(*) FROM files WHERE parent=?', (parent,))[0]
            if parent_direct_file_count == 0:
                self.cursor.execute(
                    'UPDATE directories SET has_files=FALSE WHERE path=?',
                    (parent,))
                self.remove_empty_ancestors(parent)

    def delete_items(self, paths):
        paths = [normalize_path(path) for path in paths]
        parents = [get_parent(path) for path in paths]
        sizes = [self.get_file_size(path) for path in paths]
        unique_parents = set(parents)

        with transaction(self.cursor):
            self.cursor.executemany('DELETE FROM files WHERE path=?', ((path,) for path in paths))
            self.cursor.executemany("""
                UPDATE directories SET 
                    total_size = total_size - :size,
                    total_file_count = total_file_count - 1
                WHERE path=:dirname
                """, (dict(size=size, dirname=ancestor)
                      for path, size in zip(paths, sizes)
                      for ancestor in get_ancestors(path)))
            for parent in unique_parents:
                parent_direct_file_count = self.fetcher.fetch_one(
                    'SELECT COUNT(*) FROM files WHERE parent=?', (parent,))[0]
                if parent_direct_file_count == 0:
                    self.cursor.execute(
                        'UPDATE directories SET has_files=FALSE WHERE path=?',
                        (parent,))
                    self.remove_empty_ancestors(parent)

    def remove_empty_ancestors(self, path):
        while path != b'\x00':
            total_file_count = self.get_dir_info(path)[1]
            if total_file_count == 0:
                try:
                    self.cursor.execute(
                        'DELETE FROM directories WHERE path=?', (path,))
                except sqlite3.OperationalError:
                    # the directory was already deleted, this means we already inspected the rest
                    # of the ancestors
                    return
            else:
                # it has files, but does it have subdirs? maybe not,
                # since we may have deleted the last subdir
                has_subdirs = self.fetcher.fetch_one(
                    'SELECT COUNT(*) FROM directories WHERE parent=?', (path,))[0] > 0
                if not has_subdirs:
                    self.cursor.execute(
                        'UPDATE directories SET has_subdirs=FALSE WHERE path=?',
                        (path,))

                # if this one is not empty, then the further ancestors are cannot be empty either
                # so we can return
                return
            path = get_parent(path)

    def delete_recursive(self, dirpath):
        dirpath = normalize_path(dirpath)
        total_size, total_file_count, has_subdirs, has_files = self.get_dir_info(dirpath)
        parent = get_parent(dirpath)

        with transaction(self.cursor):
            self._delete_recursive(dirpath)
            self.cursor.executemany("""
                UPDATE directories SET
                    total_size = total_size - :total_size,
                    total_file_count = total_file_count - :total_file_count,
                WHERE path=:dirname
                """, (dict(total_size=total_size, total_file_count=total_file_count,
                           dirname=ancestor) for ancestor in get_ancestors(dirpath)))
            self.remove_empty_ancestors(parent)

    def _delete_recursive(self, dirpath):
        # this function deletes rows from the files and directories tables
        # but doesn't update the total_size and total_file_count in the directories table
        # that is more efficiently done all at once in delete_recursive
        subdirs = self.fetcher.fetch_all(
            'SELECT path FROM directories WHERE parent=?', (dirpath,))
        self.cursor.execute('DELETE FROM files WHERE parent=?', (dirpath,))
        for subdir in subdirs:
            self._delete_recursive(subdir)
        self.cursor.execute('DELETE FROM directories WHERE path=?', (dirpath,))

    def verify_integrity(self):
        # check if has_subdirs, has_files, total_size, total_file_count are correct
        has_subdirs = set()
        has_files = set()
        total_size = {}
        total_file_count = {}
        for path, (shard, offset, size, crc32) in self.items():
            parent = get_parent(path)
            has_files.add(parent)

            ancestors = get_ancestors(path)
            for ancestor in ancestors:
                if ancestor != parent:
                    has_subdirs.add(ancestor)
                total_size[ancestor] = total_size.get(ancestor, 0) + size
                total_file_count[ancestor] = total_file_count.get(ancestor, 0) + 1

        dirdata = self.fetcher.fetch_all(
            'SELECT path, total_size, total_file_count, has_subdirs, has_files FROM directories')
        for path, total_size_, total_file_count_, has_subdirs_, has_files_ in dirdata:
            if path not in total_size:
                raise ValueError(f'Directory {path} is in index but has no descendant files.')

            if total_size_ != total_size[path]:
                raise ValueError(f'Total size mismatch for {path}')
            if total_file_count_ != total_file_count[path]:
                raise ValueError(f'Total file count mismatch for {path}')
            if has_subdirs_ != (path in has_subdirs):
                raise ValueError(f'has_subdirs mismatch for {path}')
            if has_files_ != (path in has_files):
                raise ValueError(f'has_files mismatch for {path}')

    def close(self):
        self.cursor.close()
        self.conn.commit()
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


@contextlib.contextmanager
def transaction(cursor):
    yield
    # succeeded = False
    # try:
    #     cursor.execute('BEGIN TRANSACTION')
    #     yield
    #     succeeded = True
    # finally:
    #     try:
    #         if succeeded:
    #             cursor.execute('COMMIT')
    #         else:
    #             cursor.execute('ROLLBACK')
    #     except sqlite3.OperationalError:
    #         # Transaction was not begun
    #         pass


class PathAlreadyInBareCatError(Exception):
    def __init__(self, path):
        super().__init__(f'Path {path} is already in the archive')
        self.path = path


class Fetcher:
    def __init__(self, conn, cursor=None, buffer_size=32):
        self.conn = conn
        self.cursor = conn.cursor() if cursor is None else cursor
        self.buffer_size = buffer_size

    def fetch_iter(self, query, params=(), cursor=None):
        cursor = self.conn.cursor() if cursor is None else cursor
        cursor.execute(query, params)
        while rows := cursor.fetchmany(self.buffer_size):
            yield from rows

    def fetch_one(self, query, params=(), cursor=None):
        cursor = self.cursor if cursor is None else cursor
        cursor.execute(query, params, )
        return cursor.fetchone()

    def fetch_all(self, query, params=(), cursor=None):
        cursor = self.cursor if cursor is None else cursor
        cursor.execute(query, params)
        return cursor.fetchall()


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

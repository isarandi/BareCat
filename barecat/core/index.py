import re
from barecat.glob_to_regex import glob_to_regex
import contextlib
import copy
import itertools
import os.path as osp
import sqlite3
from typing import Iterable

import barecat.util
from barecat.common import BarecatDirInfo, BarecatFileInfo, Order
from barecat.exceptions import (
    BarecatError, DirectoryNotEmptyBarecatError, FileExistsBarecatError,
    FileNotFoundBarecatError)
from barecat.util import normalize_path


class Index:
    def __init__(
            self, path, shard_size_limit=None, bufsize=None, readonly=True):
        is_new = not osp.exists(path)
        self.readonly = readonly
        try:
            self.conn = sqlite3.connect(
                f'file:{path}?mode={"ro" if self.readonly else "rwc"}', uri=True)
        except sqlite3.OperationalError as e:
            if readonly and not osp.exists(path):
                raise FileNotFoundError(
                    f'Index file {path} does not exist, so cannot be opened in readonly mode.'
                ) from e
            else:
                raise RuntimeError(f'Could not open index {path}') from e

        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.fetcher = Fetcher(self.conn, self.cursor, bufsize=bufsize)
        self.fetch_one = self.fetcher.fetch_one
        self.fetch_one_or_raise = self.fetcher.fetch_one_or_raise
        self.fetch_all = self.fetcher.fetch_all
        self.fetch_iter = self.fetcher.fetch_iter
        self.fetch_many = self.fetcher.fetch_many

        self._shard_size_limit_cached = None

        if is_new:
            sql_path = osp.join(osp.dirname(__file__), '../sql/schema.sql')
            self.cursor.executescript(barecat.util.read_file(sql_path))

        if not self.readonly:
            self.cursor.execute('PRAGMA recursive_triggers = ON')
            self._triggers_enabled = True
            self._foreign_keys_enabled = True
            if shard_size_limit is not None:
                self.shard_size_limit = shard_size_limit

    # READING
    def lookup_file(self, path, normalized=False):
        if not normalized:
            path = normalize_path(path)
        try:
            return self.fetch_one_or_raise("""
                SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns 
                FROM files WHERE path=?
                """, (path,), rowcls=BarecatFileInfo)
        except LookupError:
            raise FileNotFoundBarecatError(path)

    def lookup_dir(self, dirpath=None):
        dirpath = normalize_path(dirpath)
        try:
            return self.fetch_one_or_raise("""
                SELECT path, num_subdirs, num_files, size_tree, num_files_tree,
                    mode, uid, gid, mtime_ns
                FROM dirs WHERE path=?
                """, (dirpath,), rowcls=BarecatDirInfo)
        except LookupError:
            raise FileNotFoundBarecatError(f'Directory {dirpath} not found in index')

    def lookup(self, path):
        path = normalize_path(path)
        try:
            return self.lookup_file(path)
        except LookupError:
            return self.lookup_dir(path)

    def __len__(self):
        return self.num_files

    @property
    def num_files(self):
        return self.fetch_one("SELECT num_files_tree FROM dirs WHERE path=''")[0]

    @property
    def total_size(self):
        return self.fetch_one("SELECT size_tree FROM dirs WHERE path=''")[0]

    @property
    def num_dirs(self):
        return self.fetch_one("SELECT COUNT(*) FROM dirs")[0]

    def __iter__(self):
        yield from self.iter_all_fileinfos(order=Order.ANY)

    def __contains__(self, path):
        return self.isfile(path)

    def isfile(self, path):
        path = normalize_path(path)
        return self.fetch_one('SELECT 1 FROM files WHERE path=?', (path,)) is not None

    def isdir(self, path):
        path = normalize_path(path)
        return self.fetch_one('SELECT 1 FROM dirs WHERE path=?', (path,)) is not None

    def exists(self, path):
        path = normalize_path(path)
        return self.fetch_one("""
            SELECT 1
            WHERE EXISTS (SELECT 1 FROM files WHERE path = :path)
               OR EXISTS (SELECT 1 FROM dirs WHERE path = :path)
        """, dict(path=path)) is not None

    def iter_all_fileinfos(self, order: Order = Order.ANY, bufsize=None):
        query = """
            SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns
            FROM files"""
        query += order.as_query_text()
        return self.fetch_iter(query, bufsize=bufsize, rowcls=BarecatFileInfo)

    def iter_all_dirinfos(self, order: Order = Order.ANY, bufsize=None):
        query = """
            SELECT path, num_subdirs, num_files, size_tree, num_files_tree,
            mode, uid, gid, mtime_ns FROM dirs"""
        query += order.as_query_text()
        return self.fetch_iter(query, bufsize=bufsize, rowcls=BarecatDirInfo)

    def iter_all_infos(self, order: Order = Order.ANY, bufsize=None):
        query = """
            SELECT path, NULL AS shard, NULL AS offset, size_tree AS size, NULL AS crc32c,
                   mode, uid, gid, mtime_ns, num_subdirs, num_files, num_files_tree, 
                   'dir' AS type
            FROM dirs
            UNION ALL
            SELECT path, shard, offset, size, crc32c,
                   mode, uid, gid, mtime_ns, NULL AS num_subdirs, NULL AS num_files, 
                   NULL AS num_files_tree, 'file' AS type
            FROM files"""
        query += order.as_query_text()
        for row in self.fetch_iter(query, bufsize=bufsize):
            if row['type'] == 'dir':
                yield BarecatDirInfo(
                    path=row['path'], num_subdirs=row['num_subdirs'],
                    num_files=row['num_files'], size_tree=row['size'],
                    num_files_tree=row['num_files_tree'], mode=row['mode'], uid=row['uid'],
                    gid=row['gid'], mtime_ns=row['mtime_ns'])
            else:
                yield BarecatFileInfo(
                    path=row['path'], shard=row['shard'], offset=row['offset'],
                    size=row['size'], crc32c=row['crc32c'], mode=row['mode'], uid=row['uid'],
                    gid=row['gid'], mtime_ns=row['mtime_ns'])

    def iter_all_filepaths(self, order: Order = Order.ANY, bufsize=None):
        for finfo in self.iter_all_fileinfos(order=order, bufsize=bufsize):
            yield finfo.path

    def iter_all_dirpaths(self, order: Order = Order.ANY, bufsize=None):
        for dinfo in self.iter_all_dirinfos(order=order, bufsize=bufsize):
            yield dinfo.path

    def iter_all_paths(self, order: Order = Order.ANY, bufsize=None):
        query = """
            SELECT path FROM dirs
            UNION ALL
            SELECT path FROM files"""
        query += order.as_query_text()
        for row in self.fetch_iter(query, bufsize=bufsize):
            yield row['path']

    ########## Listdir-like methods ##########
    def _as_dirinfo(self, diritem: BarecatDirInfo | str):
        return diritem if isinstance(diritem, BarecatDirInfo) else self.lookup_dir(diritem)

    def _as_fileinfo(self, fileitem: BarecatFileInfo | str):
        return fileitem if isinstance(fileitem, BarecatFileInfo) else self.lookup_file(fileitem)

    def _as_path(self, item: str | BarecatDirInfo | BarecatFileInfo):
        return normalize_path(item) if isinstance(item, str) else item.path

    def list_direct_fileinfos(self, dirpath=None, order: Order = Order.ANY):
        dirpath = normalize_path(dirpath)
        query = """
            SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns
            FROM files WHERE parent=?"""
        query += order.as_query_text()
        return self.fetch_all(query, (dirpath,), rowcls=BarecatFileInfo)

    def list_subdir_dirinfos(self, dirpath=None, order: Order = Order.ANY):
        dirpath = normalize_path(dirpath)
        query = """
            SELECT path, num_subdirs, num_files, size_tree, num_files_tree,
            mode, uid, gid, mtime_ns FROM dirs WHERE parent=?"""
        query += order.as_query_text()
        return self.fetch_all(query, (dirpath,), rowcls=BarecatDirInfo)

    def iter_direct_fileinfos(
            self, diritem: BarecatDirInfo | str, order: Order = Order.ANY, bufsize=None):
        dinfo = self._as_dirinfo(diritem)
        if dinfo.num_files == 0:
            return []
        query = """
            SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns
            FROM files WHERE parent=?"""
        query += order.as_query_text()
        return self.fetch_iter(query, (dinfo.path,), bufsize=bufsize, rowcls=BarecatFileInfo)

    def iter_subdir_dirinfos(
            self, diritem: BarecatDirInfo | str, order: Order = Order.ANY, bufsize=None):
        dinfo = self._as_dirinfo(diritem)
        if dinfo.num_subdirs == 0:
            return []
        query = """
            SELECT path, num_subdirs, num_files, size_tree, num_files_tree, mode, uid, gid,
            mtime_ns
            FROM dirs WHERE parent=?"""
        query += order.as_query_text()
        return self.fetch_iter(query, (dinfo.path,), bufsize=bufsize, rowcls=BarecatDirInfo)

    def listdir_names(self, diritem: BarecatDirInfo | str, order: Order = Order.ANY):
        dinfo = self._as_dirinfo(diritem)
        query = """
            SELECT path FROM dirs WHERE parent=:parent
            UNION ALL
            SELECT path FROM files WHERE parent=:parent"""
        query += order.as_query_text()
        rows = self.fetch_all(query, dict(parent=dinfo.path))
        return [osp.basename(row['path']) for row in rows]

    def listdir_infos(self, diritem: BarecatDirInfo | str, order: Order = Order.ANY):
        dinfo = self._as_dirinfo(diritem)
        return self.list_subdir_dirinfos(dinfo.path, order=order) + self.list_direct_fileinfos(
            dinfo.path, order=order)

    def iterdir_names(self, diritem: BarecatDirInfo | str, order: Order = Order.ANY, bufsize=None):
        dinfo = self._as_dirinfo(diritem)
        query = """
            SELECT path FROM dirs WHERE parent=?
            UNION ALL
            SELECT path FROM files WHERE parent=?"""
        query += order.as_query_text()
        rows = self.fetch_iter(query, (dinfo.path, dinfo.path), bufsize=bufsize)
        return (osp.basename(row['path']) for row in rows)

    def iterdir_infos(self, diritem: BarecatDirInfo | str, order: Order = Order.ANY, bufsize=None):
        dinfo = self._as_dirinfo(diritem)
        return itertools.chain(
            self.iter_subdir_dirinfos(dinfo, order=order, bufsize=bufsize),
            self.iter_direct_fileinfos(dinfo, order=order, bufsize=bufsize))

    # glob paths
    def raw_glob_paths(self, pattern, order: Order = Order.ANY):
        pattern = normalize_path(pattern)
        query = """
            SELECT path FROM dirs WHERE path GLOB :pattern
            UNION ALL
            SELECT path FROM files WHERE path GLOB :pattern"""
        query += order.as_query_text()
        rows = self.fetch_all(query, dict(pattern=pattern))
        return [row['path'] for row in rows]

    def raw_iterglob_paths(self, pattern, order: Order = Order.ANY, only_files=False, bufsize=None):
        pattern = normalize_path(pattern)
        if only_files:
            query = """
                SELECT path FROM files WHERE path GLOB :pattern"""
        else:
            query = """
                SELECT path FROM dirs WHERE path GLOB :pattern
                UNION ALL
                SELECT path FROM files WHERE path GLOB :pattern"""
        query += order.as_query_text()
        rows = self.fetch_iter(query, dict(pattern=pattern), bufsize=bufsize)
        return (row['path'] for row in rows)

    def glob_paths(self, pattern, recursive=False, include_hidden=False, only_files=False):
        return list(
            self.iterglob_paths(
                pattern, recursive=recursive, include_hidden=include_hidden, only_files=only_files))

    def iterglob_paths(
            self, pattern, recursive=False, include_hidden=False, bufsize=None, only_files=False):
        if recursive and pattern == '**':
            if only_files:
                yield from self.iter_all_filepaths(bufsize=bufsize)
            else:
                yield from self.iter_all_paths(bufsize=bufsize)
            return

        parts = pattern.split('/')
        num_has_wildcard = sum(1 for p in parts if '*' in p or '?' in p)
        has_no_brackets = '[' not in pattern and ']' not in pattern
        has_no_question = '?' not in pattern

        num_asterisk = pattern.count('*')
        if (recursive and has_no_brackets and has_no_question and num_asterisk == 3 and
                '*' not in pattern.replace('/**/*', '')):
            yield from self.raw_iterglob_paths(
                pattern.replace('/**/*', '/*'), bufsize=bufsize, only_files=only_files)
            return

        if (recursive and has_no_brackets and has_no_question and num_asterisk == 2 and
                pattern.endswith('/**')):
            if not only_files and self.isdir(pattern[:-3]):
                yield pattern[:-3]
            yield from self.raw_iterglob_paths(
                pattern[:-1], bufsize=bufsize, only_files=only_files)
            return

        regex_pattern = glob_to_regex(pattern, recursive=recursive, include_hidden=include_hidden)
        if (not recursive or '**' not in pattern) and num_has_wildcard == 1 and has_no_brackets:
            parts = pattern.split('/')
            i_has_wildcard = next(i for i, p in enumerate(parts) if '*' in p or '?' in p)
            prefix = '/'.join(parts[:i_has_wildcard])
            wildcard_is_in_last_part = i_has_wildcard == len(parts) - 1
            if wildcard_is_in_last_part:
                info_generator = (
                    self.iter_direct_fileinfos(prefix) if only_files else self.iterdir_infos(
                        prefix))
                for info in info_generator:
                    if re.match(regex_pattern, info.path):
                        yield info.path
            else:
                suffix = '/'.join(parts[i_has_wildcard + 1:])
                further_subdirs_wanted = len(parts) > i_has_wildcard + 2
                for subdirinfo in self.iter_subdir_dirinfos(prefix):
                    if ((further_subdirs_wanted and subdirinfo.num_subdirs == 0) or
                            subdirinfo.num_entries == 0):
                        continue
                    candidate = subdirinfo.path + '/' + suffix
                    if (re.match(regex_pattern, candidate) and
                            ((self.exists(candidate) and not only_files) or
                             self.isfile(candidate))):
                        yield candidate
            return

        for candidate in self.raw_iterglob_paths(
                pattern, only_files=only_files, bufsize=bufsize):
            if re.match(regex_pattern, candidate):
                yield candidate

    ## glob infos
    def raw_iterglob_infos(self, pattern, only_files=False, bufsize=None):
        pattern = normalize_path(pattern)
        yield from self.fetch_iter("""
            SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns 
            FROM files WHERE path GLOB :pattern
            """, dict(pattern=pattern), bufsize=bufsize, rowcls=BarecatFileInfo)
        if only_files:
            return
        yield from self.fetch_iter("""
            SELECT path, num_subdirs, num_files, size_tree, num_files_tree,
                   mode, uid, gid, mtime_ns 
            FROM dirs WHERE path GLOB :pattern
            """, dict(pattern=pattern), bufsize=bufsize, rowcls=BarecatDirInfo)

    def iterglob_infos(
            self, pattern, recursive=False, include_hidden=False, bufsize=None, only_files=False):
        if recursive and pattern == '**':
            if only_files:
                yield from self.iter_all_fileinfos(bufsize=bufsize)
            else:
                yield from self.iter_all_infos(bufsize=bufsize)
            return

        parts = pattern.split('/')
        num_has_wildcard = sum(1 for p in parts if '*' in p or '?' in p)
        has_no_brackets = '[' not in pattern and ']' not in pattern
        has_no_question = '?' not in pattern

        num_asterisk = pattern.count('*')
        if (recursive and has_no_brackets and has_no_question and num_asterisk == 3 and
                '*' not in pattern.replace('/**/*', '')):
            yield from self.raw_iterglob_infos(
                pattern.replace('/**/*', '/*'), bufsize=bufsize, only_files=only_files)
            return

        if (recursive and has_no_brackets and has_no_question and num_asterisk == 2 and
                pattern.endswith('/**')):
            if not only_files and self.isdir(pattern[:-3]):
                yield pattern[:-3]
            yield from self.raw_iterglob_infos(pattern[:-1], bufsize=bufsize, only_files=only_files)
            return

        regex_pattern = glob_to_regex(pattern, recursive=recursive, include_hidden=include_hidden)
        if (not recursive or '**' not in pattern) and num_has_wildcard == 1 and has_no_brackets:
            parts = pattern.split('/')
            i_has_wildcard = next(i for i, p in enumerate(parts) if '*' in p or '?' in p)
            prefix = '/'.join(parts[:i_has_wildcard])
            wildcard_is_in_last_part = i_has_wildcard == len(parts) - 1
            if wildcard_is_in_last_part:
                info_generator = (
                    self.iter_direct_fileinfos(prefix) if only_files else self.iterdir_infos(
                        prefix))
                for info in info_generator:
                    if re.match(regex_pattern, info.path):
                        yield info
            else:
                suffix = '/'.join(parts[i_has_wildcard + 1:])
                further_subdirs_wanted = len(parts) > i_has_wildcard + 2
                for subdirinfo in self.iter_subdir_dirinfos(prefix):
                    if ((further_subdirs_wanted and subdirinfo.num_subdirs == 0) or
                            subdirinfo.num_entries == 0):
                        continue
                    candidate_path = subdirinfo.path + '/' + suffix
                    if re.match(regex_pattern, candidate_path):
                        try:
                            yield (self.lookup_file(candidate_path) if only_files
                                   else self.lookup(candidate_path))
                        except LookupError:
                            pass
            return

        for info in self.raw_iterglob_infos(pattern, only_files=only_files, bufsize=bufsize):
            if re.match(regex_pattern, info):
                yield info

    ## walking
    def walk_infos(self, rootitem, bufsize=32):
        rootinfo = self._as_dirinfo(rootitem)
        dirs_to_walk = iter([rootinfo])

        while (dinfo := next(dirs_to_walk, None)) is not None:
            subdirs = self.iter_subdir_dirinfos(dinfo, bufsize=bufsize)
            files = self.iter_direct_fileinfos(dinfo, bufsize=bufsize)
            yield dinfo, subdirs, files
            dirs_to_walk = iter(itertools.chain(subdirs, dirs_to_walk))

    def walk_names(self, rootitem, bufsize=32):
        for dinfo, subdirs, files in self.walk_infos(rootitem, bufsize=bufsize):
            yield (
                dinfo.path,
                [osp.basename(d.path) for d in subdirs],
                [osp.basename(f.path) for f in files])

    ######################
    def reverse_lookup(self, shard, offset):
        try:
            return self.fetch_one_or_raise(
                'SELECT * FROM files WHERE shard=:shard AND offset=:offset',
                dict(shard=shard, offset=offset), rowcls=BarecatFileInfo)
        except LookupError:
            raise FileNotFoundBarecatError(f'File with shard {shard} and offset {offset} not found')

    def get_last_file(self):
        try:
            return self.fetch_one_or_raise("""
                SELECT path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns 
                FROM files 
                ORDER BY shard DESC, offset DESC LIMIT 1""", rowcls=BarecatFileInfo)
        except LookupError:
            raise LookupError('Index is empty, it has no last file')

    def logical_shard_end(self, shard):
        result = self.fetch_one("""
            SELECT coalesce(MAX(offset + size), 0) as end FROM files WHERE shard=:shard
            """, dict(shard=shard))
        if result is None:
            return 0
        return result[0]

    @property
    def shard_size_limit(self):
        if self._shard_size_limit_cached is None:
            self._shard_size_limit_cached = self.fetch_one(
                "SELECT value_int FROM config WHERE key='shard_size_limit'")[0]
        return self._shard_size_limit_cached

    @shard_size_limit.setter
    def shard_size_limit(self, value):
        if self.readonly:
            raise ValueError('Cannot set shard size limit on a read-only index')
        if isinstance(value, str):
            value = barecat.util.parse_size(value)

        if value == self.shard_size_limit:
            return
        if value < self.shard_size_limit:
            largest_shard_size = max(
                (self.logical_shard_end(i) for i in range(self.num_used_shards)), default=0)
            if value < largest_shard_size:
                # Wants to shrink
                raise ValueError(
                    f'Trying to set shard size limit as {value}, which is smaller than the largest'
                    f' existing shard size {largest_shard_size}.'
                    f' Increase the shard size limit or re-shard the data first.')

        self.cursor.execute("""
            UPDATE config SET value_int=:value WHERE key='shard_size_limit'
            """, dict(value=value))
        self._shard_size_limit_cached = value

    @property
    def num_used_shards(self):
        """Return the number of shards where final, logically empty shards are not counted."""
        return self.fetch_one('SELECT coalesce(MAX(shard), -1) + 1 FROM files')[0]

    # WRITING
    def add_file(self, finfo: BarecatFileInfo):
        try:
            self.cursor.execute("""
                INSERT INTO files (
                    path, shard, offset, size,  crc32c, mode, uid, gid, mtime_ns)
                VALUES (:path, :shard, :offset, :size, :crc32c, :mode, :uid, :gid, :mtime_ns)
                """, finfo.asdict())
        except sqlite3.IntegrityError as e:
            raise FileExistsBarecatError(finfo.path) from e

    def move_file(self, path, new_shard, new_offset):
        path = normalize_path(path)
        self.cursor.execute("""
            UPDATE files
            SET shard = :shard, offset = :offset
            WHERE path = :path""", dict(shard=new_shard, offset=new_offset, path=path))

    def add_dir(self, dinfo: BarecatDirInfo, exist_ok=False):
        if dinfo.path == '' and exist_ok:
            self.cursor.execute("""
                UPDATE dirs SET mode=:mode, uid=:uid, gid=:gid, mtime_ns=:mtime_ns
                 WHERE path=''""", dinfo.asdict())
            return

        maybe_replace = 'OR REPLACE' if exist_ok else ''
        try:
            self.cursor.execute(f"""
                INSERT {maybe_replace} INTO dirs (path, mode, uid, gid, mtime_ns)
                VALUES (:path, :mode, :uid, :gid, :mtime_ns) 
                """, dinfo.asdict())
        except sqlite3.IntegrityError as e:
            raise FileExistsBarecatError(dinfo.path) from e

    def rename(self, old: str | BarecatDirInfo | BarecatFileInfo, new: str):
        if isinstance(old, BarecatFileInfo) or (isinstance(old, str) and self.isfile(old)):
            self.rename_file(old, new)
        elif isinstance(old, BarecatDirInfo) or (isinstance(old, str) and self.isdir(old)):
            self.rename_dir(old, new)
        else:
            raise FileNotFoundBarecatError(old)

    def rename_file(self, old: BarecatFileInfo | str, new: str):
        old_path = self._as_path(old)
        new_path = normalize_path(new)
        if self.exists(new_path):
            raise FileExistsBarecatError(new_path)

        try:
            self.cursor.execute("""
                UPDATE files SET path=:new_path WHERE path=:old_path
                """, dict(old_path=old_path, new_path=new_path))
        except sqlite3.IntegrityError:
            raise FileExistsBarecatError(new_path)

    def rename_dir(self, old: BarecatDirInfo | str, new: str):
        old_path = self._as_path(old)
        new_path = normalize_path(new)
        if old_path == new_path:
            return
        if old_path == '':
            raise BarecatError('Cannot rename the root directory')

        if self.exists(new_path):
            raise FileExistsBarecatError(new_path)

        dinfo = self._as_dirinfo(old)

        # We temporarily disable foreign keys because we are orphaning the files and dirs in the
        # directory
        with self.no_foreign_keys():
            try:
                # This triggers, and updates ancestors, which is good
                # We do this first, in case the new path already exists
                self.cursor.execute("""
                    UPDATE dirs SET path = :new_path WHERE path = :old_path
                    """, dict(old_path=old_path, new_path=new_path))
            except sqlite3.IntegrityError:
                raise FileExistsBarecatError(new_path)

            if dinfo.num_files > 0 or dinfo.num_subdirs > 0:
                with self.no_triggers():
                    if dinfo.num_files_tree > 0:
                        self.cursor.execute(r"""
                            UPDATE files
                            -- The substring starts with the '/' after the old dirpath
                            -- SQL indexing starts at 1
                            SET path = :new_path || substr(path, length(:old_path) + 1) 
                            WHERE path GLOB
                            replace(replace(replace(:old_path, '[', '[[]'), '?', '[?]'), '*', '[*]')
                             || '/*'
                            """, dict(old_path=old_path, new_path=new_path))
                    if dinfo.num_subdirs > 0:
                        self.cursor.execute(r"""
                            UPDATE dirs
                            SET path = :new_path || substr(path, length(:old_path) + 1) 
                            WHERE path GLOB
                            replace(replace(replace(:old_path, '[', '[[]'), '?', '[?]'), '*', '[*]')
                             || '/*'
                            """, dict(old_path=old_path, new_path=new_path))

    # DELETING
    def remove_file(self, item: BarecatFileInfo | str):
        finfo = self._as_fileinfo(item)
        self.cursor.execute('DELETE FROM files WHERE path=?', (finfo.path,))

    def remove_files(self, items: Iterable[BarecatFileInfo | str]):
        finfos = [self._as_fileinfo(x) for x in items]
        self.cursor.executemany("""
            DELETE FROM files WHERE path=:path
            """, (dict(path=f.path) for f in finfos))

    def remove_empty_dir(self, item: BarecatDirInfo | str):
        dinfo = self._as_dirinfo(item)
        if dinfo.num_files != 0 or dinfo.num_subdirs != 0:
            raise DirectoryNotEmptyBarecatError(item)
        self.cursor.execute('DELETE FROM dirs WHERE path=?', (dinfo.path,))

    def remove_recursively(self, item: BarecatDirInfo | str):
        dinfo = self._as_dirinfo(item)
        if dinfo.path == '':
            raise BarecatError('Cannot remove the root directory')

        if dinfo.num_files > 0 or dinfo.num_subdirs > 0:
            with self.no_triggers():
                # First the files, then the dirs, this way foreign key constraints are not violated
                if dinfo.num_files_tree > 0:
                    self.cursor.execute(r"""
                        DELETE FROM files WHERE path GLOB
                        replace(replace(replace(:dirpath, '[', '[[]'), '?', '[?]'), '*', '[*]')
                         || '/*'
                        """, dict(dirpath=dinfo.path))
                if dinfo.num_subdirs > 0:
                    self.cursor.execute(r"""
                        DELETE FROM dirs WHERE path GLOB 
                        replace(replace(replace(:dirpath, '[', '[[]'), '?', '[?]'), '*', '[*]') 
                         || '/*'
                        """, dict(dirpath=dinfo.path))
        # Now delete the directory itself, triggers will update ancestors, etc.
        self.cursor.execute('DELETE FROM dirs WHERE path=?', (dinfo.path,))

    def chmod(self, path, mode):
        path = normalize_path(path)
        self.cursor.execute("""UPDATE files SET mode=? WHERE path=?""", (mode, path))
        if self.cursor.rowcount > 0:
            return

        self.cursor.execute("""UPDATE dirs SET mode=? WHERE path=?""", (mode, path))
        if self.cursor.rowcount == 0:
            raise FileNotFoundBarecatError(f'Path {path} not found in index')

    def chown(self, path, uid, gid):
        path = normalize_path(path)
        self.cursor.execute("""
            UPDATE files SET uid=?, gid=? WHERE path=?
            """, (uid, gid, path))
        if self.cursor.rowcount > 0:
            return

        self.cursor.execute("""
            UPDATE dirs SET uid=?, gid=? WHERE path=?
            """, (uid, gid, path))
        if self.cursor.rowcount == 0:
            raise FileNotFoundBarecatError(f'Path {path} not found in index')

    def update_mtime(self, path, mtime_ns):
        path = normalize_path(path)
        self.cursor.execute("""
            UPDATE files SET mtime_ns = :mtime_ns WHERE path = :path
            """, dict(path=path, mtime_ns=mtime_ns))
        if self.cursor.rowcount > 0:
            return
        self.cursor.execute("""
            UPDATE dirs SET mtime_ns = :mtime_ns WHERE path = :path
            """, dict(path=path, mtime_ns=mtime_ns))
        if self.cursor.rowcount == 0:
            raise FileNotFoundBarecatError(f'Path {path} not found in index')

    def find_space(self, path: BarecatFileInfo | str, size: int):
        finfo = self._as_fileinfo(path)
        requested_space = size - finfo.size
        if requested_space <= 0:
            return finfo

        # need to check if there is space in the shard
        result = self.fetch_one("""
            SELECT offset FROM files 
            WHERE shard = :shard AND offset > :offset
            ORDER BY offset LIMIT 1
            """, dict(shard=finfo.shard, offset=finfo.offset))
        space_available = (
            result['offset'] - finfo.offset if result is not None
            else self.shard_size_limit - finfo.offset)
        if space_available >= requested_space:
            return finfo

        # find first hole large enough:
        result = self.fetch_one("""
            SELECT shard, gap_offset FROM (
                SELECT 
                    shard,
                    (offset + size) AS gap_offset,
                    LEAD(offset, 1, :shard_size_limit) OVER (PARTITION BY shard ORDER BY offset) 
                    AS gap_end
                FROM files)
            WHERE gap_end - gap_offset > :requested_size 
            ORDER BY shard, gap_offset
            LIMIT 1
            """, dict(requested_size=size - finfo.size, shard_size_limit=self.shard_size_limit))
        if result is not None:
            new_finfo = copy.copy(finfo)
            new_finfo.shard = result['shard']
            new_finfo.offset = result['gap_offset']
            return new_finfo

        # Must start new shard
        new_finfo = copy.copy(finfo)
        new_finfo.shard = self.num_used_shards
        new_finfo.offset = 0
        return new_finfo

    def verify_integrity(self):
        is_good = True
        # check if num_subdirs, num_files, size_tree, num_files_tree are correct
        self.cursor.execute(r"""
            CREATE TEMPORARY TABLE temp_dir_stats (
                path TEXT PRIMARY KEY,
                num_files INTEGER DEFAULT 0,
                num_subdirs INTEGER DEFAULT 0,
                size_tree INTEGER DEFAULT 0,
                num_files_tree INTEGER DEFAULT 0)
        """)

        self.cursor.execute(r"""
            INSERT INTO temp_dir_stats (path, num_files, num_subdirs, size_tree, num_files_tree)
            SELECT
                dirs.path,
                -- Calculate the number of files in this directory
                (SELECT COUNT(*)
                 FROM files
                 WHERE files.parent = dirs.path) AS num_files,
            
                -- Calculate the number of subdirectories in this directory
                (SELECT COUNT(*)
                 FROM dirs AS subdirs
                 WHERE subdirs.parent = dirs.path) AS num_subdirs,
            
                -- Calculate the size_tree and num_files_tree using aggregation
                coalesce(SUM(files.size), 0) AS size_tree,
                COUNT(files.path) AS num_files_tree
            FROM dirs LEFT JOIN files ON files.path GLOB
                replace(replace(replace(dirs.path, '[', '[[]'), '?', '[?]'), '*', '[*]') || '/*'
                OR dirs.path = ''
            GROUP BY dirs.path
        """)

        res = self.fetch_many("""
            SELECT 
                dirs.path,
                dirs.num_files,
                temp_dir_stats.num_files AS temp_num_files,
                dirs.num_subdirs,
                temp_dir_stats.num_subdirs AS temp_num_subdirs,
                dirs.size_tree,
                temp_dir_stats.size_tree AS temp_size_tree,
                dirs.num_files_tree,
                temp_dir_stats.num_files_tree AS temp_num_files_tree      
            FROM 
                dirs
            JOIN 
                temp_dir_stats
            ON 
                dirs.path = temp_dir_stats.path
            WHERE 
                NOT (
                    dirs.num_files = temp_dir_stats.num_files AND
                    dirs.num_subdirs = temp_dir_stats.num_subdirs AND
                    dirs.size_tree = temp_dir_stats.size_tree AND
                    dirs.num_files_tree = temp_dir_stats.num_files_tree
                )
        """, bufsize=10)

        if len(res) > 0:
            is_good = False
            print('Mismatch in dir stats:')
            for row in res:
                print('Mismatch:', dict(**row))

        integrity_check_result = self.fetch_all('PRAGMA integrity_check')
        if integrity_check_result[0][0] != 'ok':
            str_result = str([dict(**x) for x in integrity_check_result])
            print('Integrity check failed: \n' + str_result)
            is_good = False
        foreign_keys_check_result = self.fetch_all('PRAGMA foreign_key_check')
        if foreign_keys_check_result:
            str_result = str([dict(**x) for x in integrity_check_result])
            print('Foreign key check failed: \n' + str_result)
            is_good = False

        return is_good

    def merge_from_other_barecat(self, source_index_path, ignore_duplicates=False):
        """Adds the files and directories from another Barecat index to this one,
        typically used during symlink-based merging. That is, the shards in the source Barecat
        are assumed to be simply be placed next to each other, instead of being merged with the
        existing shards in this index.
        For merging the shards themselves, more complex logic is needed, and that method is
        in the Barecat class.
        """
        self.cursor.execute(f"ATTACH DATABASE 'file:{source_index_path}?mode=ro' AS sourcedb")

        # Duplicate dirs are allowed, they will be merged and updated
        self.cursor.execute("""
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
                mode = coalesce(
                    dirs.mode | excluded.mode,
                    coalesce(dirs.mode, 0) | excluded.mode,
                    dirs.mode | coalesce(excluded.mode, 0)),
                uid = coalesce(excluded.uid, dirs.uid),
                gid = coalesce(excluded.gid, dirs.gid),
                mtime_ns = coalesce(
                    max(dirs.mtime_ns, excluded.mtime_ns),
                    max(coalesce(dirs.mtime_ns, 0), excluded.mtime_ns),
                    max(dirs.mtime_ns, coalesce(excluded.mtime_ns, 0)))
            """)
        new_shard_number = self.num_used_shards
        maybe_ignore = 'OR IGNORE' if ignore_duplicates else ''
        self.cursor.execute(f"""
            INSERT {maybe_ignore} INTO files (
                path, shard, offset, size, crc32c, mode, uid, gid, mtime_ns)
            SELECT path, shard + ?, offset, size, crc32c, mode, uid, gid, mtime_ns
            FROM sourcedb.files
            """, (new_shard_number,))
        self.cursor.execute("DETACH DATABASE sourcedb")

        if ignore_duplicates:
            self.update_treestats()
        self.conn.commit()

    def update_treestats(self):
        print('Creating temporary tables for treestats')
        self.cursor.execute(r"""
            CREATE TEMPORARY TABLE tmp_treestats AS
                SELECT 
                    dirs.path,
                    coalesce(SUM(files.size), 0) AS size_tree,
                    COUNT(files.path) AS num_files_tree
                FROM dirs
                LEFT JOIN files ON files.path GLOB
                    replace(replace(replace(dirs.path, '[', '[[]'), '?', '[?]'), '*', '[*]') || '/*'
                    OR dirs.path = ''
                GROUP BY dirs.path
            """)

        print('Creating temporary tables for file counts')
        self.cursor.execute(r"""
            CREATE TEMPORARY TABLE tmp_file_counts AS
                SELECT
                    parent AS path,
                    COUNT(*) AS num_files
                FROM files
                GROUP BY parent
            """)

        print('Creating temporary tables for subdir counts')
        self.cursor.execute(r"""
            CREATE TEMPORARY TABLE tmp_subdir_counts AS
                SELECT
                    parent AS path,
                    COUNT(*) AS num_subdirs
                FROM dirs
                GROUP BY parent
            """)

        print('Updating dirs table with treestats')
        self.cursor.execute(r"""
            UPDATE dirs
            SET
                num_subdirs = COALESCE(sc.num_subdirs, 0),
                size_tree = COALESCE(ts.size_tree, 0),
                num_files_tree = COALESCE(ts.num_files_tree, 0)
            FROM tmp_file_counts fc
            LEFT JOIN tmp_subdir_counts sc ON sc.path = fc.path
            LEFT JOIN tmp_treestats ts ON ts.path = fc.path
            WHERE dirs.path = fc.path;
        """)

    @property
    def _triggers_enabled(self):
        return self.fetch_one("SELECT value_int FROM config WHERE key='use_triggers'")[0] == 1

    @_triggers_enabled.setter
    def _triggers_enabled(self, value):
        self.cursor.execute("""
            UPDATE config SET value_int=:value WHERE key='use_triggers'
            """, dict(value=int(value)))

    @contextlib.contextmanager
    def no_triggers(self):
        prev_setting = self._triggers_enabled
        if not prev_setting:
            yield
            return
        try:
            self._triggers_enabled = False
            yield
        finally:
            self._triggers_enabled = prev_setting

    @property
    def _foreign_keys_enabled(self):
        return self.fetch_one("PRAGMA foreign_keys")[0] == 1

    @_foreign_keys_enabled.setter
    def _foreign_keys_enabled(self, value):
        self.cursor.execute(f"PRAGMA foreign_keys = {'ON' if value else 'OFF'}")

    @contextlib.contextmanager
    def no_foreign_keys(self):
        prev_setting = self._foreign_keys_enabled
        if not prev_setting:
            yield
            return
        try:
            self._foreign_keys_enabled = False
            yield
        finally:
            self._foreign_keys_enabled = True

    def close(self):
        self.cursor.close()
        if not self.readonly:
            self.conn.commit()
            self.conn.execute('VACUUM')
            self.conn.execute('PRAGMA optimize')
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class Fetcher:
    def __init__(self, conn, cursor=None, bufsize=None, row_factory=sqlite3.Row):
        self.conn = conn
        self.cursor = conn.cursor() if cursor is None else cursor
        self.bufsize = bufsize if bufsize is not None else self.cursor.arraysize
        self.row_factory = row_factory

    def fetch_iter(self, query, params=(), cursor=None, bufsize=None, rowcls=None):
        cursor = self.conn.cursor() if cursor is None else cursor
        bufsize = bufsize if bufsize is not None else self.bufsize
        cursor.row_factory = rowcls.row_factory if rowcls is not None else self.row_factory
        cursor.execute(query, params)
        while rows := cursor.fetchmany(bufsize):
            yield from rows

    def fetch_one(self, query, params=(), cursor=None, rowcls=None):
        cursor = self.cursor if cursor is None else cursor
        cursor.row_factory = rowcls.row_factory if rowcls is not None else self.row_factory
        cursor.execute(query, params)
        return cursor.fetchone()

    def fetch_one_or_raise(self, query, params=(), cursor=None, rowcls=None):
        res = self.fetch_one(query, params, cursor, rowcls)
        if res is None:
            raise LookupError()
        return res

    def fetch_all(self, query, params=(), cursor=None, rowcls=None):
        cursor = self.cursor if cursor is None else cursor
        cursor.row_factory = rowcls.row_factory if rowcls is not None else self.row_factory
        cursor.execute(query, params)
        return cursor.fetchall()

    def fetch_many(self, query, params=(), cursor=None, bufsize=None, rowcls=None):
        cursor = self.cursor if cursor is None else cursor
        cursor.row_factory = rowcls.row_factory if rowcls is not None else self.row_factory
        cursor.execute(query, params)
        return cursor.fetchmany(bufsize)

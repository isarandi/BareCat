import argparse
import os.path
import sqlite3

import barecat
import barecat.cython
from barecat.consumed_threadpool import ConsumedThreadPool
from barecat.progbar import progressbar
import glob


def main():
    parser = argparse.ArgumentParser(description='Migrate index database to new version')
    parser.add_argument('path_in', type=str, help='Path to the old barecat')
    parser.add_argument('path_out', type=str, help='Path to the new barecat')

    args = parser.parse_args()
    symlink_shards(args.path_in, args.path_out)
    upgrade_schema(args.path_in, args.path_out)
    update_crc32c(args.path_out)


def upgrade_schema(path_in: str, path_out: str):
    if os.path.exists(path_out + '-sqlite-index'):
        raise FileExistsError(f'Output path {path_out}-sqlite-index already exists')
    with barecat.Index(path_out + '-sqlite-index', readonly=False) as index_out:
        # index_out.no_foreign_keys()
        c = index_out.cursor
        c.execute("COMMIT")
        c.execute('PRAGMA foreign_keys=OFF')
        c.execute('PRAGMA synchronous=OFF')
        c.execute('PRAGMA journal_mode=OFF')
        c.execute('PRAGMA recursive_triggers=ON')
        c.execute(f'ATTACH DATABASE "file:{path_in}-sqlite-index?mode=ro" AS source')
        print('Migrating dir metadata...')
        c.execute("""
            INSERT INTO dirs (path)
            SELECT path FROM source.directories
            WHERE path != ''
            """)
        print('Migrating file metadata...')
        c.execute(f"""
            INSERT INTO files (path, shard, offset, size)
            SELECT path, shard, offset, size
            FROM source.files
            """)

        index_out.conn.commit()
        c.execute("DETACH DATABASE source")


def update_crc32c(path_out: str, n_workers=8):
    with (barecat.cython.BarecatMmapCython(path_out) as sh,
          barecat.Index(path_out + '-sqlite-index', readonly=False) as index):
        c = index.cursor
        c.execute("COMMIT")
        c.execute('PRAGMA synchronous=OFF')
        c.execute('PRAGMA journal_mode=OFF')
        index._triggers_enabled = False

        print('Calculating crc32c for all files to separate database...')
        path_newcrc_temp = f'{path_out}-sqlite-index-newcrc-temp'
        with ConsumedThreadPool(
                temp_crc_writer_main, main_args=(path_newcrc_temp,), max_workers=n_workers,
                queue_size=1024) as ctp:
            for fi in progressbar(index.iter_all_fileinfos(
                    order=barecat.Order.ADDRESS), total=index.num_files):
                ctp.submit(sh.crc32c_from_address, fi.path, args=(fi.shard, fi.offset, fi.size))

        print('Updating crc32c in the barecat index...')
        c.execute(f'ATTACH DATABASE "file:{path_newcrc_temp}?mode=ro" AS newdb')
        c.execute("""
            UPDATE files 
            SET crc32c=newdb.crc32c.crc32c
            FROM newdb.crc32c
            WHERE files.path=newdb.crc32c.path
            """)
        index.conn.commit()
        c.execute("DETACH DATABASE newdb")

    os.remove(path_newcrc_temp)


def temp_crc_writer_main(dbpath, future_iter):
    with sqlite3.connect(dbpath) as conn:
        c = conn.cursor()
        c.execute("PRAGMA synchronous=OFF")
        c.execute("PRAGMA journal_mode=OFF")
        c.execute("CREATE TABLE IF NOT EXISTS crc32c (path TEXT PRIMARY KEY, crc32c INTEGER)")
        for future in future_iter:
            path = future.userdata
            crc32c = future.result()
            c.execute(
                "INSERT INTO crc32c (path, crc32c) VALUES (?, ?)", (path, crc32c))


def symlink_shards(path_in: str, path_out: str):
    shard_paths = glob.glob(f'{path_in}-shard-?????')
    for shard_path in shard_paths:
        i = int(shard_path[-5:])
        make_relative_symlink(shard_path, f'{path_out}-shard-{i:05d}', overwrite=True)


def make_relative_symlink(source, target, overwrite=False):
    relative_source = os.path.relpath(source, start=os.path.dirname(target))
    if os.path.exists(target):
        if overwrite:
            os.remove(target)
        else:
            raise FileExistsError(f'Target {target} already exists')
    os.symlink(relative_source, target)


if __name__ == '__main__':
    main()

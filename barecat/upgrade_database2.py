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
    upgrade_schema(args.path_in, args.path_out)


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
            SELECT path FROM source.dirs
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


if __name__ == '__main__':
    main()

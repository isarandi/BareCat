import argparse
import csv
import pickle
import sys

import barecat
import barecat.cli_impl as impl
from barecat.common import Order
from barecat.defrag import BarecatDefragger
from barecat.util import parse_size


def create():
    parser = argparse.ArgumentParser(
        description='Concatenate files to sharded blobs and create an sqlite index.')
    parser.add_argument('--file', type=str, help='target path', required=True)
    parser.add_argument('--null', action='store_true',
                        help='read input paths from stdin, separated by null bytes as output by '
                             'the find command with the -print0 option (otherwise newlines are '
                             'interpreted as delimiters)')
    parser.add_argument('--workers', type=int, default=None)
    parser.add_argument('--shard-size-limit', type=str, default=None,
                        help='maximum size of a shard in bytes (if not specified, '
                             'all files will be concatenated into a single shard)')
    parser.add_argument('--overwrite', action='store_true', help='overwrite existing files')

    args = parser.parse_args()
    impl.create_from_stdin_paths(
        target_path=args.file, shard_size_limit=parse_size(args.shard_size_limit),
        zero_terminated=args.null, overwrite=args.overwrite, workers=args.workers)


def create_recursive():
    # args are --file, and --shard-size-limit and --workers and --overwrite, and positional args
    # are what you wanna pack in. if ya supply a single posarg thing then ya can use also the
    # flag --strip-root and then the root will be stripped from the paths
    parser = argparse.ArgumentParser(
        description='Concatenate files to sharded blobs and create an sqlite index.')
    parser.add_argument('--file', type=str, help='target path', required=True)
    parser.add_argument('--workers', type=int, default=None)
    parser.add_argument('--shard-size-limit', type=str, default=None,
                        help='maximum size of a shard in bytes (if not specified, '
                             'all files will be concatenated into a single shard)')
    parser.add_argument('--overwrite', action='store_true', help='overwrite existing files')
    parser.add_argument('paths', type=str, nargs='+', help='paths to pack')
    parser.add_argument(
        '--strip-root', action='store_true',
        help='strip the root from the paths (only applicable if a single path is provided)')

    args = parser.parse_args()
    impl.create_recursive(
        target_path=args.file, shard_size_limit=parse_size(args.shard_size_limit),
        roots=args.paths, overwrite=args.overwrite, workers=args.workers,
        strip_root=args.strip_root)


def extract():
    parser = argparse.ArgumentParser(description='Extract files from a barecat archive.')
    parser.add_argument('--file', type=str, help='path to the archive file')
    parser.add_argument('--target-directory', type=str, help='path to the target directory')
    args = parser.parse_args()
    impl.extract(args.file, args.target_directory)


def extract_single():
    parser = argparse.ArgumentParser(description='Extract a single file from a barecat archive.')
    parser.add_argument('--barecat-file', type=str, help='path to the archive file')
    parser.add_argument('--path', type=str, help='path to the file to extract, within the archive')
    args = parser.parse_args()
    with barecat.Barecat(args.barecat_file) as reader:
        sys.stdout.buffer.write(reader[args.path])


def index_to_csv():
    parser = argparse.ArgumentParser(description='Dump the index contents as csv')
    parser.add_argument('file', type=str, help='path to the index file')
    args = parser.parse_args()

    writer = csv.writer(sys.stdout, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
    writer.writerow(['path', 'shard', 'offset', 'size', 'crc32c'])
    with barecat.Index(args.file) as index:
        for f in index.iter_all_fileinfos(order=Order.PATH):
            writer.writerow([f.path, f.shard, f.offset, f.size, f.crc32c])


def index_to_pickledict():
    parser = argparse.ArgumentParser(description='Dump the index contents as a pickled dictionary')
    parser.add_argument('file', type=str, help='path to the index file')
    parser.add_argument('outfile', type=str, help='path to the result file')
    args = parser.parse_args()

    with barecat.Index(args.file) as index_reader:
        dicti = dict(index_reader.items())

    with open(args.outfile, 'xb') as outfile:
        pickle.dump(dicti, outfile)


def merge():
    parser = argparse.ArgumentParser(
        description='Merge existing Barecat archives into one.')
    parser.add_argument('input_paths', metavar='N', type=str, nargs='+',
                        help='paths to the archives to merge')
    parser.add_argument('--output', required=True, help='output path')
    parser.add_argument('--shard-size-limit', type=str, default=None,
                        help='maximum size of a shard in bytes (if not specified, '
                             'all files will be concatenated into a single shard)')
    parser.add_argument('--overwrite', action='store_true',
                        help='overwrite existing files')
    parser.add_argument('--ignore-duplicates', action='store_true',
                        help='if true then if a later file has the same path as an earlier one,'
                             ' skip it; if false then raise an error')

    args = parser.parse_args()
    impl.merge(
        source_paths=args.input_paths, target_path=args.output,
        shard_size_limit=parse_size(args.shard_size_limit),
        overwrite=args.overwrite, ignore_duplicates=args.ignore_duplicates)


def merge_symlink():
    parser = argparse.ArgumentParser(
        description='Merge existing Barecat archives into one.')
    parser.add_argument('input_paths', metavar='N', type=str, nargs='+',
                        help='paths to the archives to merge')
    parser.add_argument('--output', required=True, help='output path')
    parser.add_argument('--overwrite', action='store_true',
                        help='overwrite existing files')
    parser.add_argument('--ignore-duplicates', action='store_true',
                        help='if true then if a later file has the same path as an earlier one,'
                             ' skip it; if false then raise an error')

    args = parser.parse_args()
    impl.merge_symlink(
        source_paths=args.input_paths, target_path=args.output,
        overwrite=args.overwrite, ignore_duplicates=args.ignore_duplicates)


def verify_integrity():
    parser = argparse.ArgumentParser(
        description='Verify the integrity of a Barecat archive, including CRC32C, directory '
                    'stats and no gaps between stored files.')
    parser.add_argument('file', type=str, help='path to the index file')
    parser.add_argument('--quick', action='store_true',
                        help='CRC32C is only verified on the last file')
    args = parser.parse_args()

    with barecat.Barecat(args.file) as bc:
        if not bc.verify_integrity(quick=args.quick):
            print(f'Integrity errors were found.')
            sys.exit(1)


def defrag():
    parser = argparse.ArgumentParser(
        description='Defragment a Barecat archive to remove gaps left by deleted files.')
    parser.add_argument('file', type=str, help='path to the index file')
    parser.add_argument('--quick', action='store_true',
                        help='faster but less thorough attempt at defrag, using the best-fit '
                             'algorithm to move the last files into gaps.')

    args = parser.parse_args()
    with barecat.Barecat(args.file, readonly=False, append_only=False) as bc:
        defragger = BarecatDefragger(bc)
        if defragger.needs_defrag():
            if args.quick:
                defragger.defrag_quick()
            else:
                defragger.defrag()


def archive2barecat():
    parser = argparse.ArgumentParser(
        description='Convert a tar or zip archive to a Barecat archive.')
    # 2 positional args are the tar file and the target barecat file
    parser.add_argument('archive_file', type=str, help='path to the tar or zip file')
    parser.add_argument('barecat_file', type=str, help='path to the target barecat file')

    parser.add_argument('--shard-size-limit', type=str, default=None,
                        help='maximum size of a shard in bytes (if not specified, '
                             'all files will be concatenated into a single shard)')
    parser.add_argument('--overwrite', action='store_true',
                        help='overwrite existing files')
    args = parser.parse_args()
    impl.archive2barecat(
        src_path=args.archive_file, target_path=args.barecat_file,
        shard_size_limit=parse_size(args.shard_size_limit), overwrite=args.overwrite)


def barecat2archive():
    parser = argparse.ArgumentParser(
        description='Convert a Barecat archive to a tar or tar or zip archive.')
    # 2 positional args are the barecat file and the target tar file
    parser.add_argument('barecat_file', type=str, help='path to the barecat file')
    parser.add_argument('archive_file', type=str, help='path to the target archive file')

    args = parser.parse_args()
    impl.barecat2archive(src_path=args.barecat_file, target_path=args.archive_file)


def mount():
    from barecat.fuse.barecat_mount import BarecatFuse
    parser = argparse.ArgumentParser(
        description='Mount a Barecat archive as a FUSE filesystem.')
    parser.add_argument('barecat_file', type=str, help='path to the barecat file')
    parser.add_argument('mount_point', type=str, help='path to the mount point')
    parser.add_argument('--writable', action='store_true', help='mount the filesystem writeable')
    parser.add_argument('--overwrite', action='store_true', help='delete existing barecat')
    parser.add_argument('--append-only', action='store_true', help='append-only mode')
    parser.add_argument('--enable-defrag', action='store_true',
                        help='enables periodic defragmentation of the data shards after '
                             'significant amount of '
                             'deleted space. Has no effect in readonly or append-only mode.')
    parser.add_argument('--shard-size-limit', type=str, default=None,
                        help='maximum size of a shard in bytes (if not specified, '
                             'it is left at the previous setting stored in the index database if '
                             'mounting an existing Barecat, or it created as unlimited)')
    parser.add_argument('--foreground', action='store_true',
                        help='run in the foreground, don\'t daemonize')
    args = parser.parse_args()
    with barecat.Barecat(
            args.barecat_file, readonly=not args.writable, append_only=args.append_only,
            overwrite=args.overwrite, shard_size_limit=args.shard_size_limit) as bc:
        barecat_fuse = BarecatFuse(bc, enable_defrag=args.enable_defrag)
        barecat_fuse.mount(
            args.mount_point, readonly=not args.writable, single_threaded=True,
            foreground=args.foreground)


def print_ncdu_json():
    parser = argparse.ArgumentParser(
        description='Print the contents of a Barecat as JSON in the format expected by ncdu.')
    parser.add_argument('file', type=str, help='path to the index file')
    args = parser.parse_args()
    impl.print_ncdu_json(args.file)

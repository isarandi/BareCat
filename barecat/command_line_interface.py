#!/usr/bin/env python

import argparse
import csv
import pickle
import sys

import barecat


def create():
    parser = argparse.ArgumentParser(
        description='Concatenate files to sharded blobs and create an sqlite index.')
    parser.add_argument('--file', type=str, help='target path', required=True)
    parser.add_argument('--null', action='store_true',
                        help='read input paths from stdin, separated by null bytes as output by '
                             'the find command with the -print0 option (otherwise newlines are '
                             'interpreted as delimiters)')
    parser.add_argument('--workers', type=int, default=None)
    parser.add_argument('--shard-size', type=str, default=None,
                        help='maximum size of a shard in bytes (if not specified, '
                             'all files will be concatenated into a single shard)')
    parser.add_argument('--overwrite', action='store_true', help='overwrite existing files')

    args = parser.parse_args()
    if args.workers is None:
        barecat.create_from_stdin_paths(
            target_path=args.file, shard_size=parse_size(args.shard_size),
            zero_terminated=args.null, overwrite=args.overwrite)
    else:
        barecat.create_from_stdin_paths_workers(
            target_path=args.file, shard_size=parse_size(args.shard_size),
            zero_terminated=args.null, overwrite=args.overwrite, workers=args.workers)


def extract():
    parser = argparse.ArgumentParser(description='Extract files from a barecat archive.')
    parser.add_argument('--file', type=str, help='path to the archive file')
    parser.add_argument('--target-directory', type=str, help='path to the target directory')
    args = parser.parse_args()
    barecat.extract(args.file, args.target_directory)


def extract_single():
    parser = argparse.ArgumentParser(description='Extract a single file from a barecat archive.')
    parser.add_argument('--barecat-file', type=str, help='path to the archive file')
    parser.add_argument('--path', type=str, help='path to the file to extract, within the archive')
    args = parser.parse_args()
    with barecat.BareCat(args.barecat_file) as reader:
        sys.stdout.buffer.write(reader[args.path])


def index_to_csv():
    parser = argparse.ArgumentParser(description='Dump the index contents as csv')
    parser.add_argument('file', type=str, help='path to the index file')
    args = parser.parse_args()

    writer = csv.writer(sys.stdout, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
    writer.writerow(['path', 'shard', 'offset', 'size', 'crc32'])
    with barecat.Index(args.file) as index_reader:
        for path, (shard, offset, size, crc32) in index_reader.items():
            writer.writerow([path, shard, offset, size, crc32])


def index_to_pickledict():
    parser = argparse.ArgumentParser(description='Dump the index contents as a pickled dictionary')
    parser.add_argument('file', type=str, help='path to the index file')
    parser.add_argument('outfile', type=str, help='path to the result file')
    args = parser.parse_args()

    with barecat.Index(args.file) as index_reader:
        dicti = dict(index_reader.items())

    with open(args.outfile, 'xb') as outfile:
        pickle.dump(dicti, outfile)


def parse_size(size):
    if size is None:
        return None
    units = dict(K=1024, M=1024 ** 2, G=1024 ** 3, T=1024 ** 4)
    size = size.upper()

    for unit, factor in units.items():
        if unit in size:
            return int(float(size.replace(unit, "")) * factor)

    return int(size)


def merge():
    parser = argparse.ArgumentParser(
        description='Merge existing BareCat archives into one.')
    parser.add_argument('input_paths', metavar='N', type=str, nargs='+',
                        help='paths to the archives to merge')
    parser.add_argument('--output', required=True, help='output path')
    parser.add_argument('--shard-size', type=str, default=None,
                        help='maximum size of a shard in bytes (if not specified, '
                             'all files will be concatenated into a single shard)')
    parser.add_argument('--overwrite', action='store_true',
                        help='overwrite existing files')

    args = parser.parse_args()
    barecat.merge(
        source_paths=args.input_paths, target_path=args.output,
        shard_size=parse_size(args.shard_size),
        overwrite=args.overwrite)


def merge_symlink():
    parser = argparse.ArgumentParser(
        description='Merge existing BareCat archives into one.')
    parser.add_argument('input_paths', metavar='N', type=str, nargs='+',
                        help='paths to the archives to merge')
    parser.add_argument('--output', required=True, help='output path')
    parser.add_argument('--overwrite', action='store_true',
                        help='overwrite existing files')

    args = parser.parse_args()
    barecat.merge_symlink(
        source_paths=args.input_paths, target_path=args.output,
        overwrite=args.overwrite)


def verify_integrity():
    parser = argparse.ArgumentParser(
        description='Verify the integrity of a BareCat archive, including CRC32, directory '
                    'stats and no gaps between stored files.')
    parser.add_argument('file', type=str, help='path to the index file')
    parser.add_argument('--fast', action='store_true',
                        help='skip verifying CRC32 for all but the last inserted file')
    args = parser.parse_args()

    with barecat.BareCat(args.file) as bc:
        bc.verify_integrity(fast=args.fast)

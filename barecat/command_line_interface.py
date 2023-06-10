#!/usr/bin/env python

import argparse

import barecat


def create():
    parser = argparse.ArgumentParser(
        description='Concatenate files to sharded blobs and create an sqlite index.')
    parser.add_argument('--file', type=str, help='target path', required=True)
    parser.add_argument('--null', action='store_true',
                        help='read input paths from stdin, separated by null bytes as output by '
                             'the find command with the -print0 option (otherwise newlines are '
                             'interpreted as delimiters)')
    parser.add_argument('--shard-size', type=str, default=None,
                        help='maximum size of a shard in bytes (if not specified, '
                             'all files will be concatenated into a single shard)')
    parser.add_argument('--overwrite', action='store_true', help='overwrite existing files')

    args = parser.parse_args()
    barecat.create_from_stdin_paths(
        target_path=args.file, shard_size=parse_size(args.shard_size),
        zero_terminated=args.null, overwrite=args.overwrite)


def extract():
    parser = argparse.ArgumentParser(description='Extract files from a barecat archive.')
    parser.add_argument('--file', type=str, help='path to the archive file')
    parser.add_argument('--target-directory', type=str, help='path to the target directory')
    args = parser.parse_args()
    barecat.extract(args.file, args.target_directory)


def index_to_json():
    parser = argparse.ArgumentParser(description='Dump the index contents as json')
    parser.add_argument('file', type=str, help='path to the index file')
    args = parser.parse_args()

    with barecat.IndexReader(args.file) as index_reader:
        stream = index_reader.items()
        it = iter(stream)
        print('{', end='')
        key, value = next(it)
        print(f'  "{key}": {value}')
        for key, value in it:
            print(f',\n"{key}": {value}', end='')
        print('\n}')


def parse_size(size):
    units = dict(K=1024, M=1024 ** 2, G=1024 ** 3, T=1024 ** 4)
    size = size.upper()

    for unit, factor in units.items():
        if unit in size:
            return int(float(size.replace(unit, "")) * factor)

    return int(size)

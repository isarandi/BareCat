#!/usr/bin/env python

import argparse
import csv
import pickle
import sys
import barecat
import json

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

def extract_single():
    parser = argparse.ArgumentParser(description='Extract a single file from a barecat archive.')
    parser.add_argument('--barecat-file', type=str, help='path to the archive file')
    parser.add_argument('--path', type=str, help='path to the file to extract, within the archive')
    args = parser.parse_args()
    with barecat.Reader(args.barecat_file) as reader:
        sys.stdout.buffer.write(reader[args.path])


def index_to_csv():
    parser = argparse.ArgumentParser(description='Dump the index contents as csv')
    parser.add_argument('file', type=str, help='path to the index file')
    args = parser.parse_args()

    writer = csv.writer(sys.stdout, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
    writer.writerow(['path', 'shard', 'offset', 'size', 'crc32'])
    with barecat.IndexReader(args.file) as index_reader:
        for path, (shard, offset, size, crc32) in index_reader.items():
            writer.writerow([path, shard, offset, size, crc32])


def index_to_pickledict():
    parser = argparse.ArgumentParser(description='Dump the index contents as a pickled dictionary')
    parser.add_argument('file', type=str, help='path to the index file')
    parser.add_argument('outfile', type=str, help='path to the result file')
    args = parser.parse_args()

    with barecat.IndexReader(args.file) as index_reader:
        dicti = dict(index_reader.items())

    with open(args.outfile, 'xb') as outfile:
        pickle.dump(dicti, outfile)


def parse_size(size):
    units = dict(K=1024, M=1024 ** 2, G=1024 ** 3, T=1024 ** 4)
    size = size.upper()

    for unit, factor in units.items():
        if unit in size:
            return int(float(size.replace(unit, "")) * factor)

    return int(size)

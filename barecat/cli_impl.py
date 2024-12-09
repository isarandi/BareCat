import glob
import itertools
import json
import os
import os.path as osp
import shutil
import stat
import sys
import time

import barecat.util
from barecat.archive_formats import get_archive_writer, iter_archive
from barecat.consumed_threadpool import ConsumedThreadPool
from barecat.core import barecat as barecat_
from barecat.core.index import BarecatDirInfo, BarecatFileInfo, Order
from barecat.core.sharder import Sharder
from barecat.progbar import progressbar


def create_from_stdin_paths(
        target_path, shard_size_limit, zero_terminated=False, overwrite=False, workers=None):
    iterator = generate_from_stdin(zero_terminated)
    create(iterator, target_path, shard_size_limit, overwrite, workers)


def create_recursive(target_path, shard_size_limit, roots, overwrite, strip_root, workers=None):
    iterator = generate_from_walks(roots, strip_root)
    create(iterator, target_path, shard_size_limit, overwrite, workers)


def generate_from_stdin(zero_terminated=False):
    if zero_terminated:
        input_paths = iterate_zero_terminated(sys.stdin.buffer)
    else:
        input_paths = (l.rstrip('\n') for l in sys.stdin)

    for input_path in progressbar(input_paths, desc='Packing files', unit=' files'):
        yield input_path, input_path


def generate_from_walks(roots, strip_root):
    for root in roots:
        if not strip_root:
            yield root, osp.basename(root)

        for dirpath, subdirnames, filenames in os.walk(root):
            for entryname in itertools.chain(filenames, subdirnames):
                full_path = osp.join(dirpath, entryname)
                relpath = osp.relpath(full_path, start=root)
                if not strip_root:
                    store_path = osp.join(osp.basename(root), relpath)
                else:
                    store_path = relpath
                yield full_path, store_path


def create(filesys_and_store_path_pairs, target_path, shard_size_limit, overwrite=False, workers=8):
    if workers is None:
        create_without_workers(
            filesys_and_store_path_pairs, target_path, shard_size_limit, overwrite)
    else:
        create_with_workers(
            filesys_and_store_path_pairs, target_path, shard_size_limit, overwrite, workers)


def create_without_workers(
        filesys_and_store_path_pairs, target_path, shard_size_limit, overwrite=False):
    with barecat_.Barecat(
            target_path, shard_size_limit=shard_size_limit, readonly=False, overwrite=overwrite,
            append_only=False) as writer:
        for filesys_path, store_path in filesys_and_store_path_pairs:
            writer.add_by_path(filesys_path, store_path)


def create_with_workers(
        filesys_and_store_path_pairs, target_path, shard_size_limit, overwrite=False, workers=8):
    if overwrite and barecat.util.exists(target_path):
        barecat.util.remove(target_path)

    with Sharder(
            target_path, shard_size_limit=shard_size_limit, readonly=False,
            append_only=False, threadsafe=True, allow_writing_symlinked_shard=False) as sharder:
        with ConsumedThreadPool(
                index_writer_main, main_args=(f'{target_path}-sqlite-index',),
                max_workers=workers) as ctp:
            for filesys_path, store_path in filesys_and_store_path_pairs:
                statresult = os.stat(filesys_path)

                if stat.S_ISDIR(statresult.st_mode):
                    dinfo = BarecatDirInfo(path=store_path)
                    dinfo.fill_from_statresult(statresult)
                    ctp.submit(userdata=dinfo)
                else:
                    finfo = BarecatFileInfo(path=store_path)
                    finfo.fill_from_statresult(statresult)
                    finfo.shard, finfo.offset = sharder.reserve(finfo.size)
                    ctp.submit(
                        sharder.add_by_path, userdata=finfo,
                        args=(filesys_path, finfo.shard, finfo.offset, finfo.size),
                        kwargs=dict(raise_if_cannot_fit=True))


def index_writer_main(target_path, future_iter):
    with barecat_.Index(target_path, readonly=False) as index_writer:
        for future in future_iter:
            info = future.userdata
            if isinstance(info, BarecatDirInfo):
                index_writer.add_dir(info)
                continue

            shard_real, offset_real, size_real, crc32c = future.result()
            info.shard = shard_real
            info.offset = offset_real
            info.crc32c = crc32c

            if info.size != size_real:
                raise ValueError('Size mismatch!')
            index_writer.add_file(info)


def extract(barecat_path, target_directory):
    with barecat_.Barecat(barecat_path) as reader:
        for path_in_archive in progressbar(reader, desc='Extracting files', unit=' files'):
            target_path = osp.join(target_directory, path_in_archive)
            os.makedirs(osp.dirname(target_path), exist_ok=True)
            with open(target_path, 'wb') as output_file:
                shutil.copyfileobj(reader.open(path_in_archive), output_file)


def merge(source_paths, target_path, shard_size_limit, overwrite=False, ignore_duplicates=False):
    with barecat_.Barecat(
            target_path, shard_size_limit=shard_size_limit, readonly=False,
            overwrite=overwrite) as writer:
        for source_path in source_paths:
            print(f'Merging files from {source_path}')
            writer.merge_from_other_barecat(source_path, ignore_duplicates=ignore_duplicates)


def merge_symlink(source_paths, target_path, overwrite=False, ignore_duplicates=False):
    with barecat_.Index(f'{target_path}-sqlite-index', readonly=False) as index_writer:
        i_out_shard = 0
        for source_path in source_paths:
            index_writer.merge_from_other_barecat(
                f'{source_path}-sqlite-index', ignore_duplicates=ignore_duplicates)
            for shard_path in sorted(glob.glob(f'{source_path}-shard-*')):
                os.symlink(
                    os.path.relpath(shard_path, start=os.path.dirname(target_path)),
                    f'{target_path}-shard-{i_out_shard:05d}')
                i_out_shard += 1


def write_index(dictionary, target_path):
    with barecat_.Index(target_path, readonly=False) as index_writer:
        for path, (shard, offset, size) in dictionary.items():
            index_writer.add_file(BarecatFileInfo(path=path, shard=shard, offset=offset, size=size))


def read_index(path):
    with barecat_.Index(path) as reader:
        return dict(reader.items())


def iterate_zero_terminated(fileobj):
    partial_path = b''
    while chunk := fileobj.read(4096):
        parts = chunk.split(b'\x00')
        parts[0] = partial_path + parts[0]
        partial_path = parts.pop()

        for input_path in parts:
            input_path = input_path.decode()
            yield input_path


def archive2barecat(src_path, target_path, shard_size_limit, overwrite=False):
    with barecat_.Barecat(
            target_path, shard_size_limit=shard_size_limit, readonly=False,
            overwrite=overwrite) as writer:
        for file_or_dir_info, fileobj in iter_archive(src_path):
            writer.add(file_or_dir_info, fileobj=fileobj, dir_exist_ok=True)


def barecat2archive(src_path, target_path):
    with barecat_.Barecat(src_path, readonly=True) as bc:
        with get_archive_writer(target_path) as target_archive:
            infos = bc.index.iter_all_infos(order=Order.PATH)
            num_total = bc.index.num_files + bc.index.num_dirs
            for entry in progressbar(infos, total=num_total, desc='Writing', unit=' entries'):
                if isinstance(entry, BarecatDirInfo):
                    target_archive.add(entry)
                else:
                    with bc.open(entry.path) as file_in_barecat:
                        target_archive.add(entry, fileobj=file_in_barecat)


def print_ncdu_json(path):
    timestamp = time.time()
    print(f'[1,1,{{"progname":"barecat","progver":"0.1.2","timestamp":{timestamp}}},')
    with barecat_.Index(path) as index_reader:
        _print_ncdu_json(index_reader, '')
    print(']')


def _print_ncdu_json(index_reader, dirpath):
    basename = '/' if dirpath == '' else osp.basename(dirpath)

    print('[', json.dumps(dict(name=basename, asize=4096, ino=0)), end='')
    subdirs, files = index_reader.listdir_with_sizes_and_counts(dirpath)
    if files:
        filedump = json.dumps([
            dict(name=osp.basename(file), asize=size, dsize=size, ino=0) for file, size in files])
        print(',', filedump[1:-1], end='')
    del files
    for subdir, _, _ in subdirs:
        print(',')
        _print_ncdu_json(index_reader, osp.join(dirpath, subdir))

    print(']', end='')

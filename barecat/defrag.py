from __future__ import annotations

import dataclasses
import os
import time
from typing import TYPE_CHECKING

from barecat.core.index import Order
from barecat.progbar import progressbar

if TYPE_CHECKING:
    from barecat.core.barecat import Barecat


class BarecatDefragger:
    def __init__(self, bc: Barecat):
        self.bc = bc
        self.index = bc.index
        self.shard_size_limit = bc.shard_size_limit
        self.readonly = bc.readonly
        self.shard_files = bc.sharder.shard_files

    def get_gaps(self):
        gaps = self.index.fetch_all("""
            WITH x AS (
                SELECT config.value_int AS shard_size_limit
                FROM config
                WHERE config.key = 'shard_size_limit'
            ),
            first_gaps AS (
                SELECT
                    f.shard,
                    0 AS offset,
                    MIN(f.offset) AS size
                FROM files f, x
                GROUP BY f.shard
            ),
            nonfirst_gaps AS (
                SELECT 
                    f.shard,
                    (f.offset + f.size) AS offset,
                    COALESCE(
                        LEAD(f.offset, 1) OVER (PARTITION BY f.shard ORDER BY f.offset),
                        x.shard_size_limit
                    ) - (f.offset + f.size) AS size
                FROM files f, x
            ),
            all_gaps AS (SELECT * FROM first_gaps UNION ALL SELECT * FROM nonfirst_gaps)
            SELECT shard, offset, size
            FROM all_gaps
            WHERE size > 0
            ORDER BY shard, offset
        """, dict(n_shard_files=len(self.shard_files)), rowcls=FragmentGap)

        empty_shard_gaps = [
            FragmentGap(shard, 0, self.shard_size_limit)
            for shard in range(len(self.shard_files))
            if self.bc.index.logical_shard_end(shard) == 0]
        gaps.extend(empty_shard_gaps)
        gaps.sort(key=lambda gap: (gap.shard, gap.offset))
        return gaps

        # gaps = []
        # prev_end = 0
        # prev_shard = -1
        # for fi in self.index.iter_all_fileinfos(order=Order.ADDRESS):
        #     if fi.shard > prev_shard:
        #         if self.shard_size_limit > prev_end and prev_shard >= 0:
        #             gaps.append(FragmentGap(prev_shard, prev_end, self.shard_size_limit -
        #             prev_end))
        #         for i in range(prev_shard + 1, fi.shard):
        #             gaps.append(FragmentGap(i, 0, self.shard_size_limit))
        #         prev_end = 0
        #     if fi.offset > prev_end:
        #         gaps.append(FragmentGap(fi.shard, prev_end, fi.offset - prev_end))
        #     prev_shard = fi.shard
        #     prev_end = fi.offset + fi.size
        # return gaps

    def needs_defrag(self):
        # check if total size of shards is larger than the sum of the sizes of the files in index
        # the getsize() function may not be fully up to date but this is only a heuristic anyway.
        return self.bc.total_physical_size_seek > self.bc.total_logical_size

    def get_defrag_info(self):
        return self.bc.total_physical_size_seek, self.bc.total_logical_size

    def defrag(self):
        if self.readonly:
            raise ValueError('Cannot defrag a read-only Barecat')

        new_shard = 0
        new_offset = 0

        old_total = self.bc.total_physical_size_seek

        try:
            for i in range(len(self.shard_files)):
                self.bc.sharder.reopen_shard(i, 'r+b')

            file_iter = self.index.iter_all_fileinfos(order=Order.ADDRESS)
            for fi in progressbar(file_iter, total=self.index.num_files, desc='Defragging'):
                if (self.shard_size_limit is not None and new_offset + fi.size >
                        self.shard_size_limit):
                    self.shard_files[new_shard].truncate(new_offset)
                    self.bc.sharder.reopen_shard(new_shard, 'rb')
                    new_shard += 1
                    new_offset = 0

                if not (new_shard == fi.shard and new_offset == fi.offset):
                    shift_n_bytes(
                        self.shard_files[fi.shard], self.shard_files[new_shard],
                        fi.offset, new_offset, fi.size)
                    self.index.move_file(fi.path, new_shard, new_offset)

                new_offset += fi.size

            # Truncate the last shard to uts real size (the others are truncated already)
            self.shard_files[new_shard].truncate(new_offset)
            # Close and delete all shards after the last one
            for i in range(new_shard + 1, len(self.shard_files)):
                self.shard_files[i].close()
                os.remove(self.shard_files[i].name)
            del self.shard_files[new_shard + 1:]

            new_total = self.bc.total_physical_size_seek
            return old_total - new_total
        finally:
            self.bc.sharder.reopen_shards()

    def defrag_quick(self, time_max_seconds=5):
        if self.readonly:
            raise ValueError('Cannot defrag a read-only Barecat')

        start_time = time.monotonic()
        # Collect all gaps in the shards
        gaps = self.get_gaps()
        freed_space = 0
        try:
            for i in range(len(self.shard_files)):
                self.bc.sharder.reopen_shard(i, 'r+b')

            for fi in self.index.iter_all_fileinfos(order=Order.ADDRESS | Order.DESC):
                moved = self.move_to_earlier_gap(fi, gaps)
                if not moved or time.monotonic() - start_time > time_max_seconds:
                    # We stop when we reach the first file that cannot be moved to an earlier gap
                    break
                freed_space += fi.size

            self.bc.truncate_all_to_logical_size()
        finally:
            self.bc.sharder.reopen_shards()

        return freed_space

    def move_to_earlier_gap(self, fi, gaps):
        for i_gap, gap in enumerate(gaps):
            if gap.shard > fi.shard or (gap.shard == fi.shard and gap.offset >= fi.offset):
                # reached the gap that is after the file, no move is possible
                return False
            if gap.size >= fi.size:
                shift_n_bytes(
                    self.shard_files[fi.shard], self.shard_files[gap.shard], fi.offset,
                    gap.offset, fi.size)
                self.index.move_file(fi.path, gap.shard, gap.offset)
                gap.size -= fi.size
                gap.offset += fi.size
                if gap.size == 0:
                    del gaps[i_gap]
                return True
        return False


def shift_n_bytes(src_file, dst_file, src_offset, dst_offset, length, bufsize=64 * 1024):
    if src_file == dst_file and src_offset < dst_offset:
        raise ValueError('This function can only shift left'
                         ' because defragging is done towards the left')

    bytes_to_copy = length
    while bytes_to_copy > 0:
        src_file.seek(src_offset)
        data = src_file.read(min(bufsize, bytes_to_copy))
        if not data:
            raise ValueError('Unexpected EOF')

        dst_file.seek(dst_offset)
        dst_file.write(data)

        len_data = len(data)
        src_offset += len_data
        dst_offset += len_data
        bytes_to_copy -= len_data


@dataclasses.dataclass
class FragmentGap:
    shard: int
    offset: int
    size: int

    @classmethod
    def row_factory(cls, cursor, row):
        field_names = [d[0] for d in cursor.description]
        return cls(**dict(zip(field_names, row)))

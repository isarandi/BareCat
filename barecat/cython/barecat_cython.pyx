# distutils: language = c
# cython: language_level = 3
import errno

import numpy as np
cimport numpy as np
np.import_array()

from libc.stdint cimport uint32_t

cdef extern from "numpy/arrayobject.h":
    void PyArray_ENABLEFLAGS(np.ndarray arr, int flags)

cdef extern from "crc32c.h" nogil:
    uint32_t crc32c(uint32_t crc, const void *buf, size_t len)

cdef extern from "sqlite3.h" nogil:
    ctypedef struct sqlite3
    ctypedef struct sqlite3_stmt

cdef extern from "barecat.h" nogil:
    struct BarecatContext:
        pass

    int barecat_read(BarecatContext *ctx, const char *path, void ** buf, size_t *size)
    int barecat_init(
            BarecatContext *ctx, const char *db_path, const char ** shard_paths, size_t num_shards)
    int barecat_destroy(BarecatContext *ctx)


cdef extern from "barecat_mmap.h" nogil:
    struct BarecatMmapContext:
        pass

    int barecat_mmap_read(BarecatMmapContext *ctx, const char *path, void ** buf, size_t *size)
    int barecat_mmap_read_from_address(BarecatMmapContext *ctx, int shard, size_t offset, size_t size, void **buf_out)
    int barecat_mmap_init(
            BarecatMmapContext *ctx, const char *db_path, const char ** shard_paths, size_t num_shards)
    int barecat_mmap_destroy(BarecatMmapContext *ctx)
    int barecat_mmap_crc32c_from_address(BarecatMmapContext *ctx, int shard, size_t offset, size_t size, uint32_t *crc_out)

import glob
from libc.stdlib cimport malloc, free


cdef class BarecatCython:
    cdef BarecatContext ctx
    cdef bint is_initialized

    def __cinit__(self, str barecat_path):
        self.is_initialized = False
        database_path = f'{barecat_path}-sqlite-index'
        database_path_b = database_path.encode('utf-8')
        cdef const char * database_path_c = database_path_b

        shard_paths = sorted(glob.glob(f'{barecat_path}-shard-?????'))
        shard_paths_bytes = [s.encode('utf-8') for s in shard_paths]
        cdef int num_shards = len(shard_paths)
        cdef const char** shard_paths_c = NULL;

        try:
            shard_paths_c = <char**> malloc(sizeof(char *) * len(shard_paths))
            if shard_paths_c == NULL:
                raise MemoryError("Could not allocate memory for paths")

            for i in range(len(shard_paths)):
                shard_paths_c[i] = shard_paths_bytes[i]

            with nogil:
                rc = barecat_init(&self.ctx, database_path_c, shard_paths_c, num_shards)

            if rc != 0:
                raise RuntimeError(f"Failed to initialize Barecat with path: {barecat_path}")
            self.is_initialized = True
        finally:
            free(shard_paths_c)



    def read(self, str path):
        if not self.is_initialized:
            raise RuntimeError(f"Called read on uninitialized Barecat")

        path_b = path.encode('utf-8')

        cdef void *buf
        cdef size_t size
        cdef int rc
        cdef const char * path_c = path_b
        with nogil:
            rc = barecat_read(&self.ctx, path_c, &buf, &size)

        if rc == -errno.ENOENT:
            raise FileNotFoundError(f"File '{path}' was not found in Barecat.")
        elif rc != 0:
            raise RuntimeError(f"Failed to read file '{path}' from Barecat.")

        cdef np.ndarray[np.uint8_t, ndim=1, mode='c'] arr = np.PyArray_SimpleNewFromData(
            1, [size], np.NPY_UINT8, buf)
        PyArray_ENABLEFLAGS(arr, np.NPY_OWNDATA)
        return memoryview(arr)


    def close(self):
        if self.is_initialized:
            with nogil:
                barecat_destroy(&self.ctx)
            self.is_initialized = False

    def __dealloc__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()



cdef class BarecatMmapCython:
    cdef BarecatMmapContext ctx
    cdef bint is_initialized

    def __cinit__(self, str barecat_path):
        self.is_initialized = False
        database_path = f'{barecat_path}-sqlite-index'
        database_path_b = database_path.encode('utf-8')
        cdef const char * database_path_c = database_path_b

        shard_paths = sorted(glob.glob(f'{barecat_path}-shard-?????'))
        shard_paths_bytes = [s.encode('utf-8') for s in shard_paths]
        cdef int num_shards = len(shard_paths)
        cdef const char** shard_paths_c = NULL;

        try:
            shard_paths_c = <char**> malloc(sizeof(char *) * len(shard_paths))
            if shard_paths_c == NULL:
                raise MemoryError("Could not allocate memory for paths")

            for i in range(len(shard_paths)):
                shard_paths_c[i] = shard_paths_bytes[i]

            with nogil:
                rc = barecat_mmap_init(&self.ctx, database_path_c, shard_paths_c, num_shards)

            if rc != 0:
                raise RuntimeError(f"Failed to initialize Barecat with path: {barecat_path}")
            self.is_initialized = True
        finally:
            free(shard_paths_c)

    def read(self, str path):
        if not self.is_initialized:
            raise RuntimeError(f"Called read on uninitialized Barecat")

        path_b = path.encode('utf-8')

        cdef void *buf
        cdef size_t size
        cdef int rc
        cdef const char * path_c = path_b
        with nogil:
            rc = barecat_mmap_read(&self.ctx, path_c, &buf, &size)

        if rc == -errno.ENOENT:
            raise FileNotFoundError(f"File '{path}' was not found in Barecat.")
        elif rc != 0:
            raise RuntimeError(f"Failed to read file '{path}' from Barecat.")

        if size == 0:
            return memoryview(b"")
        return memoryview(<const char[:size]>buf)


    def read_from_address(self, int shard, size_t offset, size_t size):
        if not self.is_initialized:
            raise RuntimeError(f"Called read on uninitialized Barecat")

        if size == 0:
            return memoryview(b"")

        cdef void *buf
        cdef int rc
        with nogil:
            rc = barecat_mmap_read_from_address(&self.ctx, shard, offset, size, &buf)

        if rc != 0:
            raise RuntimeError(
                f"Failed to read from address {shard}:{offset} with size {size} from Barecat.")

        return memoryview(<const char[:size]>buf)

    def crc32c_from_address(self, int shard, size_t offset, size_t size):
        if not self.is_initialized:
            raise RuntimeError(f"Called read on uninitialized Barecat")

        cdef uint32_t crc
        cdef int rc
        with nogil:
            rc = barecat_mmap_crc32c_from_address(&self.ctx, shard, offset, size, &crc)

        if rc != 0:
            raise RuntimeError(
                f"Failed to calculate CRC32C from address {shard}:{offset} with size {size} from Barecat.")

        return crc

    def close(self):
        if self.is_initialized:
            with nogil:
                barecat_mmap_destroy(&self.ctx)
            self.is_initialized = False

    def __dealloc__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
